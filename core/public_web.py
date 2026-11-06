"""Bounded HTTPS fetch with public-address pinning and checked redirects."""
from __future__ import annotations
import http.client
import ipaddress
import socket
import ssl
from urllib.parse import urljoin, urlsplit


def public_target(url, allowed_hosts):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise ValueError("Public research requires credential-free HTTPS on port 443")
    if parsed.hostname not in allowed_hosts:
        raise ValueError("Public research host is outside the selected allowlist")
    addresses = socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise ValueError("Public research refuses non-public addresses")
    return parsed, addresses[0][4][0]


def fetch_public(url, *, allowed_hosts, timeout, max_bytes, max_redirects, user_agent):
    redirects = []
    for _ in range(max_redirects + 1):
        parsed, address = public_target(url, allowed_hosts)
        connection = http.client.HTTPSConnection(parsed.hostname, timeout=timeout, context=ssl.create_default_context())
        # Pin the verified address; TLS still verifies the original hostname.
        connection._create_connection = lambda target, timeout, source_address=None: socket.create_connection((address, 443), timeout, source_address)
        try:
            target = parsed.path or "/"
            if parsed.query:
                target += "?" + parsed.query
            connection.request("GET", target, headers={"User-Agent": user_agent, "Accept-Encoding": "identity"})
            response = connection.getresponse()
            if response.status in (301, 302, 303, 307, 308):
                location = response.getheader("Location")
                if not location:
                    raise ValueError("Redirect has no location")
                redirects.append({"url": url, "status": response.status})
                url = urljoin(url, location)
                continue
            body = response.read(max_bytes + 1)
            if len(body) > max_bytes:
                raise ValueError("Public response exceeds byte budget")
            headers = {name: response.getheader(name) for name in ("Content-Type", "ETag", "Last-Modified")}
            return {"url": url, "status": response.status, "headers": headers, "body": body, "redirects": redirects}
        finally:
            connection.close()
    raise ValueError("Public redirect budget exhausted")
