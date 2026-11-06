"""One bounded Play review page. JSON stdin/stdout; no credential environment.

The pinned upstream parser is reused, but its network function is replaced:
the upstream convenience API suppresses failures and changes global TLS defaults.
This isolated worker restores TLS defaults and lets parsing/network failures fail.
"""
import json
import ssl
import sys
from datetime import datetime, timezone


def collect_page(request):
    original_context = ssl._create_default_https_context
    try:
        from google_play_scraper.features.reviews import _fetch_review_items
        from google_play_scraper.constants.element import ElementSpecs
        from google_play_scraper.constants.request import Formats
        import importlib
        module = importlib.import_module("google_play_scraper.features.reviews")
    finally:
        ssl._create_default_https_context = original_context
    import requests
    with requests.Session() as session:
        session.trust_env = False
        def post(url, data, headers):
            response = session.post(url, data=data, headers=headers, timeout=request["timeout_seconds"])
            response.raise_for_status()
            return response.text
        module.post = post
        rows, cursor = _fetch_review_items(Formats.Reviews.build(lang=request["language"], country=request["country"]),
            request["app_id"], 2, request["count"], None, None, request.get("cursor"))
        records = [{key: spec.extract_content(row) for key, spec in ElementSpecs.Review.items()} for row in rows]
    for record in records:
        for key, value in list(record.items()):
            if isinstance(value, datetime):
                record[key] = value.replace(tzinfo=timezone.utc).isoformat()
    if not isinstance(cursor, (str, type(None))):
        raise ValueError("Unexpected Play cursor type")
    return {"records": records, "cursor": cursor}


if __name__ == "__main__":
    try:
        print(json.dumps(collect_page(json.load(sys.stdin))))
    except Exception as error:
        print(json.dumps({"error": type(error).__name__}))
        sys.exit(1)
