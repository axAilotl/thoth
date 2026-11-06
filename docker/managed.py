"""Managed-only entrypoint: no standalone API, schedulers, shell or dotenv."""
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.managed_api import create_app
from core.managed_runtime import ManagedRuntime, ManagedRuntimeSettings


if __name__ == "__main__":
    import uvicorn

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python docker/managed.py <managed-settings.json>")
    settings = ManagedRuntimeSettings.load(sys.argv[1])
    socket = Path(settings.socket_path)
    socket.parent.mkdir(parents=True, exist_ok=True)
    os.umask(0o007)
    uvicorn.run(create_app(ManagedRuntime(settings)), uds=str(socket), workers=1,
                access_log=False, proxy_headers=False, lifespan="off")
