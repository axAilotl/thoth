"""Fixed managed collector entrypoint; request/secrets arrive on stdin only."""
import json
from pathlib import Path
import sys

from .agent_surface import AgentSurfaceService
from .config import Config
from .managed_execution import ManagedCollectionRun
from .metadata_db import MetadataDB
from .path_layout import build_path_layout


def execute(request):
    config = Config()
    config.data = json.loads(Path(request["config_file"]).read_text())
    config.set("managed.connector_version", request["connector_version"])
    layout = build_path_layout(config)
    db = MetadataDB(str(layout.database_path))
    run = ManagedCollectionRun(**request["run"])
    service = AgentSurfaceService(config, layout=layout, db=db, managed_run=run,
        account_root=Path(request["account_root"]), credentials=request["credentials"], history_exports=request.get("history_exports"))
    return service.run_connector(run.connector, execute=True)["result"]


if __name__ == "__main__":
    try:
        print(json.dumps(execute(json.load(sys.stdin))))
    except Exception as error:
        print(json.dumps({"error": type(error).__name__}))
        sys.exit(1)
