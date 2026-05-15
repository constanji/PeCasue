"""FastAPI sidecar entry.

All routers are mounted under ``/api/pipeline`` so that the URL is identical
behind the Node Express reverse-proxy and the Vite dev proxy. See plan §6.3.
"""
from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server import __version__
from server.api.agent import router as agent_router
from server.api.channels import router as channels_router
from server.api.compare import router as compare_router
from server.api.llm_configs import router as llm_configs_router
from server.api.observe import router as observe_router
from server.api.files import router as files_router
from server.api.health import router as health_router
from server.api.preview import router as preview_router
from server.api.rules import router as rules_router
from server.api.streams import router as streams_router
from server.api.tasks import router as tasks_router
from server.api.upload import router as upload_router
from server.core.llm_config_repo import bootstrap_from_env_if_empty
from server.core.paths import ensure_data_directories
from server.core.task_db import init_db
from server.rules.excel_import import ensure_special_branch_defaults

API_PREFIX = "/api/pipeline"

app = FastAPI(
    title="PeCause Pipeline Service",
    version=__version__,
    description="Sidecar that orchestrates bank-statement reconciliation pipelines.",
)

_default_origins = [
    "http://localhost:4614",
    "http://localhost:1245",
    "http://localhost:3090",
]
_extra = os.environ.get("PIPELINE_CORS_ORIGINS", "").strip()
if _extra:
    _default_origins.extend(o.strip() for o in _extra.split(",") if o.strip())

app.add_middleware(
    CORSMiddleware,
    allow_origins=_default_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix=API_PREFIX, tags=["health"])
app.include_router(tasks_router, prefix=API_PREFIX, tags=["tasks"])
app.include_router(upload_router, prefix=API_PREFIX, tags=["upload"])
app.include_router(channels_router, prefix=API_PREFIX, tags=["channels"])
app.include_router(files_router, prefix=API_PREFIX, tags=["files"])
app.include_router(rules_router, prefix=API_PREFIX, tags=["rules"])
app.include_router(agent_router, prefix=API_PREFIX, tags=["agent"])
app.include_router(llm_configs_router, prefix=API_PREFIX, tags=["llm-configs"])
app.include_router(compare_router, prefix=API_PREFIX, tags=["compare"])
app.include_router(observe_router, prefix=API_PREFIX, tags=["observe"])
app.include_router(preview_router, prefix=API_PREFIX, tags=["preview"])
app.include_router(streams_router, prefix=API_PREFIX, tags=["streams"])


@app.get("/")
def root() -> dict:
    return {
        "service": "pipeline-svc",
        "version": __version__,
        "docs": "/docs",
        "api_prefix": API_PREFIX,
    }


@app.on_event("startup")
def on_startup() -> None:
    ensure_data_directories()
    init_db()
    bootstrap_from_env_if_empty()
    ensure_special_branch_defaults()
