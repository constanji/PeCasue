"""Health and version endpoints."""
from __future__ import annotations

from fastapi import APIRouter

from server import __version__
from server.core.paths import get_data_root

router = APIRouter()


@router.get("/health")
async def health() -> dict:
    return {
        "ok": True,
        "service": "pipeline-svc",
        "version": __version__,
        "data_root": str(get_data_root()),
    }
