"""FastAPI application wiring: creates the KVStore on startup, tears it
down cleanly on shutdown, and exposes it to route handlers via app.state.

Kept deliberately thin -- all durability/WAL/batching logic lives in
storage-core; this module's only job is translating HTTP <-> KVStore calls.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from storage_core.kv.store import KVStore

from kv_server.routes import router


def create_app(
    data_path: str | Path = "data/kv_server.log",
    *,
    fsync_every_write: bool = True,
    batch_window_ms: float | None = None,
    batch_max_size: int = 100,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.store = KVStore(
            data_path,
            fsync_every_write=fsync_every_write,
            batch_window_ms=batch_window_ms,
            batch_max_size=batch_max_size,
        )
        try:
            yield
        finally:
            app.state.store.close()

    app = FastAPI(title="kv-server", version="0.1.0", lifespan=lifespan)
    app.include_router(router)
    return app


# Default app instance for `uvicorn kv_server.app:app`
app = create_app()
