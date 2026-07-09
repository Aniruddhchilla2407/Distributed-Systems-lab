"""FastAPI application wiring for the broker server.

Mirrors kv-server's shape: a BrokerRegistry is created on startup and torn
down on shutdown, exposed to route handlers via app.state.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI

from broker_server.registry import BrokerRegistry
from broker_server.routes import router


def create_app(
    data_dir: str | Path = "data/broker", *, fsync_every_write: bool = True
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.registry = BrokerRegistry(data_dir, fsync_every_write=fsync_every_write)
        try:
            yield
        finally:
            app.state.registry.close()

    app = FastAPI(title="broker-server", version="0.1.0", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()