"""CLI entrypoint: `kv-server start --port 8000 --data-path data/kv.log`"""

from __future__ import annotations

import typer
import uvicorn

app = typer.Typer(name="kv-server", help="Durable WAL-backed key-value HTTP server")


@app.callback()
def main() -> None:
    """Durable WAL-backed key-value HTTP server."""


@app.command()
def start(
    host: str = typer.Option("127.0.0.1", help="Host to bind to"),
    port: int = typer.Option(8000, help="Port to bind to"),
    data_path: str = typer.Option("data/kv_server.log", help="Path to the WAL log file"),
    fsync: bool = typer.Option(
        True, help="fsync writes (durable but slower). Disable for raw-speed benchmarking."
    ),
    batch_window_ms: float | None = typer.Option(
        None,
        help="If set, batch fsyncs every N ms instead of syncing on every write. "
        "Improves throughput under concurrent writers without weakening durability.",
    ),
    batch_max_size: int = typer.Option(
        100, help="Max writes to buffer before forcing an early flush, when batching is enabled."
    ),
) -> None:
    from kv_server.app import create_app

    server_app = create_app(
        data_path=data_path,
        fsync_every_write=fsync,
        batch_window_ms=batch_window_ms,
        batch_max_size=batch_max_size,
    )
    uvicorn.run(server_app, host=host, port=port)


if __name__ == "__main__":
    app()
