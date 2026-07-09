"""CLI entrypoint: `broker-server start --port 8001 --data-dir data/broker`"""

from __future__ import annotations

import typer
import uvicorn

app = typer.Typer(name="broker-server", help="Kafka-style topic/partition broker HTTP server")


@app.callback()
def main() -> None:
    """Kafka-style topic/partition broker HTTP server."""


@app.command()
def start(
    host: str = typer.Option("127.0.0.1", help="Host to bind to"),
    port: int = typer.Option(8001, help="Port to bind to"),
    data_dir: str = typer.Option("data/broker", help="Directory to store topic partition logs"),
    fsync: bool = typer.Option(
        True, help="fsync after every write (durable but slower). Disable for benchmarking."
    ),
) -> None:
    from broker_server.app import create_app

    server_app = create_app(data_dir=data_dir, fsync_every_write=fsync)
    uvicorn.run(server_app, host=host, port=port)


if __name__ == "__main__":
    app()