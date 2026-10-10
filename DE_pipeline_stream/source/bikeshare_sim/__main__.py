"""Entry point: `python -m bikeshare_sim`.

This is the Docker CMD (source/Dockerfile) and what `make source-run` runs locally.
It builds the app (api.create_app, with settings from env and TOML, see config.py)
and serves it with uvicorn.

Environment:
    SOURCE_HOST  bind address (default 0.0.0.0, so the container port is reachable)
    SOURCE_PORT  default 8000. docker-compose.yml maps it to ${SOURCE_API_PORT} on the host.
    LOG_LEVEL    default info
"""

import logging
import os
from types import FrameType

import uvicorn

from .api import close_streams, create_app


class Server(uvicorn.Server):
    """uvicorn with one extra step on SIGTERM/SIGINT: end the open SSE streams.

    SSE responses never end on their own. Without this, uvicorn would wait for
    them until timeout_graceful_shutdown, then cancel them and log an error. With
    it, streams end cleanly, the lifespan saves state.pkl, and clients such as
    the bridge resume later with Last-Event-ID.
    """

    def handle_exit(self, sig: int, frame: FrameType | None) -> None:
        close_streams(self.config.app)
        super().handle_exit(sig, frame)


def main() -> None:
    level = os.environ.get("LOG_LEVEL", "info")
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    config = uvicorn.Config(
        create_app(),
        host=os.environ.get("SOURCE_HOST", "0.0.0.0"),
        port=int(os.environ.get("SOURCE_PORT", "8000")),
        log_level=level,
        timeout_graceful_shutdown=5,  # safety net; docker-compose.yml allows 20 s to stop
    )
    Server(config).run()


if __name__ == "__main__":
    main()
