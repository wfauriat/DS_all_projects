"""AWS Lambda entry point for the FastAPI app.

The CDK stack points this function's handler at
`app.api.mangum_handler.handler`.

DB credentials arrive as plaintext Lambda env vars (resolved from
Secrets Manager by CloudFormation at deploy time). The cold-start
hook stitches them into the async DSN before any SQLAlchemy engine
is built. We avoid a runtime Secrets Manager call because the API
Lambda runs in PRIVATE_ISOLATED subnets with no path to AWS public
APIs.
"""

from __future__ import annotations

import os
import urllib.parse


def _configure_db_url_from_env() -> None:
    if "AWS_LAMBDA_FUNCTION_NAME" not in os.environ:
        return

    from app.config import settings

    user = urllib.parse.quote(os.environ["DB_USERNAME"], safe="")
    pwd = urllib.parse.quote(os.environ["DB_PASSWORD"], safe="")
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ["DB_NAME"]
    settings.database_url = (
        f"postgresql+asyncpg://{user}:{pwd}@{host}:{port}/{name}"
    )


_configure_db_url_from_env()

from mangum import Mangum  # noqa: E402

from app.api.main import app  # noqa: E402

handler = Mangum(app, lifespan="off")
