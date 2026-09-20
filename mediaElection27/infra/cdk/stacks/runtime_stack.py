"""Shared Lambda runtime: dependency layer for ingest/loader/api Lambdas.

Builds a single Lambda Layer containing the Python runtime dependencies
(fastapi, sqlalchemy, asyncpg, psycopg, httpx, feedparser, pydantic, etc.)
via Docker bundling against the official Python 3.12 Lambda runtime image.

Each Lambda asset stays small (just the project source); the deps come
from this shared layer at /opt/python.
"""

from pathlib import Path

from aws_cdk import BundlingOptions, Stack
from aws_cdk import aws_lambda as lambda_
from constructs import Construct

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
LAYER_INPUT = PROJECT_ROOT / "infra" / "lambda_layer"


class RuntimeStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.deps_layer = lambda_.LayerVersion(
            self,
            "DepsLayer",
            layer_version_name="media27-deps",
            code=lambda_.Code.from_asset(
                str(LAYER_INPUT),
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        (
                            "pip install -r requirements-lambda.txt "
                            "-t /asset-output/python --no-cache-dir "
                            "&& find /asset-output -name '*.pyc' -delete "
                            "&& find /asset-output -name '__pycache__' "
                            "-type d -exec rm -rf {} +"
                        ),
                    ],
                ),
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Shared runtime deps for media27 Lambdas",
        )
