"""CDK entrypoint for the media27 infrastructure.

Discovered by `cdk.json` (`app: python app.py`). Each Stack instantiated
here becomes a separate CloudFormation stack that can be deployed,
updated, and destroyed independently.
"""

from __future__ import annotations

import os

from aws_cdk import App, Environment, Tags
from stacks.api_stack import ApiStack
from stacks.data_stack import DataStack
from stacks.frontend_stack import FrontendStack
from stacks.ingest_stack import IngestStack
from stacks.network_stack import NetworkStack
from stacks.observability_stack import ObservabilityStack
from stacks.runtime_stack import RuntimeStack

REGION = "eu-west-3"

env = Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=REGION,
)

app = App()

network = NetworkStack(app, "Media27Network", env=env)

runtime = RuntimeStack(app, "Media27Runtime", env=env)

data = DataStack(
    app,
    "Media27Data",
    vpc=network.vpc,
    rds_sg=network.rds_sg,
    env=env,
)
data.add_dependency(network)

ingest = IngestStack(
    app,
    "Media27Ingest",
    vpc=network.vpc,
    lambda_vpc_sg=network.lambda_vpc_sg,
    db=data.db,
    db_secret=data.db_secret,
    deps_layer=runtime.deps_layer,
    env=env,
)
ingest.add_dependency(data)
ingest.add_dependency(runtime)

api = ApiStack(
    app,
    "Media27Api",
    vpc=network.vpc,
    lambda_vpc_sg=network.lambda_vpc_sg,
    db=data.db,
    db_secret=data.db_secret,
    deps_layer=runtime.deps_layer,
    env=env,
)
api.add_dependency(data)
api.add_dependency(runtime)

frontend = FrontendStack(app, "Media27Frontend", env=env)

observability = ObservabilityStack(
    app,
    "Media27Observability",
    lambdas=[ingest.ingest_fn, ingest.loader_fn, api.api_fn],
    db=data.db,
    env=env,
)
observability.add_dependency(ingest)
observability.add_dependency(api)

Tags.of(app).add("project", "media27")

app.synth()
