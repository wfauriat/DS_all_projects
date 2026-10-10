"""Public FastAPI surface: API Gateway HTTP API → Lambda (Mangum) → RDS.

The Lambda runs the existing FastAPI app via the Mangum adapter committed
in Week 2 (`app/api/mangum_handler.py`). It lives in the VPC so it can
reach RDS; API Gateway lives outside the VPC and handles the public-facing
HTTPS termination.

CORS is permissive (`*`) because this is a read-only public dataset. Tighten
to the CloudFront distribution origin once the frontend stack lands.
"""

from pathlib import Path

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_apigatewayv2 as apigwv2
from aws_cdk import aws_apigatewayv2_integrations as apigwv2_integrations
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
LAMBDA_EXCLUDES = [
    "frontend",
    "infra",
    "tests",
    "dev",
    ".venv",
    "raw",
    ".git",
    ".github",
    "*.egg-info",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "cdk.out",
    "*.sql",
    "*.dump",
    "*.md",
    "backup.sql",
]


class ApiStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        lambda_vpc_sg: ec2.ISecurityGroup,
        db: rds.IDatabaseInstance,
        db_secret: secretsmanager.ISecret,
        deps_layer: lambda_.ILayerVersion,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.api_fn = lambda_.Function(
            self,
            "ApiFn",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(str(PROJECT_ROOT), exclude=LAMBDA_EXCLUDES),
            handler="app.api.mangum_handler.handler",
            layers=[deps_layer],
            memory_size=512,
            timeout=Duration.seconds(30),
            environment={
                # CFN resolves these {{resolve:secretsmanager:...}} tokens at
                # deploy time so the Lambda receives plaintext credentials.
                # This avoids a runtime Secrets Manager call, which would
                # need a ~$15/mo VPC interface endpoint to reach from
                # PRIVATE_ISOLATED subnets.
                "DB_USERNAME": db_secret.secret_value_from_json(
                    "username"
                ).unsafe_unwrap(),
                "DB_PASSWORD": db_secret.secret_value_from_json(
                    "password"
                ).unsafe_unwrap(),
                "DB_HOST": db.db_instance_endpoint_address,
                "DB_PORT": db.db_instance_endpoint_port,
                "DB_NAME": "media27",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
            ),
            security_groups=[lambda_vpc_sg],
        )

        self.http_api = apigwv2.HttpApi(
            self,
            "HttpApi",
            api_name="media27-api",
            cors_preflight=apigwv2.CorsPreflightOptions(
                allow_origins=["*"],
                allow_methods=[
                    apigwv2.CorsHttpMethod.GET,
                    apigwv2.CorsHttpMethod.OPTIONS,
                ],
                allow_headers=["Content-Type"],
                max_age=Duration.days(1),
            ),
        )

        integration = apigwv2_integrations.HttpLambdaIntegration(
            "ApiIntegration",
            handler=self.api_fn,
        )

        # One catch-all route forwards every path + method to the Lambda;
        # FastAPI's own router handles dispatch from there.
        self.http_api.add_routes(
            path="/{proxy+}",
            methods=[apigwv2.HttpMethod.ANY],
            integration=integration,
        )
        self.http_api.add_routes(
            path="/",
            methods=[apigwv2.HttpMethod.ANY],
            integration=integration,
        )

        # Exposed so the FrontendStack can bake the API base URL into the
        # SPA at build time.
        CfnOutput(
            self,
            "ApiUrl",
            value=self.http_api.api_endpoint,
            description="Base URL of the media27 HTTP API",
        )
