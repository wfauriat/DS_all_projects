"""Ingest pipeline: cron → ingest Lambda → S3 → loader Lambda → RDS.

This stack implements the no-NAT split that keeps the project on Free Tier:
- Ingest Lambda has NO VPC config, so it gets free internet egress to fetch
  RSS feeds. It only writes to S3.
- Loader Lambda is IN the VPC (isolated subnets), reads from S3 via the
  gateway endpoint, and writes to RDS. It has no internet path.

Lambda handlers themselves are not implemented in this stack — they live
under `app/ingest/` and are addressed in the run.py split task. The stack
will `cdk synth` against the existing `app/` directory; packaging of deps
will be resolved when the handlers are written.
"""

from pathlib import Path

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_events
from aws_cdk import aws_logs as logs
from aws_cdk import aws_rds as rds
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

# Lambda asset root = project root, so both `app/` and `seeds/` are bundled.
# The ingest Lambda needs seeds/sources.yaml at runtime (no DB access).
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


class IngestStack(Stack):
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

        # === Raw-feed S3 bucket ===
        # Co-located here (not in DataStack) so the S3 → Loader notification
        # is intra-stack and doesn't create a cross-stack cycle.
        self.raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=False,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="glacier-30-delete-365",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER_INSTANT_RETRIEVAL,
                            transition_after=Duration.days(30),
                        ),
                    ],
                    expiration=Duration.days(365),
                ),
            ],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        common_runtime = lambda_.Runtime.PYTHON_3_12
        common_code = lambda_.Code.from_asset(
            str(PROJECT_ROOT), exclude=LAMBDA_EXCLUDES
        )

        # === Ingest Lambda: OUTSIDE the VPC (needs internet for RSS feeds) ===
        self.ingest_fn = lambda_.Function(
            self,
            "IngestFn",
            runtime=common_runtime,
            code=common_code,
            handler="app.ingest.lambda_handler.handler",
            layers=[deps_layer],
            memory_size=512,
            timeout=Duration.minutes(5),
            environment={
                "RAW_BUCKET": self.raw_bucket.bucket_name,
                "INGEST_USER_AGENT": (
                    "Mozilla/5.0 (compatible; mediaElection27/0.1; "
                    "+https://github.com/wfauriat/mediaElection27)"
                ),
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
            # Deliberately NO vpc=... — keeps the function in the AWS-managed
            # network where it gets free internet egress (no NAT needed).
        )
        self.raw_bucket.grant_write(self.ingest_fn)

        # === EventBridge cron: fire ingest 4x/day (every 6h, on the hour UTC) ===
        cron_rule = events.Rule(
            self,
            "IngestSchedule",
            schedule=events.Schedule.cron(
                minute="0",
                hour="0,6,12,18",
                month="*",
                week_day="*",
                year="*",
            ),
            description="Trigger media27 RSS ingest 4x/day",
        )
        cron_rule.add_target(targets.LambdaFunction(self.ingest_fn))

        # === Loader Lambda: IN the VPC (reaches RDS, no internet) ===
        self.loader_fn = lambda_.Function(
            self,
            "LoaderFn",
            runtime=common_runtime,
            code=common_code,
            handler="app.ingest.loader_handler.handler",
            layers=[deps_layer],
            memory_size=512,
            timeout=Duration.minutes(5),
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
        self.raw_bucket.grant_read(self.loader_fn)

        self.loader_fn.add_event_source(
            lambda_events.S3EventSource(
                self.raw_bucket,
                events=[s3.EventType.OBJECT_CREATED],
            )
        )
