"""Static SPA hosting: private S3 bucket + CloudFront + Origin Access Control.

CloudFront is the public entry point; the bucket is private and trusts only
the distribution. SPA deep-link behaviour (e.g. /leaderboard typed directly)
is handled with custom error responses that serve index.html on 403/404 so
react-router can take over client-side.

The BucketDeployment that uploads `frontend/dist/` is only attached when the
build artefacts exist locally — synth always works; a real deploy requires
`npm run build` first.
"""

from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_cloudfront as cloudfront
from aws_cdk import aws_cloudfront_origins as origins
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3_deployment
from constructs import Construct

FRONTEND_DIST = (
    Path(__file__).resolve().parent.parent.parent.parent / "frontend" / "dist"
)


class FrontendStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = s3.Bucket(
            self,
            "SpaBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.distribution = cloudfront.Distribution(
            self,
            "Cdn",
            default_root_object="index.html",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(self.bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                compress=True,
                cache_policy=cloudfront.CachePolicy.CACHING_OPTIMIZED,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
            ),
            # SPA deep-link handling: serve index.html on 403/404 so client-side
            # routing can resolve paths the bucket doesn't know about.
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(5),
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                    ttl=Duration.minutes(5),
                ),
            ],
            # PRICE_CLASS_100 = US + Canada + Europe; cheapest tier, fine for
            # French-audience portfolio. Bump to PRICE_CLASS_ALL for global.
            price_class=cloudfront.PriceClass.PRICE_CLASS_100,
        )

        if FRONTEND_DIST.exists():
            s3_deployment.BucketDeployment(
                self,
                "DeploySpa",
                sources=[s3_deployment.Source.asset(str(FRONTEND_DIST))],
                destination_bucket=self.bucket,
                distribution=self.distribution,
                distribution_paths=["/*"],
            )

        CfnOutput(
            self,
            "CdnUrl",
            value=f"https://{self.distribution.distribution_domain_name}",
            description="Public CloudFront URL for the media27 dashboard",
        )
