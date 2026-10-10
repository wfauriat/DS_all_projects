"""Network foundation for the media27 stack.

Design choice (non-obvious): the VPC contains *only* private-isolated
subnets and **no NAT gateway**. The ingest Lambda — the only component
that needs to reach the public internet (RSS feeds) — runs outside the
VPC, where Lambdas get free internet egress. A NAT gateway would cost
~$32/month, more than the rest of the Free Tier stack combined.

In-VPC components (RDS, loader Lambda, api Lambda) reach AWS services
via VPC endpoints, not the internet.
"""

from aws_cdk import Stack, Tags
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class NetworkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name="media27-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            # RDS DB Subnet Groups require >=2 AZs even for single-AZ instances.
            max_azs=2,
            # Default is 1 NAT gateway per AZ (~$32/mo each). Hard requirement
            # for this project is 0 — the ingest Lambda lives outside the VPC.
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # Gateway endpoint for S3: free, lets in-VPC Lambdas read raw feed
        # XML without needing internet egress (and therefore without a NAT).
        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        self.rds_sg = ec2.SecurityGroup(
            self,
            "RdsSg",
            vpc=self.vpc,
            description="Postgres - accepts traffic only from in-VPC Lambdas",
            allow_all_outbound=False,
        )

        self.lambda_vpc_sg = ec2.SecurityGroup(
            self,
            "LambdaVpcSg",
            vpc=self.vpc,
            description="In-VPC Lambdas (loader, api); egress open for AWS APIs",
            allow_all_outbound=True,
        )

        self.rds_sg.add_ingress_rule(
            peer=self.lambda_vpc_sg,
            connection=ec2.Port.tcp(5432),
            description="Postgres from in-VPC Lambdas",
        )

        Tags.of(self).add("project", "media27")
