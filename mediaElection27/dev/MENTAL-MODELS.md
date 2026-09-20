# Cloud mental models — IaC and data engineering

Reference notes captured during Week 4 build (the AWS CDK + GitHub Actions
deploy work). Not project-specific; the patterns generalise across AWS,
GCP, Azure.

---

## 1. Infrastructure as Code — three layers

```
WHAT YOU WRITE          WHAT GETS GENERATED       WHAT ACTUALLY RUNS
(in your repo)          (at deploy time)          (on the cloud provider)
─────────────────────────────────────────────────────────────────────────
infra/cdk/*.py     ──►  CloudFormation YAML  ──►  Real AWS resources
(CDK Python)            (cdk synth produces       (VPC, RDS, Lambda, …)
                         this JSON/YAML;
                         CloudFormation reads it)
```

Three distinct artefacts, three distinct destinations:

- **CDK Python** = a template generator. Constructs are Python objects that
  describe what you want.
- **CloudFormation template** = the lingua franca AWS speaks. JSON/YAML.
  `cdk synth` turns Python into this. Safe to run; nothing happens yet.
- **AWS resources** = the actual material things (VPCs, EC2, Lambdas, S3
  buckets). Created by CloudFormation when you `cdk deploy`.

### Key implications

1. **`cdk synth` is purely local.** Iterate freely; no AWS contact, no cost.
2. **Logical IDs (the hex-suffix names) are stable handles.** Renaming a
   construct in Python changes the logical ID → CloudFormation thinks the
   old resource disappeared → destroys and recreates. This is the #1 way
   to accidentally drop a production database.
3. **CDK is one of many IaC tools.** Terraform, Pulumi, OpenTofu, CFN
   directly. The mental model transfers.
4. **State is in the cloud, not your laptop.** CloudFormation tracks what
   exists. `cdk deploy` computes a diff and changes only what's different.

### Stack composition

```
App
 ├── NetworkStack          (VPC, subnets, security groups)
 ├── DataStack             (RDS + Secrets Manager)
 ├── IngestStack           (Lambdas + S3 + EventBridge)
 ├── ApiStack              (Lambda + API Gateway)
 ├── FrontendStack         (S3 + CloudFront)
 └── ObservabilityStack    (alarms, SNS, budgets)
```

- Each Stack maps 1:1 to a CloudFormation stack — independently
  deployable, updatable, destroyable.
- Cross-stack references (`other_stack.vpc`, `other_stack.bucket`) compile
  to CloudFormation `Outputs` + `Fn::ImportValue` automatically.
- `add_dependency()` enforces deploy order when no value reference exists.

### Construct levels (CDK-specific but instructive)

- **L1** — `CfnVpc`, `CfnBucket`. Direct 1:1 wrappers around CloudFormation
  resource types. Verbose, escape hatch.
- **L2** — `ec2.Vpc`, `s3.Bucket`. Opinionated, sensible defaults.
  Day-to-day API.
- **L3** ("patterns") — bundles of L2s for common architectures
  (`ApplicationLoadBalancedFargateService`, etc.).

---

## 2. Modern data architecture — three tiers

```
┌──────────────────────────────────────────────────────┐
│ Tier 1: RAW LANDING ZONE  (a.k.a. "data lake")       │
│   Cheap, append-only, immutable, schemaless          │
│   AWS: S3                                            │
└────────────────────────────┬─────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────┐
│ Tier 2: TRANSFORMATION LAYER  ("data plumbing")      │
│   ETL/ELT jobs: clean, join, aggregate, enrich       │
│   AWS: Glue, EMR, Lambda, Kinesis, Step Functions    │
└────────────────────────────┬─────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────┐
│ Tier 3: SERVING LAYER                                │
│   Where consumers actually query                     │
│                                                      │
│   OLAP / warehouse:  Redshift, Athena, Snowflake     │
│   OLTP / app DB:     RDS, Aurora, DynamoDB           │
│   Search:            OpenSearch                      │
│   Specialised:       Neptune (graph), Timestream     │
└────────────────────────────┬─────────────────────────┘
                             ▼
       ┌──────────────────┴──────────────────┐
       ▼                                     ▼
  BI dashboards                       Apps + ML models
  (QuickSight, Tableau,
  Metabase, Superset)
```

### Why this split

- **Tier 1 is cheap** (S3 standard ≈ $23/TB-month; Glacier under $1/TB-month).
  Keep everything forever, in its original form, partitioned by source/date.
- **Tier 2 separates "raw → useful"** from "useful → consumed." Lets you
  reprocess history when a transform changes, without re-fetching sources.
- **Tier 3 is split by access pattern.**
  - OLAP (columnar, MPP): big aggregations across history.
  - OLTP (row, low-latency): single-row reads for apps.
  - Putting OLTP queries on an OLAP system is slow; the reverse is worse.

### ETL vs ELT (the trend)

- **ETL** (Extract → Transform → Load): transform happens *before* loading
  the warehouse. Classic. Limits transforms to whatever the ETL engine can
  do efficiently.
- **ELT** (Extract → Load → Transform): load raw to a powerful warehouse
  first, transform inside it with SQL. Modern. Tools: dbt, sqlmesh.
- The shift came when Snowflake / BigQuery / Redshift got cheap enough to
  hold raw data and powerful enough to do transforms in SQL.

### What "data engineering" actually is

> **Operating the transformation layer reliably and at scale.**

Hard problems data engineers solve:
- **Schema drift** — raw inputs change shape without warning.
- **Lateness** — events arrive out of order, late, duplicated.
- **Backfill** — re-process months of history when a transform changes.
- **Data quality** — bad data poisons every downstream consumer.
- **Lineage** — when a dashboard is wrong, which transform broke it?
- **Cost** — Glue/Athena/Redshift bills scale alarmingly without discipline.

Modern "data stack" tools (dbt, Airbyte, Fivetran, Dagster, Snowflake,
Databricks) exist almost entirely to make this layer less painful.

---

## 3. How media27 maps onto both models

It's a **tiny but complete** instance of both architectures.

### As an IaC project

| Layer | media27 |
|---|---|
| Python CDK source | `infra/cdk/stacks/*.py` |
| Generated CloudFormation | `infra/cdk/cdk.out/*.template.json` |
| Real AWS resources | 6 CloudFormation stacks, ~70 resources total |

### As a data project

| Tier | media27 piece | What it becomes at 1000× scale |
|---|---|---|
| **Tier 1: Raw** | S3 raw bucket (`feeds/YYYY-MM-DD/*.xml`) | Same S3 bucket, partitioned by `source=X/date=Y/`, parquet not XML |
| **Tier 2: Transform** | Loader Lambda (parse + dedup + insert) | Airflow/Dagster DAG: parse → dedup → enrich (NER, sentiment) → write curated parquet |
| **Tier 3: Serve** | RDS Postgres (does both OLTP and analytics) | Split: Athena over parquet for analytics, RDS only for app reads |
| **Access** | FastAPI + React | Same app, plus embedded BI for ad-hoc (Metabase / Superset) |

The "getting away with it" is using one Postgres for both OLTP and
analytics — works only because the dataset is small (hundreds of MB/year).
At scale you separate.

---

## 4. AWS service map (quick reference)

### Storage
| Pattern | AWS |
|---|---|
| Object store | S3 |
| Cold archive | S3 Glacier, Deep Archive |
| Block storage | EBS |
| Shared file | EFS, FSx |
| Relational | RDS (Postgres, MySQL, …), Aurora |
| NoSQL key-value | DynamoDB |
| Graph | Neptune |
| Time series | Timestream |
| Cache | ElastiCache (Redis, Memcached) |
| Search | OpenSearch |

### Compute
| Pattern | AWS |
|---|---|
| Serverless functions | Lambda |
| Containers (serverless) | Fargate |
| Containers (orchestrated) | ECS, EKS |
| VMs | EC2 |
| Batch jobs | Batch |

### Streaming & integration
| Pattern | AWS |
|---|---|
| Pub-sub stream | Kinesis Data Streams, MSK (Kafka) |
| Stream-to-S3/warehouse | Kinesis Firehose |
| Pub-sub messaging | SNS |
| Queues | SQS |
| Event routing | EventBridge |
| DB replication | DMS |
| SaaS sync | AppFlow |

### Analytics & data processing
| Pattern | AWS |
|---|---|
| Serverless SQL on S3 | Athena |
| Data warehouse | Redshift |
| Spark/Hadoop managed | EMR |
| ETL/orchestration | Glue (Spark-based), Step Functions, MWAA (Airflow) |
| Data catalog | Glue Data Catalog |
| Governance | Lake Formation |
| BI tool | QuickSight |

### Front door / edge
| Pattern | AWS |
|---|---|
| HTTPS endpoint | API Gateway, ALB, CloudFront |
| CDN | CloudFront |
| DNS | Route 53 |
| WAF | AWS WAF |

### Identity & security
| Pattern | AWS |
|---|---|
| Identities & policies | IAM |
| Secrets | Secrets Manager, Parameter Store |
| Encryption keys | KMS |
| Audit log | CloudTrail |
| Compliance | Config, Security Hub |

### Observability
| Pattern | AWS |
|---|---|
| Metrics + alarms | CloudWatch |
| Logs | CloudWatch Logs |
| Distributed traces | X-Ray |
| Cost | Budgets, Cost Explorer, Cost Anomaly Detection |

### ML / AI
| Pattern | AWS |
|---|---|
| Full ML platform | SageMaker |
| Managed LLMs | Bedrock |
| Pre-built APIs | Comprehend, Translate, Polly, Rekognition, Textract |

---

## 5. The skills gap — what's beyond Week 4

These are the next mountains for someone who's done what media27 has done.

### Cloud / DevOps direction
- **Least-privilege IAM** — narrow `AdministratorAccess` to per-service
  scoped policies.
- **Observability depth** — CloudWatch dashboards, X-Ray traces, structured
  logging, Grafana, log-based metrics.
- **Multi-AZ / multi-region** — failover, cross-region replication, RPO/RTO.
- **Deployment strategies** — blue/green, canary, feature flags.
- **Cost optimisation** — reserved capacity, Savings Plans, S3 lifecycle
  governance, compute right-sizing.
- **Compliance** — KMS keys, secrets rotation, CloudTrail, Config rules.
- **Container workloads** — ECS, EKS, Fargate; different mental model from
  Lambda.

### Data engineering direction
- **Columnar formats** — Parquet, ORC. Replace CSVs everywhere.
- **SQL on S3** — Athena/Trino/Spark SQL fundamentals.
- **Orchestration** — Airflow / Dagster / Step Functions basics.
- **Modern transform layer** — dbt for SQL-based ELT.
- **Streaming basics** — Kafka/Kinesis, watermarks, exactly-once semantics.
- **Data quality** — Great Expectations, Soda, dbt tests.
- **Lineage and catalog** — DataHub, OpenMetadata, Glue Data Catalog.

### Cross-cutting
- **One sentence:** **AWS is a career, not a topic.** Other clouds
  (GCP, Azure) are ~70 % concept-transfer — names change, ideas don't.
- The foundation built in Week 4 (VPC + IAM + Lambda + RDS + S3 + IaC + CI)
  is what 80 % of "backend on cloud" jobs actually use day-to-day.

---

## 6. Heuristics worth remembering

- **`cdk synth` is free; `cdk deploy` is not.** Iterate on synth.
- **Cost alarms before any non-trivial deploy.** AWS Free Tier surprises
  are real. Budget at $5, alarm at $1 and $5.
- **Encryption-by-default for everything stateful.** S3-managed for S3,
  AES-256 for RDS, KMS for secrets.
- **Tag everything `project=<name>`.** Lets you find and clean up later.
- **For Lambda: layer the deps once, ship each function as a thin zip.**
  Faster cold starts, faster deploys.
- **Never put long-lived AWS access keys in CI.** OIDC federation is the
  modern answer.
- **Backups are not snapshots are not exports.** Three different things.
  The portable one is `pg_dump` (or equivalent).
- **For a small dataset (< ~1 TB), one Postgres is your OLTP + your
  warehouse + your search index. Split only when you have to.**
