"""Alerts, budgets, and Lambda/RDS health alarms.

Three layers:
- AWS Budgets: monthly $-cap with email notification when 80 % / 100 %
  thresholds are crossed.
- CloudWatch alarms on the application: any Lambda error → alarm;
  RDS CPU sustained above 80 % → alarm.
- SNS topic as a single fan-out point; subscribe one email via
  `cdk deploy -c alert_email=you@example.com`.

Two items from PLAN-v1.md deliberately deferred here:
- CloudWatch billing alarms ($1 and $5) — they must live in us-east-1
  (only region that emits billing metrics) and would require a second
  cross-region stack. AWS Budgets gives equivalent visibility at this
  scale.
- Cost Anomaly Detection — useful at higher spend, overkill at near-$0.
"""

from aws_cdk import Duration, Stack
from aws_cdk import aws_budgets as budgets
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_rds as rds
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subs
from constructs import Construct


class ObservabilityStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        lambdas: list[lambda_.IFunction],
        db: rds.IDatabaseInstance,
        monthly_budget_usd: float = 5.0,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.alerts_topic = sns.Topic(
            self,
            "AlertsTopic",
            topic_name="media27-alerts",
            display_name="media27 alerts",
        )

        alert_email = self.node.try_get_context("alert_email")
        if alert_email:
            self.alerts_topic.add_subscription(sns_subs.EmailSubscription(alert_email))

        # --- Lambda error alarms ------------------------------------------------
        for fn in lambdas:
            alarm = cloudwatch.Alarm(
                self,
                f"{fn.node.id}ErrorsAlarm",
                metric=fn.metric_errors(period=Duration.minutes(5)),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=(
                    cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
                ),
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                alarm_description=f"{fn.function_name} produced one or more errors",
            )
            alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        # --- RDS CPU alarm ------------------------------------------------------
        cpu_alarm = cloudwatch.Alarm(
            self,
            "RdsCpuHighAlarm",
            metric=db.metric_cpu_utilization(period=Duration.minutes(5)),
            threshold=80,
            evaluation_periods=3,
            datapoints_to_alarm=3,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="RDS CPU above 80 % for 15 minutes",
        )
        cpu_alarm.add_alarm_action(cw_actions.SnsAction(self.alerts_topic))

        # --- AWS Budget ---------------------------------------------------------
        # Two notifications: 80 % (early warning) and 100 % (you've hit it).
        # Email subscriber list is empty when no context email is provided —
        # the budget still tracks, just doesn't email anyone.
        subscribers: list[budgets.CfnBudget.SubscriberProperty] = (
            [
                budgets.CfnBudget.SubscriberProperty(
                    address=alert_email,
                    subscription_type="EMAIL",
                )
            ]
            if alert_email
            else []
        )

        budgets.CfnBudget(
            self,
            "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name="media27-monthly",
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(
                    amount=monthly_budget_usd,
                    unit="USD",
                ),
            ),
            notifications_with_subscribers=[
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        comparison_operator="GREATER_THAN",
                        notification_type="ACTUAL",
                        threshold=threshold,
                        threshold_type="PERCENTAGE",
                    ),
                    subscribers=subscribers,
                )
                for threshold in (80, 100)
                if subscribers
            ],
        )
