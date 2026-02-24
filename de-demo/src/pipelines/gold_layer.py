# Databricks notebook source
"""
Gold analytics layer — business-level aggregations, cross-dataset joins,
and a DQ monitoring view. Runs as part of the DE Demo Lakeflow (DLT) pipeline.
"""

import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("demo.catalog", "main")
schema = spark.conf.get("demo.schema", "de_demo")

GOLD_PROPS = {"quality": "gold"}


# ── Gold: Advisor Portfolio Summary ─────────────────────────────

@dlt.table(
    name="gold_advisor_portfolio_summary",
    comment="Advisor-level AUM and application rollup",
    table_properties=GOLD_PROPS,
)
def gold_advisor_portfolio_summary():
    clients = dlt.read("silver_clientworks")
    apps = dlt.read("silver_new_account")

    joined = clients.join(apps, on="advisor_id", how="left")

    result = (
        joined
        .groupBy("advisor_id", "account_type", "region", "risk_rating")
        .agg(
            F.countDistinct("client_id").alias("total_clients"),
            F.countDistinct("app_id").alias("total_applications"),
            F.sum("aum").alias("total_aum"),
            F.avg("aum").alias("avg_aum"),
            F.when(
                F.count("app_id") > 0,
                F.count(F.when(F.col("status") == "APPROVED", True)) / F.count("app_id")
            ).otherwise(F.lit(None)).alias("approval_rate"),
            F.avg("credit_score").alias("avg_credit_score"),
        )
        .withColumn(
            "risk_band",
            F.when(F.col("risk_rating").isin(1, 2), "LOW")
            .when(F.col("risk_rating") == 3, "MEDIUM")
            .when(F.col("risk_rating").isin(4, 5), "HIGH")
            .otherwise("UNKNOWN")
        )
        .withColumn(
            "anomaly_flag",
            F.when(
                (F.col("total_aum") > 10_000_000) & (F.col("risk_band") == "LOW"),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )

    return result


# ── Gold: Account Balance Snapshot ──────────────────────────────

@dlt.table(
    name="gold_account_balance_snapshot",
    comment="Active account balances joined with client profile",
    table_properties=GOLD_PROPS,
)
def gold_account_balance_snapshot():
    clients = dlt.read("silver_clientworks")
    accounts = dlt.read("silver_fixed_width")

    active_accounts = accounts.filter(F.col("account_status") == "AC")

    joined = clients.join(
        active_accounts,
        F.substring(clients.client_id, 1, 6) == active_accounts.client_id_prefix,
        how="inner",
    )

    result = (
        joined.select(
            clients.client_id,
            clients.client_name,
            clients.account_type,
            clients.region,
            clients.risk_rating,
            clients.aum,
            active_accounts.account_number,
            active_accounts.balance_dollars,
            active_accounts.open_date,
            active_accounts.state_code,
            active_accounts.account_status,
            active_accounts.product_code.alias("bank_product_code"),
        )
        .withColumn("days_since_open", F.datediff(F.current_date(), F.col("open_date")))
        .withColumn(
            "balance_aum_ratio",
            F.when(F.col("aum") != 0, F.col("balance_dollars") / F.col("aum")).otherwise(F.lit(None))
        )
        .withColumn(
            "anomaly_flag",
            F.when(F.col("balance_dollars") > F.col("aum") * 0.5, F.lit(True)).otherwise(F.lit(False))
        )
    )

    return result


# ── Gold: DQ Anomaly Report ─────────────────────────────────────

@dlt.table(
    name="gold_dq_anomaly_report",
    comment="Cross-dataset DQ summary for monitoring and demo",
    table_properties=GOLD_PROPS,
)
def gold_dq_anomaly_report():
    try:
        dq = spark.read.format("delta").table(f"{catalog}.{schema}.dq_results")
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, ArrayType
        schema_def = StructType([
            StructField("run_id", StringType()), StructField("run_timestamp", TimestampType()),
            StructField("dataset_name", StringType()), StructField("rule_name", StringType()),
            StructField("column", StringType()), StructField("rule_type", StringType()),
            StructField("severity", StringType()), StructField("total_count", LongType()),
            StructField("pass_count", LongType()), StructField("fail_count", LongType()),
            StructField("fail_pct", DoubleType()), StructField("sample_failures", ArrayType(StringType())),
        ])
        dq = spark.createDataFrame([], schema_def)

    result = (
        dq
        .groupBy("dataset_name", "rule_name", "severity")
        .agg(
            F.last("fail_pct").alias("latest_run_fail_pct"),
            F.count("*").alias("total_runs_checked"),
            F.sum(F.when(F.col("fail_count") > 0, 1).otherwise(0)).alias("runs_with_failures"),
            F.max("fail_pct").alias("max_fail_pct_ever"),
        )
        .withColumn(
            "status_flag",
            F.when(
                (F.col("severity") == "CRITICAL") & (F.col("runs_with_failures") > 0),
                "ACTION_REQUIRED"
            ).when(
                (F.col("severity") == "ERROR") & (F.col("latest_run_fail_pct") > 5.0),
                "INVESTIGATE"
            ).when(
                F.col("severity") == "WARNING",
                "MONITOR"
            ).otherwise("HEALTHY")
        )
        .orderBy(
            F.when(F.col("severity") == "CRITICAL", 1)
            .when(F.col("severity") == "ERROR", 2)
            .when(F.col("severity") == "WARNING", 3)
            .otherwise(4),
            F.desc("latest_run_fail_pct"),
        )
    )

    return result
