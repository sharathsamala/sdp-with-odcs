"""
DQ reporting — writes results to Delta, prints formatted summaries,
and quarantines failing rows.
"""

from typing import List

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, ArrayType,
)

from dq_framework.engine import DQResult


DQ_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("column", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("total_count", LongType(), False),
    StructField("pass_count", LongType(), False),
    StructField("fail_count", LongType(), False),
    StructField("fail_pct", DoubleType(), False),
    StructField("sample_failures", ArrayType(StringType()), True),
])


def write_dq_results(results: List[DQResult], spark: SparkSession, catalog: str, schema: str) -> None:
    table_name = f"{catalog}.{schema}.dq_results"
    rows = [
        Row(
            run_id=r.run_id,
            run_timestamp=r.run_timestamp,
            dataset_name=r.dataset_name,
            rule_name=r.rule_name,
            column=r.column,
            rule_type=r.rule_type,
            severity=r.severity,
            total_count=r.total_count,
            pass_count=r.pass_count,
            fail_count=r.fail_count,
            fail_pct=r.fail_pct,
            sample_failures=r.sample_failures,
        )
        for r in results
    ]
    df = spark.createDataFrame(rows, schema=DQ_RESULTS_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(table_name)


def print_dq_summary(results: List[DQResult]) -> None:
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    severity_color = {"CRITICAL": RED, "ERROR": YELLOW, "WARNING": BLUE}

    header = (
        f"{'DATASET':<25s} | {'RULE':<25s} | {'COLUMN':<15s} | "
        f"{'SEVERITY':<10s} | {'TOTAL':>8s} | {'FAILS':>8s} | "
        f"{'FAIL%':>7s} | STATUS"
    )
    sep = "-" * len(header)

    print(f"\n{BOLD}Data Quality Summary{RESET}")
    print(sep)
    print(header)
    print(sep)

    severity_totals = {"CRITICAL": 0, "ERROR": 0, "WARNING": 0}

    for r in results:
        color = severity_color.get(r.severity, "")
        status = f"{RED}\u274c FAIL{RESET}" if r.fail_count > 0 else f"\033[92m\u2714 PASS{RESET}"
        print(
            f"{r.dataset_name:<25s} | {r.rule_name:<25s} | {r.column:<15s} | "
            f"{color}{r.severity:<10s}{RESET} | {r.total_count:>8,d} | "
            f"{r.fail_count:>8,d} | {r.fail_pct:>6.2f}% | {status}"
        )
        if r.fail_count > 0 and r.severity in severity_totals:
            severity_totals[r.severity] += r.fail_count

    print(sep)
    print(f"{BOLD}Failure totals by severity:{RESET}")
    for sev, total in severity_totals.items():
        color = severity_color.get(sev, "")
        print(f"  {color}{sev:<10s}{RESET} : {total:,d} failures")
    print()


def quarantine_failures(
    df: DataFrame,
    dataset_name: str,
    spark: SparkSession,
    catalog: str,
    schema: str,
) -> DataFrame:
    """
    Split ``df`` into clean and quarantined DataFrames.
    Quarantined rows (``_dq_passed = False``) are written to a Delta table.
    Returns the clean DataFrame (``_dq_passed = True``).
    """
    quarantine_table = f"{catalog}.{schema}.{dataset_name}_quarantine"
    quarantine_df = df.filter(~F.col("_dq_passed"))
    clean_df = df.filter(F.col("_dq_passed"))

    if quarantine_df.head(1):
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    return clean_df
