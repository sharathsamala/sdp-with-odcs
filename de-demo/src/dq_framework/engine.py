"""
DQ execution engine — runs Rule checks against Spark DataFrames,
captures pass/fail metrics and sample failures, and annotates rows.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from dq_framework.rules_parser import Rule


class DataQualityException(Exception):
    pass


@dataclass
class DQResult:
    run_id: str
    run_timestamp: datetime
    dataset_name: str
    rule_name: str
    column: str
    rule_type: str
    severity: str
    total_count: int
    pass_count: int
    fail_count: int
    fail_pct: float
    sample_failures: List[str] = field(default_factory=list)


def _build_fail_expr(rule: Rule):
    """Return a Column expression that is True for FAILING rows."""
    col = F.col(rule.column)

    if rule.rule_type == "not_null":
        return col.isNull()

    if rule.rule_type == "accepted_values":
        values = rule.params.get("values", [])
        return ~col.isin(values) & col.isNotNull()

    if rule.rule_type == "range":
        lo = rule.params.get("min")
        hi = rule.params.get("max")
        cond = F.lit(False)
        if lo is not None:
            cond = cond | (col < lo)
        if hi is not None:
            cond = cond | (col > hi)
        return cond & col.isNotNull()

    if rule.rule_type == "regex":
        pattern = rule.params.get("pattern", ".*")
        return ~col.rlike(pattern) & col.isNotNull()

    if rule.rule_type == "not_future_date":
        return col > F.current_date()

    if rule.rule_type == "date_format":
        fmt = rule.params.get("format", "yyyy-MM-dd")
        return F.to_date(col, fmt).isNull() & col.isNotNull()

    if rule.rule_type == "exact_length":
        expected = rule.params.get("length", 0)
        return (col != expected) & col.isNotNull()

    return F.lit(False)


def _check_unique(df: DataFrame, rule: Rule, dataset_name: str, run_id: str, run_ts: datetime) -> Tuple[DataFrame, DQResult]:
    """Unique check requires a groupBy; handled separately from expression-based checks."""
    col_name = rule.column
    total = df.count()

    dupes = (
        df.filter(F.col(col_name).isNotNull())
        .groupBy(col_name)
        .agg(F.count("*").alias("_cnt"))
        .filter(F.col("_cnt") > 1)
    )

    dup_value_count = dupes.count()
    dup_row_count = dupes.agg(F.sum(F.col("_cnt") - 1)).collect()[0][0] or 0
    fail_count = int(dup_row_count)
    pass_count = total - fail_count

    samples = [
        str(r[col_name]) for r in dupes.limit(5).collect()
    ]

    # Mark duplicate rows in the original DataFrame
    window = Window.partitionBy(col_name).orderBy(F.monotonically_increasing_id())
    df = df.withColumn(
        f"_dq_fail_{rule.rule_name}",
        F.when(
            F.col(col_name).isNotNull() & (F.row_number().over(window) > 1),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    return df, DQResult(
        run_id=run_id,
        run_timestamp=run_ts,
        dataset_name=dataset_name,
        rule_name=rule.rule_name,
        column=col_name,
        rule_type=rule.rule_type,
        severity=rule.severity,
        total_count=total,
        pass_count=pass_count,
        fail_count=fail_count,
        fail_pct=round(fail_count / total * 100, 2) if total > 0 else 0.0,
        sample_failures=samples,
    )


def run_dq_checks(
    df: DataFrame,
    rules: List[Rule],
    dataset_name: str,
    run_id: str,
) -> Tuple[DataFrame, List[DQResult]]:
    """
    Execute all rules against ``df``.

    Returns:
        clean_df  — the input DataFrame with ``_dq_passed`` (bool) and
                     ``_dq_failed_rules`` (array<string>) columns added.
        results   — list of DQResult objects with per-rule metrics.
    """
    run_ts = datetime.utcnow()
    results: List[DQResult] = []
    total = df.count()
    fail_col_names: List[str] = []

    for rule in rules:
        if rule.rule_type == "unique":
            df, res = _check_unique(df, rule, dataset_name, run_id, run_ts)
            results.append(res)
            fail_col_names.append(f"_dq_fail_{rule.rule_name}")
            continue

        fail_expr = _build_fail_expr(rule)
        flag_col = f"_dq_fail_{rule.rule_name}"
        df = df.withColumn(flag_col, fail_expr)
        fail_col_names.append(flag_col)

        fail_count = df.filter(F.col(flag_col)).count()
        pass_count = total - fail_count

        samples = [
            str(r[rule.column]) for r in
            df.filter(F.col(flag_col)).select(rule.column).limit(5).collect()
        ]

        results.append(DQResult(
            run_id=run_id,
            run_timestamp=run_ts,
            dataset_name=dataset_name,
            rule_name=rule.rule_name,
            column=rule.column,
            rule_type=rule.rule_type,
            severity=rule.severity,
            total_count=total,
            pass_count=pass_count,
            fail_count=fail_count,
            fail_pct=round(fail_count / total * 100, 2) if total > 0 else 0.0,
            sample_failures=samples,
        ))

    # Compute aggregate row-level flags
    any_fail = F.lit(False)
    failed_rules_arr = []
    for fcol in fail_col_names:
        any_fail = any_fail | F.col(fcol)
        rule_name_str = fcol.replace("_dq_fail_", "")
        failed_rules_arr.append(
            F.when(F.col(fcol), F.lit(rule_name_str))
        )

    df = df.withColumn("_dq_passed", ~any_fail)
    df = df.withColumn(
        "_dq_failed_rules",
        F.array_compact(F.array(*failed_rules_arr)) if failed_rules_arr else F.array()
    )

    # Drop intermediate flag columns
    df = df.drop(*fail_col_names)

    return df, results


def check_critical_failures(results: List[DQResult]) -> None:
    """Raise DataQualityException if any CRITICAL rule has failures."""
    critical_failures = [r for r in results if r.severity == "CRITICAL" and r.fail_count > 0]
    if critical_failures:
        msgs = []
        for r in critical_failures:
            msgs.append(
                f"  - {r.rule_name} on '{r.column}': "
                f"{r.fail_count:,} failures ({r.fail_pct}%) "
                f"samples={r.sample_failures[:3]}"
            )
        raise DataQualityException(
            f"Critical DQ failures in {results[0].dataset_name}:\n" + "\n".join(msgs)
        )
