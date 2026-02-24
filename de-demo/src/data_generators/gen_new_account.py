# Databricks notebook source
"""
New Account Opening data generator — produces comma-separated CSV with intentional anomalies.
Writes to /Volumes/{catalog}/{schema}/{volume}/new_account/new_account_{run_id}.csv
"""

import sys, os, uuid
from datetime import date, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    try:
        _nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        _src_dir = "/Workspace" + "/".join(_nb.split("/")[:-2])
    except Exception:
        _src_dir = "."
sys.path.insert(0, _src_dir)

from data_generators.generator_config import (
    get_run_config, get_volume_path, AnomalyConfig, _seeded_rng,
)

spark = SparkSession.builder.getOrCreate()

_params = {}
for _k in ["catalog", "schema", "volume"]:
    try:
        _params[_k] = dbutils.widgets.get(_k)
    except Exception:
        pass
cfg = get_run_config(spark, **_params)
anom = AnomalyConfig()
rng = _seeded_rng(cfg.run_id, salt=100)

NUM_ROWS = 30_000
PRODUCT_CODES = ["CHECKING", "SAVINGS", "BROKERAGE", "IRA", "401K"]
STATUSES = ["PENDING", "APPROVED", "REJECTED", "REVIEW"]
FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
               "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson"]

# Try to load advisor_ids from the clientworks batch so 60% of rows share advisor references
clientworks_path = get_volume_path(cfg, "clientworks")
try:
    cw_advisors = [
        r.advisor_id for r in
        spark.read.option("delimiter", "|").option("header", "true")
        .csv(clientworks_path)
        .select("advisor_id")
        .filter(F.col("advisor_id").isNotNull())
        .distinct()
        .limit(500)
        .collect()
    ]
except Exception:
    cw_advisors = []

# ── Generate base rows ──────────────────────────────────────────
rows = []
for i in range(NUM_ROWS):
    if cw_advisors and rng.random() < 0.6:
        advisor = rng.choice(cw_advisors)
    else:
        advisor = str(uuid.UUID(int=rng.getrandbits(128)))

    rows.append(Row(
        app_id=str(uuid.UUID(int=rng.getrandbits(128))),
        applicant_name=f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        ssn_last4=f"{rng.randint(0, 9999):04d}",
        product_code=rng.choice(PRODUCT_CODES),
        submission_date=date(2023, 1, 1) + timedelta(days=rng.randint(0, 730)),
        branch_id=f"BR-{rng.randint(1000, 9999)}",
        status=rng.choice(STATUSES),
        advisor_id=advisor,
        annual_income=round(rng.uniform(25000, 500000), 2),
        credit_score=rng.randint(300, 850),
        source_system="NEWACCOUNT",
        _run_id=cfg.run_id,
        _run_timestamp=cfg.run_timestamp,
    ))

df = spark.createDataFrame(rows)

# ── Anomaly injection ───────────────────────────────────────────
anomaly_counts = {}

# 1. ~3% null app_id
df = df.withColumn(
    "app_id",
    F.when(F.rand(seed=101) < anom.null_rate, F.lit(None).cast(StringType())).otherwise(F.col("app_id"))
)
anomaly_counts["null_app_id"] = int(NUM_ROWS * anom.null_rate)

# 2. ~5% ssn_last4 with letters (use a higher rate via format_error_rate * 2.5)
ssn_error_rate = 0.05
bad_ssns = ["AB3X", "1Z2Y", "XXXX", "9A8B", "0O0O"]
df = df.withColumn(
    "ssn_last4",
    F.when(
        F.rand(seed=102) < ssn_error_rate,
        F.element_at(
            F.array([F.lit(s) for s in bad_ssns]),
            (F.floor(F.rand(seed=103) * len(bad_ssns)) + 1).cast("int"),
        )
    ).otherwise(F.col("ssn_last4"))
)
anomaly_counts["invalid_ssn_last4"] = int(NUM_ROWS * ssn_error_rate)

# 3. ~4% invalid product_code
invalid_products = ["MORTGAGE", "LOAN", "CD"]
df = df.withColumn(
    "product_code",
    F.when(
        F.rand(seed=104) < anom.invalid_enum_rate,
        F.element_at(
            F.array([F.lit(v) for v in invalid_products]),
            (F.floor(F.rand(seed=105) * len(invalid_products)) + 1).cast("int"),
        )
    ).otherwise(F.col("product_code"))
)
anomaly_counts["invalid_product_code"] = int(NUM_ROWS * anom.invalid_enum_rate)

# 4. ~2% future submission_date
df = df.withColumn(
    "submission_date",
    F.when(
        F.rand(seed=106) < anom.future_date_rate,
        F.date_add(F.current_date(), (F.rand(seed=107) * 365).cast("int"))
    ).otherwise(F.col("submission_date"))
)
anomaly_counts["future_submission_date"] = int(NUM_ROWS * anom.future_date_rate)

# 5. ~3% invalid status
invalid_statuses = ["CANCELLED", "EXPIRED", "UNKNOWN"]
df = df.withColumn(
    "status",
    F.when(
        F.rand(seed=108) < anom.out_of_range_rate,
        F.element_at(
            F.array([F.lit(v) for v in invalid_statuses]),
            (F.floor(F.rand(seed=109) * len(invalid_statuses)) + 1).cast("int"),
        )
    ).otherwise(F.col("status"))
)
anomaly_counts["invalid_status"] = int(NUM_ROWS * anom.out_of_range_rate)

# 6. ~3% out-of-range credit_score
df = df.withColumn(
    "credit_score",
    F.when(
        F.rand(seed=110) < anom.out_of_range_rate,
        F.when(F.rand(seed=111) < 0.5, F.lit(100)).otherwise(F.lit(999))
    ).otherwise(F.col("credit_score"))
)
anomaly_counts["invalid_credit_score"] = int(NUM_ROWS * anom.out_of_range_rate)

# 7. ~2% negative annual_income
df = df.withColumn(
    "annual_income",
    F.when(F.rand(seed=112) < anom.format_error_rate, F.lit(-50000.0)).otherwise(F.col("annual_income"))
)
anomaly_counts["negative_annual_income"] = int(NUM_ROWS * anom.format_error_rate)

# ── Write output ────────────────────────────────────────────────
output_path = get_volume_path(cfg, "new_account") + f"new_account_{cfg.run_id}.csv"

df.coalesce(1).write.option("header", "true").mode("overwrite").csv(
    output_path.rstrip("/") + "_tmp"
)

tmp_dir = output_path.rstrip("/") + "_tmp"
files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")]
if files:
    dbutils.fs.cp(files[0], output_path)
    dbutils.fs.rm(tmp_dir, recurse=True)

# ── Summary ─────────────────────────────────────────────────────
clean_rows = NUM_ROWS - sum(anomaly_counts.values())
print(f"\n{'='*60}")
print(f"New Account Generator Summary  (run_id: {cfg.run_id})")
print(f"{'='*60}")
print(f"Total rows generated : {NUM_ROWS:,}")
print(f"Clean rows (approx)  : {clean_rows:,}")
print(f"Anomalies injected:")
for k, v in anomaly_counts.items():
    print(f"  {k:30s} : {v:,}")
print(f"Output path: {output_path}")
print(f"{'='*60}\n")
