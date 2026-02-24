# Databricks notebook source
"""
ClientWorks data generator — produces pipe-delimited CSV with intentional anomalies.
Writes to /Volumes/{catalog}/{schema}/{volume}/clientworks/clientworks_{run_id}.csv
"""

import sys, os, uuid, random
from datetime import date, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType,
)

# Resolve imports — works for both DABs notebook_task and spark_python_task
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
rng = _seeded_rng(cfg.run_id)

NUM_ROWS = 50_000
REGIONS = ["NORTHEAST", "SOUTHEAST", "MIDWEST", "WEST", "INTERNATIONAL"]
ACCOUNT_TYPES = ["INDIVIDUAL", "JOINT", "TRUST", "CORPORATE"]
FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
               "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
               "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee"]

# ── Generate base rows ──────────────────────────────────────────
rows = []
for i in range(NUM_ROWS):
    rows.append(Row(
        client_id=str(uuid.UUID(int=rng.getrandbits(128))),
        client_name=f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        account_type=rng.choice(ACCOUNT_TYPES),
        risk_rating=rng.randint(1, 5),
        advisor_id=str(uuid.UUID(int=rng.getrandbits(128))),
        created_date=date(2015, 1, 1) + timedelta(days=rng.randint(0, 3650)),
        aum=round(rng.uniform(1000, 5_000_000), 2),
        relationship_manager=f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        region=rng.choice(REGIONS),
        is_active=rng.random() > 0.1,
        source_system="CLIENTWORKS",
        _run_id=cfg.run_id,
        _run_timestamp=cfg.run_timestamp,
    ))

df = spark.createDataFrame(rows)

# ── Anomaly injection ───────────────────────────────────────────
anomaly_counts = {}

# 1. ~3% null client_id
null_frac = anom.null_rate
df = df.withColumn(
    "client_id",
    F.when(F.rand(seed=42) < null_frac, F.lit(None).cast(StringType())).otherwise(F.col("client_id"))
)
anomaly_counts["null_client_id"] = int(NUM_ROWS * null_frac)

# 2. ~2% duplicate client_id (copy from earlier rows)
dup_count = int(NUM_ROWS * anom.duplicate_rate)
sample_ids = df.filter(F.col("client_id").isNotNull()).select("client_id").limit(dup_count).collect()
if sample_ids:
    dup_ids = [r.client_id for r in sample_ids][:min(50, len(sample_ids))]
    df = df.withColumn("_row_num", F.monotonically_increasing_id())
    threshold = NUM_ROWS - dup_count
    df = df.withColumn(
        "client_id",
        F.when(
            (F.col("_row_num") >= threshold) & F.col("client_id").isNotNull(),
            F.element_at(
                F.array([F.lit(i) for i in dup_ids]),
                (F.col("_row_num") % len(dup_ids) + 1).cast("int"),
            )
        ).otherwise(F.col("client_id"))
    ).drop("_row_num")
anomaly_counts["duplicate_client_id"] = dup_count

# 3. ~4% invalid account_type
invalid_types = ["PERSONAL", "BUSINESS", "OTHER"]
df = df.withColumn(
    "account_type",
    F.when(
        F.rand(seed=43) < anom.invalid_enum_rate,
        F.element_at(
            F.array([F.lit(v) for v in invalid_types]),
            (F.floor(F.rand(seed=44) * len(invalid_types)) + 1).cast("int"),
        )
    ).otherwise(F.col("account_type"))
)
anomaly_counts["invalid_account_type"] = int(NUM_ROWS * anom.invalid_enum_rate)

# 4. ~3% negative aum
df = df.withColumn(
    "aum",
    F.when(F.rand(seed=45) < anom.out_of_range_rate, F.lit(-999999.0)).otherwise(F.col("aum"))
)
anomaly_counts["negative_aum"] = int(NUM_ROWS * anom.out_of_range_rate)

# 5. ~2% future created_date
df = df.withColumn(
    "created_date",
    F.when(
        F.rand(seed=46) < anom.future_date_rate,
        F.date_add(F.current_date(), (F.rand(seed=47) * 730).cast("int"))
    ).otherwise(F.col("created_date"))
)
anomaly_counts["future_created_date"] = int(NUM_ROWS * anom.future_date_rate)

# 6. ~2% out-of-range risk_rating
bad_ratings = [0, 6, 99]
df = df.withColumn(
    "risk_rating",
    F.when(
        F.rand(seed=48) < anom.format_error_rate,
        F.element_at(
            F.array([F.lit(v) for v in bad_ratings]),
            (F.floor(F.rand(seed=49) * len(bad_ratings)) + 1).cast("int"),
        )
    ).otherwise(F.col("risk_rating"))
)
anomaly_counts["invalid_risk_rating"] = int(NUM_ROWS * anom.format_error_rate)

# ── Write output ────────────────────────────────────────────────
output_path = get_volume_path(cfg, "clientworks") + f"clientworks_{cfg.run_id}.csv"

df.coalesce(1).write.option("header", "true").option("delimiter", "|").mode("overwrite").csv(
    output_path.rstrip("/") + "_tmp"
)

# Move the single part file to the final name
tmp_dir = output_path.rstrip("/") + "_tmp"
files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")]
if files:
    dbutils.fs.cp(files[0], output_path)
    dbutils.fs.rm(tmp_dir, recurse=True)

# ── Summary ─────────────────────────────────────────────────────
clean_rows = NUM_ROWS - sum(anomaly_counts.values())
print(f"\n{'='*60}")
print(f"ClientWorks Generator Summary  (run_id: {cfg.run_id})")
print(f"{'='*60}")
print(f"Total rows generated : {NUM_ROWS:,}")
print(f"Clean rows (approx)  : {clean_rows:,}")
print(f"Anomalies injected:")
for k, v in anomaly_counts.items():
    print(f"  {k:30s} : {v:,}")
print(f"Output path: {output_path}")
print(f"{'='*60}\n")
