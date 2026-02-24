# Databricks notebook source
"""
Fixed-width account file generator — produces 80-char fixed-width text with anomalies.
Writes to /Volumes/{catalog}/{schema}/{volume}/fixed_width/accounts_{run_id}.txt
"""

import sys, os, uuid
from datetime import date, timedelta

from pyspark.sql import SparkSession

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
rng = _seeded_rng(cfg.run_id, salt=200)

NUM_ROWS = 10_000
US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY",
]
ACCOUNT_STATUSES = ["AC", "CL", "FR"]
PRODUCT_CODES = ["CHK ", "SAV ", "BRK ", "IRA "]
FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
               "Linda", "David", "Elizabeth"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez"]

# Try to load client_id prefixes from clientworks for join keys
clientworks_path = get_volume_path(cfg, "clientworks")
try:
    cw_ids = [
        r.client_id[:6] for r in
        spark.read.option("delimiter", "|").option("header", "true")
        .csv(clientworks_path)
        .select("client_id")
        .filter("client_id IS NOT NULL")
        .limit(2000)
        .collect()
    ]
except Exception:
    cw_ids = []

# ── Generate base rows ──────────────────────────────────────────
lines = []
anomaly_counts = {
    "truncated_line": 0,
    "invalid_state_code": 0,
    "invalid_account_status": 0,
    "negative_balance": 0,
    "invalid_date_format": 0,
}

for i in range(NUM_ROWS):
    acct_num = f"{rng.randint(0, 999999999999):012d}"
    holder = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
    holder_padded = holder.ljust(30)[:30]
    open_dt = date(2010, 1, 1) + timedelta(days=rng.randint(0, 5475))
    open_dt_str = open_dt.strftime("%Y%m%d")
    balance = rng.randint(0, 50_000_000)
    balance_str = f"{balance:012d}"
    state = rng.choice(US_STATES)
    status = rng.choice(ACCOUNT_STATUSES)
    product = rng.choice(PRODUCT_CODES)
    if cw_ids and rng.random() < 0.7:
        prefix = rng.choice(cw_ids)
    else:
        prefix = str(uuid.UUID(int=rng.getrandbits(128)))[:6]
    rec_type = "ACCT"

    # Anomaly 3: ~3% invalid state_code
    if rng.random() < anom.out_of_range_rate:
        state = rng.choice(["XX", "ZZ", "99"])
        anomaly_counts["invalid_state_code"] += 1

    # Anomaly 4: ~2% invalid account_status
    if rng.random() < anom.format_error_rate:
        status = rng.choice(["PD", "OP", "NA"])
        anomaly_counts["invalid_account_status"] += 1

    # Anomaly 5: ~3% negative balance
    if rng.random() < anom.out_of_range_rate:
        balance = -abs(rng.randint(1000, 999999))
        balance_str = f"{balance:012d}"
        anomaly_counts["negative_balance"] += 1

    # Anomaly 6: ~2% invalid date format
    if rng.random() < anom.format_error_rate:
        open_dt_str = rng.choice(["20251332", "XXXXXXXX", "00000000", "20231301"])
        anomaly_counts["invalid_date_format"] += 1

    line = (
        f"{acct_num}"
        f"{holder_padded}"
        f"{open_dt_str}"
        f"{balance_str}"
        f"{state}"
        f"{status}"
        f"{product}"
        f"{prefix}"
        f"{rec_type}"
    )

    # Anomaly 1: ~2% truncated lines
    if rng.random() < anom.format_error_rate:
        trunc_len = rng.randint(30, 70)
        line = line[:trunc_len]
        anomaly_counts["truncated_line"] += 1

    lines.append(line)

# ── Write output ────────────────────────────────────────────────
output_path = get_volume_path(cfg, "fixed_width") + f"accounts_{cfg.run_id}.txt"
content = "\n".join(lines)
dbutils.fs.put(output_path, content, overwrite=True)

# ── Summary ─────────────────────────────────────────────────────
total_anomalies = sum(anomaly_counts.values())
clean_rows = NUM_ROWS - total_anomalies
print(f"\n{'='*60}")
print(f"Fixed-Width Generator Summary  (run_id: {cfg.run_id})")
print(f"{'='*60}")
print(f"Total rows generated : {NUM_ROWS:,}")
print(f"Clean rows (approx)  : {clean_rows:,}")
print(f"Anomalies injected:")
for k, v in anomaly_counts.items():
    print(f"  {k:30s} : {v:,}")
print(f"Output path: {output_path}")
print(f"{'='*60}\n")
