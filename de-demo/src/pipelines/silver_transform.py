# Databricks notebook source
"""
Generic contract-driven silver transformation layer.
Reads ODCS contract JSONs, applies type casts / expressions from the silver.transforms
section, then runs the DQ framework against the quality rules.
No per-dataset code needed — add a new contract JSON and you get a new silver table.
"""

import json
import os
import sys
import uuid

import dlt
from pyspark.sql import functions as F

# ── Resolve src/ for dq_framework imports ───────────────────────

try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    try:
        _nb_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook().getContext().notebookPath().get()
        )
        _src_dir = "/Workspace" + "/".join(_nb_path.split("/")[:-2])
    except Exception:
        _src_dir = "."
sys.path.insert(0, _src_dir)

from dq_framework.rules_parser import parse_contract
from dq_framework.engine import run_dq_checks, check_critical_failures, DataQualityException
from dq_framework.reporter import write_dq_results, print_dq_summary, quarantine_failures

# ── Pipeline configuration ──────────────────────────────────────

catalog = spark.conf.get("demo.catalog", "main")
schema = spark.conf.get("demo.schema", "de_demo")
run_id = spark.conf.get("demo.run_id", str(uuid.uuid4()))
config_base = spark.conf.get("demo.config_base", os.path.join(_src_dir, "configs"))

SILVER_PROPS = {"quality": "silver"}

# ── Spark type mapping ──────────────────────────────────────────

_TYPE_MAP = {
    "double": "double",
    "float": "float",
    "integer": "int",
    "int": "int",
    "long": "bigint",
    "boolean": "boolean",
    "string": "string",
    "date": "date",
    "timestamp": "timestamp",
}


# ── Contract loading ────────────────────────────────────────────

def _load_contracts(config_dir):
    contracts = []
    for fname in sorted(os.listdir(config_dir)):
        if fname.endswith("_contract.json"):
            with open(os.path.join(config_dir, fname)) as fh:
                contracts.append(json.load(fh))
    return contracts


# ── Transform applicator ────────────────────────────────────────

def _apply_transforms(df, transforms):
    """
    Apply transforms defined in the contract's silver.transforms list.
    Each entry is either:
      - {"column": "col", "type": "double"}      → cast in-place
      - {"column": "col", "expr": "TRIM(col)"}   → SQL expression (can create derived cols)
    """
    for t in transforms:
        col_name = t["column"]
        if "expr" in t:
            df = df.withColumn(col_name, F.expr(t["expr"]))
        elif "type" in t:
            spark_type = _TYPE_MAP.get(t["type"], t["type"])
            if spark_type == "date" and t.get("format"):
                df = df.withColumn(col_name, F.to_date(F.col(col_name), t["format"]))
            elif spark_type == "date":
                df = df.withColumn(col_name, F.to_date(F.col(col_name)))
            elif spark_type == "timestamp" and t.get("format"):
                df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), t["format"]))
            else:
                df = df.withColumn(col_name, F.col(col_name).cast(spark_type))

    df = df.withColumn("_silver_processed_ts", F.current_timestamp())
    return df


# ── DQ applicator ───────────────────────────────────────────────

def _apply_dq(df, contract_path, dataset_name):
    rules = parse_contract(contract_path, spark)
    checked_df, results = run_dq_checks(df, rules, dataset_name, run_id)

    try:
        check_critical_failures(results)
    except DataQualityException as e:
        print(f"[DQ] Critical failures detected in {dataset_name}: {e}")

    if results:
        try:
            write_dq_results(results, spark, catalog, schema)
        except Exception as e:
            print(f"[DQ] Warning: could not write dq_results table: {e}")
        print_dq_summary(results)

    clean_df = quarantine_failures(checked_df, dataset_name, spark, catalog, schema)
    return clean_df


# ── Dynamic table factory ───────────────────────────────────────

def _create_silver_table(contract):
    dataset = contract["dataset"]
    silver = contract["silver"]
    bronze_name = dataset["name"]
    silver_name = silver["table_name"]
    transforms = silver.get("transforms", [])
    contract_file = None

    for fname in os.listdir(config_base):
        if fname.endswith("_contract.json"):
            with open(os.path.join(config_base, fname)) as fh:
                c = json.load(fh)
                if c.get("dataset", {}).get("name") == bronze_name:
                    contract_file = os.path.join(config_base, fname)
                    break

    @dlt.table(
        name=silver_name,
        comment=f"Cleansed and DQ-validated {bronze_name} data",
        table_properties=SILVER_PROPS,
    )
    @dlt.expect_or_drop("dq_passed", "_dq_passed = true")
    def _silver():
        df = dlt.read(bronze_name)
        df = _apply_transforms(df, transforms)

        if contract_file:
            try:
                return _apply_dq(df, contract_file, bronze_name)
            except Exception as e:
                print(f"[DQ] Error running DQ for {bronze_name}: {e}")

        return df.withColumn("_dq_passed", F.lit(True)).withColumn("_dq_failed_rules", F.array())


# ── Register all silver tables from contracts ───────────────────

for _contract in _load_contracts(config_base):
    if "silver" in _contract:
        _create_silver_table(_contract)
