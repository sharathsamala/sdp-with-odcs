# Databricks notebook source
"""
Generic contract-driven bronze ingestion layer.
Reads all ODCS contract JSONs from the config directory and dynamically creates
DLT streaming tables — no per-dataset code needed.
"""

import json
import os

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DoubleType, BooleanType, TimestampType,
)

# ── Pipeline configuration ──────────────────────────────────────

catalog = spark.conf.get("demo.catalog", "main")
schema = spark.conf.get("demo.schema", "de_demo")
volume = spark.conf.get("demo.volume", "raw_files")
config_base = spark.conf.get("demo.config_base", ".")

BRONZE_PROPS = {"quality": "bronze", "pipelines.reset.allowed": "true"}

_SPARK_TYPES = {
    "string": StringType(), "long": LongType(), "integer": IntegerType(),
    "int": IntegerType(), "double": DoubleType(), "boolean": BooleanType(),
    "timestamp": TimestampType(),
}


# ── Contract loading ────────────────────────────────────────────

def _load_contracts(config_dir):
    contracts = []
    for fname in sorted(os.listdir(config_dir)):
        if fname.endswith("_contract.json"):
            with open(os.path.join(config_dir, fname)) as fh:
                contracts.append(json.load(fh))
    return contracts


def _resolve_path(template):
    return (
        template
        .replace("${catalog}", catalog)
        .replace("${schema}", schema)
        .replace("${volume}", volume)
    )


# ── Schema builders ─────────────────────────────────────────────

def _commented_field(name, spark_type=StringType(), description=""):
    if description:
        return StructField(name, spark_type, metadata={"comment": description})
    return StructField(name, spark_type)


def _build_reader_schema(columns):
    """All CSV columns are StringType for safe raw landing."""
    return StructType([StructField(c["name"], StringType()) for c in columns])


def _build_table_schema(columns, extra_fields=None):
    """Build a full output schema with column comments from the contract.
    Passed to @dlt.table(schema=...) so DLT sets Unity Catalog column comments."""
    fields = []
    col_lookup = {c["name"]: c for c in columns}

    for c in columns:
        spark_type = _SPARK_TYPES.get(c.get("type", "string"), StringType())
        fields.append(_commented_field(c["name"], spark_type, c.get("description", "")))

    for ef in (extra_fields or []):
        fields.append(ef)

    return StructType(fields)


# ── Expectation builder ─────────────────────────────────────────

def _build_expectations(quality_rules):
    """
    Translate ODCS quality rules into DLT @dlt.expect SQL expressions.
    Only CRITICAL rules become bronze-level expectations (monitoring, not dropping).
    """
    expectations = {}
    for rule in quality_rules:
        if rule["severity"].upper() != "CRITICAL":
            continue
        col = rule["column"]
        rt = rule["rule_type"]
        params = rule.get("params", {})

        if rt == "not_null":
            expectations[rule["rule_name"]] = f"`{col}` IS NOT NULL"
        elif rt == "accepted_values":
            vals = ",".join(f"'{v}'" for v in params.get("values", []))
            expectations[rule["rule_name"]] = f"`{col}` IN ({vals})"
        elif rt == "regex":
            expectations[rule["rule_name"]] = f"`{col}` RLIKE '{params.get('pattern', '.*')}'"
        elif rt == "exact_length":
            expectations[rule["rule_name"]] = f"`{col}` = {params.get('length', 0)}"
    return expectations


_META_FIELDS = [
    _commented_field("_source_file", StringType(), "Path to the source file"),
    _commented_field("_ingestion_ts", TimestampType(), "Timestamp when the row was ingested"),
]


# ── Dynamic table factory ───────────────────────────────────────

def _create_bronze_table(contract):
    source = contract["source"]
    dataset = contract["dataset"]
    table_name = dataset["name"]
    source_path = _resolve_path(source["path"])
    fmt = source["format"]
    options = source.get("options", {})
    expectations = _build_expectations(contract.get("quality", []))
    fixed_cols = source.get("fixed_width_columns")
    columns = dataset["columns"]

    if fmt == "text" and fixed_cols:
        table_schema = _build_table_schema(columns, _META_FIELDS)

        @dlt.table(
            name=table_name,
            comment=dataset.get("description", ""),
            table_properties=BRONZE_PROPS,
            schema=table_schema,
        )
        @dlt.expect_all(expectations)
        def _fixed():
            raw = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "text")
                .load(source_path)
            )
            df = raw
            for col_def in fixed_cols:
                col_expr = F.substring(F.col("value"), col_def["start"], col_def["length"])
                if col_def.get("trim", False):
                    col_expr = F.trim(col_expr)
                if col_def.get("cast"):
                    col_expr = col_expr.cast(col_def["cast"])
                df = df.withColumn(col_def["name"], col_expr)

            return (
                df
                .withColumn("_line_length", F.length(F.col("value")))
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_ingestion_ts", F.current_timestamp())
                .drop("value")
            )
    else:
        reader_schema = _build_reader_schema(columns)
        table_schema = _build_table_schema(columns, _META_FIELDS)

        @dlt.table(
            name=table_name,
            comment=dataset.get("description", ""),
            table_properties=BRONZE_PROPS,
            schema=table_schema,
        )
        @dlt.expect_all(expectations)
        def _delimited():
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", fmt)
                .option("inferSchema", "false")
            )
            for k, v in options.items():
                reader = reader.option(k, v)

            return (
                reader
                .schema(reader_schema)
                .load(source_path)
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_ingestion_ts", F.current_timestamp())
            )


# ── Register all bronze tables from contracts ───────────────────

for _contract in _load_contracts(config_base):
    _create_bronze_table(_contract)
