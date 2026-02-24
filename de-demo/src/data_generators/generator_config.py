"""
Shared configuration and anomaly injection utilities for all data generators.
Each generator imports RunConfig, AnomalyConfig, and helper functions from here.
"""

import uuid
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@dataclass
class RunConfig:
    catalog: str
    schema: str
    volume: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    run_timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AnomalyConfig:
    null_rate: float = 0.03
    duplicate_rate: float = 0.02
    invalid_enum_rate: float = 0.04
    out_of_range_rate: float = 0.03
    future_date_rate: float = 0.02
    format_error_rate: float = 0.02


def get_run_config(spark: SparkSession = None, **overrides) -> RunConfig:
    """
    Build RunConfig from direct overrides, spark.conf, or defaults.
    On serverless (Spark Connect), spark.conf for custom keys is unavailable —
    callers should pass catalog/schema/volume as overrides from dbutils.widgets.
    """
    defaults = {"catalog": "main", "schema": "de_demo", "volume": "raw_files"}
    resolved = {}

    for key, default in defaults.items():
        if key in overrides:
            resolved[key] = overrides[key]
            continue
        if spark is not None:
            try:
                resolved[key] = spark.conf.get(f"demo.{key}")
                continue
            except Exception:
                pass
        resolved[key] = default

    return RunConfig(
        catalog=resolved["catalog"],
        schema=resolved["schema"],
        volume=resolved["volume"],
    )


def get_volume_path(config: RunConfig, dataset_name: str) -> str:
    return f"/Volumes/{config.catalog}/{config.schema}/{config.volume}/{dataset_name}/"


def _seeded_rng(run_id: str, salt: int = 0) -> random.Random:
    seed = hash(run_id + str(salt)) & 0xFFFFFFFF
    return random.Random(seed)


def inject_anomalies(
    df: DataFrame,
    config: AnomalyConfig,
    rules: dict,
    run_id: str,
) -> DataFrame:
    """
    Inject anomalies into a PySpark DataFrame based on the rules dict.

    ``rules`` maps anomaly type to a callable that receives (df, rate, rng_seed)
    and returns the modified DataFrame.  Each generator defines its own rules dict
    so that the anomaly shapes are dataset-specific while the rates stay centralised.
    """
    rng = _seeded_rng(run_id)

    rate_map = {
        "null": config.null_rate,
        "duplicate": config.duplicate_rate,
        "invalid_enum": config.invalid_enum_rate,
        "out_of_range": config.out_of_range_rate,
        "future_date": config.future_date_rate,
        "format_error": config.format_error_rate,
    }

    for anomaly_type, apply_fn in rules.items():
        rate = rate_map.get(anomaly_type, 0.0)
        if rate > 0:
            seed = rng.randint(0, 2**31)
            df = apply_fn(df, rate, seed)

    return df
