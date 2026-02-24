"""
ODCS v3 contract parser — reads JSON data contracts and emits typed Rule objects
that the DQ engine can execute against Spark DataFrames.
"""

import json
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


class ContractParseError(Exception):
    pass


@dataclass
class Rule:
    rule_name: str
    column: str
    rule_type: str
    severity: str
    params: Dict[str, Any] = field(default_factory=dict)
    check_description: str = ""


REQUIRED_TOP_LEVEL = {"apiVersion", "kind", "dataset", "quality"}
SUPPORTED_RULE_TYPES = {
    "not_null", "unique", "accepted_values", "range",
    "regex", "not_future_date", "date_format", "exact_length",
}


def validate_contract_schema(contract: dict) -> None:
    missing = REQUIRED_TOP_LEVEL - set(contract.keys())
    if missing:
        raise ContractParseError(
            f"Contract missing required top-level fields: {', '.join(sorted(missing))}. "
            f"Expected ODCS v3 structure with: {', '.join(sorted(REQUIRED_TOP_LEVEL))}"
        )

    if contract.get("kind") != "DataContract":
        raise ContractParseError(
            f"Contract kind must be 'DataContract', got '{contract.get('kind')}'"
        )

    quality = contract.get("quality")
    if not isinstance(quality, list):
        raise ContractParseError("Contract 'quality' field must be a list of rule objects")

    for i, rule in enumerate(quality):
        for req in ("rule_name", "column", "rule_type", "severity"):
            if req not in rule:
                raise ContractParseError(
                    f"Quality rule at index {i} missing required field '{req}'"
                )


def _read_contract_file(contract_path: str, spark) -> str:
    """
    Read contract JSON from workspace files, Volumes, or DBFS.
    Workspace paths (/Workspace/...) are FUSE-mounted and use Python I/O.
    Volume/DBFS paths use the Hadoop filesystem API.
    """
    # Workspace files are FUSE-mounted — use Python file I/O
    if contract_path.startswith("/Workspace") or os.path.isfile(contract_path):
        with open(contract_path, "r") as f:
            return f.read()

    # Volume / DBFS paths — try Hadoop FS, then spark.read.text
    try:
        return spark._jvm.org.apache.commons.io.IOUtils.toString(
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).open(
                spark._jvm.org.apache.hadoop.fs.Path(contract_path)
            ),
            "UTF-8",
        )
    except Exception:
        rows = spark.read.text(contract_path).collect()
        return "\n".join([row.value for row in rows])


def parse_contract(contract_path: str, spark) -> List[Rule]:
    """
    Read an ODCS v3 JSON contract and return a list of Rule objects.

    Supports reading from:
      - Workspace files (/Workspace/... — FUSE-mounted)
      - Unity Catalog Volumes (/Volumes/...)
      - DBFS (dbfs:/...)
      - Local filesystem paths
    """
    try:
        raw = _read_contract_file(contract_path, spark)
    except Exception as e:
        raise ContractParseError(f"Failed to read contract at '{contract_path}': {e}")

    try:
        contract = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ContractParseError(f"Invalid JSON in contract '{contract_path}': {e}")

    validate_contract_schema(contract)

    rules: List[Rule] = []
    for entry in contract["quality"]:
        rt = entry["rule_type"]
        if rt not in SUPPORTED_RULE_TYPES:
            raise ContractParseError(
                f"Unsupported rule_type '{rt}' in rule '{entry['rule_name']}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_RULE_TYPES))}"
            )

        rules.append(Rule(
            rule_name=entry["rule_name"],
            column=entry["column"],
            rule_type=rt,
            severity=entry["severity"].upper(),
            params=entry.get("params", {}),
            check_description=entry.get("description", ""),
        ))

    return rules
