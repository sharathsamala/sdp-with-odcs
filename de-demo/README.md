# DE Demo — Contract-Driven Data Engineering with Databricks Asset Bundles

End-to-end data engineering project where **ODCS (Open Data Contract Standard) v3 JSON contracts are the single source of truth** for bronze and silver layers. Add a new data source by dropping a contract JSON — no pipeline code changes needed. Only the gold layer is hand-built for business-specific logic.

## Architecture

```
                    ┌─────────────────────────────┐
                    │   ODCS Contract JSONs        │
                    │   (src/configs/*_contract)   │
                    └─────────┬───────────────────-┘
                              │
            ┌─────────────────┼──────────────────┐
            ▼                 ▼                  ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │  source.path │  │  dataset.*   │  │  quality.*   │
    │  source.fmt  │  │  columns     │  │  DQ rules    │
    │  source.opts │  │  description │  │  severity    │
    └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
           │                 │                 │
     Bronze Layer      Schema + Col      Silver Layer
     (Auto Loader)     Comments (UC)     (DQ Framework)
```

### Ingestion Pipeline (`pipeline_ingestion`)

A Lakeflow (DLT) pipeline that reads the generated files through three layers:

| Layer | What the contract controls | What's manual |
|---|---|---|
| **Bronze** | Source path, file format, delimiter, schema, column comments, critical expectations | Nothing |
| **Silver** | Type casts, SQL transforms, DQ rules, quarantine logic | Nothing |
| **Gold** | — | Business aggregations, cross-dataset joins |

## Contract-Driven Design

The ODCS contract JSON is the **only file you need to create** to add a new data source to bronze and silver. The pipeline code is fully generic.

### Contract Structure

```json
{
  "apiVersion": "v3.0.0",
  "kind": "DataContract",
  "id": "urn:datacontract:bronze-my-source",
  "info": {
    "title": "My Source Data Contract",
    "version": "1.0.0",
    "owner": "data-engineering",
    "domain": "my-domain"
  },
  "dataset": {
    "name": "bronze_my_source",
    "description": "Becomes the table comment in Unity Catalog",
    "columns": [
      {"name": "col_a", "type": "string", "description": "Becomes UC column comment"},
      {"name": "col_b", "type": "string", "description": "Another column"}
    ]
  },
  "source": {
    "path": "/Volumes/${catalog}/${schema}/${volume}/my_source/",
    "format": "csv",
    "options": {"header": "true", "delimiter": ","}
  },
  "silver": {
    "table_name": "silver_my_source",
    "transforms": [
      {"column": "col_a", "type": "integer"},
      {"column": "col_b", "expr": "UPPER(TRIM(col_b))"}
    ]
  },
  "quality": [
    {
      "rule_name": "col_a_not_null",
      "column": "col_a",
      "rule_type": "not_null",
      "severity": "critical",
      "description": "col_a must not be null"
    }
  ]
}
```

### What each section controls

| Section | Bronze | Silver | Unity Catalog |
|---|---|---|---|
| `dataset.name` | DLT table name | Read source | — |
| `dataset.description` | Table comment | — | Table COMMENT |
| `dataset.columns[].description` | Column comments | — | Column COMMENT |
| `source.path` | Auto Loader load path | — | — |
| `source.format` | `cloudFiles.format` | — | — |
| `source.options` | Reader options (delimiter, header, etc.) | — | — |
| `source.fixed_width_columns` | Substring parsing for text files | — | — |
| `silver.table_name` | — | DLT table name | — |
| `silver.transforms` | — | Type casts and SQL expressions | — |
| `quality[].severity=critical` | DLT `expect` (monitor) | DQ framework check | — |
| `quality[].severity=error/warning` | — | DQ framework check + quarantine | — |

### Silver transform types

| Transform | Example | Effect |
|---|---|---|
| Type cast | `{"column": "amount", "type": "double"}` | `F.col("amount").cast("double")` |
| Date cast | `{"column": "created", "type": "date"}` | `F.to_date(F.col("created"))` |
| Date with format | `{"column": "dt", "type": "date", "format": "yyyyMMdd"}` | `F.to_date(F.col("dt"), "yyyyMMdd")` |
| SQL expression | `{"column": "name", "expr": "TRIM(name)"}` | `F.expr("TRIM(name)")` |
| Derived column | `{"column": "balance_usd", "expr": "cents / 100.0"}` | Creates new column from expression |

### Supported DQ rule types

`not_null`, `unique`, `accepted_values`, `range`, `regex`, `not_future_date`, `date_format`, `exact_length`

## Adding a New Data Source

1. Create `src/configs/my_source_contract.json` following the contract structure above
2. Land your source files into `/Volumes/{catalog}/{schema}/{volume}/my_source/`
3. Deploy and run — bronze and silver tables are created automatically
4. Build gold-layer aggregations manually in `gold_layer.py` if needed

No changes to `bronze_ingestion.py` or `silver_transform.py` required.

A mock data generator pipeline (`pipeline_data_generator`) is also included to produce synthetic financial-services data with intentionally injected anomalies for testing.

## Quick Start

```bash
# Validate
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Generate mock test data (optional — only needed for demo)
databricks bundle run pipeline_data_generator --target dev

# Run the ingestion pipeline
databricks bundle run pipeline_ingestion --target dev
```

## Using with AI Coding Assistants

This project is designed to work well with AI coding assistants. The contract-driven architecture means you can ask any assistant to "add a new data source" and it only needs to produce a single JSON file.

### Cursor

1. Open the project in Cursor
2. Install the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) for Databricks-aware completions:
   ```bash
   curl -sSL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh | bash -s -- --silent
   ```
3. Configure the Databricks CLI profile:
   ```bash
   databricks auth login --profile my-profile --host https://your-workspace.cloud.databricks.com
   ```
4. Update `databricks.yml` targets to use your profile and host
5. Ask Cursor to add a new source:
   > "Add a new contract for a transactions CSV with columns: txn_id, account_id, amount, txn_date, category. Include not_null, range, and accepted_values quality rules."

### Claude Code

1. Clone the repo and open it in your terminal
2. Point Claude at the contract structure:
   > "Look at src/configs/clientworks_contract.json as a reference. Create a new contract for an employee_data source that lands as CSV in /Volumes/.../employee_data/. Columns: emp_id (string), name (string), department (string), salary (double), hire_date (date). Add quality rules and silver transforms."
3. Claude will produce the contract JSON — deploy and run

### OpenAI Codex / ChatGPT

1. Share the contract schema (the "Contract Structure" section above) as context
2. Describe your source data and ask for a contract:
   > "Generate an ODCS v3 contract JSON for a market_data feed. CSV format, comma-delimited, columns: symbol (string), price (double), volume (long), trade_date (date), exchange (string). Quality rules: symbol not null, price > 0, volume > 0, trade_date not future, exchange in NYSE/NASDAQ/LSE."
3. Save the output as `src/configs/market_data_contract.json`

### Databricks AI Dev Kit Skills

The `.cursor/skills/` directory (provided by the AI Dev Kit) gives your assistant deep knowledge of Databricks patterns. Relevant skills for this project:

- **Declarative Pipelines** — DLT table creation, expectations, streaming
- **Unity Catalog** — catalog/schema/volume management, table properties
- **Databricks Config** — bundle YAML, variables, targets
- **Spark Structured Streaming** — Auto Loader, cloudFiles options

When working in Cursor with the AI Dev Kit installed, these skills are automatically available to the assistant.

## Bundle Variables

| Variable | Default | Description |
|---|---|---|
| `catalog` | `sharath_lpl_catalog` | Unity Catalog catalog |
| `schema` | `de_demo` | Target schema |
| `volume` | `raw_files` | Volume for raw landing files |

## Project Structure

```
de-demo/
├── databricks.yml                        # Bundle config, variables, targets
├── resources/
│   ├── pipeline_data_generator.yml       # Job: 3-task data generator workflow
│   └── pipeline_ingestion.yml            # DLT pipeline + trigger job
├── src/
│   ├── configs/                          # ← Drop new contracts here
│   │   ├── clientworks_contract.json     # Pipe-delimited CSV contract
│   │   ├── new_account_contract.json     # Comma CSV contract
│   │   └── fixed_width_contract.json     # Fixed-width text contract
│   ├── pipelines/                        # Generic — no per-source code
│   │   ├── bronze_ingestion.py           # Contract-driven Auto Loader DLT
│   │   ├── silver_transform.py           # Contract-driven DQ + transforms
│   │   └── gold_layer.py                 # Manual business aggregations
│   ├── dq_framework/                     # Reusable DQ engine
│   │   ├── rules_parser.py              # ODCS contract → Rule objects
│   │   ├── engine.py                    # Run checks, capture results
│   │   └── reporter.py                  # Write results, quarantine rows
│   └── data_generators/                  # Mock data generator (for demo/testing)
├── scripts/
│   └── deploy_and_run.sh                # One-command deploy + run
└── tests/
```

## Data Quality Framework

The `src/dq_framework/` package implements contract-driven data quality:

1. **Rules Parser** — reads ODCS v3 JSON contracts and emits typed `Rule` objects
2. **Engine** — executes rules against Spark DataFrames, annotates rows with `_dq_passed` and `_dq_failed_rules`
3. **Reporter** — writes per-rule metrics to a `dq_results` Delta table and quarantines failing rows

Results are surfaced in the `gold_dq_anomaly_report` table for monitoring.
