# DE Demo — Databricks Asset Bundles Project

End-to-end data engineering demo showcasing mock data generation, Auto Loader bronze ingestion, DQ-driven silver transforms, and gold analytics — all orchestrated via Databricks Asset Bundles.

## Two-Pipeline Architecture

### Pipeline 1: Data Generator (`pipeline_data_generator`)

A Databricks Workflow job with three sequential tasks that produce synthetic financial-services data with **intentionally injected anomalies** (nulls, duplicates, invalid enums, out-of-range values, future dates, format errors). Each run writes timestamped files into Unity Catalog Volumes.

| Task | Output Format | Rows | Landing Path |
|---|---|---|---|
| `gen_clientworks` | Pipe-delimited CSV | 50,000 | `/Volumes/.../clientworks/` |
| `gen_new_account` | Comma-separated CSV | 30,000 | `/Volumes/.../new_account/` |
| `gen_fixed_width` | Fixed-width text | 10,000 | `/Volumes/.../fixed_width/` |

### Pipeline 2: Ingestion Pipeline (`pipeline_ingestion`)

A Lakeflow (DLT) pipeline that reads the generated files through three layers:

| Layer | Tables | Description |
|---|---|---|
| **Bronze** | `bronze_clientworks`, `bronze_new_account`, `bronze_fixed_width` | Auto Loader streaming ingestion with loose DLT expectations |
| **Silver** | `silver_clientworks`, `silver_new_account`, `silver_fixed_width` | Type casting, normalization, and ODCS contract-driven DQ checks via the custom DQ framework. Failing rows are quarantined. |
| **Gold** | `gold_advisor_portfolio_summary`, `gold_account_balance_snapshot`, `gold_dq_anomaly_report` | Business-level aggregations, cross-dataset joins, and a DQ monitoring view |

## Data Quality Framework

The `src/dq_framework/` package implements contract-driven data quality:

1. **Rules Parser** — reads ODCS v3 JSON contracts and emits typed `Rule` objects
2. **Engine** — executes rules against Spark DataFrames, captures pass/fail counts and sample failures
3. **Reporter** — writes results to a `dq_results` Delta table and quarantines bad rows

ODCS contracts live in `src/configs/` and define column-level expectations (not_null, unique, accepted_values, range, regex, not_future_date, date_format, exact_length).

## Quick Start

```bash
# Validate the bundle
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# Run the data generator first
databricks bundle run pipeline_data_generator --target dev

# Then run the ingestion pipeline
databricks bundle run pipeline_ingestion --target dev

# Or use the helper script
./scripts/deploy_and_run.sh dev
```

## Bundle Variables

| Variable | Default | Description |
|---|---|---|
| `catalog` | `main` | Unity Catalog catalog |
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
│   ├── data_generators/
│   │   ├── generator_config.py           # Shared config, anomaly injection
│   │   ├── gen_clientworks.py            # Pipe-delimited CSV generator
│   │   ├── gen_new_account.py            # CSV generator
│   │   └── gen_fixed_width.py            # Fixed-width text generator
│   ├── pipelines/
│   │   ├── bronze_ingestion.py           # Auto Loader DLT bronze tables
│   │   ├── silver_transform.py           # DQ-checked silver tables
│   │   └── gold_layer.py                 # Business aggregations
│   ├── dq_framework/
│   │   ├── rules_parser.py              # ODCS contract → Rule objects
│   │   ├── engine.py                    # Run checks, capture results
│   │   └── reporter.py                  # Write results, quarantine rows
│   └── configs/
│       ├── clientworks_contract.json     # ODCS v3 contract
│       ├── new_account_contract.json     # ODCS v3 contract
│       └── fixed_width_contract.json     # ODCS v3 contract
├── scripts/
│   └── deploy_and_run.sh                # One-command deploy + run
└── tests/
```
