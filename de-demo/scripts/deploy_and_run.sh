#!/usr/bin/env bash
#
# Deploy and run the DE Demo bundle.
# Usage: ./scripts/deploy_and_run.sh [dev|prod]
#
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

TARGET="${1:-dev}"

# Read bundle variables (with defaults matching databricks.yml)
CATALOG="${BUNDLE_VAR_catalog:-main}"
SCHEMA="${BUNDLE_VAR_schema:-de_demo}"
VOLUME="${BUNDLE_VAR_volume:-raw_files}"

msg()  { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}!${NC} $*"; }
fail() { echo -e "  ${RED}✗${NC} $*" >&2; exit 1; }
step() { echo -e "\n${BOLD}$*${NC}"; }

cd "$(dirname "$0")/.."

step "Step 1: Validate bundle"
databricks bundle validate --target "$TARGET" || fail "Bundle validation failed"
msg "Bundle validated"

step "Step 2: Deploy bundle to target=$TARGET"
databricks bundle deploy --target "$TARGET" || fail "Bundle deployment failed"
msg "Bundle deployed"

step "Step 3: Workspace URLs"
echo "  Data Generator Job:   Open in Databricks Workspace → Workflows"
echo "  Ingestion Pipeline:   Open in Databricks Workspace → Delta Live Tables"

step "Step 4: Data Generator"
read -rp "  Run data generator now? (y/n) " run_gen
if [[ "$run_gen" =~ ^[Yy]$ ]]; then
    echo "  Running pipeline_data_generator..."
    databricks bundle run pipeline_data_generator --target "$TARGET" || fail "Data generator failed"
    msg "Data generator completed"

    echo ""
    echo "  Checking generated files in volumes..."
    databricks fs ls "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/clientworks/" 2>/dev/null && msg "clientworks files found" || warn "No clientworks files yet"
    databricks fs ls "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/new_account/" 2>/dev/null && msg "new_account files found" || warn "No new_account files yet"
    databricks fs ls "dbfs:/Volumes/${CATALOG}/${SCHEMA}/${VOLUME}/fixed_width/" 2>/dev/null && msg "fixed_width files found" || warn "No fixed_width files yet"
else
    warn "Skipping data generator"
fi

step "Step 5: Ingestion Pipeline"
read -rp "  Run ingestion pipeline now? (y/n) " run_ingest
if [[ "$run_ingest" =~ ^[Yy]$ ]]; then
    echo "  Running pipeline_ingestion..."
    databricks bundle run pipeline_ingestion --target "$TARGET" || fail "Ingestion pipeline failed"
    msg "Ingestion pipeline completed"
else
    warn "Skipping ingestion pipeline"
fi

step "Done!"
echo ""
echo "  Useful links (open in your Databricks workspace):"
echo "    • Workflows:           /workflows"
echo "    • Delta Live Tables:   /pipelines"
echo "    • dq_results table:    SELECT * FROM ${CATALOG}.${SCHEMA}.dq_results"
echo "    • DQ anomaly report:   SELECT * FROM ${CATALOG}.${SCHEMA}_silver_gold.gold_dq_anomaly_report"
echo ""
msg "Deployment and run complete for target=$TARGET"
