#!/usr/bin/env bash
# Build the platform from empty containers, in dependency order.
set -euo pipefail
cd "$(dirname "$0")"

./setup_op_layer.sh
./setup_staging_layer.sh
./setup_dw_layer.sh
./setup_cdc.sh
./run_spark_job.sh dimensions/dim_date.py

echo
echo "Platform ready. Next: trigger dim_op_to_staging in Airflow."
