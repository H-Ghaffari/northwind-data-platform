#!/usr/bin/env bash
# ===========================================================================
# Run a PySpark job inside the Airflow scheduler container.
#
# The scheduler is where DAGs will execute these same modules later, so
# running them here first proves the job works in the environment that will
# actually host it — not merely on the developer's machine.
#
# Usage:  ./scripts/run_spark_job.sh dimensions/dim_date.py
# ===========================================================================
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <job_path_relative_to_spark/jobs>"
    echo "Example: $0 dimensions/dim_date.py"
    exit 1
fi

cd "$(dirname "$0")/.."

JOB="$1"
CONTAINER="airflow_scheduler"

if ! docker compose ps --status running --services | grep -qx "${CONTAINER}"; then
    echo "ERROR: ${CONTAINER} is not running. Start it with: docker compose up -d"
    exit 1
fi

echo "==> Running ${JOB}"
docker compose exec -T \
    -e PYTHONPATH=/opt/spark-jobs/jobs \
    "${CONTAINER}" \
    python "/opt/spark-jobs/jobs/${JOB}"
