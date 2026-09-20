#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Switch the platform between its two ingestion paths.
#
# The batch and streaming stacks together need roughly 21 GB, which is more
# than the reference machine has. They share SQL Server, ClickHouse and
# Grafana, so switching means stopping one set of containers and starting
# the other — the shared ones are never touched and keep their state.
#
#   ./scripts/mode.sh batch    phase 1 — Airflow, PostgreSQL staging
#   ./scripts/mode.sh stream   phase 2 — Kafka, MongoDB
#   ./scripts/mode.sh core     shared services only
#   ./scripts/mode.sh status   what is running right now
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

BATCH_SERVICES=(northwind_staging airflow_meta airflow_webserver airflow_scheduler)
STREAM_SERVICES=(kafka kafka_ui northwind_audit cdc_producer cdc_consumer)

stop_services() {
  local label="$1"; shift
  echo "→ stopping ${label} services"
  docker compose stop "$@" 2>/dev/null || true
}

case "${1:-}" in
  batch)
    stop_services "streaming" "${STREAM_SERVICES[@]}"
    echo "→ starting batch stack"
    docker compose --profile batch up -d
    ;;
  stream)
    stop_services "batch" "${BATCH_SERVICES[@]}"
    echo "→ starting streaming stack"
    docker compose --profile stream up -d
    ;;
  core)
    stop_services "batch" "${BATCH_SERVICES[@]}"
    stop_services "streaming" "${STREAM_SERVICES[@]}"
    echo "→ starting shared services"
    docker compose up -d
    ;;
  status)
    docker compose ps
    exit 0
    ;;
  *)
    echo "usage: $0 {batch|stream|core|status}" >&2
    exit 1
    ;;
esac

echo
docker compose ps