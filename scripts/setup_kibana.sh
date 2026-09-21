#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Create the Kibana data view over the audit indices.
#
# A data view clicked together in the UI lives only in Kibana's own index and
# is gone after a volume reset. Created by script, it is part of the
# repository — the same reasoning as Grafana's provisioned datasources.
#
# Idempotent: an existing data view is left alone.
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; source .env; set +a

KIBANA="http://localhost:${KIBANA_EXTERNAL_PORT:-25601}"
DATA_VIEW_ID="northwind-audit"

echo "→ waiting for Kibana"
for _ in $(seq 1 40); do
  curl -s "${KIBANA}/api/status" | grep -q '"level":"available"' && break
  sleep 5
done

STATUS=$(curl -s -o /dev/null -w '%{http_code}' \
  "${KIBANA}/api/data_views/data_view/${DATA_VIEW_ID}")

if [[ "${STATUS}" == "200" ]]; then
  echo "  data view already exists"
else
  curl -s -X POST "${KIBANA}/api/data_views/data_view" \
    -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
    -d "{
      \"data_view\": {
        \"id\": \"${DATA_VIEW_ID}\",
        \"title\": \"northwind-audit-*\",
        \"name\": \"Northwind audit\",
        \"timeFieldName\": \"@timestamp\"
      }
    }" > /dev/null
  echo "  data view created"
fi

echo
echo "Open ${KIBANA}/app/discover and choose 'Northwind audit'."