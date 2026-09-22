#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Provision Kibana: data views with fixed ids, and the project dashboard.
#
# Objects clicked together in the UI live only in Kibana's own index and are
# gone after a volume reset. Created by script, they are part of the
# repository — the same reasoning as Grafana's provisioned datasources.
#
# The data view ids are fixed rather than generated. The exported dashboard
# refers to its data views by id, so a dashboard built against randomly
# generated ones would not import on any other machine.
#
# The dashboard itself is built in the UI and exported to
# elk/kibana/northwind-streaming.ndjson, because Lens objects are too
# intricate to write reliably by hand. This script imports that file when it
# exists.
#
# Idempotent: existing data views are left alone, the dashboard is
# overwritten with the committed version.
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
set -a; source .env; set +a

KIBANA="http://localhost:${KIBANA_EXTERNAL_PORT:-25601}"
DASHBOARD_FILE="elk/kibana/northwind-streaming.ndjson"

echo "→ waiting for Kibana"
for _ in $(seq 1 40); do
  curl -s "${KIBANA}/api/status" | grep -q '"level":"available"' && break
  sleep 5
done

data_view() {
  local id="$1" title="$2" name="$3" status
  status=$(curl -s -o /dev/null -w '%{http_code}' \
    "${KIBANA}/api/data_views/data_view/${id}")

  if [[ "${status}" == "200" ]]; then
    echo "  ${name}: exists"
    return
  fi

  curl -s -X POST "${KIBANA}/api/data_views/data_view" \
    -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
    -d "{\"data_view\": {
          \"id\": \"${id}\",
          \"title\": \"${title}\",
          \"name\": \"${name}\",
          \"timeFieldName\": \"@timestamp\"
        }}" > /dev/null
  echo "  ${name}: created"
}

echo "→ data views"
data_view northwind-audit "northwind-audit-*" "Northwind audit"
data_view packetbeat      "packetbeat-*"      "Packetbeat flows"

echo "→ dashboard"
if [[ -f "${DASHBOARD_FILE}" ]]; then
  RESULT=$(curl -s -X POST "${KIBANA}/api/saved_objects/_import?overwrite=true" \
    -H 'kbn-xsrf: true' --form file=@"${DASHBOARD_FILE}")
  if echo "${RESULT}" | grep -q '"success":true'; then
    echo "  imported from ${DASHBOARD_FILE}"
  else
    echo "  import failed: ${RESULT}" >&2
    exit 1
  fi
else
  echo "  ${DASHBOARD_FILE} not found — build the dashboard in Kibana, then export it"
fi

echo
echo "Kibana: ${KIBANA}/app/dashboards"