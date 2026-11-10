#!/bin/sh
# A PublicationOccurrence retains what a calendar source said.  Its source
# kind must survive PageComposition: an event is not a generic story merely
# because its owner row is shared with other foreign publications.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page occurrence event: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-occurrence-event.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas"
for type in internet_source publication_occurrence; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"; done
db="$tmp/cms.db"
"$cms" schema migrate --db "$db" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"events","source":"events-owner","label":"Owner calendar","says":"what its organizer publishes","url":"https://calendar.example.test/events.ics","twin":"events-owner","state":"observed","status":"published","tier":"WARM","kind":"event","extract":"ics:vevent"}' --ns root --db "$db" --schemas "$schemas" >/dev/null
future=$(date -u -v+2H +%Y-%m-%dT%H:%M:%SZ)
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"$cms" entry create publication_occurrence "{\"occurrence_id\":\"owner-event-1\",\"source_twin\":\"events-owner\",\"source_ref\":\"events-owner\",\"canonical_href\":\"https://calendar.example.test/event/owner-event-1\",\"title\":\"Owner-backed calendar event\",\"summary\":\"The organizer's observed notice.\",\"published_at\":\"$future\",\"observed_at\":\"$now\",\"receipt_ref\":\"foreign/receipts/events-owner.json\",\"rights_basis\":\"public_calendar_observation\",\"retention\":\"archive_local\",\"visibility\":\"public\",\"assertion_state\":\"OBSERVED\",\"cannot_infer\":\"this is not an invitation or reservation\",\"status\":\"published\"}" --ns root --db "$db" --schemas "$schemas" >/dev/null
"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$((29200 + $$ % 400))
BONFYRE_CMS_DB="$db" "$web" serve --db "$tmp/matrix.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 60); do curl -fsS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null && break; sleep 0.2; done
page=$(curl -fsS -H 'Host: aurekai.life' "http://127.0.0.1:$port/whats-on")
printf '%s' "$page" | grep -q 'Owner-backed calendar event' || { echo 'page occurrence event: FAIL (owner event absent from agenda)'; exit 1; }
printf '%s' "$page" | grep -q 'class="agenda"' || { echo 'page occurrence event: FAIL (owner event did not retain event rhythm)'; exit 1; }
printf '%s' "$page" | grep -q 'not an invitation or reservation' && { echo 'page occurrence event: FAIL (owner boundary leaked into visible copy)'; exit 1; }
echo 'page occurrence event: PASS'
