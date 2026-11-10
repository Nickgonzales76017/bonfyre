#!/bin/sh
# A write to the store is an event. A hook is handed the write itself -- the
# kind, the entry, the row before and after -- and from that a donor
# Notification is decided and its message held in the queue. Nothing is sent.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${1:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
compiler="$root/cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler"
queue="$root/cmd/BonfyreQueue/bonfyre-queue"
hook="$root/cmd/BonfyreFrappeCompiler/tools/held_message_hook.sh"
app="$root/integrations/frappe-bench/apps/erpnext"
for need in "$cms" "$compiler" "$queue"; do
  [ -x "$need" ] || { echo "hook held message: SKIP ($(basename "$need") not built)"; exit 0; }
done
[ -d "$app" ] || { echo "hook held message: SKIP (no donor metadata)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"
mkdir "$tmp/types"

"$compiler" --root "$root" --emit-cms-schema "$tmp/types/fiscal_year.json" \
  --cms-doctype "Fiscal Year" -o "$tmp/pack" "$app" >/dev/null 2>&1
type=$(jq -r .name "$tmp/types/fiscal_year.json")
"$cms" schema apply "$tmp/types/fiscal_year.json" --db "$db" --schemas "$tmp/types" >/dev/null 2>&1

# The hook is handed the write: record what it was given, then decide.
cat >"$tmp/hook.sh" <<HOOK
#!/bin/sh
printf '%s %s %s\n' "\$BONFYRE_CMS_EVENT" "\$BONFYRE_CMS_TYPE" "\$BONFYRE_CMS_ENTRY_ID" >>"$tmp/seen"
printf '%s\n' "\${BONFYRE_CMS_PREVIOUS_JSON:-none}" >>"$tmp/previous"
exec "$hook" "$app" "Fiscal Year" "$tmp/queue.db"
HOOK
chmod +x "$tmp/hook.sh"
sqlite3 "$db" "INSERT INTO _hooks(content_type,event,command) VALUES('$type','after_create','$tmp/hook.sh'),('$type','after_update','$tmp/hook.sh')"

run() { "$cms" entry "$@" --db "$db" --schemas "$tmp/types" >/dev/null 2>"$tmp/err"; }
depth() { "$queue" depth --family held-message --db "$tmp/queue.db" 2>/dev/null | jq -r .total; }

# Created by hand: the donor's rule fires only for a year the system made.
run create "$type" --ns fixture '{"year":"2026","year_start_date":"2026-01-01","year_end_date":"2026-12-31","auto_created":0}'
[ "$(cat "$tmp/seen")" = "after_create $type 1" ]
[ ! -e "$tmp/queue.db" ] || [ "$(depth)" = 0 ]

# Created automatically: one held message, written from the row.
run create "$type" --ns fixture '{"year":"2027","year_start_date":"2027-01-01","year_end_date":"2027-12-31","auto_created":1}'
[ "$(depth)" = 1 ]
mission=$("$queue" claim-next witness --family held-message --db "$tmp/queue.db" | jq -r .mission_id)
"$queue" mission "$mission" --db "$tmp/queue.db" | jq -r .input_uri >"$tmp/held.json"
jq -e '.state == "composed" and .transmitted == false and .notification == "Notification for new fiscal year"
       and (.document | endswith(":2")) and (.body | contains("2027-01-01 Fri"))
       and (.recipients | length > 0) and all(.recipients[]; .address == null)' "$tmp/held.json" >/dev/null \
  || { echo "FAIL: held message in the queue is not the composed one" >&2; exit 1; }

# The same write presented again files nothing new.
BONFYRE_CMS_EVENT=after_create BONFYRE_CMS_TYPE="$type" BONFYRE_CMS_ENTRY_ID=2 \
  BONFYRE_CMS_ENTRY_JSON='{"year":"2027","year_start_date":"2027-01-01","year_end_date":"2027-12-31","auto_created":1}' \
  "$hook" "$app" "Fiscal Year" "$tmp/queue.db"
[ "$(depth)" = 1 ]

# An update is handed the row as it stood before.
run update "$type" 1 --ns fixture '{"year":"2026-27"}' || { cat "$tmp/err" >&2; echo "FAIL: update refused" >&2; exit 1; }
[ "$(sed -n 3p "$tmp/seen")" = "after_update $type 1" ]
sed -n 3p "$tmp/previous" | jq -e '.year == "2026"' >/dev/null
sed -n 1p "$tmp/previous" | grep -qx none

echo "hook held message: PASS"
