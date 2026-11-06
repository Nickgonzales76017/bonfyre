#!/bin/sh
# A field required only while a condition holds must be refused by the store
# under the same rule the compiled DocType states, not a second copy of it.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${1:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
compiler="$root/cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler"
app="$root/integrations/frappe-bench/apps/frappe"
[ -x "$cms" ] || { echo "conditional required: SKIP (bonfyre-cms not built)"; exit 0; }
[ -x "$compiler" ] && [ -d "$app" ] || { echo "conditional required: SKIP (no compiled donor metadata)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"
mkdir "$tmp/types"

"$compiler" --root "$root" --emit-cms-schema "$tmp/types/number_card.json" \
  --cms-doctype "Number Card" -o "$tmp/pack" "$app" >/dev/null 2>&1
type=$(jq -r .name "$tmp/types/number_card.json")
# Number Card says: a document type is mandatory only for a "Document Type" card.
[ "$(jq -r '.attributes.document_type.requiredWhen' "$tmp/types/number_card.json")" = "eval: doc.type == 'Document Type'" ]
[ "$(jq -r '.attributes.document_type.required' "$tmp/types/number_card.json")" = false ]

"$cms" schema apply "$tmp/types/number_card.json" --db "$db" --schemas "$tmp/types" >/dev/null
create() { "$cms" entry create "$type" --ns fixture "$1" --db "$db" --schemas "$tmp/types" >"$tmp/out" 2>"$tmp/err"; }
count() { sqlite3 "$db" "select count(*) from \"$type\""; }

# The condition does not hold: the field may be absent. (A Custom card is
# required to name a method instead, by a rule of its own.)
create '{"label":"Custom card","type":"Custom","method":"cards.count"}' || { cat "$tmp/err" >&2; echo "FAIL: unconditional entry refused" >&2; exit 1; }
[ "$(count)" = 1 ]

# The condition holds and the field is absent: refused, and nothing is stored.
if create '{"label":"Doc card","type":"Document Type","function":"Count"}'; then
  echo "FAIL: entry missing a conditionally required field was stored" >&2; exit 1
fi
grep -q 'document_type is required while its condition holds' "$tmp/err"
[ "$(count)" = 1 ]

# The condition holds and the field is given: stored.
create '{"label":"Doc card","type":"Document Type","function":"Count","document_type":"ToDo"}' \
  || { cat "$tmp/err" >&2; echo "FAIL: complete entry refused" >&2; exit 1; }
[ "$(count)" = 2 ]

# An update that makes the condition hold without the field is rolled back.
first=$(sqlite3 "$db" "select id from \"$type\" order by id limit 1")
if "$cms" entry update "$type" "$first" '{"type":"Document Type"}' --db "$db" --schemas "$tmp/types" >/dev/null 2>"$tmp/err"; then
  echo "FAIL: update into a missing conditional requirement was kept" >&2; exit 1
fi
[ "$(sqlite3 "$db" "select type from \"$type\" where id=$first")" = "Custom" ]

echo "conditional required: PASS"
