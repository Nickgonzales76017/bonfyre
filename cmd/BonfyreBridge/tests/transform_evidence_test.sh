#!/bin/sh
# Transformation evidence on fixtures: cited casting rules -> schema mapping ->
# rehearsal on a throwaway target -> diff receipt -> re-check.
#
#   SQL: a SQLite shop (fixtures/transform/legacy_shop.sql) mapped to
#        PostgreSQL by the casting rules pgloader documents (a ForeignTwin
#        observation retained under fixtures/ with its receipt), rehearsed on
#        a PostgreSQL 16 cluster started here on a private socket.
#   ERP: an Odoo-shaped res_partner mapped onto the ERPNext Customer DocType
#        as BonfyreFrappeCompiler compiled it, typed by Frappe's own MariaDB
#        type map, rehearsed on SQLite.
#
# Every database lives in a temp directory and is discarded.  Nothing here
# reaches a customer system or the network.
set -eu
bin=${1:?bridge binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
here=$(cd "$(dirname "$0")" && pwd)
fx="$here/fixtures/transform"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bftransform.XXXXXX")
cleanup() {
  [ -d "$tmp/pg" ] && pg_ctl -D "$tmp/pg" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM
pass=0
fail() { echo "transform evidence: FAIL: $1" >&2; exit 1; }
ok() { pass=$((pass + 1)); }
has() { printf '%s' "$1" | grep -q "$2"; }
jq_() { python3 -I -c "import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2], {'d': d}))" "$@"; }

sqlite3 "$tmp/shop.db" < "$fx/legacy_shop.sql"
sqlite3 "$tmp/odoo.db" < "$fx/odoo_partner.sql"
cp "$tmp/shop.db" "$tmp/shop.pristine.db"

# ---- rules from a receipted observation --------------------------------
out=$("$bin" rules --format pgloader-casting --receipt "$fx/pgloader-docs-ref-sqlite.receipt.json" \
        --body "$fx/pgloader-docs-ref-sqlite.rst.body" --out "$tmp/pg.rules.json")
has "$out" '"rules":18' || fail "pgloader rules: $out"; ok
[ "$(jq_ "$tmp/pg.rules.json" "[r['target'] for r in d['rules'] if r['match']=='double'][0]")" = "double precision" ] || fail "multi-word target"; ok
[ "$(jq_ "$tmp/pg.rules.json" "sum(r['duplicate'] for r in d['rules'])")" = 1 ] || fail "duplicate nvarchar rule not marked"; ok
# Every rule's excerpt is the exact byte range of the retained body.
python3 -I - "$tmp/pg.rules.json" "$fx/pgloader-docs-ref-sqlite.rst.body" <<'EOF' || fail "excerpt is not the recorded byte range"
import json, sys
d = json.load(open(sys.argv[1])); b = open(sys.argv[2], 'rb').read()
for r in d['rules']:
    assert b[r['byte_offset']:r['byte_offset'] + r['byte_length']].decode() == r['excerpt'], r
EOF
ok
# A body that no longer hashes to its receipt is refused.
cp "$fx/pgloader-docs-ref-sqlite.rst.body" "$tmp/changed.body"; printf ' ' >> "$tmp/changed.body"
if "$bin" rules --format pgloader-casting --receipt "$fx/pgloader-docs-ref-sqlite.receipt.json" --body "$tmp/changed.body" --out "$tmp/x.json" 2>/dev/null; then fail "changed body accepted"; fi; ok
# A receipt the policy refused is refused.
sed 's/"policy_decision":"allow"/"policy_decision":"deny"/' "$fx/pgloader-docs-ref-sqlite.receipt.json" > "$tmp/deny.json"
if "$bin" rules --format pgloader-casting --receipt "$tmp/deny.json" --body "$fx/pgloader-docs-ref-sqlite.rst.body" --out "$tmp/x.json" 2>/dev/null; then fail "refused receipt accepted"; fi; ok
out=$("$bin" rules --format frappe-type-map --file "$fx/frappe-mariadb-type-map.txt" --const-file "$fx/frappe-mariadb-type-map.txt" --out "$tmp/fr.rules.json")
has "$out" '"rules":34' || fail "frappe rules: $out"; ok
[ "$(jq_ "$tmp/fr.rules.json" "[r['target'] for r in d['rules'] if r['match']=='Data'][0]")" = "varchar(140)" ] || fail "VARCHAR_LEN not resolved"; ok

# ---- schema mapping -----------------------------------------------------
out=$("$bin" schema-map --from "sqlite:$tmp/shop.db" --plan "$fx/legacy_shop.plan.json" --rules "$tmp/pg.rules.json" \
        --out "$tmp/shop.map.json" --emit-sql "$tmp/shop.pg.sql")
has "$out" '"tables":4,"columns":20,"typed_by_rule":20' || fail "shop mapping: $out"; ok
[ "$(jq_ "$tmp/shop.map.json" "[c['target_type'] for t in d['tables'] for c in t['columns'] if t['source']=='product' and c['source']=='unit_price'][0]")" = "numeric(10,2)" ] || fail "typemod kept for decimal"; ok
[ "$(jq_ "$tmp/shop.map.json" "[c['target_type'] for t in d['tables'] for c in t['columns'] if c['source']=='name'][0]")" = "text" ] || fail "varchar typemod dropped"; ok
[ "$(jq_ "$tmp/shop.map.json" "sorted(e['kind']+':'+e['ref'] for e in d['exceptions'])")" = "['check-not-carried:product', 'not-carried:customer_name_trim', 'not-carried:order_total']" ] || fail "shop exceptions"; ok
# The rule each column cites is a byte range in the observed body.
[ "$(jq_ "$tmp/shop.map.json" "[c['rule']['excerpt'] for t in d['tables'] for c in t['columns'] if c['source']=='photo'][0]")" = "type blob       to bytea" ] || fail "blob rule citation"; ok
cmp -s "$tmp/shop.db" "$tmp/shop.pristine.db" || fail "schema-map wrote to the source"; ok

out=$("$bin" schema-map --from "sqlite:$tmp/odoo.db" --plan "$fx/odoo_partner.plan.json" --rules "$tmp/fr.rules.json" \
        --frappe-schema "$fx/erpnext-customer.schema-ir.json" --out "$tmp/odoo.map.json" --emit-sql "$tmp/odoo.sql")
has "$out" '"typed_by_rule":5' || fail "erp mapping: $out"; ok
[ "$(jq_ "$tmp/odoo.map.json" "sorted(e['kind']+':'+e['ref'] for e in d['exceptions'])")" = "['column-not-carried:res_partner', 'rehearsal-engine-differs:tabCustomer']" ] || fail "erp exceptions: $(jq_ "$tmp/odoo.map.json" "d['exceptions']")"; ok
# A value outside the Select options, and an uncovered required field, are named.
sed 's/"Individual"/"Person"/; s/{"source": "name", "target": "customer_name"},//' "$fx/odoo_partner.plan.json" > "$tmp/bad.plan.json"
"$bin" schema-map --from "sqlite:$tmp/odoo.db" --plan "$tmp/bad.plan.json" --rules "$tmp/fr.rules.json" \
  --frappe-schema "$fx/erpnext-customer.schema-ir.json" --out "$tmp/bad.map.json" >/dev/null
k=$(jq_ "$tmp/bad.map.json" "sorted(set(e['kind'] for e in d['exceptions']))")
has "$k" frappe-select-value-outside-options || fail "select option check"; has "$k" frappe-required-uncovered || fail "required check"; ok
# A mapping edited by hand no longer matches its digest and is refused downstream.
sed 's/"numeric(10,2)"/"numeric(12,2)"/' "$tmp/shop.map.json" > "$tmp/forged.map.json"

# ---- rehearsal on PostgreSQL --------------------------------------------
if command -v initdb >/dev/null && command -v pg_ctl >/dev/null && command -v psql >/dev/null; then
  initdb -D "$tmp/pg" -A trust -U bf >/dev/null 2>&1 || fail "initdb"
  pg_ctl -D "$tmp/pg" -o "-k $tmp -c listen_addresses='' -p 54329" -l "$tmp/pg.log" -w start >/dev/null || fail "postgres start"
  psql -X -q -h "$tmp" -p 54329 -U bf -d postgres -c 'CREATE DATABASE rehearsal' >/dev/null
  url="postgres://bf@localhost:54329/rehearsal?host=$tmp"
  psql -X -q -v ON_ERROR_STOP=1 -h "$tmp" -p 54329 -U bf -d rehearsal -f "$tmp/shop.pg.sql" >/dev/null || fail "rehearsal SQL refused by PostgreSQL"
  ok
  BFURL="postgresql:///rehearsal?host=$tmp&port=54329&user=bf"; export BFURL
  set +e
  out=$("$bin" map-verify --from "sqlite:$tmp/shop.db" --to env:BFURL --mapping "$tmp/shop.map.json" --out "$tmp/shop.diff.json"); rc=$?
  set -e
  [ $rc = 5 ] || fail "shop verify exit $rc: $out"
  has "$out" '"rows_and_values_match":true,"types_match":true,"constraints_match":false,"objects_carried":false,"behavior_matches":false' || fail "shop verdict: $out"; ok
  [ "$(jq_ "$tmp/shop.diff.json" "d['source_rows'], d['target_rows']")" = "(15, 15)" ] || fail "row totals"; ok
  # LIKE is case-insensitive for ASCII in SQLite and case-sensitive in PostgreSQL.
  [ "$(jq_ "$tmp/shop.diff.json" "[(p['source_result'], p['target_result']) for p in d['probes'] if p['id']=='name-like-a'][0]")" = "('1;2;', '1;')" ] || fail "LIKE probe"; ok
  [ "$(jq_ "$tmp/shop.diff.json" "[p['same'] for p in d['probes'] if p['id']!='name-like-a']")" = "[True, True]" ] || fail "other probes"; ok
  [ "$(jq_ "$tmp/shop.diff.json" "sorted(set(e['kind']+':'+e['ref'] for e in d['exceptions']))")" = "['behavior-differs:name-like-a', 'constraint-count-differs:product', 'object-absent:customer_name_trim', 'object-absent:order_total']" ] || fail "shop diff exceptions: $(jq_ "$tmp/shop.diff.json" "d['exceptions']")"; ok
  # Re-check: same inputs measure the same.
  set +e
  out=$("$bin" map-verify --from "sqlite:$tmp/shop.db" --to env:BFURL --mapping "$tmp/shop.map.json" --out "$tmp/shop.diff2.json" --expect "$tmp/shop.diff.json"); rc=$?
  set -e
  [ $rc = 5 ] && has "$out" '"recheck":"unchanged"' || fail "recheck of unchanged rehearsal: $rc $out"; ok
  # One changed value, one deleted row, one retyped column: each is named, and the re-check refuses.
  psql -X -q -h "$tmp" -p 54329 -U bf -d rehearsal -c "UPDATE product SET unit_price = 4.51 WHERE id = 10; DELETE FROM sales_order_line WHERE id = 1003; ALTER TABLE customer ALTER COLUMN vip TYPE integer" >/dev/null
  set +e
  out=$("$bin" map-verify --from "sqlite:$tmp/shop.db" --to env:BFURL --mapping "$tmp/shop.map.json" --out "$tmp/shop.diff3.json" --expect "$tmp/shop.diff.json"); rc=$?
  set -e
  [ $rc = 4 ] || fail "drift not refused: $rc $out"; ok
  [ "$(jq_ "$tmp/shop.diff3.json" "[(c['differing_rows'], c['first_differing_keys']) for t in d['tables'] for c in t['columns'] if t['target']=='product' and c['target']=='unit_price'][0]")" = "(1, ['10|'])" ] || fail "changed value not located"; ok
  [ "$(jq_ "$tmp/shop.diff3.json" "[(t['state'], t['missing_keys']) for t in d['tables'] if t['target']=='sales_order_line'][0]")" = "('rows_differ', ['1003|'])" ] || fail "deleted row not located"; ok
  [ "$(jq_ "$tmp/shop.diff3.json" "[c['type_matches'] for t in d['tables'] for c in t['columns'] if c['target']=='vip'][0]")" = "False" ] || fail "retyped column unnoticed"; ok
  # The password never reaches the receipt.
  BFURL2="postgresql://bf:sekret@/rehearsal?host=$tmp&port=54329"; export BFURL2
  "$bin" map-verify --from "sqlite:$tmp/shop.db" --to env:BFURL2 --mapping "$tmp/shop.map.json" --out "$tmp/pw.json" >/dev/null || true
  [ -s "$tmp/pw.json" ] || fail "password URL not read"
  if grep -q sekret "$tmp/pw.json"; then fail "password in receipt"; fi; ok
  # A forged mapping is refused.
  if "$bin" map-verify --from "sqlite:$tmp/shop.db" --to env:BFURL --mapping "$tmp/forged.map.json" --out "$tmp/x.json" 2>/dev/null; then fail "forged mapping accepted"; fi; ok
  # An unreachable target is never reported as matching types or constraints.
  BFURL3="postgresql:///nosuchdb?host=$tmp&port=54329&user=bf"; export BFURL3
  set +e; out=$("$bin" map-verify --from "sqlite:$tmp/shop.db" --to env:BFURL3 --mapping "$tmp/shop.map.json" --out "$tmp/x.json" 2>/dev/null); rc=$?; set -e
  [ $rc = 5 ] && has "$out" '"matched":false,"rows_and_values_match":false,"types_match":false,"constraints_match":false' || fail "unreachable target: $rc $out"; ok
  cmp -s "$tmp/shop.db" "$tmp/shop.pristine.db" || fail "map-verify wrote to the source"; ok
  cp "$tmp/shop.map.json" "$tmp/shop.diff.json" "$tmp/pg.rules.json" "${BONFYRE_TRANSFORM_KEEP:-$tmp}/" 2>/dev/null || true
else
  echo "transform evidence: PostgreSQL leg SKIPPED (initdb/pg_ctl/psql not installed)"
fi

# ---- ERP rehearsal on SQLite --------------------------------------------
sqlite3 "$tmp/erp.db" < "$tmp/odoo.sql"
out=$("$bin" map-verify --from "sqlite:$tmp/odoo.db" --to "sqlite:$tmp/erp.db" --mapping "$tmp/odoo.map.json" --out "$tmp/odoo.diff.json") || fail "erp verify: $out"
has "$out" '"matched":true' || fail "erp verdict: $out"; ok
[ "$(jq_ "$tmp/odoo.diff.json" "[c['target_type'] for t in d['tables'] for c in t['columns'] if c['target']=='disabled'][0]")" = "int(1)" ] || fail "Check typed by Frappe map"; ok
# A value map that drifts on the target is caught by the value-mapped column's checksum.
sqlite3 "$tmp/erp.db" "UPDATE \"tabCustomer\" SET customer_type='Partnership' WHERE name='PARTNER-2'"
set +e; out=$("$bin" map-verify --from "sqlite:$tmp/odoo.db" --to "sqlite:$tmp/erp.db" --mapping "$tmp/odoo.map.json" --out "$tmp/odoo.diff2.json" --expect "$tmp/odoo.diff.json"); rc=$?; set -e
[ $rc = 4 ] || fail "erp drift exit $rc"; ok
[ "$(jq_ "$tmp/odoo.diff2.json" "[c['first_differing_keys'] for t in d['tables'] for c in t['columns'] if c['target']=='customer_type'][0]")" = "['PARTNER-2|']" ] || fail "value-map drift key"; ok
[ "$(jq_ "$tmp/odoo.diff2.json" "[p['same'] for p in d['probes']]")" = "[True, True]" ] || fail "erp probes"; ok
if [ -n "${BONFYRE_TRANSFORM_KEEP:-}" ]; then cp "$tmp/odoo.map.json" "$tmp/odoo.diff.json" "$tmp/fr.rules.json" "$BONFYRE_TRANSFORM_KEEP/"; fi

echo "transform evidence: PASS ($pass checks)"
