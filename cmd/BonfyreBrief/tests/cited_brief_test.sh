#!/bin/sh
# bonfyre-brief cited over a throwaway native CMS, hand-written ForeignTwin
# receipts and a throwaway WorkGraph. Nothing here reads the network or a
# live store unless BONFYRE_TEST_LIVE_CMS names one, and then read-only.
set -eu
brief=${1:?bonfyre-brief binary required}
case "$brief" in /*) ;; *) brief="$PWD/$brief" ;; esac
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root=$(CDPATH= cd -- "$here/../.." && pwd)
cms=${BONFYRE_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
[ -x "$cms" ] || cms="$HOME/.bonfyre/bin/bonfyre-cms"
queue=${BONFYRE_QUEUE_BINARY:-"$root/cmd/BonfyreQueue/bonfyre-queue"}
[ -x "$queue" ] || queue="$HOME/.bonfyre/bin/bonfyre-queue"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-cited-brief.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
fail() { echo "cited brief: $*" >&2; exit 1; }
pass=0; ok() { pass=$((pass + 1)); }

mkdir -p "$tmp/schemas" "$tmp/bodies" "$tmp/receipts"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$root/cmd/BonfyreCMS/content-types/source_collection.json" "$tmp/schemas/"
db="$tmp/cms.db"
"$cms" schema migrate --db "$db" --schemas "$tmp/schemas" >/dev/null 2>&1

# Three documents: one in scope (Markdown), one in scope (HTML), one in the
# same collection but another family, and one in another namespace.
cat > "$tmp/bodies/kiln.body" <<'EOF'
# Kilnware Ledger

[![badge](https://img.example/migrate-badge.svg)](https://ci.example/migrate)

Kilnware Ledger exports every glaze batch as CSV so a migration can replay it.

## Export

Run `kiln export --all` to write the ledger and firing schedule before any migration.

Released under the GNU AGPL; adopting code needs a rights review.
EOF
cat > "$tmp/bodies/portal.body" <<'EOF'
<!doctype html>
<html><head><title>Portal migrate head text should never appear</title></head>
<body><nav>Migrate navigation chrome that is not the document</nav>
<main>
<h1>Portal operator guide</h1>
<p>The portal export writes &lt;batch&gt; files that a migration tool can read back.</p>
<script>var migrate = "the export script body must not be cited";</script>
</main></body></html>
EOF
cat > "$tmp/bodies/unrelated.body" <<'EOF'
# Unrelated payroll notes
Payroll export and migration checklist that belongs to a different brief entirely.
EOF
cp "$tmp/bodies/unrelated.body" "$tmp/bodies/othernamespace.body"
sha() { shasum -a 256 "$1" | cut -d' ' -f1; }
receipt() { # twin body status policy
    printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"%s","source_url":"https://docs.example/%s","observed_at":"2026-10-30T01:00:00Z","policy_decision":"%s","http_status":%s,"content_sha256":"%s","body_path":"%s","http_evidence":{"content_type":"%s"}}\n' \
        "$1" "$1" "$4" "$3" "$(sha "$tmp/bodies/$2.body")" "$tmp/bodies/$2.body" "$5" > "$tmp/receipts/$1-20261030T010000Z-0000000000.json"
}
receipt kiln-doc kiln 200 allow text/plain
receipt portal-doc portal 200 allow text/html
receipt unrelated-doc unrelated 200 allow text/plain
receipt other-ns-doc othernamespace 200 allow text/plain
# A refused observation of the same twin must never be the one used.
printf '{"twin_id":"kiln-doc","observed_at":"2026-10-30T09:00:00Z","policy_decision":"deny","http_status":200,"content_sha256":"%s","body_path":"%s"}\n' \
    "$(sha "$tmp/bodies/kiln.body")" "$tmp/bodies/kiln.body" > "$tmp/receipts/kiln-doc-20261030T090000Z-1111111111.json"

row() { # ns ref family twin body
    "$cms" entry create internet_source --ns "$1" --db "$db" --schemas "$tmp/schemas" \
        "{\"status\":\"draft\",\"says\":\"fixture\",\"family\":\"$3\",\"source\":\"$2\",\"label\":\"$2 doc\",\"url\":\"https://docs.example/$4\",\"twin\":\"$4\",\"state\":\"observed\",\"note\":\"retained sha256 $(sha "$tmp/bodies/$5.body") fixture\"}" >/dev/null 2>&1
}
row fx doc:kiln software-documentation:pottery kiln-doc kiln
row fx doc:portal software-documentation:pottery portal-doc portal
row fx doc:unrelated software-documentation:payroll unrelated-doc unrelated
row other doc:other-ns software-documentation:pottery other-ns-doc othernamespace
"$cms" entry create source_collection --ns fx --db "$db" --schemas "$tmp/schemas" \
    '{"status":"draft","collection_id":"fx.pottery","subject_address":"fx:pottery","refresh_policy":"manual","material_kind":["markdown"],"source_selector":{"source_refs":["doc:kiln","doc:portal","doc:unrelated","doc:other-ns"]}}' >/dev/null 2>&1
[ "$(sqlite3 "$db" "select count(*) from internet_source")" = 4 ] || fail "fixture CMS rows not created"

# An unfamiliar brief type the shipped catalogue has never seen.
cat > "$tmp/types.json" <<'EOF'
{"schema":"bonfyre.migration-brief-types.v1","program_template":"research-to-migration","estimate_label":"Planning estimate, not a quote.",
 "cannot_infer":["a cited passage -> the customer's system matches it"],
 "types":[{"id":"pottery-ledger","title":"Pottery ledger move","recipient":"a studio moving its glaze ledger",
  "collections":["fx/fx.pottery","fx/fx.missing"],"families":["software-documentation:pottery"],
  "selectors":["migrat","export"],
  "criteria":[{"id":"inv","text":"Every batch is listed.","needs":["batch_count"]},{"id":"bk","text":"A backup restores.","needs":["backup_location","restore_minutes"]}],
  "estimate":{"low":100,"high":250,"currency":"USD","unit":"ledger","scale_fact":"ledger_count","basis":"reviewer hours"},
  "tasks":[{"operator":"queue.migration.inventory-intake","title":"Read the inventory"}]}]}
EOF
run() { "$brief" cited --type pottery-ledger --types "$tmp/types.json" --cms "$db" --receipts "$tmp/receipts" --generated-at 2026-10-30T12:00:00Z "$@"; }

run --subject user:7 --scope self --out "$tmp/a" > "$tmp/a.out" || fail "brief not written"
j="$tmp/a/brief.json"
[ "$(jq -r '[.sources[].ref]|join(",")' "$j")" = "doc:kiln,doc:portal" ] || fail "wrong sources: $(jq -c '[.sources[].ref]' "$j")"; ok
[ "$(jq '.excluded_outside_type' "$j")" = 1 ] || fail "out-of-family source not counted as excluded"; ok
if grep -q -i payroll "$tmp/a/brief.json" "$tmp/a/brief.md"; then fail "unrelated source leaked into the brief"; fi; ok
jq -e '.exceptions|map(.kind)|index("source-row-missing")' "$j" >/dev/null || fail "other-namespace ref was not refused"; ok
jq -e '.exceptions|map(.kind)|index("collection-missing")' "$j" >/dev/null || fail "missing collection not recorded"; ok
jq -e '.exceptions|map(.kind)|index("license-review")' "$j" >/dev/null || fail "AGPL mention not raised for review"; ok
[ "$(jq -r '.sources[0].receipt' "$j")" = "kiln-doc-20261030T010000Z-0000000000.json" ] || fail "a refused receipt was used"; ok

# Every excerpt is the exact bytes at its recorded offset in the verified body.
n=0
for i in 0 1; do
    body=$(jq -r ".sources[$i].ref" "$j" | sed 's/doc://'); [ "$body" = other-ns ] && continue
    jq -c ".sources[$i].citations[]" "$j" > "$tmp/c.jsonl"
    while IFS= read -r c; do
        off=$(printf '%s' "$c" | jq '.byte_offset'); len=$(printf '%s' "$c" | jq '.byte_length')
        want=$(printf '%s' "$c" | jq -j '.excerpt')
        got=$(dd if="$tmp/bodies/$body.body" bs=1 skip="$off" count="$len" 2>/dev/null)
        [ "$got" = "$want" ] || fail "excerpt not exact at $body:$off"
        n=$((n + 1))
    done < "$tmp/c.jsonl"
done
[ "$n" -ge 3 ] || fail "too few citations ($n)"; ok
for chrome in 'head text' 'navigation chrome' 'script body' 'migrate-badge' ; do
    if jq -r '.sources[].citations[].excerpt' "$j" | grep -q -F "$chrome"; then fail "non-document text cited: $chrome"; fi
done; ok
grep -q -F 'writes <batch> files' "$tmp/a/brief.md" || fail "recipient page did not decode HTML entities"; ok
[ "$(jq -r '[.criteria[].status]|unique|join(",")' "$j")" = unassessed ] || fail "criteria assessed without inventory"; ok
[ "$(jq '[.criteria[].verified]|any' "$j")" = false ] || fail "a criterion claimed verification"; ok
[ "$(jq '.estimate.is_estimate' "$j")" = true ] && [ "$(jq '.estimate.total_low' "$j")" = null ] || fail "estimate not labeled or scaled without units"; ok
[ "$(jq '[.proposed_tasks[].admitted]|any' "$j")" = false ] || fail "a task claimed admission"; ok
[ "$(jq -c '.effects' "$tmp/a/receipt.json")" = '{"network":false,"publication":false,"spend":false,"work_admitted":false}' ] || fail "receipt effects"; ok
[ "$(jq -r '.brief_json_sha256' "$tmp/a/receipt.json")" = "$(sha "$j")" ] && [ "$(jq -r '.brief_md_sha256' "$tmp/a/receipt.json")" = "$(sha "$tmp/a/brief.md")" ] || fail "receipt digests do not match outputs"; ok

# Returning to the same inputs reproduces the same brief, byte for byte.
run --subject user:7 --scope self --out "$tmp/a2" >/dev/null
cmp -s "$j" "$tmp/a2/brief.json" && cmp -s "$tmp/a/brief.md" "$tmp/a2/brief.md" || fail "same inputs gave a different brief"; ok

# Inventory belongs to one subject and scope, and names its grant.
printf '{"subject":"user:8","scope":"self","grant_ref":"grant-1","facts":{"batch_count":3}}' > "$tmp/inv-other.json"
set +e; run --subject user:7 --scope self --out "$tmp/x" --inventory "$tmp/inv-other.json" >/dev/null 2>&1; rc=$?; set -e
[ "$rc" = 3 ] && [ ! -e "$tmp/x/brief.json" ] || fail "another account's inventory was accepted ($rc)"; ok
printf '{"subject":"user:7","scope":"self","facts":{"batch_count":3}}' > "$tmp/inv-nogrant.json"
set +e; run --subject user:7 --scope self --out "$tmp/x" --inventory "$tmp/inv-nogrant.json" >/dev/null 2>&1; rc=$?; set -e
[ "$rc" = 3 ] || fail "inventory without a grant was accepted"; ok
printf '{"subject":"user:7","scope":"institution","grant_ref":"g","facts":{}}' > "$tmp/inv-scope.json"
set +e; run --subject user:7 --scope self --out "$tmp/x" --inventory "$tmp/inv-scope.json" >/dev/null 2>&1; rc=$?; set -e
[ "$rc" = 3 ] || fail "inventory for another scope was accepted"; ok
printf '{"subject":"user:7","scope":"self","grant_ref":"connection-grant-42","facts":{"batch_count":118,"backup_location":"s3://studio/backups","ledger_count":2}}' > "$tmp/inv.json"
run --subject user:7 --scope self --out "$tmp/b" --inventory "$tmp/inv.json" >/dev/null
jb="$tmp/b/brief.json"
[ "$(jq -r '[.criteria[].status]|join(",")' "$jb")" = "stated-by-customer-inventory,needs-inventory" ] || fail "inventory statuses: $(jq -c '[.criteria[].status]' "$jb")"; ok
[ "$(jq -r '.criteria[1].missing|join(",")' "$jb")" = restore_minutes ] || fail "missing fact not named"; ok
[ "$(jq '.estimate.total_low,.estimate.total_high' "$jb" | tr '\n' ' ')" = "200 500 " ] || fail "estimate not scaled by stated units"; ok
[ "$(jq -r '.inventory_grant' "$jb")" = connection-grant-42 ] || fail "grant not carried"; ok
[ "$(jq -r '.brief_id' "$jb")" != "$(jq -r '.brief_id' "$j")" ] || fail "inventory did not change brief identity"; ok
# The same inputs under another account are another brief.
run --subject user:8 --scope self --out "$tmp/c" >/dev/null
[ "$(jq -r '.brief_id' "$tmp/c/brief.json")" != "$(jq -r '.brief_id' "$j")" ] || fail "brief identity not account-bound"; ok
set +e; run --subject 'user:7;drop' --scope self --out "$tmp/x" >/dev/null 2>&1; rc=$?; set -e
[ "$rc" = 3 ] || fail "malformed subject accepted"; ok

# A reviewer holding an old revision is told the sources changed.
digest=$(jq -r '.sources_digest' "$j")
run --subject user:7 --scope self --out "$tmp/d" --expect-sources-digest "$digest" >/dev/null || fail "matching revision refused"; ok
printf '\nA later line about migration exports.\n' >> "$tmp/bodies/kiln.body"
set +e; run --subject user:7 --scope self --out "$tmp/e" --expect-sources-digest "$digest" > "$tmp/stale.out" 2>/dev/null; rc=$?; set -e
# The changed body no longer matches its receipt, so it drops out and the digest moves.
[ "$rc" = 4 ] && grep -q '"stale":true' "$tmp/stale.out" || fail "stale revision not refused ($rc)"; ok
run --subject user:7 --scope self --out "$tmp/f" >/dev/null
jq -e '.exceptions[]|select(.kind=="body-changed" and .ref=="doc:kiln")' "$tmp/f/brief.json" >/dev/null || fail "changed body not reported"; ok
[ "$(jq -r '[.sources[].ref]|join(",")' "$tmp/f/brief.json")" = doc:portal ] || fail "changed body still cited"; ok
# With no usable source nothing is written.
printf 'x' >> "$tmp/bodies/portal.body"
set +e; run --subject user:7 --scope self --out "$tmp/g" >/dev/null 2>&1; rc=$?; set -e
[ "$rc" = 5 ] && [ ! -e "$tmp/g/brief.json" ] || fail "brief written without a usable source ($rc)"; ok

# The brief reaches the account's Work and Outputs through the existing
# WorkGraph owner and account-work projection, and only that account's.
wg="$tmp/workgraph.db"; mission="migration-brief-$(jq -r '.brief_id' "$jb")"
"$queue" enqueue migration.brief "file://$tmp/b/receipt.json" --mission "$mission" --db "$wg" >/dev/null
token=$("$queue" claim "$mission" job brief-composer --db "$wg" | jq -r '.claim_token')
"$queue" checkpoint-save "$mission" job brief-composer "$token" --expected-revision 0 --artifact-uri "file://$tmp/b/brief.md" --db "$wg" >/dev/null
"$queue" ack "$mission" job brief-composer "$token" --checkpoint-revision 1 --output-uri "file://$tmp/b/brief.md" --db "$wg" >/dev/null
"$queue" account-work index --db "$tmp/work7.db" --subject user:7 --scope self --owner user:7 --source "workgraph=$wg"
"$queue" account-work outputs --db "$tmp/work7.db" --subject user:7 --scope self > "$tmp/out7.mp"
LC_ALL=C grep -a -q -F "account-workgraph-$mission" "$tmp/out7.mp" || fail "brief missing from the account's Outputs"; ok
[ "$(sqlite3 "$tmp/work7.db" "select output_ref from account_work_step where work_id='account-workgraph-$mission'")" = "file://$tmp/b/brief.md" ] || fail "output does not name the brief"; ok
"$queue" account-work outputs --db "$tmp/work7.db" --subject user:8 --scope self > "$tmp/out8.mp" 2>/dev/null || true
if LC_ALL=C grep -a -q -F "$mission" "$tmp/out8.mp"; then fail "another account read this account's Outputs"; fi; ok

# The shipped catalogue's CAD, civic and document types read their draft
# collections by the exact ids the catalogue names, and a LICENSE file that
# spells out "GNU Lesser General Public License" is raised for review while an
# MPL, Apache or MIT source is not.
cdb="$tmp/catalogue.db"
"$cms" schema migrate --db "$cdb" --schemas "$tmp/schemas" >/dev/null 2>&1
fixture() { # twin ref family collection-ns text...
    twin=$1 ref=$2 fam=$3 ns=$4; shift 4
    printf '%s\n' "$@" > "$tmp/bodies/$twin.body"
    receipt "$twin" "$twin" 200 allow text/plain
    "$cms" entry create internet_source --ns "$ns" --db "$cdb" --schemas "$tmp/schemas" \
        "{\"status\":\"draft\",\"says\":\"fixture\",\"family\":\"software-documentation:$fam\",\"source\":\"$ref\",\"label\":\"$ref\",\"url\":\"https://docs.example/$twin\",\"twin\":\"$twin\",\"state\":\"observed\",\"note\":\"retained sha256 $(sha "$tmp/bodies/$twin.body") fixture\"}" >/dev/null 2>&1
}
d=aurekai-migration-briefs-draft
fixture fc-readme 'github-doc:FreeCAD/FreeCAD@main:README.md' cad $d '# FreeCAD' '' 'FreeCAD imports and exports IFC, STEP and DXF so a building model can move between tools.'
fixture fc-license 'github-doc:FreeCAD/FreeCAD@main:LICENSE' cad $d '                  GNU LESSER GENERAL PUBLIC LICENSE' '                       Version 2.1, February 1999' '' 'Everyone is permitted to copy and distribute verbatim copies of this license document.'
fixture ids-manual 'github-doc:buildingSMART/IDS@development:Documentation/UserManual/README.md' cad $d '# Information Delivery Specification' '' 'An IDS file lists specifications that an IFC model is checked against before handover.'
fixture ofn-readme 'github-doc:OpenFn/lightning@main:README.md' civic $d '# Lightning' '' 'Lightning runs workflow integration between government data systems through an API.'
fixture ofn-license 'github-doc:OpenFn/lightning@main:LICENSE' civic $d '                    GNU GENERAL PUBLIC LICENSE' '' 'Everyone is permitted to copy and distribute verbatim copies of this license document.'
fixture crvs-readme 'github-doc:opencrvs/opencrvs-core@develop:README.md' civic $d '# OpenCRVS' '' 'OpenCRVS is a civil registration system with an API for birth and death registration data.' '' 'This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.'
fixture tika-readme 'github-doc:apache/tika@main:README.md' document $d '# Apache Tika' '' 'Apache Tika detects and extracts metadata and text from PDF and office document formats.' '' 'Licensed under the Apache License, Version 2.0.'
fixture presidio-readme 'github-doc:microsoft/presidio@main:README.MD' document $d '# Presidio' '' 'Presidio can detect PII in text and images, then anonymize or redact it under rules a person sets.' '' 'Released under the MIT License.'
fixture restic-readme 'github-doc:restic/restic@master:README.md' verify $d '# restic' '' 'restic makes an encrypted backup and can restore any snapshot to verify the archive.'
coll() { # id refs-json
    "$cms" entry create source_collection --ns $d --db "$cdb" --schemas "$tmp/schemas" \
        "{\"status\":\"draft\",\"collection_id\":\"$1\",\"subject_address\":\"fx:$1\",\"refresh_policy\":\"manual\",\"material_kind\":[\"markdown\"],\"source_selector\":{\"source_refs\":$2}}" >/dev/null 2>&1
}
coll aurekai.migration.brief-input.cad '["github-doc:FreeCAD/FreeCAD@main:README.md","github-doc:FreeCAD/FreeCAD@main:LICENSE","github-doc:buildingSMART/IDS@development:Documentation/UserManual/README.md"]'
coll aurekai.migration.brief-input.civic '["github-doc:OpenFn/lightning@main:README.md","github-doc:OpenFn/lightning@main:LICENSE","github-doc:opencrvs/opencrvs-core@develop:README.md"]'
coll aurekai.migration.brief-input.document '["github-doc:apache/tika@main:README.md","github-doc:microsoft/presidio@main:README.MD"]'
coll aurekai.migration.brief-input.rollback '["github-doc:restic/restic@master:README.md"]'
shipped() { "$brief" cited --type "$1" --types "$here/data/migration-brief-types.json" --cms "$cdb" --receipts "$tmp/receipts" \
    --generated-at 2026-10-30T12:00:00Z --subject user:7 --scope self --out "$tmp/cat/$1" >/dev/null || fail "shipped type $1 not written"; }
refs() { jq -r '[.sources[].ref]|sort|join(",")' "$tmp/cat/$1/brief.json"; }
flags() { jq -r '[.exceptions[]|select(.kind=="license-review").ref]|sort|join(",")' "$tmp/cat/$1/brief.json"; }
shipped cad-ifc
[ "$(refs cad-ifc)" = "github-doc:FreeCAD/FreeCAD@main:LICENSE,github-doc:FreeCAD/FreeCAD@main:README.md,github-doc:buildingSMART/IDS@development:Documentation/UserManual/README.md" ] || fail "cad-ifc sources: $(refs cad-ifc)"; ok
[ "$(flags cad-ifc)" = "github-doc:FreeCAD/FreeCAD@main:LICENSE" ] || fail "cad-ifc licence flags: $(flags cad-ifc)"; ok
shipped civic-legacy-modernization
[ "$(refs civic-legacy-modernization)" = "github-doc:OpenFn/lightning@main:LICENSE,github-doc:OpenFn/lightning@main:README.md,github-doc:opencrvs/opencrvs-core@develop:README.md" ] || fail "civic sources: $(refs civic-legacy-modernization)"; ok
[ "$(flags civic-legacy-modernization)" = "github-doc:OpenFn/lightning@main:LICENSE" ] || fail "civic licence flags (MPL must not be flagged): $(flags civic-legacy-modernization)"; ok
shipped document-archive-extraction
[ "$(refs document-archive-extraction)" = "github-doc:apache/tika@main:README.md,github-doc:microsoft/presidio@main:README.MD,github-doc:restic/restic@master:README.md" ] || fail "document sources: $(refs document-archive-extraction)"; ok
[ -z "$(flags document-archive-extraction)" ] || fail "Apache/MIT sources flagged: $(flags document-archive-extraction)"; ok
jq -e '.criteria[]|select(.id=="personal-data" and .status=="unassessed" and .verified==false)' "$tmp/cat/document-archive-extraction/brief.json" >/dev/null || fail "personal-data criterion assessed without inventory"; ok
n=0
for t in cad-ifc civic-legacy-modernization document-archive-extraction; do
    jq -r '.sources[]|.receipt as $r|.citations[]|[($r|sub("-20261030T010000Z-0000000000.json$";"")),.byte_offset,.byte_length,(.excerpt|@base64)]|@tsv' "$tmp/cat/$t/brief.json" > "$tmp/cc.tsv"
    while IFS="$(printf '\t')" read -r twin off len want; do
        got=$(dd if="$tmp/bodies/$twin.body" bs=1 skip="$off" count="$len" 2>/dev/null | base64 | tr -d '\n')
        [ "$got" = "$want" ] || fail "$t excerpt not exact at $twin:$off"
        n=$((n + 1))
    done < "$tmp/cc.tsv"
done
[ "$n" -ge 9 ] || fail "too few shipped-type citations ($n)"; ok

# Transformation evidence: DatabaseBridge receipts cited by the SQL/ERP brief.
# The bridge builds them here from its own fixtures; nothing touches a server.
bridge=${BONFYRE_BRIDGE_BINARY:-"$root/cmd/BonfyreBridge/bonfyre-bridge"}
if [ -x "$bridge" ]; then
    bfx="$root/cmd/BonfyreBridge/tests/fixtures/transform"
    ev="$tmp/ev"; mkdir -p "$ev"
    sqlite3 "$ev/shop.db" < "$bfx/legacy_shop.sql"; sqlite3 "$ev/odoo.db" < "$bfx/odoo_partner.sql"
    "$bridge" rules --format pgloader-casting --receipt "$bfx/pgloader-docs-ref-sqlite.receipt.json" --body "$bfx/pgloader-docs-ref-sqlite.rst.body" --out "$ev/pg.rules.json" >/dev/null
    "$bridge" rules --format frappe-type-map --file "$bfx/frappe-mariadb-type-map.txt" --const-file "$bfx/frappe-mariadb-type-map.txt" --out "$ev/fr.rules.json" >/dev/null
    "$bridge" schema-map --from "sqlite:$ev/shop.db" --plan "$bfx/legacy_shop.plan.json" --rules "$ev/pg.rules.json" --out "$ev/shop.map.json" >/dev/null
    "$bridge" schema-map --from "sqlite:$ev/odoo.db" --plan "$bfx/odoo_partner.plan.json" --rules "$ev/fr.rules.json" \
        --frappe-schema "$bfx/erpnext-customer.schema-ir.json" --out "$ev/odoo.map.json" --emit-sql "$ev/odoo.sql" >/dev/null
    sqlite3 "$ev/erp.db" < "$ev/odoo.sql"
    "$bridge" map-verify --from "sqlite:$ev/odoo.db" --to "sqlite:$ev/erp.db" --mapping "$ev/odoo.map.json" --out "$ev/odoo.diff.json" >/dev/null || fail "erp rehearsal did not match"
    # The pgloader page is a cited source too, and its receipt is in the store.
    cp "$bfx/pgloader-docs-ref-sqlite.rst.body" "$tmp/bodies/pgl-sqlite.body"
    receipt pgl-sqlite pgl-sqlite 200 allow text/plain
    cp "$bfx/pgloader-docs-ref-sqlite.receipt.json" "$tmp/receipts/"
    "$cms" entry create internet_source --ns $d --db "$cdb" --schemas "$tmp/schemas" \
        "{\"status\":\"draft\",\"says\":\"fixture\",\"family\":\"software-documentation:sql\",\"source\":\"github-doc:dimitri/pgloader@master:docs/ref/sqlite.rst\",\"label\":\"pgloader SQLite reference\",\"url\":\"https://docs.example/pgl\",\"twin\":\"pgl-sqlite\",\"state\":\"observed\",\"note\":\"retained sha256 $(sha "$tmp/bodies/pgl-sqlite.body") fixture\"}" >/dev/null 2>&1
    coll aurekai.migration.brief-input.sql '["github-doc:dimitri/pgloader@master:docs/ref/sqlite.rst"]'
    sqlb() { "$brief" cited --type sql-erp-migration --types "$here/data/migration-brief-types.json" --cms "$cdb" --receipts "$tmp/receipts" \
        --generated-at 2026-10-30T12:00:00Z --subject user:7 --scope self "$@"; }
    sqlb --out "$tmp/sql0" >/dev/null || fail "sql brief without evidence"
    sqlb --out "$tmp/sql1" --evidence "$ev/shop.map.json" --evidence "$ev/odoo.map.json" --evidence "$ev/odoo.diff.json" >/dev/null || fail "sql brief with evidence"
    jq -e 'has("transformation_evidence")|not' "$tmp/sql0/brief.json" >/dev/null || fail "evidence section without evidence"; ok
    [ "$(jq '.transformation_evidence|length' "$tmp/sql1/brief.json")" = 3 ] || fail "evidence not carried"; ok
    [ "$(jq -r '.brief_id' "$tmp/sql0/brief.json")" != "$(jq -r '.brief_id' "$tmp/sql1/brief.json")" ] || fail "evidence did not change the brief identity"; ok
    dd=$(jq -r '.diff_digest' "$ev/odoo.diff.json")
    [ "$(jq -r '.criteria[]|select(.id=="diff").rehearsal_evidence|join(",")' "$tmp/sql1/brief.json")" = "$dd" ] || fail "diff criterion does not cite the diff receipt"; ok
    [ "$(jq '.criteria[]|select(.id=="objects").rehearsal_evidence|length' "$tmp/sql1/brief.json")" = 2 ] || fail "objects criterion does not cite both mappings"; ok
    jq -e '.criteria[]|select(.id=="diff" and .verified==false and .status=="unassessed")' "$tmp/sql1/brief.json" >/dev/null || fail "a fixture rehearsal upgraded a criterion"; ok
    jq -e '[.transformation_evidence[]|select(.kind=="schema-map")|.column_mappings[]|select(.source=="product.unit_price" and .target_type=="numeric(10,2)" and .rule.excerpt=="type decimal to numeric     using float-to-string")]|length==1' "$tmp/sql1/brief.json" >/dev/null || fail "column mapping not cited to its rule"; ok
    jq -e '.sources[]|select(.ref=="github-doc:dimitri/pgloader@master:docs/ref/sqlite.rst")' "$tmp/sql1/brief.json" >/dev/null || fail "pgloader page not a cited source"; ok
    grep -q '^## What a rehearsal measured' "$tmp/sql1/brief.md" || fail "rehearsal section missing"; ok
    grep -q '"type decimal to numeric     using float-to-string" \[line 208\] typed product.unit_price, sales_order_line.unit_price' "$tmp/sql1/brief.md" || fail "rule line in brief.md"; ok
    grep -q 'res_partner to tabCustomer: 4 rows here, 4 there; 5 of 5 columns have matching checksums' "$tmp/sql1/brief.md" || fail "diff line in brief.md"; ok
    grep -q 'not on your system' "$tmp/sql1/brief.md" || fail "rehearsal scope not stated"; ok
    jq -e '.exceptions[]|select(.kind=="evidence-rules-vendored")' "$tmp/sql1/brief.json" >/dev/null || fail "vendored rule source not named"; ok
    jq -e '[.exceptions[]|select(.kind=="evidence-rules-unreceipted")]|length==0' "$tmp/sql1/brief.json" >/dev/null || fail "receipted rules called unreceipted"; ok
    jq -e '.evidence_digest|length==64' "$tmp/sql1/receipt.json" >/dev/null || fail "receipt lacks evidence digest"; ok
    # Same inputs, same bytes; order of --evidence does not matter.
    sqlb --out "$tmp/sql2" --evidence "$ev/odoo.diff.json" --evidence "$ev/odoo.map.json" --evidence "$ev/shop.map.json" >/dev/null
    [ "$(jq -r .brief_id "$tmp/sql2/brief.json")" = "$(jq -r .brief_id "$tmp/sql1/brief.json")" ] || fail "evidence order changed the identity"; ok
    sqlb --out "$tmp/sql3" --evidence "$ev/shop.map.json" --evidence "$ev/odoo.map.json" --evidence "$ev/odoo.diff.json" >/dev/null
    cmp -s "$tmp/sql1/brief.md" "$tmp/sql3/brief.md" || fail "evidence brief not reproducible"; ok
    # A diff without the mapping it measured, and a hand-edited mapping, are refused.
    set +e
    sqlb --out "$tmp/sqlx" --evidence "$ev/odoo.diff.json" >/dev/null 2>&1; rc1=$?
    sed 's/"numeric(10,2)"/"numeric(12,2)"/' "$ev/shop.map.json" > "$ev/forged.json"
    sqlb --out "$tmp/sqlx" --evidence "$ev/forged.json" >/dev/null 2>&1; rc2=$?
    set -e
    [ "$rc1" = 3 ] || fail "diff without its mapping accepted ($rc1)"; ok
    [ "$rc2" = 3 ] || fail "forged mapping accepted ($rc2)"; ok
    # Without the rule source's receipt, the mapping is shown but named unreceipted.
    mkdir -p "$tmp/rx"; cp "$tmp/receipts/"pgl-sqlite-* "$tmp/rx/"
    "$brief" cited --type sql-erp-migration --types "$here/data/migration-brief-types.json" --cms "$cdb" --receipts "$tmp/rx" \
        --generated-at 2026-10-30T12:00:00Z --subject user:7 --scope self --out "$tmp/sql4" --evidence "$ev/shop.map.json" >/dev/null
    jq -e '.exceptions[]|select(.kind=="evidence-rules-unreceipted")' "$tmp/sql4/brief.json" >/dev/null || fail "unreceipted rule source not named"; ok
    [ -n "${BONFYRE_BRIEF_KEEP:-}" ] && cp -R "$tmp/sql1/." "$BONFYRE_BRIEF_KEEP/" || true
else
    echo "cited brief: transformation evidence SKIPPED (build cmd/BonfyreBridge or set BONFYRE_BRIDGE_BINARY)"
fi

# Optional read-only pass over a real CMS and receipt store.
if [ -n "${BONFYRE_TEST_LIVE_CMS:-}" ]; then
    before=$(sha "$BONFYRE_TEST_LIVE_CMS")
    for t in hosting-account-acquisition php-rescue sql-erp-migration cad-ifc civic-legacy-modernization verification-rollback document-archive-extraction; do
        "$brief" cited --type "$t" --types "$here/data/migration-brief-types.json" --cms "$BONFYRE_TEST_LIVE_CMS" \
            --receipts "${BONFYRE_TEST_LIVE_RECEIPTS:-$HOME/.bonfyre/foreign/receipts}" --subject user:19 --scope self --out "$tmp/live/$t" >/dev/null || fail "live $t"
        [ "$(jq '[.sources[].citations|length]|add' "$tmp/live/$t/brief.json")" -gt 0 ] || fail "live $t cited nothing"
    done
    ok
fi
echo "cited brief: $pass checks passed"
