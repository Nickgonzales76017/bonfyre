#!/bin/sh
# Hosting intake to verified brief, on fixtures only:
#   site inbox answers -> bonfyre-brief inventory (+ exported cPanel, InterWorx
#   or Plesk listings) -> bonfyre-brief cited --inventory -> a brief whose
#   sources digest replays to the same bytes.
# The export fixtures follow the sample outputs in the panels' public API
# documentation (tests/fixtures/hosting/sources.json names each page and its
# ForeignTwin receipt). Nothing here connects to a panel, server or customer
# system; the adapters run once under a sandbox that denies the network.
# With BONFYRE_TEST_WEB_BINARY the intake comes from the practice site's own
# form and inbox in a throwaway site home instead of the inbox fixture.
set -eu
brief=${1:?bonfyre-brief binary required}
case "$brief" in /*) ;; *) brief="$PWD/$brief" ;; esac
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root=$(CDPATH= cd -- "$here/../.." && pwd)
fx="$here/tests/fixtures/hosting"
cms=${BONFYRE_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
[ -x "$cms" ] || cms="$HOME/.bonfyre/bin/bonfyre-cms"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-hosting-inventory.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
fail() { echo "hosting inventory: $*" >&2; exit 1; }
pass=0; ok() { pass=$((pass + 1)); }
sha() { shasum -a 256 "$1" | cut -d' ' -f1; }
inv() { "$brief" inventory --subject user:7 --scope self "$@"; }
exports() { p=$1; shift; for f in "$@"; do printf -- '--export %s:%s ' "$p" "$f"; done; }
refused() { # expected-rc args...
    want=$1; shift
    set +e; inv --out "$tmp/refused.json" "$@" >/dev/null 2>&1; rc=$?; set -e
    [ "$rc" = "$want" ] || fail "expected exit $want, got $rc for: $*"
    [ ! -e "$tmp/refused.json" ] || fail "a refused run wrote an inventory: $*"; ok
}

# ---- cPanel: WHM listaccts + API 2 addon domains, mailboxes, databases ----
# shellcheck disable=SC2046
inv --out "$tmp/cp.json" --intake "$fx/intake/site-inbox.json" --reference lead-7f3a1c --grant fixture-export-grant-1 \
    $(exports cpanel "$fx"/cpanel/*.json) >/dev/null || fail "cPanel inventory not written"
j="$tmp/cp.json"
[ "$(jq -c '[.facts.site_count,.facts.mailbox_count,.facts.database_count]' "$j")" = "[3,4,3]" ] || fail "cPanel counts: $(jq -c .facts "$j")"; ok
[ "$(jq -r '.facts.domain_list|join(",")' "$j")" = "alpha-bakery.example,beta-florist.example,gamma-catering.example" ] || fail "cPanel domains"; ok
# The "Main Account" row is not a mailbox; Info@Alpha-Bakery and info@alpha-bakery are one.
jq -e '.inputs[]|select(.listing=="cpanel-api2-listpops" and .file=="api2-listpops-alphab.json" and .items==3)' "$j" >/dev/null || fail "main account row counted as a mailbox"; ok
# maxpop/maxsql are limits, never counts: beta's maxsql "5" and maxpop "25" do not appear.
[ "$(jq '[.facts[]|numbers]|map(select(.==25 or .==5))|length' "$j")" = 0 ] || fail "a panel limit was read as a count"; ok
[ "$(jq -c '.disagreements' "$j")" = '[{"fact":"mailbox_count","intake":"5","export":"4"}]' ] || fail "cPanel disagreement: $(jq -c .disagreements "$j")"; ok
[ "$(jq -r '.provenance.mailbox_count+" "+.provenance.dns_provider+" "+.grant_kind' "$j")" = "export:cpanel intake export-authorization" ] || fail "cPanel provenance"; ok
[ "$(jq -c '.effects' "$j")" = '{"network":false,"panel_connection":false,"cms_write":false,"publication":false}' ] || fail "effects"; ok
# Same inputs, same bytes.
# shellcheck disable=SC2046
inv --out "$tmp/cp2.json" --intake "$fx/intake/site-inbox.json" --reference lead-7f3a1c --grant fixture-export-grant-1 $(exports cpanel "$fx"/cpanel/*.json) >/dev/null
cmp -s "$j" "$tmp/cp2.json" || fail "inventory bytes are not reproducible"; ok

# ---- InterWorx: listAccounts, listAllDomains, listEmailBoxes, listMysqlDatabases ----
# shellcheck disable=SC2046
inv --out "$tmp/iw.json" --intake "$fx/intake/site-inbox.json" --reference lead-11b9e0 --grant fixture-export-grant-2 \
    $(exports interworx "$fx"/interworx/*.json) >/dev/null || fail "InterWorx inventory not written"
j="$tmp/iw.json"
[ "$(jq -c '[.facts.site_count,.facts.subdomain_count,.facts.mailbox_count,.facts.database_count]' "$j")" = "[3,1,4,3]" ] || fail "InterWorx counts: $(jq -c .facts "$j")"; ok
# master + secondary are sites; the pointer is a domain; the subdomain is counted apart.
[ "$(jq -r '.facts.domain_list|join(",")' "$j")" = "delta-architects.example,echo-dental.example,echo-orthodontics.example,echodental.example" ] || fail "InterWorx domains"; ok
! grep -q 'FIXTUREONLY\|crypt_password\|\$6\$' "$j" || fail "a password hash reached the inventory"; ok
[ "$(jq '.secret_fields_dropped' "$j")" = 4 ] || fail "secret fields not counted"; ok
# "about four" is not a count: it stays out of facts, is named, and disagrees with the listing.
[ "$(jq -c '.unparsed_answers' "$j")" = '["site_count"]' ] && jq -e '.disagreements[]|select(.fact=="site_count" and .intake=="about four" and .export=="3")' "$j" >/dev/null || fail "unparsed answer handling"; ok
# The CLI's bare payload array (-o json) reads like the API reply.
jq '.payload' "$fx/interworx/siteworx-mysql-db-listMysqlDatabases-echodent.json" > "$tmp/bare.json"
inv --out "$tmp/bare-inv.json" --grant g $(exports interworx "$tmp/bare.json") >/dev/null
[ "$(jq '.facts.database_count' "$tmp/bare-inv.json")" = 2 ] || fail "bare CLI payload not read"; ok

# ---- Plesk: XML API site get ----
inv --out "$tmp/pl.json" --intake "$fx/intake/site-inbox.json" --reference lead-0000aa --grant fixture-export-grant-3 \
    --export "plesk:$fx/plesk/xml-site-get.xml" >/dev/null || fail "Plesk inventory not written"
j="$tmp/pl.json"
[ "$(jq -c '[.facts.site_count,(.facts.domain_list|length),.panel_error_results,.secret_fields_dropped]' "$j")" = "[2,3,1,1]" ] || fail "Plesk: $(jq -c '{facts,panel_error_results}' "$j")"; ok
! grep -q 'FIXTURE-NOT-A-PASSWORD' "$j" || fail "a Plesk password reached the inventory"; ok
# A site get says nothing about mail or databases, so those stay the customer's word.
[ "$(jq -r '.provenance.mailbox_count+" "+.provenance.database_count' "$j")" = "intake intake" ] || fail "Plesk mail/db provenance"; ok
jq -e '.not_read_from_exports|index("mailbox_count") and index("database_count")' "$j" >/dev/null || fail "Plesk gaps not named"; ok

# ---- Intake alone: the customer's statement, no export grant ----
inv --out "$tmp/in.json" --intake "$fx/intake/site-inbox.json" --reference lead-7f3a1c >/dev/null
[ "$(jq -r '.grant_ref+" "+.grant_kind' "$tmp/in.json")" = "intake:lead-7f3a1c customer-statement" ] || fail "intake grant"; ok
[ "$(jq -c '[.facts.site_count,.facts.mailbox_count]' "$tmp/in.json")" = "[3,5]" ] || fail "intake counts are not integers"; ok
! grep -q 'owner@fixture.example\|Fixture Owner' "$tmp/in.json" || fail "the requester's name or contact was copied"; ok

# ---- Refusals ----
refused 3 --grant g $(exports cpanel "$fx/refused/cpanel-whm-failed.json")
refused 3 --grant g $(exports interworx "$fx/refused/interworx-failed.json")
refused 3 --grant g $(exports cpanel "$fx/refused/unknown-shape.json")
refused 3 $(exports cpanel "$fx/cpanel/whm-listaccts.json")              # no grant
refused 3 --intake "$fx/intake/site-inbox.json" --reference lead-nope     # unknown request
refused 3 --intake "$fx/cpanel/whm-listaccts.json" --reference lead-7f3a1c # not an inbox
refused 2 --grant g --export "directadmin:$fx/cpanel/whm-listaccts.json"  # no adapter
set +e; "$brief" inventory --subject user:7 --scope world --out "$tmp/refused.json" --intake "$fx/intake/site-inbox.json" --reference lead-7f3a1c >/dev/null 2>&1; rc=$?; set -e
[ "$rc" = 3 ] || fail "scope not refused"; ok

# ---- Shape: every inventory written above satisfies data/hosting-inventory.cue ----
if command -v cue >/dev/null 2>&1; then
    for f in cp iw pl in bare-inv; do cue vet -c "$here/data/hosting-inventory.cue" "$tmp/$f.json" || fail "$f.json breaks hosting-inventory.cue"; done; ok
    jq '.grant_kind="customer-statement"' "$tmp/cp.json" > "$tmp/forged.json"
    ! cue vet -c "$here/data/hosting-inventory.cue" "$tmp/forged.json" >/dev/null 2>&1 || fail "an export read as a customer statement passed the constraint"; ok
else
    echo "hosting inventory: CUE vet SKIPPED (cue not installed)"
fi

# ---- No network: the adapters run unchanged under a sandbox that denies it ----
if command -v sandbox-exec >/dev/null 2>&1; then
    # shellcheck disable=SC2046
    sandbox-exec -p '(version 1)(allow default)(deny network*)' "$brief" inventory --subject user:7 --scope self --out "$tmp/sbx.json" \
        --intake "$fx/intake/site-inbox.json" --reference lead-7f3a1c --grant fixture-export-grant-1 $(exports cpanel "$fx"/cpanel/*.json) >/dev/null \
        || fail "inventory needed something the no-network sandbox denied"
    cmp -s "$tmp/sbx.json" "$tmp/cp.json" || fail "sandboxed inventory differs"; ok
else
    echo "hosting inventory: no-network sandbox SKIPPED (sandbox-exec not present)"
fi

# ---- The brief: a throwaway CMS holding the hosting collections ----
[ -x "$cms" ] || { echo "hosting inventory: brief stage SKIPPED (bonfyre-cms not built)"; echo "hosting inventory: $pass checks passed"; exit 0; }
mkdir -p "$tmp/schemas" "$tmp/bodies" "$tmp/receipts"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$root/cmd/BonfyreCMS/content-types/source_collection.json" "$tmp/schemas/"
db="$tmp/cms.db"
"$cms" schema migrate --db "$db" --schemas "$tmp/schemas" >/dev/null 2>&1
doc() { # twin ref text...
    twin=$1 ref=$2; shift 2
    printf '%s\n' "$@" > "$tmp/bodies/$twin.body"
    printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"%s","source_url":"https://docs.example/%s","observed_at":"2026-10-30T01:00:00Z","policy_decision":"allow","http_status":200,"content_sha256":"%s","body_path":"%s","http_evidence":{"content_type":"text/plain"}}\n' \
        "$twin" "$twin" "$(sha "$tmp/bodies/$twin.body")" "$tmp/bodies/$twin.body" > "$tmp/receipts/$twin-20261030T010000Z-0000000000.json"
    "$cms" entry create internet_source --ns aurekai --db "$db" --schemas "$tmp/schemas" \
        "{\"status\":\"draft\",\"says\":\"fixture\",\"family\":\"software-documentation:hosting\",\"source\":\"$ref\",\"label\":\"$ref\",\"url\":\"https://docs.example/$twin\",\"twin\":\"$twin\",\"state\":\"observed\",\"note\":\"retained sha256 $(sha "$tmp/bodies/$twin.body") fixture\"}" >/dev/null 2>&1
}
doc fx-cpmig 'webdoc:cpanel-cpmigration' '# cPMigration' '' 'Copies cPanel accounts from a source server with pkgacct and restores each account on the destination.'
doc fx-iworx 'webdoc:interworx-cli' '# InterWorx CLI' '' 'The nodeworx command line lists SiteWorx accounts and can print the result as json for a migration inventory.'
coll() { "$cms" entry create source_collection --ns aurekai --db "$db" --schemas "$tmp/schemas" \
    "{\"status\":\"draft\",\"collection_id\":\"$1\",\"subject_address\":\"fx:$1\",\"refresh_policy\":\"manual\",\"material_kind\":[\"markdown\"],\"source_selector\":{\"source_refs\":$2}}" >/dev/null 2>&1; }
coll aurekai.migration.brief-input.hosting-controls '["webdoc:cpanel-cpmigration","webdoc:interworx-cli"]'
coll aurekai.migration.brief-input.primary '[]'
cited() { "$brief" cited --type hosting-account-acquisition --types "$here/data/migration-brief-types.json" --cms "$db" --receipts "$tmp/receipts" \
    --generated-at 2026-10-30T12:00:00Z --subject user:7 --scope self "$@"; }
status() { jq -r '[.criteria[]|.id+"="+.status]|join(" ")' "$1/brief.json"; }

cited --out "$tmp/b-cp" --inventory "$tmp/cp.json" > "$tmp/b-cp.out" || fail "brief over the cPanel inventory not written"
b="$tmp/b-cp/brief.json"
[ "$(jq '.sources|length' "$b")" = 2 ] && [ "$(jq '[.sources[].citations|length]|add' "$b")" -gt 0 ] || fail "hosting docs not cited"; ok
[ "$(status "$tmp/b-cp")" = "inventory=disputed frozen-source=stated-by-customer-inventory dns-plan=stated-by-customer-inventory mail-continuity=disputed" ] || fail "cPanel statuses: $(status "$tmp/b-cp")"; ok
[ "$(jq '[.criteria[].verified]|any' "$b")" = false ] || fail "a criterion claimed verification"; ok
jq -e '.exceptions[]|select(.kind=="inventory-disagreement" and .ref=="mailbox_count")' "$b" >/dev/null && jq -e '.exceptions[]|select(.kind=="inventory-draft")' "$b" >/dev/null || fail "disagreement or draft not raised"; ok
[ "$(jq -c '[.estimate.units_stated,.estimate.total_low,.estimate.total_high]' "$b")" = "[3,900,2700]" ] || fail "estimate not scaled by the listing's site count"; ok
[ "$(jq -r '.criteria[0].provenance.site_count' "$b")" = "export:cpanel" ] || fail "criterion provenance missing"; ok
grep -q 'disagree on this' "$tmp/b-cp/brief.md" && grep -q 'Mailboxes: you told us 5 and your exported listing shows 4' "$tmp/b-cp/brief.md" || fail "brief.md does not say where answers and listing disagree"; ok
! grep -q 'mailbox_count\|site_count\|export:cpanel' "$tmp/b-cp/brief.md" || fail "brief.md carries field names"; ok
[ "$(jq -r '.inventory_digest' "$tmp/b-cp/receipt.json")" = "$(sha "$tmp/cp.json")" ] || fail "receipt does not pin the inventory"; ok

# Export alone: the inventory criterion reads from the listing; backup and DNS stay open.
inv --out "$tmp/cp-only.json" --grant fixture-export-grant-1 $(exports cpanel "$fx"/cpanel/*.json) >/dev/null
cited --out "$tmp/b-exp" --inventory "$tmp/cp-only.json" >/dev/null
[ "$(status "$tmp/b-exp")" = "inventory=read-from-exported-listing frozen-source=needs-inventory dns-plan=needs-inventory mail-continuity=read-from-exported-listing" ] || fail "export-only statuses: $(status "$tmp/b-exp")"; ok
grep -q 'exported account listing states what this needs. Nobody has checked it against the live server' "$tmp/b-exp/brief.md" || fail "export-only wording"; ok

# InterWorx and Plesk inventories reach the same brief type.
cited --out "$tmp/b-iw" --inventory "$tmp/iw.json" >/dev/null
jq -e '.exceptions[]|select(.kind=="inventory-unparsed" and .ref=="site_count")' "$tmp/b-iw/brief.json" >/dev/null || fail "unparsed answer not raised"; ok
cited --out "$tmp/b-pl" --inventory "$tmp/pl.json" >/dev/null
[ "$(status "$tmp/b-pl")" = "inventory=stated-by-customer-inventory frozen-source=stated-by-customer-inventory dns-plan=stated-by-customer-inventory mail-continuity=stated-by-customer-inventory" ] || fail "Plesk statuses: $(status "$tmp/b-pl")"; ok

# Verified brief: the reviewer's sources digest replays to identical bytes; a stale one is refused.
digest=$(jq -r '.sources_digest' "$b")
cited --out "$tmp/b-cp-replay" --inventory "$tmp/cp.json" --expect-sources-digest "$digest" >/dev/null || fail "replay at the reviewed digest refused"
cmp -s "$b" "$tmp/b-cp-replay/brief.json" && cmp -s "$tmp/b-cp/brief.md" "$tmp/b-cp-replay/brief.md" || fail "replayed brief differs"; ok
printf '\n' >> "$tmp/bodies/fx-cpmig.body"
set +e; cited --out "$tmp/b-stale" --inventory "$tmp/cp.json" --expect-sources-digest "$digest" >/dev/null 2>&1; rc=$?; set -e
[ "$rc" != 0 ] && [ ! -e "$tmp/b-stale/brief.json" ] || fail "a changed source still produced the reviewed brief ($rc)"; ok

# ---- Optional: intake from the practice site's own form and inbox ----
web=${BONFYRE_TEST_WEB_BINARY:-}
if [ -n "$web" ]; then
    case "$web" in /*) ;; *) web="$PWD/$web" ;; esac
    for organ in BonfyreCMS/bonfyre-cms BonfyreIngest/bonfyre-ingest BonfyreParagraph/bonfyre-paragraph BonfyreRecall/bonfyre-recall; do
        [ -x "$root/cmd/$organ" ] || fail "practice intake needs cmd/$organ built"
    done
    export BONFYRE_CMS_BINARY="$root/cmd/BonfyreCMS/bonfyre-cms" BONFYRE_INGEST_BINARY="$root/cmd/BonfyreIngest/bonfyre-ingest"
    export BONFYRE_PARAGRAPH_BINARY="$root/cmd/BonfyreParagraph/bonfyre-paragraph" BONFYRE_RECALL_BINARY="$root/cmd/BonfyreRecall/bonfyre-recall"
    export BONFYRE_CMS_SCHEMAS="$root/cmd/BonfyreCMS/content-types" BONFYRE_SITE_ROOT="$root/site/matrix"
    home="$tmp/site"; mkdir -p "$home"
    port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
    trap '"$web" site stop --home "$home" >/dev/null 2>&1 || true; rm -rf "$tmp"' EXIT INT TERM
    sh "$here/practice-site/build.sh" "$web" "$home" "$port" >/dev/null || fail "practice site build"
    status=$(curl -s -o /dev/null -w '%{http_code}' -m 15 --resolve "migrations.localhost:$port:127.0.0.1" -X POST "http://migrations.localhost:$port/contact" \
        --data-urlencode 'name=Form Owner' --data-urlencode 'contact=form@example.test' --data-urlencode 'message=About: Hosting account move.' \
        --data-urlencode 'about=Hosting account move' --data-urlencode 'ask_source_panel=cPanel' --data-urlencode 'ask_site_count=3' \
        --data-urlencode 'ask_domain_list=alpha-bakery.example beta-florist.example gamma-catering.example' --data-urlencode 'ask_mailbox_count=4' \
        --data-urlencode 'ask_database_count=3' --data-urlencode 'ask_dns_provider=Cloudflare' --data-urlencode 'ask_backup_location=Offsite bucket')
    [ "$status" = 200 ] || fail "the hosting form refused the request ($status)"
    "$web" site inbox migrations --json --home "$home" --port "$port" > "$tmp/live-inbox.json"
    ref=$(jq -r '.messages[0].reference' "$tmp/live-inbox.json")
    # shellcheck disable=SC2046
    inv --out "$tmp/form.json" --intake "$tmp/live-inbox.json" --reference "$ref" --grant fixture-export-grant-1 $(exports cpanel "$fx"/cpanel/*.json) >/dev/null \
        || fail "the practice site's inbox did not become an inventory"
    [ "$(jq -c '.disagreements' "$tmp/form.json")" = '[]' ] && [ "$(jq -r '.facts.dns_provider' "$tmp/form.json")" = Cloudflare ] || fail "form answers: $(jq -c '{facts,disagreements}' "$tmp/form.json")"; ok
    cited --out "$tmp/b-form" --inventory "$tmp/form.json" >/dev/null
    [ "$(status "$tmp/b-form")" = "inventory=read-from-exported-listing frozen-source=stated-by-customer-inventory dns-plan=stated-by-customer-inventory mail-continuity=read-from-exported-listing" ] || fail "form brief statuses: $(status "$tmp/b-form")"; ok
    ! ls "$home" | grep -q '^share' || fail "the practice draft was shared"; ok
else
    echo "hosting inventory: practice-site intake SKIPPED (set BONFYRE_TEST_WEB_BINARY)"
fi

# ---- Optional: the shipped hosting brief over the real CMS and receipts, read-only ----
if [ -n "${BONFYRE_TEST_LIVE_CMS:-}" ]; then
    before=$(sha "$BONFYRE_TEST_LIVE_CMS")
    "$brief" cited --type hosting-account-acquisition --types "$here/data/migration-brief-types.json" --cms "$BONFYRE_TEST_LIVE_CMS" \
        --receipts "${BONFYRE_TEST_LIVE_RECEIPTS:-$HOME/.bonfyre/foreign/receipts}" --subject user:7 --scope self \
        --inventory "$tmp/cp.json" --out "${BONFYRE_TEST_LIVE_OUT:-$tmp/live}" >/dev/null || fail "live hosting brief"
    [ "$(jq '.sources|length' "${BONFYRE_TEST_LIVE_OUT:-$tmp/live}/brief.json")" -ge 5 ] || fail "live brief read fewer than five hosting documents"
    [ "$(sha "$BONFYRE_TEST_LIVE_CMS")" = "$before" ] || fail "the live CMS changed"; ok
fi
echo "hosting inventory: $pass checks passed"
