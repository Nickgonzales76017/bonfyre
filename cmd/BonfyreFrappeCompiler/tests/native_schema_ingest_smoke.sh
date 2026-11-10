#!/bin/sh
# Native Frappe metadata extraction must work without the retired Python helper.
set -eu

compiler="$1"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

"$compiler" --root "$root" --phase 1 --emit-pack "$tmp/frappe.pack.json" \
  "$root/integrations/frappe-bench/apps/frappe" >"$tmp/compiler.out"

grep -q 'Schema graph built: [1-9][0-9]* doctypes' "$tmp/compiler.out"
grep -q '"kind":"AppPack"' "$tmp/frappe.pack.json"
grep -q '"name":"User Group Member"' "$tmp/frappe.pack.json"

# A compiled DocType must become a safe, declarative native CMS schema.  This
# is deliberately not a Frappe runtime import: sensitive, layout, table and
# controller grammar stay outside the CMS record boundary.
"$compiler" --root "$root" --phase 1 \
  --emit-cms-schema "$tmp/crm-deal.cms.json" --cms-doctype "CRM Deal" \
  "$root/integrations/frappe-bench/apps/crm" >"$tmp/cms-compiler.out"

grep -q '"name":"frappe_crm_crm_deal"' "$tmp/crm-deal.cms.json"
grep -q '"displayName":"CRM Deal"' "$tmp/crm-deal.cms.json"
grep -q '"forbidden_inference":"A CMS record is not a Frappe document' "$tmp/crm-deal.cms.json"
grep -q '"organization_name"' "$tmp/crm-deal.cms.json"
if grep -q '"access_token"' "$tmp/crm-deal.cms.json"; then
  echo "credential field escaped into CMS schema" >&2
  exit 1
fi

# Interaction fingerprints are measured from compiled DocType metadata, not
# hand-labelled app pages. They can name renderer grammars, but never execute
# a Frappe controller or imply a Frappe permission/effect boundary.
"$compiler" --root "$root" --phase 1 \
  --emit-interaction-fingerprints "$tmp/crm.interaction-fingerprints.json" \
  "$root/integrations/frappe-bench/apps/crm" >"$tmp/fingerprint-compiler.out"
python3 - "$tmp/crm.interaction-fingerprints.json" <<'PY'
import json, sys
payload = json.load(open(sys.argv[1], encoding="utf-8"))
assert payload["schema"] == "bonfyre.frappe-interaction-fingerprints.v1"
assert payload["app"] == "crm"
assert payload["fingerprints"]
assert all("record-form" in row["interaction_roles"] for row in payload["fingerprints"])
assert all("permission" in row["cannot_infer"] for row in payload["fingerprints"])
PY
echo "native Frappe schema ingest: PASS"
