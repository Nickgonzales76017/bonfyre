#!/bin/sh
# A conditional field property is worth compiling only if every declaration is
# accounted for -- compiled under the donor's own semantics, or refused with
# the operation it is missing -- and if one expression shared by seven fields
# is one plan with seven uses rather than seven copies.
set -eu

compiler="$1"
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

# result EXPR DOC -> true | false | the refusal
result() {
  "$compiler" --eval-condition "$1" --doc "$2" | jq -r 'if .status != "compiled" then .missing_operation elif .result == null then "error" else .result end' || true
}
expect() {
  got=$(result "$2" "$3")
  [ "$got" = "$1" ] || { echo "FAIL: $2 on $3 gave $got, expected $1" >&2; exit 1; }
}

# The donor's two branches disagree about an empty list, and both are kept.
expect false 'items' '{"items":[]}'
expect true  'eval:doc.items' '{"items":[]}'
expect true  'items' '{"items":[{"a":1}]}'
# Loose and strict equality are different operations.
expect true  'eval:doc.x == 1' '{"x":"1"}'
expect false 'eval:doc.x === 1' '{"x":"1"}'
expect true  'eval:doc.a == null' '{}'
expect false 'eval:doc.a === null' '{}'
# The supported calls, as the donor defines them.
expect true  'eval:in_list(["A","B"], doc.t) && !doc.z' '{"t":"B"}'
expect false 'eval:["Int","Float"].indexOf(doc.fieldtype) !== -1' '{"fieldtype":"Data"}'
expect true  'eval:!(["URL","Text"]).includes(doc.type);' '{"type":"File"}'
expect true  'eval:cint(doc.n) > 3' '{"n":"007"}'
expect true  'eval:doc.rows && doc.rows.length' '{"rows":[1]}'
# Where the donor expression would throw, this refuses instead of answering.
expect false 'eval:doc.name?.includes("http")' '{}'
expect error 'eval:doc.name.includes("http")' '{}'
expect error 'eval:parent.docstatus == 0' '{}'
# A name from the donor's client is read from what this estate binds: it runs
# no developer mode, and a name it binds nothing for is refused, not guessed.
expect false 'eval:frappe.boot.developer_mode' '{}'
expect false 'eval:frappe.boot.versions.payments' '{}'
expect true  'eval:frappe.boot.versions.erpnext' '{}'
expect error 'eval:frappe.session.user == "x"' '{}'
# cur_frm.doc is the form's document, which the donor also calls parent.
[ "$("$compiler" --eval-condition 'eval:cur_frm.doc.employee' --doc '{}' --parent-doc '{"employee":"E1"}' | jq -r .result)" = true ]
# Outside the supported operations a declaration names what it is missing.
expect 'arrow_callback:every' 'eval:doc.items.every((item) => !item.dn_detail)' '{}'
expect 'call:flt' 'eval:flt(doc.x) > 0' '{}'
expect 'client_script:toggle' 'fn:toggle' '{}'
# A second statement would run and be discarded by the donor; it is refused.
expect 'syntax:trailing:doc' 'eval:doc.a; doc.b' '{}'

# The source is preserved even where it is probably a defect.
"$compiler" --eval-condition 'eval:doc.report_type == "Report Builder" || "Custom Report"' \
  --doc '{"report_type":"Query Report"}' >"$tmp/always.json"
jq -e '.constant == "always_true" and .result == true and .reads == ["report_type"]' "$tmp/always.json" >/dev/null
"$compiler" --eval-condition 'doc.icon' --doc '{"icon":"x"}' >"$tmp/bare.json"
jq -e '.constant == "always_false" and .result == false' "$tmp/bare.json" >/dev/null

"$compiler" --root "$root" --emit-condition-plans "$tmp/plans.json" --emit-surface-db "$tmp/surfaces.db" \
  -o "$tmp/pack" "$root/integrations/frappe-bench/apps/frappe" >"$tmp/out"

jq -e '
  .schema == "bonfyre.frappe-condition-plans.v1"
  and (.counts.declarations > 500)
  # every declaration is exactly one of compiled or refused-with-a-name
  and (.counts.declarations == (.uses | length))
  and (.counts.compiled + .counts.unsupported == .counts.declarations)
  and ([.uses[] | select(.status == "compiled" and (.plan_id == "" or .missing_operation != ""))] | length == 0)
  and ([.uses[] | select(.status == "unsupported" and (.plan_id != "" or .missing_operation == ""))] | length == 0)
  and (.counts.unsupported == ([.missing_operations[].declarations] | add // 0))
  # a plan is one body: its id names one text, and its uses are counted
  and ((.plans | map(.plan_id) | unique | length) == (.plans | length))
  and ((.plans | map(.plan) | unique | length) == (.plans | length))
  and (([.plans[].uses] | add) == .counts.compiled)
  and (.counts.shapes <= .counts.plans)
  and (.cannot_infer | length > 0)
  # an identity is 128 bits of the SHA-256 of the plan text
  and all(.plans[]; .plan_id | test("^cp-[0-9a-f]{32}$"))
  and (.environment.frappe.boot.developer_mode == 0)
  # every use is said to a reader one way or another
  and all(.uses[]; .kind == "words" or .kind == "always" or .kind == "never" or .kind == "depends" or .kind == "unevaluated")
  and all(.uses[] | select(.kind == "words"); (.said | length > 0) and (.said | test("eval:|doc\\.") | not))
' "$tmp/plans.json" >/dev/null || { echo "FAIL: condition plan accounting" >&2; exit 1; }

# Event repeats one condition on seven weekday fields: one plan, seven uses,
# and a change to repeat_on reaches exactly those seven.
jq -e '
  [.uses[] | select(.doctype == "Event" and (.source | test("Weekly")))] as $week
  | ($week | length == 7) and ($week | map(.plan_id) | unique | length == 1)
  and ([.read_edges[] | select(.doctype == "Event" and .read == "repeat_on")] | length == 7)
  and ([.read_edges[] | select(.doctype == "Event" and .read == "color")] | length == 0)
' "$tmp/plans.json" >/dev/null || { echo "FAIL: Event weekday sharing" >&2; exit 1; }

# The always-true expression in Report is reported, and still compiled as written.
jq -e '[.plans[] | select(.constant == "always_true" and (.plan | test("Custom Report")))] | length == 1' \
  "$tmp/plans.json" >/dev/null || { echo "FAIL: constant expression not reported" >&2; exit 1; }

# The store the composer reads holds one row per use, said in the DocType's
# own labels: Event's weekdays read as a sentence, not as an expression.
q() { sqlite3 "$tmp/surfaces.db" "$1"; }
[ "$(q "SELECT COUNT(*) FROM frappe_condition")" = "$(jq -r .counts.declarations "$tmp/plans.json")" ]
[ "$(q "SELECT said FROM frappe_condition WHERE program_id='frappe:frappe:Event' AND field='monday'")" = 'Repeat this Event is ticked and Repeat On is "Weekly"' ]
[ "$(q "SELECT COUNT(DISTINCT plan_id) FROM frappe_condition WHERE program_id='frappe:frappe:Event' AND said LIKE '%Weekly%'")" = 1 ]
[ "$(q "SELECT kind FROM frappe_condition WHERE program_id='frappe:frappe:Report' AND field='json'")" = always ]

jq -r '"condition plans: \(.counts.declarations) declarations, \(.counts.compiled) compiled, \(.counts.unsupported) refused by name, \(.counts.plans) plans, \(.counts.shapes) shapes"' "$tmp/plans.json"
echo "condition plan contract: PASS"
