#!/bin/sh
set -eu
bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
db="$scratch/economy.db"
run() { "$bin" --db "$db" verify "$@"; }
refuse() { if run "$@" >"$scratch/refused.stdout" 2>"$scratch/refused.stderr"; then
  echo "Verification erroneously accepted: $*" >&2; exit 1
  fi
}
run case create houston-35 itad foreign:houston-asset 14500 >/dev/null
run need add houston-35 model model 3 seller:manifest >/dev/null
run need add houston-35 boot condition 5 seller:untested >/dev/null
run need add houston-35 serial serial 3 seller:no-serials >/dev/null
run need add houston-35 owner ownership 5 seller:ownership-unknown >/dev/null
run offer add houston-35 seller-photo provider:seller owner 0 0 0 - estimate conditional - - >/dev/null
run offer add houston-35 field-visual provider:field-team field 3000 2000 5000 trip:houston quoted ready quote:field-v1 available:field-v1 >/dev/null
run offer add houston-35 field-serial provider:field-team field 1000 1000 5000 trip:houston quoted ready quote:serial-v1 available:field-v1 >/dev/null
run offer add houston-35 ownership-desk provider:records desk 500 1000 0 - estimate conditional - - >/dev/null
run covers add houston-35 seller-photo model >/dev/null
refuse covers add houston-35 seller-photo boot
run covers add houston-35 field-visual model >/dev/null
run covers add houston-35 field-visual boot >/dev/null
run covers add houston-35 field-serial serial >/dev/null
run covers add houston-35 ownership-desk owner >/dev/null
refuse offer add houston-35 fraudulent-share provider:other field 1000 1000 5000 trip:houston quoted ready quote:x available:x
refuse offer add houston-35 quote-without-receipt provider:none desk 1 1 0 - quoted ready - available:x
run plan houston-35 --mode exploratory >"$scratch/exploratory"
grep -q 'status=candidate_full' "$scratch/exploratory"
grep -q 'planned_cents=13500' "$scratch/exploratory"
grep -q 'COVERAGE.*4/4' "$scratch/exploratory"
! grep -q '^ROUTE.*seller-photo' "$scratch/exploratory"
if run plan houston-35 --mode documented >"$scratch/documented-before"; then
  echo "Expected missing documented ownership route" >&2; exit 1
fi
grep -q '^UNRESOLVED.*owner' "$scratch/documented-before"
run offer add houston-35 licensed-owner provider:licensed licensed 2000 0 0 - quoted ready quote:title-review available:licensed >/dev/null
run covers add houston-35 licensed-owner owner >/dev/null
run plan houston-35 --mode documented >"$scratch/documented-after"
grep -q 'status=candidate_full' "$scratch/documented-after"
grep -q 'planned_cents=14000' "$scratch/documented-after"
if run plan houston-35 --budget-cents 1000 >"$scratch/budget"; then
  echo "Budget constraint was ignored" >&2; exit 1
fi
grep -q 'status=over_budget' "$scratch/budget"
grep -q '^AUTHORITY.*does not verify evidence' "$scratch/documented-after"
run case create dallas-property property parcel:dallas-example 25000 >/dev/null
run need add dallas-property title title 5 county:record >/dev/null
run offer add dallas-property title-from-web provider:public desk 0 400 0 - estimate conditional - - >/dev/null
refuse covers add dallas-property title-from-web title
run offer add dallas-property title-from-seller provider:seller owner 0 0 0 - estimate conditional - - >/dev/null
refuse covers add dallas-property title-from-seller title
run offer add dallas-property title-counsel provider:counsel licensed 9000 2000 0 - estimate conditional - - >/dev/null
run covers add dallas-property title-counsel title >/dev/null
run plan dallas-property --mode exploratory >"$scratch/property"
grep -q 'planned_cents=11000' "$scratch/property"
if run plan dallas-property --mode documented >"$scratch/unconfirmed"; then
  echo "Estimate masqueraded as documented quote" >&2; exit 1
fi
grep -q '^UNRESOLVED.*title' "$scratch/unconfirmed"
refuse case create ../bad itad foreign:wrong 100
refuse case create houston-35 itad foreign:houston-asset 14500
run intake source-review-v1 property ForeignTwin:parcel-verified 20000 \
  parcel identity 5 CMS:parcel-verified \
  owner ownership 5 CMS:parcel-verified \
  title title 5 CMS:parcel-verified > "$scratch/intake"
grep -q 'owner_readback=exact' "$scratch/intake"
grep -q 'INTAKE.*created.*count=3' "$scratch/intake"
run intake source-review-v1 property ForeignTwin:parcel-verified 20000 \
  title title 5 CMS:parcel-verified \
  parcel identity 5 CMS:parcel-verified \
  owner ownership 5 CMS:parcel-verified > "$scratch/replay"
grep -q 'INTAKE.*unchanged.*count=3' "$scratch/replay"
refuse intake source-review-v1 property ForeignTwin:parcel-verified 20000 \
  parcel identity 5 CMS:parcel-verified \
  title title 5 CMS:another-source \
  owner ownership 5 CMS:parcel-verified
refuse intake rejected-review property ForeignTwin:rejected 20000 \
  title title 5 CMS:source \
  title title 5 CMS:source
refuse intake invalid-review property ForeignTwin:source 20000 \
  title dragon 5 CMS:source
test "$(sqlite3 "$db" "select count(*) from verification_needs where case_id='source-review-v1';")" = 3
test "$(sqlite3 "$db" "select count(*) from verification_cases where case_id='rejected-review';")" = 0
printf '%s\n' "verification planner: PASS (shared-visit costs, constrained proof methods, quoted-vs-estimate, provider identity, budget, title refusal)"
