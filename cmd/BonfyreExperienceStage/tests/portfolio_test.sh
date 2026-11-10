#!/bin/sh
set -eu
bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
programs="experience-maturities.yaff"
"$bin" portfolio audit --contracts "$programs" --root ../.. > "$tmp/audit"
[ "$(grep -Ec '^[0-9][0-9][[:space:]]' "$tmp/audit")" = 30 ]
grep -q 'PORTFOLIO.*programs=30.*classes=8+8+14.*missing_owner=0' "$tmp/audit"
grep -q '06.*local-app-guide.*public_kind=guide' "$tmp/audit"
grep -q '07.*local-app-guide.*public_kind=guide' "$tmp/audit"
grep -q '09.*github-foundation.*public_kind=legacy-foundation' "$tmp/audit"
grep -q '17.*new-program.*public_kind=published-preview' "$tmp/audit"
grep -q '21.*new-program.*witness_source=present_unexecuted' "$tmp/audit"
"$bin" portfolio order --contracts "$programs" > "$tmp/order"
[ "$(grep -Ec '^[0-9][0-9][[:space:]]' "$tmp/order")" = 30 ]
grep -q 'ORDER.*dependency_only_not_authorization' "$tmp/order"
"$bin" portfolio show 17 --contracts "$programs" --root ../.. > "$tmp/show"
grep -q 'PROGRAM.*17.*acquisition-brief-studio' "$tmp/show"
grep -q 'STAGE.*compose' "$tmp/show"
grep -q 'RELEASE_GATE.*state=unwitnessed' "$tmp/show"
"$bin" portfolio cards --contracts "$programs" --root ../.. > "$tmp/cards"
[ "$(grep -c '<li data-state=' "$tmp/cards")" = 18 ]
grep -q 'data-state="guide"' "$tmp/cards"
grep -q 'data-state="case-study"' "$tmp/cards"
grep -q 'data-state="published-preview"' "$tmp/cards"
grep -q 'https://brief-studio-upgrade.aurekai-work-samples.pages.dev/brief-studio/' "$tmp/cards"
! grep -q '<strong>verification-network</strong>' "$tmp/cards"
refuse() {
 if "$bin" portfolio audit --contracts "$1" --root ../.. > "$tmp/out" 2>"$tmp/err";then
   echo "Maturity contract mistakenly accepted $1" >&2
   exit 1
 fi
 grep -q 'portfolio refused' "$tmp/err" || { cat "$tmp/err" >&2; exit 1; }
}
sed 's/  owner cmd\/BonfyreEconomy\/src\/verification_planner.c/  owner ..\/..\/private/' "$programs" > "$tmp/path.yaff"
refuse "$tmp/path.yaff"
sed 's/  entry -/  entry https:\/\/evil.example\/maturity/' "$programs" > "$tmp/origin.yaff"
refuse "$tmp/origin.yaff"
sed 's/  depends 21,29/  depends 30,29/' "$programs" > "$tmp/cycle.yaff"
refuse "$tmp/cycle.yaff"
sed 's/maturity 30 community-funding-vouchers/maturity 30 retail-procurement/' "$programs" > "$tmp/duplicate.yaff"
refuse "$tmp/duplicate.yaff"
sed 's/  class new-program/  class existing-tool/' "$programs" > "$tmp/class.yaff"
refuse "$tmp/class.yaff"
printf '%s\n' 'experience portfolio: PASS (30 audited, owners present, DAG, 18 declared routes, docs and previews separated, invalid paths/origins/cycles/duplicates refused)'
