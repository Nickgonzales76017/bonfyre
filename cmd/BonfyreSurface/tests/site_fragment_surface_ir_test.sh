#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
surface="$root/cmd/BonfyreSurface/bonfyre-surface-ir"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
schema="$root/cmd/BonfyreCMS/content-types/site_fragment.json"

[ -x "$surface" ] || { echo "surface ir: SKIP (compiler not built)"; exit 0; }
[ -x "$cms" ] || { echo "surface ir: SKIP (cms not built)"; exit 0; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"

"$cms" schema apply "$schema" --db "$db" >/dev/null

"$cms" entry create site_fragment \
  '{"coordinate":"protocols.hero.public.default","page":"protocols","slot":"hero","audience":"public","kind":"copy","position":10,"heading":"Published hero","body":"Visible","status":"published","source_ref":"fixture:published"}' \
  --ns aurekai --db "$db" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null

"$cms" entry create site_fragment \
  '{"coordinate":"protocols.hero.public.draft","page":"protocols","slot":"hero","audience":"public","kind":"copy","position":20,"heading":"Draft hero","body":"Hidden","status":"draft","source_ref":"fixture:draft"}' \
  --ns aurekai --db "$db" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null

"$cms" entry create site_fragment \
  '{"coordinate":"protocols.footer.public.default","page":"protocols","slot":"footer","audience":"public","kind":"notice","position":10,"heading":"Published footer","body":"Visible footer","status":"published"}' \
  --ns aurekai --db "$db" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null

"$cms" entry create site_fragment \
  '{"coordinate":"protocols.hero.market.default","page":"protocols","slot":"hero","audience":"market","kind":"copy","position":10,"heading":"Market hero","body":"Wrong audience","status":"published"}' \
  --ns aurekai --db "$db" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null

slot=$("$surface" compile --db "$db" --namespace aurekai --page protocols --slot hero --audience public)
printf '%s' "$slot" | grep -q '"selection":"exact"'
printf '%s' "$slot" | grep -q '"heading":"Published hero"'
! printf '%s' "$slot" | grep -q 'Draft hero'
! printf '%s' "$slot" | grep -q 'Published footer'
! printf '%s' "$slot" | grep -q 'Market hero'
printf '%s' "$slot" | grep -q '"may_override_exact_coordinate":false'

page=$("$surface" compile --db "$db" --namespace aurekai --page protocols --audience public)
printf '%s' "$page" | grep -q '"heading":"Published hero"'
printf '%s' "$page" | grep -q '"heading":"Published footer"'
! printf '%s' "$page" | grep -q 'Draft hero'
! printf '%s' "$page" | grep -q 'Market hero'

"$cms" ann build site_fragment --db "$db" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null
stats=$("$cms" ann stats site_fragment --db "$db" --schemas "$root/cmd/BonfyreCMS/content-types")
printf '%s' "$stats" | grep -q '"indexed_members":4'

echo "surface ir authored-content contract: PASS"
