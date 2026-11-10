#!/bin/sh
set -eu

# Materialize the checked-in Ghost marketplace reference census as reference
# candidates. These rows are deliberately not published SiteProfiles: the
# census has marketplace metadata, not a separately observed masthead, slot
# geometry, responsive posture or interaction contract for every reference.

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${BONFYRE_CMS_BIN:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
db=${BONFYRE_CMS_DB:-/var/lib/bonfyre/cms/bonfyre_cms.db}
schemas="$root/cmd/BonfyreCMS/content-types"
type_schema="$schemas/presentation_template.json"
fingerprint_schema="$schemas/design_fingerprint.json"
pattern_schema="$schemas/surface_pattern.json"
compiler="$root/cmd/BonfyreCMS/tools/ghost_marketplace_templates.mjs"
pattern="$root/cmd/BonfyreCMS/data/presentation-template-surface.json"

[ -x "$cms" ] || { echo "presentation templates: bonfyre-cms not executable: $cms" >&2; exit 2; }
[ -f "$db" ] || { echo "presentation templates: CMS database not found: $db" >&2; exit 2; }
command -v sqlite3 >/dev/null 2>&1 || { echo "presentation templates: sqlite3 is required" >&2; exit 2; }

"$cms" schema apply "$type_schema" --db "$db" --schemas "$schemas" >/dev/null
"$cms" schema apply "$fingerprint_schema" --db "$db" --schemas "$schemas" >/dev/null
"$cms" schema apply "$pattern_schema" --db "$db" --schemas "$schemas" >/dev/null

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
jsonl="$tmp/presentation-templates.jsonl"
fingerprints="$tmp/design-fingerprints.jsonl"
bun "$compiler" --out "$jsonl" --fingerprints-out "$fingerprints" --expect 238

generated=$(wc -l < "$jsonl" | tr -d ' ')
[ "$generated" = 238 ] || {
  echo "presentation templates: compiler produced $generated rows, expected 238" >&2
  exit 3
}
generated_fingerprints=$(wc -l < "$fingerprints" | tr -d ' ')
[ "$generated_fingerprints" = 238 ] || {
  echo "presentation templates: compiler produced $generated_fingerprints fingerprints, expected 238" >&2
  exit 3
}

existing=$(sqlite3 "$db" "SELECT count(*) FROM presentation_template WHERE namespace='aurekai';")
case "$existing" in
  0)
    result=$("$cms" entry import-jsonl presentation_template "$jsonl" --ns aurekai --db "$db" --schemas "$schemas")
    printf '%s\n' "$result"
    ;;
  238)
    existing_candidates=$(sqlite3 "$db" "SELECT count(*) FROM presentation_template WHERE namespace='aurekai' AND status='reference_candidate';")
    [ "$existing_candidates" = 238 ] || {
      echo "presentation templates: existing rows predate profile-study boundary; explicit migration/review required" >&2
      exit 4
    }
    echo '{"presentation_reference_candidates":238,"state":"already_materialized","namespace":"aurekai"}'
    ;;
  *)
    echo "presentation templates: refusing partial/ambiguous generation ($existing rows already present)" >&2
    exit 4
    ;;
esac

existing_fingerprints=$(sqlite3 "$db" "SELECT count(*) FROM design_fingerprint WHERE namespace='aurekai';")
case "$existing_fingerprints" in
  0)
    "$cms" entry import-jsonl design_fingerprint "$fingerprints" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null
    ;;
  238) ;;
  *)
    echo "presentation templates: refusing partial/ambiguous fingerprint generation ($existing_fingerprints rows already present)" >&2
    exit 4
    ;;
esac

# One generic SurfacePattern exposes every presentation template through the
# existing compiler. A caller selects one with:
#   /_surface/presentation-templates?template=ghost-wire
# The descriptor reads one owner and carries no effect authority.
pattern_count=$(sqlite3 "$db" "SELECT count(*) FROM surface_pattern WHERE pattern='presentation-templates' AND status='published';")
case "$pattern_count" in
  0)
    "$cms" entry create surface_pattern "@$pattern" --ns aurekai --db "$db" --schemas "$schemas" >/dev/null
    ;;
  1) ;;
  *)
    echo "presentation templates: duplicate published presentation-templates surface pattern ($pattern_count)" >&2
    exit 5
    ;;
esac

# The original six seed templates and the full census share a few stable
# template keys. The one public descriptor is therefore explicitly bound to
# the compiled census namespace; selection remains by authored template key,
# never by whichever duplicate happened to sort first.
sqlite3 "$db" "UPDATE surface_pattern SET record_namespace='aurekai' WHERE pattern='presentation-templates' AND status='published' AND reads_type='presentation_template';"

count=$(sqlite3 "$db" "SELECT count(*) FROM presentation_template WHERE namespace='aurekai' AND status='reference_candidate';")
fingerprint_count=$(sqlite3 "$db" "SELECT count(*) FROM design_fingerprint WHERE namespace='aurekai' AND status='reference_candidate';")
families=$(sqlite3 "$db" "SELECT count(DISTINCT family) FROM presentation_template WHERE namespace='aurekai' AND status='reference_candidate';")
pattern_count=$(sqlite3 "$db" "SELECT count(*) FROM surface_pattern WHERE pattern='presentation-templates' AND status='published';")

[ "$count" = 238 ] || { echo "presentation templates: reference candidate count is $count, expected 238" >&2; exit 6; }
[ "$fingerprint_count" = 238 ] || { echo "presentation templates: reference candidate fingerprint count is $fingerprint_count, expected 238" >&2; exit 6; }
[ "$pattern_count" = 1 ] || { echo "presentation templates: read pattern is not uniquely published" >&2; exit 7; }

printf '{"presentation_reference_candidates":%s,"design_reference_candidates":%s,"families":%s,"namespace":"aurekai","verdict":"installed_not_selectable"}\n' "$count" "$fingerprint_count" "$families"
