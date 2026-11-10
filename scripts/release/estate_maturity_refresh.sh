#!/bin/sh
# Explicit foreground owner-state refresh for estate maturity. This is not the
# autonomous operator and does not alter its STOP/admission state.
set -eu

ROOT=""
HOME_DIR=""
APPLY=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --root) shift; ROOT=${1:-} ;;
    --home) shift; HOME_DIR=${1:-} ;;
    --apply) APPLY=1 ;;
    *) echo "estate-maturity-refresh: invalid argument $1" >&2; exit 64 ;;
  esac
  shift
done
[ "$APPLY" -eq 1 ] || { echo "estate-maturity-refresh: --apply required" >&2; exit 64; }
[ -n "$ROOT" ] && [ -d "$ROOT/cmd" ] || { echo "estate-maturity-refresh: invalid project root" >&2; exit 66; }
[ -n "$HOME_DIR" ] && [ -d "$HOME_DIR/.bonfyre" ] || { echo "estate-maturity-refresh: invalid Bonfyre home" >&2; exit 66; }

estate="$HOME_DIR/.bonfyre/estate-local"
cms="$estate/cms.db"
items="$estate/items.db"
frontier="$estate/frontier.db"
fabric="$estate/subject_fabric.db"
foreign="$estate/foreign"
control="$estate/control.db"
account="$HOME_DIR/.bonfyre/state/account-work.db"
scene="$ROOT/cmd/BonfyreSurfaceScene/bonfyre-surface-scene"
semantic="$ROOT/cmd/BonfyreSemanticOptimizer/bonfyre-semantic-optimizer"
maturity="$ROOT/cmd/BonfyreSurfaceMaturity/bonfyre-surface-maturity"
source_frontier="$ROOT/cmd/BonfyreSourceFrontier/bonfyre-source-frontier"
cms_bin="$HOME_DIR/.bonfyre/bin/bonfyre-cms"
scoped="$HOME_DIR/.bonfyre/bin/bonfyre-scoped-demand"
queue="$HOME_DIR/.bonfyre/bin/bonfyre-queue"
bun="$HOME_DIR/.bun/bin/bun"
precompute="$ROOT/cmd/BonfyreExternalEstate/tools/precompute_subject_fabric.mjs"
readers="$ROOT/scripts/publisher_readers.mjs"
organ="$ROOT/scripts/organ_drift.sh"
schemas="$ROOT/cmd/BonfyreCMS/content-types"

for path in "$cms" "$items" "$frontier" "$fabric"; do [ -f "$path" ] || { echo "estate-maturity-refresh: missing $path" >&2; exit 66; }; done
[ -d "$foreign/receipts" ] && [ -d "$foreign/bodies" ] || { echo "estate-maturity-refresh: ForeignTwin estate unavailable" >&2; exit 66; }
for path in "$scene" "$semantic" "$maturity" "$source_frontier" "$cms_bin" "$scoped" "$queue" "$bun" "$organ"; do [ -x "$path" ] || { echo "estate-maturity-refresh: executable unavailable $path" >&2; exit 66; }; done
for path in "$precompute" "$readers"; do [ -f "$path" ] || { echo "estate-maturity-refresh: owner source unavailable $path" >&2; exit 66; }; done
command -v sqlite3 >/dev/null 2>&1 || { echo "estate-maturity-refresh: sqlite3 required" >&2; exit 69; }
command -v shasum >/dev/null 2>&1 || { echo "estate-maturity-refresh: shasum required" >&2; exit 69; }

tmp=$(mktemp -d "$estate/.maturity-refresh.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
candidate="$tmp/subject_fabric.db"
manifest="$tmp/subject_fabric.json"
old_hash=$(shasum -a 256 "$fabric" | awk '{print $1}')
sqlite3 "$fabric" ".backup '$tmp/subject_fabric.before.db'"

export BONFYRE_CMS_BINARY="$cms_bin"
export BONFYRE_CMS_SCHEMAS="$schemas"

# Publisher identity, joined only on evidence the estate already holds: the
# census channel's root, an official-website host (or one label below it, never
# a shared publishing host), and a name every retained item agrees on. The
# ledger is read from a copy so this step never moves admission state; the CMS
# owner applies the rows stale-safe, so a name or QID set since is never
# overwritten. What stays unresolved is DISCOVER or HUMAN_VERIFY work.
sqlite3 "$frontier" ".backup '$tmp/frontier.identity.db'"
"$source_frontier" census-link --db "$tmp/frontier.identity.db" --cms "$cms" --items "$items" \
  --identity-backfill-out "$tmp/identity-backfill.jsonl" > "$tmp/identity-backfill.json"
"$cms_bin" entry import-jsonl internet_source "$tmp/identity-backfill.jsonl" --mode update \
  --db "$cms" --schemas "$schemas" > "$tmp/identity-applied.json" 2>"$tmp/identity-applied.log"
identity_rows=$(wc -l < "$tmp/identity-backfill.jsonl" | tr -d ' ')
"$source_frontier" gaps --db "$tmp/frontier.identity.db" > "$tmp/identity-gaps.json"

sites=$(sqlite3 "$cms" "SELECT DISTINCT site_id FROM site_program WHERE status='published' AND json_valid(capability_programs) AND json_array_length(capability_programs)>0 ORDER BY site_id;")
[ -n "$sites" ] || { echo "estate-maturity-refresh: no capability-program SiteProgram is published" >&2; exit 65; }
site_count=0
for site in $sites; do
  case "$site" in *[!A-Za-z0-9_.:-]*|'') echo "estate-maturity-refresh: unsafe site id" >&2; exit 65;; esac
  compiled=$("$scene" compile --db "$cms" --ns root --site "$site" --actor-scope public --schemas "$schemas")
  printf '%s\n' "$compiled" | grep -Eq '"apertures":\[[^]]+' || { echo "estate-maturity-refresh: $site has no compiled capability aperture" >&2; exit 65; }
  "$scene" sync --db "$cms" --ns root --site "$site" --actor-scope public --schemas "$schemas" > "$tmp/scene-$site.json"
  site_count=$((site_count + 1))
done
persisted_action_scenes=$(sqlite3 "$cms" "SELECT COUNT(*) FROM surface_scene WHERE status='published' AND json_valid(operations) AND json_array_length(json_extract(operations,'$.apertures'))>0;")
[ "$persisted_action_scenes" -gt 0 ] || { echo "estate-maturity-refresh: SurfaceScene sync persisted no action apertures" >&2; exit 65; }

"$bun" --no-env-file --no-install "$precompute"   --cms-db "$cms" --demand-db "$cms" --frontier-db "$frontier"   --scoped-demand-bin "$scoped" --surface-scene-bin "$scene" --scene-db "$cms"   --out "$candidate" --manifest "$manifest" > "$tmp/precompute.log"
"$bun" --no-env-file --no-install "$readers"   --fabric-db "$candidate" --estate "$estate" --carry-from "$fabric" --apply > "$tmp/readers.log"
"$bun" --no-env-file --no-install "$precompute" --repair-actions-from-scenes "$candidate" > "$tmp/actions.json"

integrity=$(sqlite3 "$candidate" "PRAGMA integrity_check;")
[ "$integrity" = "ok" ] || { echo "estate-maturity-refresh: candidate fabric integrity failed" >&2; exit 74; }
bundles=$(sqlite3 "$candidate" "SELECT COUNT(*) FROM subject_bundle;")
readers_count=$(sqlite3 "$candidate" "SELECT COUNT(*) FROM subject_reader WHERE state IN ('admitted','active');")
scenes_count=$(sqlite3 "$candidate" "SELECT COUNT(*) FROM subject_scene;")
actions=$(sqlite3 "$candidate" "SELECT COUNT(*) FROM subject_action;")
publisher_bundle_gap=$(sqlite3 "$cms" "ATTACH DATABASE '$candidate' AS fabric; SELECT COUNT(DISTINCT p.subject_address) FROM site_presentation p WHERE p.status='published' AND p.subject_address LIKE 'institution:publisher:%' AND NOT EXISTS (SELECT 1 FROM fabric.subject_bundle b WHERE b.subject=p.subject_address);")
publisher_reader_gap=$(sqlite3 "$cms" "ATTACH DATABASE '$candidate' AS fabric; SELECT COUNT(DISTINCT p.subject_address) FROM site_presentation p WHERE p.status='published' AND p.subject_address LIKE 'institution:publisher:%' AND NOT EXISTS (SELECT 1 FROM fabric.subject_reader r WHERE r.subject=p.subject_address AND r.state IN ('admitted','active'));")
frontier_reader_gap=$(sqlite3 "$cms" "ATTACH DATABASE '$frontier' AS frontier; ATTACH DATABASE '$candidate' AS fabric; SELECT COUNT(DISTINCT f.census_source) FROM frontier.channel f JOIN internet_source s ON s.source=f.census_source AND s.status='published' WHERE f.state IN ('admitted','active') AND COALESCE(f.census_source,'')<>'' AND NOT EXISTS (SELECT 1 FROM fabric.subject_reader r WHERE r.census_source=f.census_source AND r.state IN ('admitted','active'));")
[ "$bundles" -gt 0 ] && [ "$readers_count" -gt 0 ] && [ "$scenes_count" -gt 0 ] && [ "$actions" -gt 0 ] || { echo "estate-maturity-refresh: candidate fabric is shallow" >&2; exit 65; }
[ "$publisher_bundle_gap" -eq 0 ] && [ "$publisher_reader_gap" -eq 0 ] && [ "$frontier_reader_gap" -eq 0 ] || { echo "estate-maturity-refresh: candidate fabric loses owner coverage" >&2; exit 65; }

current_hash=$(shasum -a 256 "$fabric" | awk '{print $1}')
[ "$current_hash" = "$old_hash" ] || { echo "estate-maturity-refresh: live fabric changed during candidate build" >&2; exit 75; }
mv -f "$candidate" "$fabric"

"$queue" account-work index   --db "$account" --subject user:19 --scope self --owner user:19   --source "operator=$HOME_DIR/.bonfyre/operator/operator.sqlite3"   --source "fabric=$HOME_DIR/.bonfyre/dev-fabric/fabric.sqlite3"   --source "workgraph=$HOME_DIR/.bonfyre/edge-workgraph/workgraph.db"   --source "workgraph=$HOME_DIR/.bonfyre/estate/native-architecture-workgraph.db"   --source "workgraph=$HOME_DIR/.bonfyre/estate-fabric/fabric.db" > "$tmp/account.json"

"$semantic" estate-evidence-sync --db "$control" --cms "$cms" --foreign-root "$foreign" > "$tmp/evidence.json"

audit=$("$maturity" estate-audit "$estate")
printf '%s\n' "$audit" | grep -q '"identity":"backfill"' || { echo "estate-maturity-refresh: identity axis changed unexpectedly" >&2; exit 65; }
printf '%s\n' "$audit" | grep -q '"action":"covered"' || { echo "estate-maturity-refresh: action axis still open" >&2; exit 65; }
printf '%s\n' "$audit" | grep -q '"account":"covered"' || { echo "estate-maturity-refresh: account axis still open" >&2; exit 65; }
printf '%s\n' "$audit" | grep -q '"evidence":"covered"' || { echo "estate-maturity-refresh: evidence axis still open" >&2; exit 65; }
printf '%s\n' "$audit" | grep -q '"cross_axis_open":1' || { echo "estate-maturity-refresh: more than identity remains open" >&2; printf '%s\n' "$audit" >&2; exit 65; }

plan=$("$maturity" backfill-plan "$estate")
printf '%s\n' "$plan" | grep -q '"owner":"BonfyreSourceFrontier"' || { echo "estate-maturity-refresh: identity backfill owner missing" >&2; exit 65; }
for owner in 'SubjectFabric/AuthorityGraph' 'Fabric/AurekaiActivity/BonfyreMeter' 'EvidenceGraph/ProofFrontierGraph/ForeignTwinGraph'; do
  if printf '%s\n' "$plan" | grep -q "\"owner\":\"$owner\""; then echo "estate-maturity-refresh: closed axis returned to plan: $owner" >&2; exit 65; fi
done

for target in bonfyre-surface-scene bonfyre-semantic-optimizer bonfyre-surface-maturity; do
  "$organ" --sync --only "$target" --json > "$tmp/$target-sync.json"
  "$organ" --verify --only "$target" --json > "$tmp/$target-verify.json"
done

# Detail-page facets (topics, printed names, credit lines, linked publishers)
# are derived from the archive and the retained bodies; written aside and moved
# into place so the running server never reads a half-built store.
web="$ROOT/cmd/BonfyreWebAddress/bonfyre-web-address"
if [ -x "$web" ]; then
  "$web" detail-depth derive "$items" "$foreign/bodies" "$estate/detail-depth.next.db" > "$tmp/detail-depth.json"
  mv -f "$estate/detail-depth.next.db" "$estate/detail-depth.db"
fi

printf '{"schema":"bonfyre.estate-maturity-refresh.v1","capability_sites":%s,"persisted_action_scenes":%s,"bundles":%s,"readers":%s,"scenes":%s,"actions":%s,"publisher_bundle_gap":%s,"publisher_reader_gap":%s,"frontier_reader_gap":%s}\n'   "$site_count" "$persisted_action_scenes" "$bundles" "$readers_count" "$scenes_count" "$actions" "$publisher_bundle_gap" "$publisher_reader_gap" "$frontier_reader_gap"
printf '%s\n' "$audit"
printf '%s\n' "$plan"
printf '{"schema":"bonfyre.estate-identity-backfill.v1","rows":%s,"backfill":%s,"applied":%s,"gaps":%s}\n' \
  "$identity_rows" "$(cat "$tmp/identity-backfill.json")" "$(cat "$tmp/identity-applied.json")" "$(cat "$tmp/identity-gaps.json")"
"$maturity" lake-check "$estate"
