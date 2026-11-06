#!/bin/sh
# A command named index must discover, or be named something else.
#
# It did not. The capability set was a compiled-in table plus rows seeded once,
# and `index` called rebuild_index, which rebuilds a full-text index over the
# rows that already exist. Nineteen organs were installed into the managed
# binary directory and the registry reported exactly the same count, so the
# estate could not see a capability it had just gained. The failure was silent
# because re-indexing succeeds.
#
# The other half is pricing. A discovered organ has no measured cost, and the
# honest record of that is not zero. Zero is a price, and this estate already
# refuses elsewhere to let an unpriced capability read as a free one.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# A directory that looks like an install tree with one organ the table cannot
# know about.
cp "$bin" "$tmp/bonfyre-capability"
printf '#!/bin/sh\nexit 0\n' > "$tmp/bonfyre-invented-organ"
chmod +x "$tmp/bonfyre-invented-organ"
printf '#!/bin/sh\nexit 0\n' > "$tmp/bonfyre-mcp-proof"
chmod +x "$tmp/bonfyre-mcp-proof"
printf '#!/bin/sh\nexit 0\n' > "$tmp/bonfyre-ghost.archive"
chmod +x "$tmp/bonfyre-ghost.archive"
printf '{"bonfyre-mcp-proof":"BonfyreMCPProof"}\n' > "$tmp/.families.json"

export BONFYRE_BIN_DIR="$tmp"
export BONFYRE_CAPABILITY_DB="$tmp/cap.db"

out=$("$tmp/bonfyre-capability" index 2>&1)
printf '%s\n' "$out" | grep -q 'newly discovered' || {
  echo "index did not report discovery: $out" >&2; exit 1; }

listing=$("$tmp/bonfyre-capability" list 2>/dev/null)

# The organ nobody compiled in must be present.
printf '%s\n' "$listing" | grep -q 'invented-organ' || {
  echo 'an installed organ was not discovered' >&2; exit 1; }
printf '%s\n' "$listing" | grep -q 'ghost' && {
  echo 'an archived executable was indexed as a capability' >&2; exit 1; }

# And it must not carry a price nobody measured.
row=$(printf '%s\n' "$listing" | grep 'invented-organ')
printf '%s\n' "$row" | grep -q 'unmeasured' || {
  echo "a discovered organ was given a cost: $row" >&2; exit 1; }
printf '%s\n' "$row" | grep -qE '0\.0000' && {
  echo "a discovered organ was priced at zero, which reads as free: $row" >&2; exit 1; }

# Exact managed-family identity must win over lossy filename CamelCasing.
mcp=$("$tmp/bonfyre-capability" show mcp-proof)
printf '%s\n' "$mcp" | grep -q '^name          : BonfyreMCPProof$' || {
  echo "managed family identity was not retained: $mcp" >&2; exit 1; }
printf '%s\n' "$mcp" | grep -q '^stage         : evidence/proof$' || {
  echo "acronym family missed Atlas stage: $mcp" >&2; exit 1; }
printf '%s\n' "$mcp" | grep -q '^artifact_out  : Evidence$' || {
  echo "acronym family missed Atlas artifact: $mcp" >&2; exit 1; }

# Discovery is idempotent: running it twice must not duplicate or re-count.
again=$("$tmp/bonfyre-capability" index 2>&1)
printf '%s\n' "$again" | grep -q '0 newly discovered' || {
  echo "second index re-discovered: $again" >&2; exit 1; }

# A curated capability keeps its latency tier, which a scan does not know, and
# its cost reads unmeasured: the dollar figures this table carried were typed in.
printf '%s\n' "$listing" | grep -E '^ingest ' | grep 'instant' | grep -q 'unmeasured' || {
  echo 'a curated row lost its latency tier to discovery, or shows a cost nobody measured' >&2; exit 1; }

# A discovered capability must be matchable, not merely listable. match used to
# score the compiled-in table while list read the database, so the registry
# could show an organ it had just found and then fail to return it for the need
# it answers. That made discovery cosmetic.
cat > "$tmp/atlas-probe.txt" <<'PROBE'
PROBE
matched=$("$tmp/bonfyre-capability" match "invented organ" 2>/dev/null || true)
printf '%s\n' "$matched" | grep -q 'invented-organ' || {
  echo 'a discovered organ was listable but not matchable' >&2; exit 1; }

# And a curated capability must still win on its own curated keywords.
"$tmp/bonfyre-capability" match "ingest a file into the pipeline" 2>/dev/null | grep -q 'ingest' || {
  echo 'a curated capability stopped matching its own keywords' >&2; exit 1; }

# A capability must rank for its own name. The id is the shortest true name a
# capability has and was the one field never scored, and an organ named
# BonfyreSign is one token to a splitter, so the organ whose whole purpose is
# signing did not rank for the verb "sign".
printf '#!/bin/sh\nexit 0\n' > "$tmp/bonfyre-widget-press"
chmod +x "$tmp/bonfyre-widget-press"
"$tmp/bonfyre-capability" index >/dev/null 2>&1
"$tmp/bonfyre-capability" match "widget-press" 2>/dev/null | grep -q 'widget-press' || {
  echo 'a capability did not rank for its own id' >&2; exit 1; }
# BonfyreWidgetPress must split so the parts are reachable.
"$tmp/bonfyre-capability" match "press" 2>/dev/null | grep -q 'widget-press' || {
  echo 'a camel-case organ name did not split into matchable words' >&2; exit 1; }

# Source-only owner-header enrichment: the atlas currently describes Physics
# generically. Its real native header documents Hamiltonian, trajectory,
# branch and compare, so a request describing the work must find it without
# a second skill catalogue, a learned model or a manufactured cost.
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
BONFYRE_REPO_ROOT="$repo"; export BONFYRE_REPO_ROOT
printf '#!/bin/sh\nexit 0\n' > "$tmp/bonfyre-physics"
chmod +x "$tmp/bonfyre-physics"
"$tmp/bonfyre-capability" index >/dev/null 2>&1
physics=$("$tmp/bonfyre-capability" match "Simulate Hamiltonian trajectories, branch state and compare trajectory drift")
first=$(printf '%s\n' "$physics" | awk 'NR==4 {print $2}')
[ "$first" = physics ] || {
  echo "native Physics was not ranked first for the source-described job: $physics" >&2
  exit 1
}

# Feature macros and secondary C comment blocks are source layouts too.
# Rank native command intentions, not installed-name coincidences.
for organ in meter embed frappe-compiler; do
  printf '#!/bin/sh\nexit 0\n' > "$tmp/bonfyre-$organ"
  chmod +x "$tmp/bonfyre-$organ"
done
"$tmp/bonfyre-capability" index >/dev/null 2>&1
for case in \
  'meter|Observed usage accrued economics funding selection basis explicit position' \
  'embed|Embed artifacts into vector space for retrieval and similarity' \
  'frappe-compiler|Compile Frappe applications into stratified business runtime'
do
  expected=${case%%|*}; query=${case#*|}
  result=$("$tmp/bonfyre-capability" match "$query")
  found=$(printf '%s\n' "$result" | awk 'NR==4 {print $2}')
  [ "$found" = "$expected" ] || {
    echo "owner semantics not discoverable for $expected: $result" >&2
    "$tmp/bonfyre-capability" show "$expected" >&2 || true
    exit 1
  }
done

# The default metric is the one that measured better. Word overlap ranked the
# right capability first nine times in ten; fusing a learned cosine managed
# seven, because cosine over one-line descriptions separates weakly enough that
# its ordering is close to arbitrary. The learned metric stays reachable and
# stays out of the default path, and the output always names which ran.
"$tmp/bonfyre-capability" match "ingest a file into the pipeline" 2>/dev/null \
  | head -1 | grep -q 'metric: word overlap]' || {
  echo 'a ranking was produced without naming its metric' >&2; exit 1; }

# Every ranking path names its metric, including the full-text fallback.
"$tmp/bonfyre-capability" match "invented organ" 2>/dev/null | head -1 | grep -q 'metric:' || {
  echo 'a ranking path produced an ordering without naming the metric' >&2; exit 1; }

# An old help probe may not masquerade as an executable, priced command.
sqlite3 "$BONFYRE_CAPABILITY_DB" "INSERT INTO capabilities(id,name,description,binary,command,hardware_tier,cost_estimate,latency_tier,source,updated) VALUES('finance','BonfyreFinance','Usage:','bonfyre-finance','layer','cpu',0.0,'fast','probed',1);"
repair=$("$tmp/bonfyre-capability" index)
printf '%s\n' "$repair" | grep -q '1 legacy help probes refreshed' || { echo "missing atlas repair: $repair" >&2; exit 1; }
finance=$("$tmp/bonfyre-capability" show finance)
printf '%s\n' "$finance" | grep -q 'source        : atlas-projection' || { echo 'missing atlas provenance' >&2; exit 1; }
printf '%s\n' "$finance" | grep -q 'cost est      : unmeasured' || { echo 'invented zero cost' >&2; exit 1; }
printf '%s\n' "$finance" | grep -q '^command       : -$' || { echo 'unverified command remained' >&2; exit 1; }
"$tmp/bonfyre-capability" index | grep -q '0 legacy help probes refreshed' || { echo 'repair not idempotent' >&2; exit 1; }
"$tmp/bonfyre-capability" match "qualified supplier quote and purchase invoice" | grep -q 'offer' || { echo 'missing offer capability' >&2; exit 1; }
echo 'capability discovery: PASS (found, unpriced, idempotent, curated intact, matchable, lexical by default)'
