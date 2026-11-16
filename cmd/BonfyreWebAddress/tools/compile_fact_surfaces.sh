#!/bin/sh
# schemas/control/fact-surfaces.cue -> generated/native_fact_surfaces.h (the
# renderer's table) and architecture/generated/fact-surfaces.yaff (the
# consumer of every wired fact and one interaction per producer), so a row
# changes the page and the atlas together.  --check compares instead of writing.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
check=0; [ "${1:-}" = "--check" ] && check=1
header="$here/generated/native_fact_surfaces.h"
yaff="$repo/architecture/generated/fact-surfaces.yaff"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/fact-surfaces.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
fail() { echo "compile_fact_surfaces: $*" >&2; exit 1; }

cue vet -c "$repo/schemas/control/fact-surfaces.cue"
rows=$(cd "$repo/schemas/control" && cue export . -e factSurfaces --out json)
routes=$(cue export "$repo/schemas/control/web-routes.cue" -e web_routes --out json)
# A page row reads a store this program is declared to open, and nothing else.
stores=$(cd "$repo" && cue export ./schemas/storage/stores/ -e store_schemas --out json | jq -c '
  [to_entries[] | .key as $owner | (.value.stores // {}) | to_entries[] |
   select($owner == "cmd/BonfyreWebAddress" or ((.value.opened_by // []) | index("cmd/BonfyreWebAddress"))) |
   select(.value.kind == "sqlite") |
   {name: .key, macro: .value.macro, env: (.value.env // null),
    policy: (if .value.lease then "LEASE" elif .value.durability then "DURABILITY" else null end)}]')
bad=$(printf '%s\n' "$rows" | jq -r --argjson s "$stores" '.[] | select(.read) | .read.store as $n |
  select([$s[] | select(.name == $n and .policy != null and (.env != null or $n == "matrix"))] | length == 0) | .fact')
[ -z "$bad" ] || fail "page rows name a store WebAddress does not open with a declared policy and variable: $bad"
bad=$(printf '%s\n' "$rows" | jq -r '.[] | select(.read) | select((.read.columns | length) > 12) | .fact')
[ -z "$bad" ] || fail "more than twelve columns: $bad"
bad=$(printf '%s\n' "$rows" | jq -r '.[] | select(.read) | .read.sql | select(test("(?i)\\b(insert|update|delete|replace|attach|pragma|create|drop|alter|vacuum)\\b"))')
[ -z "$bad" ] || fail "a read is not a plain SELECT: $bad"
bad=$(printf '%s\n' "$rows" | jq -r --argjson r "$routes" '.[] | select(.route and (.on // "estate") == "estate") | .route as $p |
  select([$r[] | select(.path == $p)] | length == 0) | .route')
[ -z "$bad" ] || fail "route rows name paths web-routes.cue does not declare: $bad"
dup=$(printf '%s\n' "$rows" | jq -r '[.[] | .fact] | group_by(.) | map(select(length > 1) | .[0]) | .[]')
[ -z "$dup" ] || fail "a fact is declared twice: $dup"
dup=$(printf '%s\n' "$rows" | jq -r '[.[] | select(.slug) | .slug] | group_by(.) | map(select(length > 1) | .[0]) | .[]')
[ -z "$dup" ] || fail "a slug is used twice: $dup"
# Every producer is a real architecture.
ids=$(cd "$repo" && find architecture -name '*.yaff' ! -path 'architecture/generated/fact-surfaces.yaff' -exec awk '/^architecture /{print $2}' {} + | sort -u)
for owner in $(printf '%s\n' "$rows" | jq -r '.[].owner' | sort -u); do
  printf '%s\n' "$ids" | grep -qx "$owner" || fail "owner $owner is not an architecture"
done

{
  echo "/* Generated from schemas/control/fact-surfaces.cue by tools/compile_fact_surfaces.sh. Do not edit. */"
  echo "#ifndef BONFYRE_NATIVE_FACT_SURFACES_H"
  echo "#define BONFYRE_NATIVE_FACT_SURFACES_H"
  echo "#include \"store_schema.h\""
  echo "typedef struct {"
  echo "    const char *fact, *owner, *audience, *title, *lede, *empty, *presentation;"
  echo "    const char *slug, *route, *slot, *on;                 /* page rows: slug; route rows: route, slot, on */"
  echo "    const char *store, *env, *home_file, *sql, *count_sql;"
  echo "    const char *const *columns; unsigned ncolumns; int limit, cols_desktop, cols_phone;"
  echo "    BfStoreDurability policy;"
  echo "} BfFactSurface;"
  printf '%s\n' "$rows" | jq -r 'to_entries[] | select(.value.read) |
    "static const char *const bf_fact_columns_\(.key)[] = {\(.value.read.columns | map(@json) | join(", "))};"'
  echo "static const BfFactSurface bf_fact_surfaces[] = {"
  printf '%s\n' "$rows" | jq -r --argjson s "$stores" '
    def str(v): if v != null then v | @json else "NULL" end;
    to_entries[] | .key as $i | .value as $r |
    (if $r.read then ($s[] | select(.name == $r.read.store)) else null end) as $st |
    "    {\(str($r.fact)), \(str($r.owner)), \(str($r.audience)), \(str($r.title)), \(str($r.lede)), \(str($r.empty)), \(str($r.presentation)),\n" +
    "     \(str($r.slug)), \(str($r.route)), \(str($r.slot)), \(str($r.on)),\n" +
    (if $r.read then
      "     \(str($r.read.store)), \(if $st.env then "BF_STORE_" + $st.macro + "_ENV" else "\"BONFYRE_MATRIX_DB\"" end), \(str($r.read.home_file)), \(str($r.read.sql)), \(str($r.read.count_sql)),\n" +
      "     bf_fact_columns_\($i), \($r.read.columns | length), \($r.read.limit), \($r.layout.desktop), \($r.layout.phone), BF_STORE_\($st.macro)_\($st.policy)},"
     else
      "     NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, \($r.layout.desktop), \($r.layout.phone), {0}},"
     end)'
  echo "};"
  echo "#define BF_FACT_SURFACES (sizeof(bf_fact_surfaces) / sizeof(bf_fact_surfaces[0]))"
  echo "#endif"
} > "$tmp/header"

witness="cmd/BonfyreWebAddress/tests/fact_surfaces_test.sh"
{
  echo "atlas fact-surfaces"
  echo
  echo "# Generated from schemas/control/fact-surfaces.cue by"
  echo "# cmd/BonfyreWebAddress/tools/compile_fact_surfaces.sh. Do not edit."
  echo
  echo "architecture fact-surface-registry"
  echo "  canonical_name FactSurfaceRegistry"
  echo "  family surface"
  echo "  domain Surface / Fact surfaces"
  echo "  layer surface/web"
  echo "  semantic_question Which fact an organ produces reaches which person, on which page, through which read?"
  echo "  purpose One CUE registry names each produced fact, its producer, its read (a declared store and one bounded SELECT, or the declared route that already shows it), its audience and where it shows. One WebAddress renderer draws every page row at /x/<slug> inside the estate shell; /x lists every row the viewer may see."
  echo "  maturity implemented"
  printf '%s\n' "$rows" | jq -r '.[] | "  consumes \(.fact)"'
  echo "  invariant every page row reads one declared store read-only through its declared lease, with one SELECT bound to a row limit, a progress deadline and a per-cell byte cap"
  echo "  invariant public rows are only facts already public; member rows need a signed-in session; admin rows need the bonfyre admin grant /studio/admin checks"
  echo "  invariant no row submits, publishes, pays or changes a record"
  echo "  forbidden_inference a fact shown -> it is current, verified or the viewer may act on it"
  echo "  forbidden_inference a fact listed in the registry -> that its producer is healthy or still running"
  echo "  forbidden_inference an empty surface -> that the producer has nothing; the store may be absent on this host"
  echo "  known_gap a fact whose producer writes no store WebAddress may open (files, JSON receipts, stores absent on this host) has no row; tools/compile_fact_surfaces.sh lists none of them"
  echo "  source_path schemas/control/fact-surfaces.cue"
  echo "  source_path cmd/BonfyreWebAddress/tools/compile_fact_surfaces.sh"
  echo "  source_path cmd/BonfyreWebAddress/src/fact_surface_web.h"
  echo "  source_path schemas/storage/stores/cmd-BonfyreAdmission.cue"
  echo "  witness $witness builds stores from nothing, shows a public row to anyone, sends a signed-out visitor to sign in for an admin row, refuses a signed-in non-admin, and lists only permitted rows on /x"
  echo
  printf '%s\n' "$rows" | jq -r '.[] |
    def id: ascii_downcase | gsub("[^a-z0-9]+"; "-");
    "interaction \(.owner)-fact-\(.fact | gsub("(?<a>[a-z0-9])(?<b>[A-Z])"; "\(.a)-\(.b)") | id)-reaches-people\n" +
    "  source \(.owner)\n" +
    "  destination fact-surface-registry\n" +
    "  relation produced_fact_to_\(.audience)_surface\n" +
    (if .read then
      "  preserves the records the producer wrote to the \(.read.store) store, read as written, newest first, at most \(.read.limit) rows and a total count\n" +
      "  drops columns the row does not name, cells past 240 bytes, rows past the limit and every write path\n"
     else
      "  preserves the page the producer already serves at \(.route) (\(.slot)), unchanged\n" +
      "  drops nothing the route shows; the row adds no read of its own\n"
     end) +
    "  forbidden_inference \(.fact) shown at \(.slug // .route | if startswith("/") then . else "/x/" + . end) -> it is current, verified or the viewer may act on it\n" +
    "  invalidates_on the producer changing its \(if .read then "table or columns" else "route" end), or the row'"'"'s audience changing\n" +
    "  witness '"$witness"'\n" +
    "  binding schemas/control/fact-surfaces.cue\n" +
    "  binding cmd/BonfyreWebAddress/src/fact_surface_web.h\n"'
  echo "interaction fact-surface-registry-renders-in-web-address"
  echo "  source fact-surface-registry"
  echo "  destination cmd-web-address"
  echo "  relation registry_rows_to_shell_pages"
  echo "  preserves each row's audience check, store lease, row limit and plain words"
  echo "  drops every write path; the renderer has none"
  echo "  forbidden_inference a page under /x -> a new authority, a verified record or a permitted action"
  echo "  invalidates_on the route table, the shell or the session checks changing"
  echo "  witness $witness"
  echo "  binding cmd/BonfyreWebAddress/generated/native_fact_surfaces.h"
  echo "  binding cmd/BonfyreWebAddress/src/fact_surface_web.h"
} > "$tmp/yaff"

if [ "$check" = 1 ]; then
  cmp -s "$tmp/header" "$header" || fail "$header is stale; run tools/compile_fact_surfaces.sh"
  cmp -s "$tmp/yaff" "$yaff" || fail "$yaff is stale; run tools/compile_fact_surfaces.sh"
  echo "fact surfaces: $(printf '%s\n' "$rows" | jq length) rows agree"
  exit 0
fi
mkdir -p "$(dirname "$yaff")"
mv "$tmp/header" "$header"; mv "$tmp/yaff" "$yaff"
echo "fact surfaces: $(printf '%s\n' "$rows" | jq length) rows ($(printf '%s\n' "$rows" | jq '[.[] | select(.read)] | length') pages)"
