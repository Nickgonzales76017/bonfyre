#!/bin/sh
# schemas/control/web-routes.cue -> generated/native_web_routes.h
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
out="$here/generated/native_web_routes.h"; tmp="$out.tmp.$$"
trap 'rm -f "$tmp"' EXIT
cue vet -c "$repo/schemas/control/web-routes.cue"
routes=$(cue export "$repo/schemas/control/web-routes.cue" -e web_routes --out json)
# A records row reads a store this program is declared to open, and nothing else.
stores=$(cd "$repo" && cue export ./schemas/storage/stores/ -e store_schemas --out json | jq -c '
  [to_entries[] | .key as $owner | (.value.stores // {}) | to_entries[] |
   select($owner == "cmd/BonfyreWebAddress" or ((.value.opened_by // []) | index("cmd/BonfyreWebAddress"))) |
   {name: .key, kind: .value.kind, owner: $owner}]')
missing=$(printf '%s\n' "$routes" | jq -r --argjson stores "$stores" '
  [.[] | select(.match == "records") | .records.store] - [$stores[].name] | .[]')
if [ -n "$missing" ]; then
  echo "compile_web_routes: records rows name stores not declared as opened by cmd/BonfyreWebAddress: $missing" >&2; exit 1
fi
# The semantic row is the not-found tail's configuration: exactly one, last.
printf '%s\n' "$routes" | jq -e '([.[] | select(.match == "semantic")] | length) == 1 and (.[-1].match == "semantic")' >/dev/null || {
  echo "compile_web_routes: the table must end in exactly one semantic row" >&2; exit 1; }
# A suggested page is a page: a path with no trailing slash and no private segment.
bad=$(printf '%s\n' "$routes" | jq -r '.[] | select(.title and ((.path // "") | (. == "" or test("/_|/$") and . != "/"))) | .path')
if [ -n "$bad" ]; then echo "compile_web_routes: titled rows must name a page path: $bad" >&2; exit 1; fi
{
  echo "/* Generated from schemas/control/web-routes.cue by tools/compile_web_routes.sh. Do not edit. */"
  echo "#ifndef BONFYRE_NATIVE_WEB_ROUTES_H"
  echo "#define BONFYRE_NATIVE_WEB_ROUTES_H"
  echo "#include \"../src/web_request.h\""
  echo "#include \"../src/web_route_kinds.h\""
  # An owner compiled elsewhere keeps its own argument list; it is called
  # with the request fields its row names.
  printf '%s\n' "$routes" | jq -r '
    [.[] | select(.call)] | unique_by(.owner)[] |
    "static int bf_route_call_\(.owner)(const BfRequest *r) {\n    return \(.owner)(\(.call | map(if . == "query_opt" then "bf_request_query_opt(r)" else "r->" + . end) | join(", ")));\n}"'
  # Each record set asks its store whether the path is one of its rows.
  printf '%s\n' "$routes" | jq -r --argjson stores "$stores" '
    [.[] | select(.match == "records") | .records] | unique_by(.name)[] | . as $r |
    ($stores[] | select(.name == $r.store)) as $s |
    "/* records \($r.name): store \($r.store) (\($s.kind), declared by \($s.owner)) */\nstatic int bf_route_records_\($r.name)(const char *target) {\n    return bf_route_records_match(bf_route_store_open_\($r.store), \($r.match_sql | @json), target);\n}"'
  echo "static const BfRoute bf_web_routes[] = {"
  printf '%s\n' "$routes" | jq -r '
    def kind: {"exact": "BF_ROUTE_EXACT", "strict": "BF_ROUTE_STRICT", "prefix": "BF_ROUTE_PREFIX", "any": "BF_ROUTE_ANY",
               "records": "BF_ROUTE_RECORDS", "semantic": "BF_ROUTE_SEMANTIC"}[.match];
    def when: (.when // []) | if length == 0 then "0" else map("BF_WHEN_" + ascii_upcase) | join(" | ") end;
    def str(v): if v != null then v | @json else "NULL" end;
    .[] | "    {\(str(.path)), \(kind), \(if .call then "bf_route_call_" else "" end)\(.owner), \(when), \(str(.as)), \(if .match == "records" then "bf_route_records_" + .records.name else "NULL" end), \(str(.title))},"'
  echo "};"
  echo "/* The records a not-found page may offer, each from its declared store. */"
  echo "static const BfRouteSuggestSet bf_web_route_suggest_sets[] = {"
  printf '%s\n' "$routes" | jq -r '
    [.[] | select(.match == "records" and .records.suggest_sql) | .records] | unique_by(.name)[] |
    "    {\(.name | @json), bf_route_store_open_\(.store), \(.suggest_sql | @json)},"'
  echo "    {NULL, NULL, NULL},"
  echo "};"
  printf '%s\n' "$routes" | jq -r '.[-1].semantic |
    "static const BfRouteSemantic bf_web_route_semantic = {\(.ranker | @json), \(.limit), \(.min_score)};"'
  echo "static inline int bf_web_route_dispatch(const BfRequest *request) {"
  echo "    return bf_route_dispatch(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]), request);"
  echo "}"
  echo "static inline int bf_web_route_ask(BfRouteOwner owner, const BfRequest *request) {"
  echo "    return bf_route_ask(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]), owner, request);"
  echo "}"
  echo "/* The not-found tail: 404 with the nearest declared pages and records. */"
  echo "static int bf_web_route_not_found(const char *path) {"
  echo "    return bf_route_not_found_page(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]),"
  echo "                                   bf_web_route_suggest_sets, sizeof(bf_web_route_suggest_sets) / sizeof(bf_web_route_suggest_sets[0]) - 1,"
  echo "                                   &bf_web_route_semantic, path);"
  echo "}"
  echo "#endif"
} > "$tmp"
mv "$tmp" "$out"
trap - EXIT
