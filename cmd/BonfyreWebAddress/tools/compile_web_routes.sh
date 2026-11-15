#!/bin/sh
# schemas/control/web-routes.cue -> generated/native_web_routes.h
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
out="$here/generated/native_web_routes.h"; tmp="$out.tmp.$$"
cue vet -c "$repo/schemas/control/web-routes.cue"
routes=$(cue export "$repo/schemas/control/web-routes.cue" -e web_routes --out json)
{
  echo "/* Generated from schemas/control/web-routes.cue by tools/compile_web_routes.sh. Do not edit. */"
  echo "#ifndef BONFYRE_NATIVE_WEB_ROUTES_H"
  echo "#define BONFYRE_NATIVE_WEB_ROUTES_H"
  echo "#include \"../src/web_request.h\""
  # An owner compiled elsewhere keeps its own argument list; it is called
  # with the request fields its row names.
  printf '%s\n' "$routes" | jq -r '
    [.[] | select(.call)] | unique_by(.owner)[] |
    "static int bf_route_call_\(.owner)(const BfRequest *r) {\n    return \(.owner)(\(.call | map(if . == "query_opt" then "bf_request_query_opt(r)" else "r->" + . end) | join(", ")));\n}"'
  echo "static const BfRoute bf_web_routes[] = {"
  printf '%s\n' "$routes" | jq -r '
    def kind: {"exact": "BF_ROUTE_EXACT", "strict": "BF_ROUTE_STRICT", "prefix": "BF_ROUTE_PREFIX", "any": "BF_ROUTE_ANY"}[.match];
    def when: (.when // []) | if length == 0 then "0" else map("BF_WHEN_" + ascii_upcase) | join(" | ") end;
    .[] | "    {\(if .path != null then "\"" + .path + "\"" else "NULL" end), \(kind), \(if .call then "bf_route_call_" else "" end)\(.owner), \(when), \(if .as then "\"" + .as + "\"" else "NULL" end)},"'
  echo "};"
  echo "static inline int bf_web_route_dispatch(const BfRequest *request) {"
  echo "    return bf_route_dispatch(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]), request);"
  echo "}"
  echo "static inline int bf_web_route_ask(BfRouteOwner owner, const BfRequest *request) {"
  echo "    return bf_route_ask(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]), owner, request);"
  echo "}"
  echo "#endif"
} > "$tmp"
mv "$tmp" "$out"
