#!/bin/sh
# schemas/control/web-routes.cue -> generated/native_web_routes.h
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
out="$here/generated/native_web_routes.h"; tmp="$out.tmp.$$"
cue vet -c "$repo/schemas/control/web-routes.cue"
{
  echo "/* Generated from schemas/control/web-routes.cue by tools/compile_web_routes.sh. Do not edit. */"
  echo "#ifndef BONFYRE_NATIVE_WEB_ROUTES_H"
  echo "#define BONFYRE_NATIVE_WEB_ROUTES_H"
  echo "#include \"../src/web_request.h\""
  echo "static const BfRoute bf_web_routes[] = {"
  cue export "$repo/schemas/control/web-routes.cue" -e web_routes --out json |
    jq -r '.[] | "    {\"\(.path)\", \(if .match=="prefix" then "BF_ROUTE_PREFIX" else "BF_ROUTE_EXACT" end), \(.owner)},"'
  echo "};"
  echo "static inline int bf_web_route_dispatch(const BfRequest *request) {"
  echo "    return bf_route_dispatch(bf_web_routes, sizeof(bf_web_routes) / sizeof(bf_web_routes[0]), request);"
  echo "}"
  echo "#endif"
} > "$tmp"
mv "$tmp" "$out"
