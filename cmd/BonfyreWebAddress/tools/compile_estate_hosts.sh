#!/bin/sh
# schemas/control/estate-hosts.cue -> generated/native_estate_hosts.h
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
out="$here/generated/native_estate_hosts.h"; tmp="$out.tmp.$$"
cue vet -c "$repo/schemas/control/estate-hosts.cue"
json=$(cue export "$repo/schemas/control/estate-hosts.cue" -e estate_hosts --out json)
{
  echo "/* Generated from schemas/control/estate-hosts.cue by tools/compile_estate_hosts.sh. Do not edit. */"
  echo "#ifndef BONFYRE_NATIVE_ESTATE_HOSTS_H"
  echo "#define BONFYRE_NATIVE_ESTATE_HOSTS_H"
  echo "#include <stdio.h>"
  echo "#include <string.h>"
  printf '%s\n' "$json" | jq -r '.[] | "#define BF_ORIGIN_\(.territory | ascii_upcase) \"https://\(.host)\""'
  echo "typedef struct { const char *territory, *host, *origin; int territory_role; } BfEstateHost;"
  echo "static const BfEstateHost bf_estate_hosts[] = {"
  printf '%s\n' "$json" | jq -r '.[] | "    {\"\(.territory)\", \"\(.host)\", BF_ORIGIN_\(.territory | ascii_upcase), \(if .role=="territory" then 1 else 0 end)},"'
  echo "};"
  echo "/* The origin (https://host, no trailing slash) a territory answers on, or NULL. */"
  echo "static inline const char *bf_estate_origin(const char *territory) {"
  echo "    for (size_t i = 0; territory && i < sizeof(bf_estate_hosts) / sizeof(bf_estate_hosts[0]); i++)"
  echo "        if (!strcmp(bf_estate_hosts[i].territory, territory)) return bf_estate_hosts[i].origin;"
  echo "    return NULL;"
  echo "}"
  echo "/* Print a territory URL: origin then path, as written. Returns 0 for an unknown territory. */"
  echo "static inline int bf_estate_url_put(FILE *out, const char *territory, const char *path) {"
  echo "    const char *origin = bf_estate_origin(territory);"
  echo "    if (!out || !origin) return 0;"
  echo "    fputs(origin, out);"
  echo "    if (path) fputs(path, out);"
  echo "    return 1;"
  echo "}"
  echo "#endif"
} > "$tmp"
mv "$tmp" "$out"
