#!/bin/sh
# Compile the approved site-fragment Mustach renderer to a native constant.
# This does not bypass account, source, or publication checks.
#   compile_authoring_fragment.sh --write | --check
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
source="$here/templates/site_fragment.mustache"
output="$here/../BonfyreWebAddress/generated/native_site_fragment_template.h"
[ "$(wc -c < "$source")" -le 8192 ] || { echo "unsafe approved fragment template" >&2; exit 1; }
if grep -Eiq '\{\{\{|\{\{&|\{\{=|<script|<iframe|onerror=' "$source"; then echo "unsafe approved fragment template" >&2; exit 1; fi
digest=$(shasum -a 256 "$source" | cut -d' ' -f1)
generated=$(printf '/* GENERATED; source: cmd/BonfyreSurfaceScene/templates/site_fragment.mustache\n * SHA256: %s\n * Source owners and publication rules are separate. */\n#ifndef BF_NATIVE_SITE_FRAGMENT_TEMPLATE_H\n#define BF_NATIVE_SITE_FRAGMENT_TEMPLATE_H\nstatic const char bf_native_site_fragment_template[]=%s;\n#endif\nx' "$digest" "$(jq -Rs . < "$source")")
generated=${generated%x}
case "${1:-}" in
  --write) printf '%s' "$generated" > "$output"; echo "native site-fragment template generated" ;;
  --check) [ "$(printf '%sx' "$generated")" = "$(cat "$output"; printf x)" ] || { echo "stale native fragment template" >&2; exit 1; }; echo "native site-fragment Mustach parity PASS" ;;
  *) exit 2 ;;
esac
