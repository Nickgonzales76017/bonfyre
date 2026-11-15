#!/bin/sh
# schemas/storage/stores/*.cue -> <owner>/generated/store_schema.h, one header
# per native owner, each fragment a string macro holding exactly the DDL text.
#   scripts/compile-store-schemas.sh          regenerate every header
#   scripts/compile-store-schemas.sh --check  refuse if any header has drifted
set -eu
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
mode=${1:-write}
cd "$repo"
cue vet -c ./schemas/storage/stores/
json=$(cue export ./schemas/storage/stores/ -e store_schemas --out json)
drift=0
for owner in $(printf '%s\n' "$json" | jq -r 'keys[]'); do
  out="$owner/generated/store_schema.h"
  guard=$(printf '%s' "$owner" | tr '[:lower:]/' '[:upper:]_' | sed 's/[^A-Z0-9_]/_/g')
  tmp=$(mktemp "${TMPDIR:-/tmp}/store-schema.XXXXXX")
  {
    echo "/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */"
    echo "#ifndef BONFYRE_STORE_SCHEMA_${guard}_H"
    echo "#define BONFYRE_STORE_SCHEMA_${guard}_H"
    # One C literal per statement line; adjacent literals concatenate to the
    # fragment byte for byte (jq's JSON escapes are C escapes for this text).
    printf '%s\n' "$json" | jq -r --arg o "$owner" '
      .[$o].fragments[] |
      ([.statements[].sql | [splits("(?<=\n)")] | .[] | select(. != "")] | map("    " + tojson) | join(" \\\n")) as $body |
      "/* \(.source) */\n#define \(.macro) \\\n\($body)"'
    echo "#endif"
  } > "$tmp"
  if grep -q '\\u00' "$tmp"; then echo "store schema: $owner holds a control character C cannot take as written" >&2; rm -f "$tmp"; exit 1; fi
  if [ "$mode" = --check ]; then
    if ! cmp -s "$tmp" "$out"; then echo "store schema: $out differs from schemas/storage/stores; run scripts/compile-store-schemas.sh" >&2; drift=1; fi
    rm -f "$tmp"
  else
    mkdir -p "$owner/generated"
    mv "$tmp" "$out"
  fi
done
[ "$drift" -eq 0 ] || exit 1
[ "$mode" = --check ] && echo "store schema: headers agree with schemas/storage/stores" || true
