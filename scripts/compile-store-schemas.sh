#!/bin/sh
# schemas/storage/stores/*.cue -> <owner>/generated/store_schema.h, one header
# per native owner: each fragment a string macro holding exactly the DDL text,
# and each declared store its kind, shape constants and durability policy
# (lib/libbonfyre/include/bf_store_durability.h). An owner named in a store's
# opened_by gets that store's constants too, so a shared file is declared once.
#   scripts/compile-store-schemas.sh          regenerate every header
#   scripts/compile-store-schemas.sh --check  refuse if any header has drifted
set -eu
repo=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
mode=${1:-write}
cd "$repo"
cue vet -c ./schemas/storage/stores/
json=$(cue export ./schemas/storage/stores/ -e store_schemas --out json)
drift=0
owners=$(printf '%s\n' "$json" | jq -r '[keys[], (.[] | (.stores // {})[] | .opened_by[])] | unique[]')
for owner in $owners; do
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
      (.[$o].fragments // [])[] |
      ([.statements[].sql | [splits("(?<=\n)")] | .[] | select(. != "")] | map("    " + tojson) | join(" \\\n")) as $body |
      "/* \(.source) */\n#define \(.macro) \\\n\($body)"'
    # Declared stores this owner opens: its own, then those it is named in.
    printf '%s\n' "$json" | jq -r --arg o "$owner" '
      def c: tojson;
      def sqltype: {"int64":"INTEGER","float64":"REAL","utf8":"TEXT","bool":"INTEGER","timestamp":"TEXT"}[.];
      [to_entries[] | .key as $declarer | (.value.stores // {}) | to_entries[] |
        select($declarer == $o or (.value.opened_by | index($o))) | {declarer: $declarer, name: .key, s: .value}] |
      sort_by(.declarer, .name)[] |
      .s as $s | "BF_STORE_\($s.macro)" as $p | $s.durability as $d |
      "/* store \(.name): \($s.kind), declared by \(.declarer) */",
      "#define \($p)_KIND \($s.kind | c)",
      (if $s.env then "#define \($p)_ENV \($s.env | c)" else empty end),
      (if $d then "#define \($p)_DURABILITY { \(.name | c), \($d.journal_mode | c), \($d.synchronous | c), \($d.wal_autocheckpoint), \($d.journal_size_limit)LL, \($d.wal_truncate_pages), \($d.busy_timeout_ms), \($d.read_txn_max_ms) }" else empty end),
      (if $s.attach then "#define \($p)_ATTACH \("ATTACH DATABASE ? AS \($s.attach.as)" | c)", "#define \($p)_ATTACH_READONLY \(if $s.attach.readonly then 1 else 0 end)" else empty end),
      (if $s.kind == "vector" then $s.vector as $v |
        ({"float32":"float","int8":"int8","bit":"bit"}[$v.quantization]) as $elem |
        "#define \($p)_VECTOR_TABLE \($v.table | c)",
        (if $v.dimension == "runtime" then "#define \($p)_VECTOR_DIMENSION 0 /* runtime: the owner passes the width to _VECTOR_DDL */" else "#define \($p)_VECTOR_DIMENSION \($v.dimension)" end),
        "#define \($p)_VECTOR_METRIC \($v.metric | c)",
        "#define \($p)_VECTOR_QUANTIZATION \($v.quantization | c)",
        "#define \($p)_VECTOR_MODEL \($v.model | c)",
        "#define \($p)_VECTOR_DDL \("CREATE VIRTUAL TABLE IF NOT EXISTS \($v.table) USING vec0(id TEXT PRIMARY KEY, embedding \($elem)[\(if $v.dimension == "runtime" then "%d" else $v.dimension end)]\(if $v.metric == "l2" then "" else " distance_metric=\($v.metric)" end))" | c)"
       else empty end),
      (if $s.kind == "ranker" then $s.ranker as $r |
        "#define \($p)_RANKER_KEY_SPACE \($r.key_space | c)",
        "#define \($p)_RANKER_KEY_BITS \($r.key_bits)",
        "#define \($p)_RANKER_WEIGHTS \($r.weights | c)"
       else empty end),
      (if $s.kind == "columnar" then $s.columnar as $k |
        "#define \($p)_COLUMNAR_FORMAT \($k.format | c)",
        "#define \($p)_COLUMNAR_COLD_GLOB \($k.cold_glob | c)",
        "#define \($p)_COLUMNAR_HOT_TABLE \($k.hot_table | c)",
        "#define \($p)_COLUMNAR_PROMOTE \($k.promote | c)",
        "#define \($p)_COLUMNAR_COLUMNS \([$k.columns[] | "\(.name):\(.type)"] | join(",") | c)",
        "#define \($p)_COLUMNAR_COLUMN_COUNT \($k.columns | length)",
        "#define \($p)_COLUMNAR_HOT_DDL \("CREATE TABLE IF NOT EXISTS \($k.hot_table)(\([$k.columns[] | "\(.name) \(.type | sqltype)"] | join(", ")));" | c)"
       else empty end),
      (if $s.kind == "dynamic" then $s.dynamic as $y |
        "#define \($p)_DYNAMIC_DECLARING_TABLE \($y.declaring_table | c)",
        "#define \($p)_DYNAMIC_SHARD \($y.shard | c)",
        "#define \($p)_DYNAMIC_COLUMNS \($y.columns | join(",") | c)",
        "#define \($p)_DYNAMIC_SELECT \("SELECT \($y.columns | join(", ")) FROM \($y.declaring_table)" | c)",
        (if $y.shard != "none" then "#define \($p)_DYNAMIC_ATTACH \("ATTACH DATABASE ?1 AS ?2" | c)" else empty end)
       else empty end)'
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
