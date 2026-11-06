#!/bin/bash
# Golden vectors for bonfyre-decide (books, monitor, manifest).
#
#   tests/parity.sh                 check the C build against tests/golden
#   tests/parity.sh --record DIR    record vectors from the programs in DIR
#                                   (bonfyre-books/-monitor/-manifest there)
#
# Each line of tests/cases.txt runs once. A model-assisted case talks to
# tests/mock_front.c, a deterministic stand-in for the bonfyre-moe front, so
# answers are a function of the prompt. Deliberate corrections of the Bun
# programs are listed in tests/CORRECTIONS and their vectors re-recorded. Recorded: exit status, stderr, and
# stdout (in full when small, else its sha256), with run timings and machine
# paths normalised. The vectors were recorded from the retired Bun programs
# (components/BonfyreDecision, archived 2026-10-22), so this is their behaviour.
set -u
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd); organ=$(dirname "$here"); repo=$(CDPATH='' cd -- "$organ/../.." && pwd)
mode=check; bin=$organ
if [ "${1:-}" = --record ]; then mode=record; bin=$(CDPATH='' cd -- "$2" && pwd); fi
make -s -C "$organ" >/dev/null || exit 1
tmp=$(mktemp -d); trap 'kill $mock 2>/dev/null; rm -rf "$tmp"' EXIT
cc -O2 -std=gnu11 -D_GNU_SOURCE -I"$repo/lib/libbonfyre/include" "$here/mock_front.c" "$repo/lib/libbonfyre/libbonfyre.a" -lm -o "$tmp/mock_front" || exit 1
head -c 300000 /dev/zero > "$tmp/fixture-model-Q4.gguf"
# The archive path is part of each monitor case's digest: keep it stable.
archive=cmd/BonfyreDecide/tests/fixtures/.items.db
rm -f "$repo/$archive"; sqlite3 "$repo/$archive" < "$here/fixtures/items.sql"
port=$((20000 + $$ % 20000))
"$tmp/mock_front" $port "$tmp/fixture-model-Q4.gguf" >/dev/null 2>&1 & mock=$!
for _ in 1 2 3 4 5 6 7 8 9 10; do curl -s -o /dev/null "http://127.0.0.1:$port/props" && break; sleep 0.2; done
export BONFYRE_HOME="$tmp/home" BONFYRE_OCR_BIN="$here/fixtures/bin/bonfyre-ocr"
unset BONFYRE_LOCAL_MODEL BONFYRE_LOCAL_MODEL_PREFER BONFYRE_ITEM_ARCHIVE_DB
EX="$repo/cmd/BonfyreBooks/examples"; FIX="$here/fixtures"
"$bin/bonfyre-books" learn --history "$EX/history.csv" --out "$tmp/learned.json" >/dev/null || exit 1
mkdir -p "$here/golden"
n=0; fail=0
while IFS= read -r line; do
  n=$((n + 1)); args=()
  read -ra words <<< "$line"
  tool=${words[0]}
  for w in "${words[@]:1}"; do
    if [ "$w" = MOCK ]; then args+=(--model-endpoint "http://127.0.0.1:$port"); continue; fi
    w=${w//ARCHIVE/$archive}; w=${w//EX/$EX}; w=${w//FIX/$FIX}; w=${w//TMP/$tmp}
    case "$w" in */*) ;; *) w=${w//_/ } ;; esac
    args+=("$w")
  done
  (cd "$repo" && "$bin/bonfyre-$tool" "${args[@]}" > "$tmp/out" 2> "$tmp/err"; echo "exit $?" >> "$tmp/err")
  for f in out err; do
    sed -E "s#$tmp#TMP#g; s#$FIX#FIX#g; s#$EX#EX#g; s#127\.0\.0\.1:$port#MOCK#g; s#(\"(seconds|latency_ms|producer_ms)\": )[-0-9.e+]+#\1T#; s#[0-9.]+s on this machine#Ts on this machine#; s# in [0-9.]+s\.# in Ts.#" "$tmp/$f" > "$tmp/$f.n"
  done
  for side in TMP/booked.csv TMP/itad.tsv TMP/itad.held.tsv TMP/learned-out.json; do
    p=${side/TMP/$tmp}; [ -f "$p" ] && { printf '%s sha256 %s\n' "$side" "$(shasum -a 256 < "$p" | cut -d' ' -f1)" >> "$tmp/err.n"; rm -f "$p"; }
  done
  if [ "$(wc -c < "$tmp/out.n")" -gt 4096 ]; then printf 'stdout sha256 %s bytes %s\n' "$(shasum -a 256 < "$tmp/out.n" | cut -d' ' -f1)" "$(wc -c < "$tmp/out.n" | tr -d ' ')" > "$tmp/out.g"
  else cp "$tmp/out.n" "$tmp/out.g"; fi
  cat "$tmp/err.n" "$tmp/out.g" > "$tmp/got"
  if [ $mode = record ]; then cp "$tmp/got" "$here/golden/$n"
  elif ! cmp -s "$tmp/got" "$here/golden/$n"; then
    fail=$((fail + 1)); echo "FAIL $n: $line"; diff "$here/golden/$n" "$tmp/got" | head -12
  fi
done < "$here/cases.txt"
[ $mode = record ] && echo "recorded $n vectors from $bin" && exit 0
[ $fail = 0 ] && echo "bonfyre-decide parity: $n vectors PASS"
exit $fail
