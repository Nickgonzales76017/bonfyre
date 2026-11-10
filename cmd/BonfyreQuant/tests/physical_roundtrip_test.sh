#!/bin/sh
# The benchmark must agree with independently saved and decoded weights.
#   physical_roundtrip_test.sh QUANT SLI MODEL TENSOR
set -eu
quant=$1 sli=$2 model=$3 tensor=$4
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bqfp-roundtrip.XXXXXX")
trap 'rm -rf "$tmp"' 0 1 2 15
run() { rc=0; out=$("$@" 2>"$tmp/err") || rc=$?; [ "$rc" -eq 0 ] || [ "$rc" -eq 2 ] || { cat "$tmp/err" >&2; exit 1; }; printf '%s\n' "$out"; }
run "$quant" encode "$model" --tensor "$tensor" --bits 3 --out "$tmp/tensor.bqfp" >/dev/null
run "$sli" decode --model "$tmp/tensor.bqfp" --out "$tmp/tensor.f32" >/dev/null
comparison=$(run "$quant" compare "$model" --tensor "$tensor" --weights "$tmp/tensor.f32")
benchmark=$(run "$quant" roundtrip "$model" --tensor "$tensor" --bits 3)
for metric in cos rmse; do
  a=$(printf '%s\n' "$comparison" | sed -n "s/.*$metric=\([0-9.]*\).*/\1/p" | head -1)
  b=$(printf '%s\n' "$benchmark" | sed -n "s/.*$metric=\([0-9.]*\).*/\1/p" | head -1)
  [ -n "$a" ] && [ "$a" = "$b" ] || { echo "FAIL $metric: compare=$a roundtrip=$b" >&2; exit 1; }
done
bpw=$(printf '%s\n' "$benchmark" | sed -n 's/.*bits per weight: \([0-9.]*\).*/\1/p' | head -1)
artifact_bytes=$(wc -c < "$tmp/tensor.bqfp" | tr -d ' ')
decoded_bytes=$(wc -c < "$tmp/tensor.f32" | tr -d ' ')
awk -v bpw="$bpw" -v a="$artifact_bytes" -v d="$decoded_bytes" 'BEGIN { actual = a * 8 / (d / 4); diff = bpw - actual; if (diff < 0) diff = -diff; if (bpw == "" || diff > 0.00051) { printf "FAIL bits per weight: reported %s, physical %f\n", bpw, actual > "/dev/stderr"; exit 1 } }'
echo 'BQFP benchmark matches physical bytes and independent decode: PASS'
