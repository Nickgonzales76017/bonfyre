#!/bin/sh
# What this proves: a tensor of a real model goes through the one .bqfp
# contract end to end. bonfyre-quant encodes it, bonfyre-sli rebuilds the
# weights with the shared reader, bonfyre-quant measures those against the
# model, and bonfyre-fpqx aligns two different tensors' codebooks. A file cut
# short anywhere is refused by both readers and leaves no output behind.
# What this does not prove: that the decoded weights are good enough for any
# use beyond the cosine printed; that is one tensor pair of one model.
set -eu
quant=${1:?bonfyre-quant required}
sli=${2:?bonfyre-sli required}
fpqx=${3:?bonfyre-fpqx required}
model=${4:?a real .safetensors model with at least two tensors of 256 weights is required}
for b in quant sli fpqx; do eval "p=\$$b"; case "$p" in /*) ;; *) eval "$b=\"$(pwd)/$p\"";; esac; done
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-bqfp-roundtrip.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

for n in 0 1; do
  "$quant" encode "$model" --skip "$n" --bits 3 --out "$tmp/t$n.bqfp" > "$tmp/encode$n.out"
  "$sli" inspect --model "$tmp/t$n.bqfp" > "$tmp/inspect$n.out"
  grep -q 'tensors:  1' "$tmp/inspect$n.out"
  "$sli" decode --model "$tmp/t$n.bqfp" --out "$tmp/t$n.f32" > /dev/null
  # compare exits 0 only at cosine 0.99 or better against the model's own tensor
  "$quant" compare "$model" --skip "$n" --weights "$tmp/t$n.f32" > "$tmp/compare$n.out"
  grep -q 'cos=0\.99' "$tmp/compare$n.out"
done

# Weights decoded for one tensor are not the other's.
if "$quant" compare "$model" --skip 1 --weights "$tmp/t0.f32" > /dev/null 2>&1; then
  echo 'one tensor was accepted as another' >&2; exit 1
fi

"$fpqx" align "$tmp/t0.bqfp" "$tmp/t1.bqfp" --out "$tmp/align" > "$tmp/align.out"
grep -q 'anchors:' "$tmp/align.out"
! grep -q 'cosine_mean: 1\.0000' "$tmp/align.out"

# A file that loses its last block, half its codebook, or its record is refused.
size=$(wc -c < "$tmp/t0.bqfp" | tr -d ' ')
for keep in $((size - 1)) $((size - 280)) 400 100 8; do
  head -c "$keep" "$tmp/t0.bqfp" > "$tmp/cut.bqfp"
  if "$sli" decode --model "$tmp/cut.bqfp" --out "$tmp/cut.f32" > /dev/null 2>&1; then
    echo "bonfyre-sli decoded a file cut to $keep of $size bytes" >&2; exit 1
  fi
  test ! -e "$tmp/cut.f32"
  if "$sli" inspect --model "$tmp/cut.bqfp" > /dev/null 2>&1; then
    echo "bonfyre-sli inspected a file cut to $keep of $size bytes" >&2; exit 1
  fi
  rm -rf "$tmp/cut-align"
  if "$fpqx" align "$tmp/cut.bqfp" "$tmp/t1.bqfp" --out "$tmp/cut-align" > /dev/null 2>&1; then
    echo "bonfyre-fpqx aligned a file cut to $keep of $size bytes" >&2; exit 1
  fi
done

echo "bqfp round trip: PASS (two tensors, $(sed -n "s/.*\(cos=[0-9.]*\).*/\1/p" "$tmp/compare0.out") and $(sed -n "s/.*\(cos=[0-9.]*\).*/\1/p" "$tmp/compare1.out"); five truncations refused by both readers)"
