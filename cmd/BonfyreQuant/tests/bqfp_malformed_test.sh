#!/bin/sh
# What this proves: both readers of the .bqfp contract refuse a file that is
# wrong in any of the ways below and leave no output behind, and both accept
# a file holding two tensors. Every case starts from a tensor bonfyre-quant
# encoded from a real model and changes named bytes of it.
# What this does not prove: anything about the quality of an encoding; that
# is bqfp_roundtrip_test.sh.
set -eu
quant=${1:?bonfyre-quant required}
sli=${2:?bonfyre-sli required}
fpqx=${3:?bonfyre-fpqx required}
model=${4:?a real .safetensors model is required}
for b in quant sli fpqx; do eval "p=\$$b"; case "$p" in /*) ;; *) eval "$b=\"$(pwd)/$p\"";; esac; done
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-bqfp-malformed.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

"$quant" encode "$model" --bits 3 --out "$tmp/good.bqfp" > /dev/null
size=$(wc -c < "$tmp/good.bqfp" | tr -d ' ')

# Layout offsets: file header 0..15 (magic, version, tensor count, bits);
# name 16..271; elements 272; blocks 280; tiles 288; codebook from 296.
patched() { # patched NAME OFFSET PRINTF-BYTES
  cp "$tmp/good.bqfp" "$tmp/$1.bqfp"
  printf "$3" | dd of="$tmp/$1.bqfp" bs=1 seek="$2" conv=notrunc 2>/dev/null
}
head -c $((size - 1)) "$tmp/good.bqfp" > "$tmp/short-block.bqfp"
head -c 301 "$tmp/good.bqfp" > "$tmp/short-codebook.bqfp"
patched bad-magic 0 'nope'
patched bad-version 4 '\021'
patched bad-bits 12 '\000'
patched zero-elements 272 '\000\000\000\000\000\000\000\000'
patched wrong-block-count 280 '\377\377'
patched too-many-tiles 288 '\001\001\000\000'
cp "$tmp/good.bqfp" "$tmp/unterminated-name.bqfp"
head -c 256 /dev/zero | tr '\0' 'x' | dd of="$tmp/unterminated-name.bqfp" bs=1 seek=16 conv=notrunc 2>/dev/null

# Two tensors: the count set to 2 and the tensor's record and payload repeated.
patched two-tensors 8 '\002'
tail -c +17 "$tmp/good.bqfp" >> "$tmp/two-tensors.bqfp"
two=$(wc -c < "$tmp/two-tensors.bqfp" | tr -d ' ')
head -c $((two - 1)) "$tmp/two-tensors.bqfp" > "$tmp/truncated-second.bqfp"

"$sli" inspect --model "$tmp/good.bqfp" > /dev/null
"$sli" inspect --model "$tmp/two-tensors.bqfp" | grep -q 'tensors:  2'
"$sli" decode --model "$tmp/good.bqfp" --out "$tmp/good.f32" > /dev/null
test -s "$tmp/good.f32"
"$fpqx" align "$tmp/good.bqfp" "$tmp/two-tensors.bqfp" --out "$tmp/valid" > /dev/null
test -s "$tmp/valid/fpqx_alignment.json"

refused=0
for case in short-block short-codebook bad-magic bad-version bad-bits zero-elements wrong-block-count too-many-tiles unterminated-name truncated-second; do
  if "$sli" inspect --model "$tmp/$case.bqfp" > /dev/null 2>&1; then echo "bonfyre-sli inspected $case" >&2; exit 1; fi
  if "$fpqx" align "$tmp/good.bqfp" "$tmp/$case.bqfp" --out "$tmp/out-$case" > /dev/null 2>&1; then echo "bonfyre-fpqx aligned $case" >&2; exit 1; fi
  test ! -e "$tmp/out-$case" || { echo "bonfyre-fpqx left output for $case" >&2; exit 1; }
  # A second tensor cut short does not stop the first being decoded; every
  # other case damages the tensor decode reads.
  if [ "$case" != truncated-second ]; then
    if "$sli" decode --model "$tmp/$case.bqfp" --out "$tmp/bad.f32" > /dev/null 2>&1; then echo "bonfyre-sli decoded $case" >&2; exit 1; fi
    test ! -e "$tmp/bad.f32" || { echo "bonfyre-sli left a decode for $case" >&2; exit 1; }
  fi
  refused=$((refused + 1))
done
echo "bqfp malformed files: PASS (2 valid accepted, $refused malformed refused by both readers)"
