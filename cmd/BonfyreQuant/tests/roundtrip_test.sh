#!/bin/sh
# What this proves: the organ measures the model it is given, tensor by tensor,
# and has nothing to report without one.
#   sh tests/roundtrip_test.sh ./bonfyre-quant /path/to/model.safetensors
set -eu
bin=${1:?bonfyre-quant required}
model=${2:?a real .safetensors model is required}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-quant-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

"$bin" roundtrip "$model" --bits 3 --limit 2 > "$tmp/two" || [ $? -eq 2 ]
grep -q 'tensors measured: 2' "$tmp/two"
grep -Eq 'cos=0\.[0-9]+|cos=1\.0' "$tmp/two"
grep -q 'bits per weight:' "$tmp/two"
! grep -qi 'synthetic\|self-test' "$tmp/two"

# a different slice of the model is a different measurement
"$bin" roundtrip "$model" --bits 3 --limit 1 > "$tmp/one" || [ $? -eq 2 ]
[ "$(grep 'elements:' "$tmp/one")" != "$(grep 'elements:' "$tmp/two")" ]

if "$bin" roundtrip >/dev/null 2>&1; then echo 'measured with no model' >&2; exit 1; fi
if "$bin" benchmark >/dev/null 2>&1; then echo 'benchmarked with no model' >&2; exit 1; fi
printf 'not a model\n' > "$tmp/text"
if "$bin" roundtrip "$tmp/text" >/dev/null 2>&1; then echo 'measured a text file' >&2; exit 1; fi
if "$bin" roundtrip "$model" --tensor no-such-tensor-name >/dev/null 2>&1; then echo 'measured a tensor that is not there' >&2; exit 1; fi
printf '%s\n' 'quant roundtrip: PASS'
