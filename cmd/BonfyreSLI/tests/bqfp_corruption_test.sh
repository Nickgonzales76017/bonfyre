#!/bin/sh
# The .bqfp contract has one malformed-file test, shared by both readers, in
# cmd/BonfyreQuant/tests/bqfp_malformed_test.sh. It starts from a tensor
# encoded from a real model, so BONFYRE_TEST_MODEL must name a .safetensors
# file. This file used to build its cases from bytes typed into embedded
# Python; it now runs the shared test so `make test` here still means the same.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cmd=$(CDPATH= cd -- "$here/.." && pwd)
: "${BONFYRE_TEST_MODEL:?set BONFYRE_TEST_MODEL to a real .safetensors model}"
for organ in BonfyreQuant/bonfyre-quant BonfyreSLI/bonfyre-sli BonfyreFPQx/bonfyre-fpqx; do
  test -x "$cmd/$organ" || { echo "build $cmd/$organ first" >&2; exit 1; }
done
exec sh "$cmd/BonfyreQuant/tests/bqfp_malformed_test.sh" \
  "$cmd/BonfyreQuant/bonfyre-quant" "$cmd/BonfyreSLI/bonfyre-sli" "$cmd/BonfyreFPQx/bonfyre-fpqx" "$BONFYRE_TEST_MODEL"
