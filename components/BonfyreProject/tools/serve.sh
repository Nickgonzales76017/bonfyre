#!/bin/sh
# Stable caller entrypoint; the answer compiler and signer are native.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
exec bun "$here/serve.mjs" "$@"
