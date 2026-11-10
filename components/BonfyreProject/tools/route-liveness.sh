#!/bin/sh
# Sample retained route receipts; this remains a bounded source reread in production.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
exec bun "$root/components/BonfyreProject/tools/project-liveness.mjs" "$@"
