#!/bin/sh
# PURE reducer and admission contract. PURE_CMS/PURE_JOB enable the live slice.
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cd "$repo_root"
exec bun test scripts/tests/pure-native.test.mjs
