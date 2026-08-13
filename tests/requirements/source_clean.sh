#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

# Runtime state belongs below BONFYRE_STATE_DIR, never in the checkout.  Keep
# this deliberately narrow: checked-in fixtures and compiled developer tools
# are allowed, while Fabric databases, controller evidence, sockets and pid
# files are not source-owned artifacts.
git -C "$root" diff --check

if find "$root" -path "$root/.git" -prune -o \
  \( -name fabric.db -o -name evidence.jsonl -o -name command-evidence.tsv -o \
     -name '*.sock' -o -name '*.pid' \) -print | grep -q .; then
  echo 'generated Fabric state exists in the source checkout' >&2
  exit 1
fi

test ! -e "$root/completion/runs"
printf 'source-clean\n'
