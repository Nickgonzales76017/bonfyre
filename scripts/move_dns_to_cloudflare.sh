#!/bin/sh
# Explicit --apply only: copy DNS through native XML/Bun, verify the complete
# desired record set, then request the nameserver change. Acceptance != propagation.
set -eu
refs=${BONFYRE_CLOUD_REFS:-${HOME:?}/.bonfyre/secrets/cloud.refs.env}
if [ -r "$refs" ]; then
    set -a
    . "$refs"
    set +a
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
bun_bin=${BONFYRE_BUN:-$(command -v bun || printf '%s/.bun/bin/bun' "$HOME")}
exec "$bun_bin" "$script_dir/dns-migration.mjs" "$@"
