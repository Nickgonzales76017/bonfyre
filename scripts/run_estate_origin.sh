#!/bin/sh
# The estate's published pages as a local origin behind the tunnel.  The keys
# for the object store are read from the estate's secrets by name; nothing is
# printed.
set -eu
refs=${BONFYRE_CLOUD_REFS:-${HOME:?}/.bonfyre/secrets/cloud.refs.env}
[ -r "$refs" ] || { echo "no secrets at $refs" >&2; exit 2; }
set -a; . "$refs" >/dev/null 2>&1; set +a
repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
bun_bin=${BONFYRE_BUN:-$(command -v bun || printf '%s/.bun/bin/bun' "$HOME")}
exec "$bun_bin" "$repo/deploy/origin/estate-origin.mjs"
