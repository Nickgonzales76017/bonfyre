#!/bin/sh
# Purge exactly what changed, at every address it is served from.
#
# The failure this replaces: every publish in a working session called
# purge_everything on all four zones. Each one of those throws away the whole
# edge cache, so the next reader re-fetches the pages, the catalogue parquets
# and a 33 MB WebAssembly engine from the origin -- and a session of forty
# publishes took the account past its request budget and put all four
# territories behind HTTP 429. A cache purge is not free and purging all of it
# to change one page is not proportionate.
#
#   scripts/estate_purge.sh /press /edition            # paths, all territories
#   scripts/estate_purge.sh --everything               # only when truly needed
set -eu

refs=${BONFYRE_CLOUD_REFS:-${HOME:?}/.bonfyre/secrets/cloud.refs.env}
[ -r "$refs" ] || { echo "Cloudflare references are unavailable" >&2; exit 2; }
set -a
. "$refs"
set +a
bun_bin=${BONFYRE_BUN:-$(command -v bun || printf '%s/.bun/bin/bun' "$HOME")}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$bun_bin" "$script_dir/cloudflare-reconcile.mjs" purge "$@"
