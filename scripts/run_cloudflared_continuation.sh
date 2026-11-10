#!/bin/sh
# Starts the existing remotely-managed Cloudflare connector without placing its
# short-lived run token in source, a plist, or a command argument.
set -eu

refs=${BONFYRE_CLOUD_REFS:-${HOME:?}/.bonfyre/secrets/cloud.refs.env}

[ -r "$refs" ] || { echo "Cloudflare references are unavailable" >&2; exit 2; }
set -a
. "$refs"
set +a

: "${CLOUDFLARE_API_TOKEN:?Cloudflare API token is required}"
: "${CLOUDFLARE_ACCOUNT_ID:?Cloudflare account id is required}"

# launchd has a minimal PATH; Bun's standard user install remains discoverable.
bun_bin=${BONFYRE_BUN:-$(command -v bun || printf '%s/.bun/bin/bun' "$HOME")}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "$bun_bin" "$script_dir/cloudflare-reconcile.mjs" connector
