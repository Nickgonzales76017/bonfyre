#!/bin/sh
# Read only retained x402 bodies; published prices are not contracted rates.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
exec bun "$root/components/BonfyreProject/tools/economic-views.mjs" price-book "$@"
