#!/bin/sh
# Register only proven-free routes against non-thin published-price markets.
# A ranked spread is not revenue, demand, or an agreed sale.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
exec bun "$root/components/BonfyreProject/tools/economic-views.mjs" route-arbitrage "$@"
