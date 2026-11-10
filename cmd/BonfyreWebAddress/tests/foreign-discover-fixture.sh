#!/bin/sh
# Deterministic MachineMembrane passport witness.  The HTTP edge calls this by
# argv (never a shell string); its output mirrors the small carrier-selection
# portion of scripts/foreign-discover.mjs without contacting the network.
set -eu
root=${1:?expected public origin}
printf '{\n  "root": "%s",\n  "observed_at": "2026-08-31T00:00:00Z",\n  "robots_allows_us": true,\n  "best_carrier": "feed",\n  "why": "fixture publisher declares a feed"\n}\n' "$root"
