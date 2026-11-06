#!/usr/bin/env bash
set -euo pipefail

binary="$1"
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
export BONFYRE_ENTITY_DB="$root/entity.db"

# Distinct values must produce distinct entities even when resolved inside the
# same second. The id was previously entity-<unix_seconds>-<modal>, so a whole
# batch collided into one entity and INSERT OR IGNORE silently swallowed it.
for name in "ACME HOLDINGS LLC" "ACME HOLDINGS II LLC" "BETA TRUST" \
            "GAMMA SFR LP" "DELTA HOUSING AUTHORITY"; do
  "$binary" resolve text "$name" >/dev/null
done
"$binary" status | grep -q 'entities    : 5'
"$binary" status | grep -q 'observations: 5'

# Identity derives from the observed value, so re-resolving is idempotent:
# it must resolve, never create a sixth entity.
"$binary" resolve text "ACME HOLDINGS LLC" | grep -q '^resolved: '
"$binary" status | grep -q 'entities    : 5'

# Each entity keeps its own display name, so search can find any of them --
# not just whichever value happened to be observed first.
"$binary" search ACME | grep -q 'ACME HOLDINGS LLC'
"$binary" search ACME | grep -q 'ACME HOLDINGS II LLC'
"$binary" search BETA | grep -q 'BETA TRUST'

# An id must be stable across processes, not clock-dependent.
first="$("$binary" resolve text "BETA TRUST" | sed 's/.*entity=\([^ ]*\).*/\1/')"
second="$("$binary" resolve text "BETA TRUST" | sed 's/.*entity=\([^ ]*\).*/\1/')"
[ "$first" = "$second" ]
case "$first" in entity-text-*) ;; *) echo "id not value-derived: $first"; exit 1;; esac

# Normalization means punctuation and case do not fork an identity.
"$binary" resolve text "beta trust" | grep -q "entity=$first"
"$binary" status | grep -q 'entities    : 5'

echo "entity native smoke: PASS"
