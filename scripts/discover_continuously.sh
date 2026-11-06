#!/bin/sh
# Keep finding publishers, and ask each what it declares.
#
# Two things compound: a search engine that can be asked returns publishers the
# estate has never heard of, and a publisher asked directly declares its own
# channels about one time in five -- ten times better than guessing addresses.
# So this does both, in that order, on a schedule, and stops at the caps the
# estate's own governance sets.
set -eu
ESTATE="${BONFYRE_ESTATE:-$HOME/.bonfyre/estate-local}"
REPO="${BONFYRE_REPO:-$HOME/Developer/Bonfyre}"
STATE="$ESTATE/discovery"; mkdir -p "$STATE"
LOG="$STATE/discover.log"
note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

LOCK="$STATE/running.lock"
mkdir "$LOCK" 2>/dev/null || exit 0
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT INT TERM

[ -f "$HOME/.bonfyre/secrets/provider.refs.env" ] && . "$HOME/.bonfyre/secrets/provider.refs.env"
[ "${BONFYRE_EXTERNAL_READS_ENABLED:-0}" = "1" ] || { note "governance has external reads off"; exit 0; }

stamp=$(date -u +%Y%m%dT%H%M%SZ)
found="$STATE/exa-$stamp.jsonl"
swept="$STATE/sweep-$stamp.jsonl"
FRONTIER="$REPO/cmd/BonfyreSourceFrontier/bonfyre-source-frontier"

note "$(bun "$REPO/cmd/BonfyreSourceFrontier/tools/discover_with_exa.mjs" \
        --out "$found" --queries "${BONFYRE_DISCOVER_QUERIES:-80}" --per-query 50 2>/dev/null | tail -1)"
[ -s "$found" ] && note "$("$FRONTIER" import --db "$ESTATE/frontier.db" --label EXA_DISCOVERY --jsonl "$found" 2>&1 | tail -1)"

# And one family this estate is short of, rotated, so a territory that asks for
# material nobody has ever gone looking for stops being empty for that reason.
# The search names the family it sought; what is filed under it is the intent
# of the search, which measured carriage later replaces.
FAMILY=$(awk -v n="$(date -u +%j)" 'BEGIN{split("vulnerabilities gaming",f," "); print f[(n%2)+1]}')
sought="$STATE/family-$FAMILY-$stamp.jsonl"
bun "$REPO/cmd/BonfyreSourceFrontier/tools/discover_with_exa.mjs" --out "$sought" \
    --family "$FAMILY" --queries 6 --per-query 25 >/dev/null 2>&1 || true
if [ -s "$sought" ]; then
    label="FAMILY_$(printf '%s' "$FAMILY" | tr 'a-z' 'A-Z')"
    note "$("$FRONTIER" import --db "$ESTATE/frontier.db" --label "$label" --jsonl "$sought" 2>&1 | tail -1)"
fi

# Then ask whatever publishers are still unasked what they declare.
note "$(bun "$REPO/cmd/BonfyreSourceFrontier/tools/sweep_root_grammars.mjs" \
        --db "$ESTATE/frontier.db" --project "$REPO/components/BonfyreProject/bonfyre-project" \
        --foreign "$ESTATE/foreign" --out "$swept" --limit "${BONFYRE_SWEEP_LIMIT:-600}" \
        --workers 10 --evidence "$STATE/sweep-evidence.jsonl" 2>/dev/null | tail -1)"
[ -s "$swept" ] && note "$("$FRONTIER" import --db "$ESTATE/frontier.db" --label PUBLISHER_SWEEP --jsonl "$swept" 2>&1 | tail -1)"

find "$STATE" -name 'exa-*.jsonl' -mtime +7 -delete 2>/dev/null || true
find "$STATE" -name 'sweep-*.jsonl' -mtime +7 -delete 2>/dev/null || true
