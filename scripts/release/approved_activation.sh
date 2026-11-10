#!/bin/sh
# Releases go live from the person's own click in Aurekai, never from a terminal ask.
#
#   approved_activation.sh offer RELEASE_DIR SUMMARY   put an accepted release on the work board for approval
#   approved_activation.sh run                         activate each approved release, report back on its task
#
# offer refuses a release without acceptance receipts. run acts only on an approval
# file the work board wrote for an open release-approval goal, activates through
# public_surface_rollout.mjs (which restores the previous service on failure), and
# records the outcome on the same goal. One run at a time.
set -u
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
op=${BONFYRE_OPERATOR_DB:-$HOME/.bonfyre/operator/operator.sqlite3}
approvals=${BONFYRE_APPROVAL_DIR:-$HOME/.bonfyre/state/approvals}
releases=$HOME/.bonfyre/releases
bun=${BONFYRE_BUN_BINARY:-$HOME/.bun/bin/bun}
# Acceptance runs tools beside bun (bunx for Tailwind); launchd's PATH does not carry them.
PATH="$(dirname -- "$bun"):$PATH"; export PATH
q() { printf '%s' "$1" | sed "s/'/''/g"; }

case "${1:-}" in
offer)
    dir=${2:?release directory}; summary=${3:?what this release changes}
    name=$(basename -- "$dir")
    case "$name" in *[!A-Za-z0-9-]*|'') echo "release name must be letters, digits and hyphens" >&2; exit 2 ;; esac
    [ "$(cd -- "$dir" 2>/dev/null && pwd -P)" = "$(cd -- "$releases/$name" 2>/dev/null && pwd -P)" ] || { echo "not a release under $releases" >&2; exit 2; }
    [ -f "$dir/accepted.mpk" ] || { echo "$name has no acceptance receipt; it cannot be offered" >&2; exit 1; }
    now=$(date +%s); id=$(printf 'release-approval:%s' "$name" | shasum | cut -c1-32)
    details="Release $name passed acceptance and is ready to go live on aurekai.life, .live, .shop and .work.
What it changes: $summary
Approve puts it live. If any check fails during the switch, the previous release is restored automatically and this task says why."
    sqlite3 "$op" "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,created_at,updated_at) VALUES('$id','release-approval:$name','$(q "Approve release $name")','$(q "$details")',90,'blocked','release-approval',$now,$now) ON CONFLICT(dedupe_key) DO UPDATE SET details=excluded.details,updated_at=excluded.updated_at;" || exit 1
    echo "offered $name on the work board"
    ;;
run)
    mkdir -p "$approvals/done" "$approvals/failed" && chmod 700 "$approvals"
    lock="$approvals/.running"; mkdir "$lock" 2>/dev/null || exit 0
    trap 'rmdir "$lock"' EXIT INT TERM
    for file in "$approvals"/*.approved; do
        [ -f "$file" ] || continue
        release=$(awk -F'\t' '$1=="release"{print $2}' "$file"); goal=$(awk -F'\t' '$1=="goal"{print $2}' "$file")
        case "$release$goal" in *[!A-Za-z0-9-]*|'') mv "$file" "$approvals/failed/"; continue ;; esac
        open=$(sqlite3 "$op" "SELECT count(*) FROM goals WHERE id='$goal' AND dedupe_key='release-approval:$release' AND source='release-approval' AND state NOT IN('resolved','superseded')")
        dir="$releases/$release"; now=$(date +%s); log="$approvals/$release.log"
        if [ "$open" != 1 ] || [ ! -f "$dir/accepted.mpk" ]; then
            mv "$file" "$approvals/failed/"; continue
        fi
        # Acceptance is attested against the environment that runs it, so the
        # release is verified again here, in the same environment that activates it.
        mkdir -p /private/tmp/bonfyre-workboard-focus
        sqlite3 "$op" "UPDATE goals SET last_error='$(q "Approved; checking the release again before it goes live (about 25 minutes)")',updated_at=$now WHERE id='$goal'"
        if (cd "$root" && "$bun" scripts/release/public_surface_rollout.mjs verify "$dir" && "$bun" scripts/release/public_surface_rollout.mjs activate "$dir") > "$log" 2>&1; then
            sqlite3 "$op" "UPDATE goals SET state='resolved',last_error='$(q "Live since $(date '+%a %d %b %Y %H:%M')")',updated_at=$now WHERE id='$goal'"
            mv "$file" "$approvals/done/"
        else
            why=$(grep -E 'Error|failed|refus' "$log" | tail -1 | cut -c1-300)
            sqlite3 "$op" "UPDATE goals SET last_error='$(q "Activation did not go live; the previous release kept serving. ${why:-See $log}")',updated_at=$now WHERE id='$goal'"
            mv "$file" "$approvals/failed/"
        fi
    done
    ;;
*) echo "usage: approved_activation.sh offer RELEASE_DIR SUMMARY | run" >&2; exit 2 ;;
esac
