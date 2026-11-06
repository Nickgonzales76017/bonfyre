#!/bin/sh
# One time only: install the release-approval watcher and activate the first
# release that carries the work board's Approve button. After this, releases are
# offered as work board tasks and go live from Approve; nothing is typed here again.
#
#   sh scripts/release/bootstrap_approvals.sh ~/.bonfyre/releases/activity-public-20261021-XXXXXX
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
dir=${1:?staged release directory}
# Acceptance runs below, in this environment; a staged release is enough to start.
[ -f "$dir/stage.mpk" ] || { echo "that is not a staged release" >&2; exit 1; }
agent="$HOME/Library/LaunchAgents/com.bonfyre.release-approvals.plist"
mkdir -p "$HOME/.bonfyre/state/approvals" "$HOME/.bonfyre/logs"; chmod 700 "$HOME/.bonfyre/state/approvals"
sed "s#__HOME__#$HOME#g" "$root/deploy/launchd/com.bonfyre.release-approvals.plist" > "$agent"
plutil -lint "$agent" >/dev/null
launchctl bootout "gui/$(id -u)" "$agent" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$agent"
echo "release approvals: watcher installed"
# Acceptance is attested against the environment that runs it; verify here so activation matches.
mkdir -p /private/tmp/bonfyre-workboard-focus
bun=${BONFYRE_BUN_BINARY:-$HOME/.bun/bin/bun}
echo "release approvals: checking $(basename "$dir") again in this environment (about 25 minutes)"
cd "$root" && "$bun" scripts/release/public_surface_rollout.mjs verify "$dir" && "$bun" scripts/release/public_surface_rollout.mjs activate "$dir"
echo "release approvals: $(basename "$dir") is live; later releases arrive as Approve tasks on the work board"
