#!/bin/sh
# Isolated current-owner reconciliation; never mutates the serving index.
set -eu
queue=${1:?native Queue required}
home=${BONFYRE_TEST_ACCOUNT_HOME:-"$HOME"}
db="$home/.bonfyre/state/account-work.db"
[ -f "$db" ] || { echo "account shadow: SKIP (no serving index)"; exit 0; }
root="$home/.bonfyre"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/account-work-shadow.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
for src in "$root/operator/operator.sqlite3" "$root/dev-fabric/fabric.sqlite3" "$root/edge-workgraph/workgraph.db" "$root/estate/native-architecture-workgraph.db" "$root/estate-fabric/fabric.db"; do
  if [ ! -f "$src" ] || [ -L "$src" ]; then echo "account shadow: REFUSED unavailable declared owner $src" >&2; exit 1; fi
done
"$queue" account-work index --db "$tmp/index.db" --subject user:19 --scope self --owner user:19 \
  --source "operator=$root/operator/operator.sqlite3" \
  --source "fabric=$root/dev-fabric/fabric.sqlite3" \
  --source "workgraph=$root/edge-workgraph/workgraph.db" \
  --source "workgraph=$root/estate/native-architecture-workgraph.db" \
  --source "workgraph=$root/estate-fabric/fabric.db" >/dev/null
echo "account shadow: current owner sources (fresh isolated index)"
"$queue" account-work audit --db "$tmp/index.db" --subject user:19 --scope self
echo "account shadow: serving index (unchanged)"
"$queue" account-work audit --db "$db" --subject user:19 --scope self
