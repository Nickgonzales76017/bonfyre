#!/bin/sh
# Exercises preview snapshots against temporary SQLite stores only.
set -eu
umask 077
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-preview-snapshot-test.XXXXXX")
trap 'rm -rf "$scratch"' 0
trap 'exit 130' 1 2 3 15
mkdir -p "$scratch/live"
for db in matrix auth cms; do
 sqlite3 "$scratch/live/$db.db" \
  "CREATE TABLE witness (name TEXT PRIMARY KEY, number INTEGER); INSERT INTO witness VALUES ('test', 1);"
done
export BONFYRE_PREVIEW_LIVE="$scratch/live"
export BONFYRE_PREVIEW_SNAPSHOT_HOME="$scratch/shared"
export BONFYRE_PREVIEW_HOME="$scratch/lane-a"
sh "$here/preview-snapshot.sh" prepare >/dev/null
test -s "$BONFYRE_PREVIEW_SNAPSHOT_HOME/.ready"
for db in matrix auth cms; do
 test "$(sqlite3 "$BONFYRE_PREVIEW_HOME/$db.db" "SELECT number FROM witness WHERE name='test'")" = 1
done
# A lane is pinned to its shared snapshot; live modifications do not change it.
sqlite3 "$scratch/live/cms.db" 'UPDATE witness SET number=2'
BONFYRE_PREVIEW_HOME="$scratch/lane-b" sh "$here/preview-snapshot.sh" prepare >/dev/null
test "$(sqlite3 "$scratch/lane-b/cms.db" 'SELECT number FROM witness')" = 1
# A deliberately refreshed base creates new clones, but old lane files remain immutable.
BONFYRE_PREVIEW_HOME="$scratch/lane-c" sh "$here/preview-snapshot.sh" refresh >/dev/null
test "$(sqlite3 "$scratch/lane-c/cms.db" 'SELECT number FROM witness')" = 2
test "$(sqlite3 "$scratch/lane-a/cms.db" 'SELECT number FROM witness')" = 1
# Refuse live paths, source symlinks, and shared==lane aliases.
if BONFYRE_PREVIEW_HOME="$scratch/live" sh "$here/preview-snapshot.sh" prepare >/dev/null 2>&1; then
 echo 'preview snapshot must refuse live store as a lane' >&2; exit 1
fi
if BONFYRE_PREVIEW_SNAPSHOT_HOME="$scratch/live" sh "$here/preview-snapshot.sh" prepare >/dev/null 2>&1; then
 echo 'preview snapshot must refuse live store as shared cache' >&2; exit 1
fi
if BONFYRE_PREVIEW_HOME="$scratch/shared" sh "$here/preview-snapshot.sh" prepare >/dev/null 2>&1; then
 echo 'preview snapshot must refuse cache==lane' >&2; exit 1
fi
mv "$scratch/live/auth.db" "$scratch/live/auth.original"
ln -s "$scratch/live/auth.original" "$scratch/live/auth.db"
if BONFYRE_PREVIEW_HOME="$scratch/lane-d" sh "$here/preview-snapshot.sh" prepare >/dev/null 2>&1; then
 echo 'preview snapshot must refuse linked live database source' >&2; exit 1
fi
test ! -e "$scratch/shared/.preparing"
echo 'preview snapshot: PASS (COW lanes, refresh isolation, and path refusals)'
