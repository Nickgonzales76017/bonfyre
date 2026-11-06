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
# A write to the lane must not change the retained shared snapshot or other lanes.
sqlite3 "$scratch/lane-a/matrix.db" 'UPDATE witness SET number=9'
test "$(sqlite3 "$scratch/shared/matrix.db" 'SELECT number FROM witness')" = 1
test "$(sqlite3 "$scratch/lane-b/matrix.db" 'SELECT number FROM witness')" = 1
# A missing ready marker forces a complete rebuild, not stale mixed metadata.
rm "$scratch/shared/.ready"
sqlite3 "$scratch/live/matrix.db" 'UPDATE witness SET number=3'
BONFYRE_PREVIEW_HOME="$scratch/lane-d" sh "$here/preview-snapshot.sh" prepare >/dev/null
test "$(sqlite3 "$scratch/lane-d/matrix.db" 'SELECT number FROM witness')" = 3
test "$(sqlite3 "$scratch/lane-d/cms.db" 'SELECT number FROM witness')" = 2
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
before=$(find "$scratch/live" -type d | wc -l | tr -d ' ')
if BONFYRE_PREVIEW_HOME="$scratch/live/new/preview" sh "$here/preview-snapshot.sh" prepare >/dev/null 2>&1; then
 echo 'preview snapshot created under live estate' >&2; exit 1
fi
test ! -e "$scratch/live/new"
test "$before" = "$(find "$scratch/live" -type d | wc -l | tr -d ' ')"
# A failed SQLite backup must not retain a temp file or a ready marker.
mkdir "$scratch/fail-bin"
printf '#!/bin/sh\nexit 81\n' > "$scratch/fail-bin/sqlite3"
chmod 700 "$scratch/fail-bin/sqlite3"
if PATH="$scratch/fail-bin:$PATH" BONFYRE_PREVIEW_HOME="$scratch/lane-failure" sh "$here/preview-snapshot.sh" refresh >/dev/null 2>&1; then
 echo 'preview snapshot accepted a failed SQLite backup' >&2; exit 1
fi
test ! -e "$scratch/shared/.ready"
for temp in "$scratch/shared"/matrix.db.* "$scratch/shared"/auth.db.* "$scratch/shared"/cms.db.*; do
 test ! -e "$temp" || { echo "preview snapshot left interrupted temp file" >&2; exit 1; }
done
BONFYRE_PREVIEW_HOME="$scratch/lane-recovered" sh "$here/preview-snapshot.sh" refresh >/dev/null
test -s "$scratch/shared/.ready"
test "$(sqlite3 "$scratch/lane-recovered/cms.db" 'SELECT number FROM witness')" = 2
mv "$scratch/live/auth.db" "$scratch/live/auth.original"
ln -s "$scratch/live/auth.original" "$scratch/live/auth.db"
if BONFYRE_PREVIEW_HOME="$scratch/lane-d" sh "$here/preview-snapshot.sh" prepare >/dev/null 2>&1; then
 echo 'preview snapshot must refuse linked live database source' >&2; exit 1
fi
test ! -e "$scratch/shared/.preparing"
echo 'preview snapshot: PASS (COW lanes, refresh isolation, and path refusals)'
