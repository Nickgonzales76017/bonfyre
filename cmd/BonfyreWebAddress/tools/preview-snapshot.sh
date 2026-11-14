#!/bin/sh
# Make one consistent SQLite backup per database, then APFS-clone lane-private copies.
# Never point a preview at live database files. No ordinary full-copy fallback.
set -eu
umask 077
action=${1:-prepare}
case "$action" in prepare|refresh) ;; *) echo 'usage: preview-snapshot.sh [prepare|refresh]' >&2; exit 2;; esac
live=${BONFYRE_PREVIEW_LIVE:-"$HOME/.bonfyre/estate-local"}
shared=${BONFYRE_PREVIEW_SNAPSHOT_HOME:-"$HOME/.bonfyre/preview-shared"}
home=${BONFYRE_PREVIEW_HOME:?Set BONFYRE_PREVIEW_HOME to a private lane directory}
[ -d "$live" ] || { echo 'preview snapshot: missing live SQLite source' >&2; exit 1; }
live=$(CDPATH='' cd -- "$live" && pwd -P)
# Check the closest existing ancestor BEFORE mkdir: aliases inside the live
# estate must not even create an empty preview directory in production.
for target in "$shared" "$home"; do
 case "$target" in /*) ;; *) echo 'preview snapshot: use absolute paths' >&2; exit 2 ;; esac
 case "$target" in "$live"|"$live"/*)
  echo 'preview snapshot: cannot place preview files under live estate' >&2; exit 2 ;;
 esac
 parent=$target
 until [ -d "$parent" ]; do
  parent=${parent%/*}
  [ -n "$parent" ] || parent=/
 done
 physical=$(CDPATH='' cd -- "$parent" && pwd -P)
 case "$physical" in "$live"|"$live"/*)
  echo 'preview snapshot: path aliases live estate' >&2; exit 2 ;;
 esac
done
mkdir -p "$shared" "$home"
shared=$(CDPATH='' cd -- "$shared" && pwd -P)
home=$(CDPATH='' cd -- "$home" && pwd -P)
case "$shared" in "$live"|"$live"/*) echo 'preview snapshot: shared snapshot cannot be in live estate' >&2; exit 2;; esac
case "$home" in "$live"|"$live"/*) echo 'preview snapshot: lane cannot be in live estate' >&2; exit 2;; esac
case "$home" in "$shared"/*) echo 'preview snapshot: nested lane inside snapshot refused' >&2; exit 2 ;; esac
case "$shared" in "$home"/*) echo 'preview snapshot: nested snapshot inside lane refused' >&2; exit 2 ;; esac
[ "$home" != "$shared" ] && [ "$home" != / ] && [ "$shared" != / ] || {
 echo 'preview snapshot: separate private lane and shared snapshot directories required' >&2; exit 2;
}
for db in matrix auth cms; do
 [ -f "$live/$db.db" ] && [ ! -L "$live/$db.db" ] || {
  echo "preview snapshot: missing or linked live $db.db" >&2; exit 1;
 }
done
lock="$shared/.preparing"
held=0
temp_matrix=''
temp_auth=''
temp_cms=''
cleanup() {
 [ -z "$temp_matrix" ] || rm -f "$temp_matrix"
 [ -z "$temp_auth" ] || rm -f "$temp_auth"
 [ -z "$temp_cms" ] || rm -f "$temp_cms"
 if [ "$held" = 1 ]; then rmdir "$lock" 2>/dev/null || :; fi
}
trap cleanup 0
trap 'exit 130' 1 2 3 15
tries=0
until mkdir "$lock" 2>/dev/null; do
 tries=$((tries+1))
 [ "$tries" -le 100 ] || { echo 'preview snapshot: preparation lock busy; check for a stale owner' >&2; exit 1; }
 sleep 0.2
done
held=1
[ ! -L "$shared/.ready" ] || { echo 'preview snapshot: linked ready marker refused' >&2; exit 2; }
if [ -f "$shared/.ready" ]; then
 for db in matrix auth cms; do
  [ -s "$shared/$db.db" ] && [ ! -L "$shared/$db.db" ] || {
   echo 'preview snapshot: ready marker has missing or linked database' >&2; exit 1;
  }
 done
fi
if [ ! -f "$shared/.ready" ] || [ "$action" = refresh ]; then
 # One source backup consumes real blocks; subsequent copy-on-write clones are cheap.
 need=65536
 for db in matrix auth cms; do
  size=$(du -k "$live/$db.db" | awk '{print $1}')
  need=$((need+size))
 done
 free=$(df -Pk "$shared" | awk 'END {print $4}')
 [ "$free" -ge "$need" ] || {
  echo "preview snapshot: insufficient disk ($free KiB free, $need KiB required)" >&2
  exit 1
 }
 # If interrupted midway through three atomic renames, a mixed generation
 # must never be served as ready on the next invocation.
 rm -f "$shared/.ready"
 for db in matrix auth cms; do
  tmp=$(mktemp "$shared/$db.db.XXXXXXXX")
  case "$db" in
    matrix) temp_matrix=$tmp ;;
    auth) temp_auth=$tmp ;;
    cms) temp_cms=$tmp ;;
  esac
  sqlite3 "$live/$db.db" ".backup '$tmp'"
  [ -s "$tmp" ] || { echo "preview snapshot: failed backing up $db" >&2; exit 1; }
  # A WAL-mode backup carries no wal file, and read-only opens of it fail
  # until something first touches it read-write; normalize before cloning.
  sqlite3 "$tmp" "PRAGMA journal_mode=DELETE;"
 done
 mv -f "$temp_matrix" "$shared/matrix.db"
 mv -f "$temp_auth" "$shared/auth.db"
 mv -f "$temp_cms" "$shared/cms.db"
 date -u '+%Y-%m-%dT%H:%M:%SZ' > "$shared/.ready"
fi
for db in matrix auth cms; do
 if [ -e "$home/$db.db" ] || [ -L "$home/$db.db" ]; then
  [ -f "$home/$db.db" ] && [ ! -L "$home/$db.db" ] || {
   echo "preview snapshot: refusing linked or invalid $db destination" >&2; exit 1;
  }
  continue
 fi
 tmp="$home/$db.db.$$"
 if cp -c "$shared/$db.db" "$tmp" 2>/dev/null; then :
 elif { rm -f "$tmp"; cp --reflink=always "$shared/$db.db" "$tmp" 2>/dev/null; }; then :
 else
  rm -f "$tmp"
  echo 'preview snapshot: copy-on-write clone unavailable; refusing full multi-lane database copy' >&2
  exit 1
 fi
 mv "$tmp" "$home/$db.db"
done
printf 'preview snapshot: lane %s using COW files from %s (last refresh %s)\n' \
 "$home" "$shared" "$(cat "$shared/.ready")"
