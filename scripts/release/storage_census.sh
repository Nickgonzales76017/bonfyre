#!/bin/sh
# Read-only physical-estate census. No temp files, deletion, or release mutation.
set -eu
# Inspect is read-only. The explicit --cms-wal-checkpoint --confirm operation
# asks SQLite itself for a TRUNCATE checkpoint; no manual WAL file mutation.
db="$HOME/.bonfyre/estate-local/cms.db"
if [ "$#" -eq 1 ] && [ "$1" = "--cms-wal-diagnose" ]; then
  echo "CMS_WAL_DIAGNOSIS timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) db=$db"
  [ -f "$db" ] && [ ! -L "$db" ] || { echo "REFUSED: canonical CMS is unavailable or symlinked" >&2; exit 75; }
  for part in "$db" "$db-wal" "$db-shm"; do
    if [ -f "$part" ] && [ ! -L "$part" ]; then
      stat -f "FILE %N bytes=%z modified=%Sm" -t "%Y-%m-%dT%H:%M:%S%z" "$part"
      du -sk "$part" | awk '{printf "ALLOC_KIB %s %s\n",$1,$2}'
    else printf "NOT_PRESENT %s\n" "$part"; fi
  done
  df -Pk "$db" | awk 'END {printf "PHYSICAL_FREE_KIB %s\n",$4}'
  if [ -x /usr/bin/sqlite3 ]; then
    /usr/bin/sqlite3 -readonly "$db" "PRAGMA journal_mode; PRAGMA page_size;" 2>&1 | sed 's/^/SQLITE_READONLY /'
  fi
  echo "LAUNCH_OWNER (read-only):"
  if command -v launchctl >/dev/null 2>&1; then launchctl list | grep -F "com.bonfyre.public-web-address" || :; fi
  echo "WEB_AND_CMS_PROCESSES (pid ppid elapsed executable; no arguments):"
  ps -axo pid,ppid,etime,comm | awk 'NR==1 || /bonfyre-web-address|bonfyre-cms/' | head -n 50
  echo "DETACHED_SITE_HOME_PID (default bonfyre site home; identity only):"
  owned="$HOME/.bonfyre/sites/serve.pid"
  if [ -f "$owned" ] && [ ! -L "$owned" ]; then
    owned_pid=$(awk 'NR==1 {print $1}' "$owned")
    printf "SITE_PID_RECORD %s\n" "$owned_pid"
    case "$owned_pid" in
      ""|*[!0-9]*) echo "SITE_PID_PROCESS INVALID_RECORD" ;;
      *)
        recorded_exec=$(ps -p "$owned_pid" -o comm= 2>/dev/null || :)
        if [ -z "$recorded_exec" ]; then echo "SITE_PID_PROCESS NOT_RUNNING";
        else
          case "$recorded_exec" in
            "$HOME/Developer/Bonfyre/cmd/BonfyreWebAddress/bonfyre-web-address") echo "SITE_PID_PROCESS CHECKOUT_BINARY_UNVERIFIED_PORT" ;;
            "$HOME/.bonfyre/releases/"*/bin/bonfyre-web-address) echo "SITE_PID_PROCESS FROZEN_RELEASE_UNVERIFIED_PORT" ;;
            *) echo "SITE_PID_PROCESS OTHER_OR_UNVERIFIED_EXECUTABLE" ;;
          esac
          if command -v lsof >/dev/null 2>&1; then
            lsof -nP -a -p "$owned_pid" -iTCP -sTCP:LISTEN 2>/dev/null |
              awk 'NR>1 && NF>=9 {printf "SITE_PID_LISTENER %s\n",$9}' | head -n 10 || :
          fi
        fi
        ;;
    esac
  else echo "SITE_PID_RECORD NOT_PRESENT"; fi
  echo "DETACHED_CHECKOUT_WEB_LISTENERS (no signals, no command arguments):"
  if command -v lsof >/dev/null 2>&1; then
    ps -axo pid=,ppid=,comm= | awk '$2==1 && index($0,"/cmd/BonfyreWebAddress/bonfyre-web-address"){print $1}' | head -n 24 |
    while IFS= read -r webpid; do
      [ -n "$webpid" ] || continue
      state=$(ps -p "$webpid" -o stat= 2>/dev/null | tr -d " " || :)
      ports=$(lsof -nP -a -p "$webpid" -iTCP -sTCP:LISTEN 2>/dev/null | awk 'NR>1 && NF>=9 {print $9}' | head -n 8 | paste -sd, - || :)
      printf "DETACHED_WEB pid=%s state=%s listeners=%s\n" "$webpid" "${state:-gone}" "${ports:--}"
    done
  fi
  echo "CMS_FILE_OPEN_HANDLES (open is not proof of an active read transaction):"
  if command -v lsof >/dev/null 2>&1; then lsof -nP "$db" "$db-wal" "$db-shm" 2>/dev/null | head -n 75 || :; fi
  echo "READ_ONLY: no checkpoint, schema change, release or process action was attempted."
  exit 0
fi
if [ "$#" -ne 0 ]; then
  [ "$#" -eq 2 ] && [ "$1" = "--cms-wal-checkpoint" ] && [ "$2" = "--confirm" ] || {
    echo "Usage: sh scripts/release/storage_census.sh [--cms-wal-checkpoint --confirm]" >&2
    exit 64
  }
  wal="$db-wal"
  plist="$HOME/Library/LaunchAgents/com.bonfyre.public-web-address.plist"
  stop="$HOME/.bonfyre/operator/STOP"
  lock="$HOME/.bonfyre/state/.cms-wal-maintenance.lock"
  [ -f "$stop" ] && [ ! -L "$stop" ] || { echo "REFUSED: operator STOP is not present" >&2; exit 75; }
  [ -f "$db" ] && [ ! -L "$db" ] && [ -f "$wal" ] && [ ! -L "$wal" ] || {
    echo "REFUSED: canonical database and WAL must be existing regular, non-symlink files" >&2; exit 75;
  }
  [ ! -e "$HOME/.bonfyre/state/nightly-checkout.lock" ] || { echo "REFUSED: nightly checkout lock is present" >&2; exit 75; }
  [ -f "$plist" ] && [ -x /usr/libexec/PlistBuddy ] && [ -x /usr/bin/sqlite3 ] || {
    echo "REFUSED: service identity or macOS SQLite CLI unavailable" >&2; exit 75;
  }
  live=$(/usr/libexec/PlistBuddy -c "Print :EnvironmentVariables:BONFYRE_CMS_DB" "$plist" 2>/dev/null || :)
  [ "$live" = "$db" ] || { echo "REFUSED: installed WebAddress CMS differs from canonical path" >&2; exit 75; }
  mode=$(/usr/bin/sqlite3 -readonly "$db" "PRAGMA journal_mode;" 2>/dev/null || :)
  [ "$mode" = wal ] || { echo "REFUSED: canonical CMS is not readable in WAL mode" >&2; exit 75; }
  before_kib=$(du -sk "$wal" | awk '{print $1}')
  free_kib=$(df -Pk "$db" | awk 'END {print $4}')
  [ "$before_kib" -ge 1048576 ] && [ "$free_kib" -ge 786432 ] || {
    echo "REFUSED: checkpoint needs a WAL >=1 GiB and >=768 MiB free" >&2; exit 75;
  }
  [ ! -e "$lock" ] || { echo "REFUSED: another CMS WAL maintenance lock exists" >&2; exit 75; }
  mkdir "$lock" || { echo "REFUSED: cannot establish maintenance lock" >&2; exit 75; }
  trap 'rmdir "$lock" 2>/dev/null || :' EXIT HUP INT TERM
  echo "CMS_WAL_CHECKPOINT: operator paused, installed DB identity verified, WAL mode confirmed."
  echo "CMS_WAL_OPEN_HANDLES (diagnostic; other services may remain online):"
  if command -v lsof >/dev/null 2>&1; then lsof -nP "$db" "$wal" "$db-shm" 2>/dev/null | head -n 35 || :; fi
  result=$(/usr/bin/sqlite3 -batch -separator '|' "$db" "PRAGMA busy_timeout=5000; PRAGMA wal_checkpoint(TRUNCATE);" 2>&1) || {
    echo "CHECKPOINT_REFUSED: SQLite reported an error; no files manually changed" >&2
    printf '%s\n' "$result" >&2
    exit 75
  }
  outcome=$(printf '%s\n' "$result" | tail -n 1)
  old_ifs=$IFS; IFS='|'
  read -r busy remaining checkpointed <<EOF
$outcome
EOF
  IFS=$old_ifs
  case "$busy" in 0) ;; *) echo "CHECKPOINT_BUSY: $outcome; active readers or writers may prevent truncation" >&2; exit 75 ;; esac
  after_kib=$(du -sk "$wal" 2>/dev/null | awk '{print $1}' || :)
  [ -n "$after_kib" ] || after_kib=0
  physical_free=$(df -Pk "$db" | awk 'END {print $4}')
  printf "CMS_WAL_CHECKPOINT_OK before_kib=%s after_kib=%s df_free_kib=%s result=%s\n" "$before_kib" "$after_kib" "$physical_free" "$outcome"
  echo "SQLite completed the checkpoint. A recurring WAL requires investigating long-lived readers and write volume."
  exit 0
fi
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
printf 'BONFYRE_STORAGE_CENSUS repo=%s\n' "$repo"
printf '\n== Volume (df is physical; du can double-count APFS clones) ==\n'
df -h /System/Volumes/Data "$HOME" 2>/dev/null || df -h "$HOME"
printf '\n== Filesystem capacity ==\n'
df -Pk "$HOME" | awk 'END { printf "available_kib=%s capacity_kib=%s\n", $4, $2 }'
top_children() {
  title=$1
  dir=$2
  count=$3
  printf '\n== %s ==\n' "$title"
  if [ ! -d "$dir" ] || [ -L "$dir" ]; then
    echo "NOT_PRESENT $dir"
    return
  fi
  printf 'PARENT %s\n' "$dir"
  {
    for entry in "$dir"/* "$dir"/.[!.]* "$dir"/..?*; do
      if [ -e "$entry" ] || [ -L "$entry" ]; then
        du -sk "$entry" 2>/dev/null || :
      fi
    done
  } | sort -nr | head -n "$count"
}
top_children 'Home directories (top 25)' "$HOME" 25
top_children 'Bonfyre estate roots (top 35)' "$HOME/.bonfyre" 35
top_children 'Bonfyre operator subdivisions (top 20)' "$HOME/.bonfyre/operator" 20
top_children 'Release candidates (top 30)' "$HOME/.bonfyre/releases" 30
top_children 'Developer workspaces (top 20)' "$HOME/Developer" 20
top_children 'Bonfyre checkout (top 30)' "$repo" 30
top_children 'Library (top 25)' "$HOME/Library" 25
top_children 'Library caches (top 25)' "$HOME/Library/Caches" 25
top_children 'User cache (top 20)' "$HOME/.cache" 20
top_children 'Bun state (top 12)' "$HOME/.bun" 12
top_children 'Downloads (top 15)' "$HOME/Downloads" 15
top_children 'Estate-local subtrees (protect databases)' "$HOME/.bonfyre/estate-local" 35
top_children 'Estate-local representations' "$HOME/.bonfyre/estate-local/representations" 25
top_children 'Bonfyre cached models and worktrees' "$HOME/.cache/bonfyre" 30
top_children 'Bonfyre verification evidence' "$HOME/.bonfyre/operator/verification" 25
top_children 'Bonfyre vendor distribution' "$HOME/.bonfyre/vendor" 20
top_children 'Bonfyre rollback trees' "$HOME/.bonfyre/rollback" 20
top_children 'Application Support top 25' "$HOME/Library/Application Support" 25
top_children 'App containers top 20' "$HOME/Library/Containers" 20
top_children 'Codex state top 18' "$HOME/.codex" 18
top_children 'Local tools top 18' "$HOME/.local" 18
top_children 'VS Code extensions top 15' "$HOME/.vscode" 15
printf '\n== Named storage groups ==\n'
for item in "$HOME/.bonfyre/density" "$HOME/.bonfyre/rollback" \
  "$HOME/.bonfyre/estate-local" "$HOME/.bonfyre/estate" \
  "$HOME/.bonfyre/workspace-packages" "$HOME/BonfyreModels" \
  "$HOME/.bonfyre/models" "$HOME/Developer/Bonfyre-converge" \
  "$HOME/Library/Application Support/Google/Chrome" \
  "$HOME/Library/Application Support/Code"; do
  if [ -e "$item" ]; then du -sk "$item" 2>/dev/null || :; fi
done
printf '\n== APFS local snapshots (read-only; do not delete blindly) ==\n'
if command -v tmutil >/dev/null 2>&1; then
  tmutil listlocalsnapshots / 2>&1 | head -n 35 || :
fi
printf '\n== APFS data volume snapshots (read-only) ==\n'
if command -v diskutil >/dev/null 2>&1; then
  diskutil apfs listSnapshots /System/Volumes/Data 2>&1 | head -n 45 || :
fi
printf '\n== Release references (file paths only; not contents) ==\n'
for item in "$HOME/Library/LaunchAgents/com.bonfyre.public-web-address.plist" \
  "$HOME/.bonfyre/density/releases-excluded-live-rollback-referenced-20261030.txt"; do
  if [ -e "$item" ]; then printf 'REVIEW %s\n' "$item"; fi
done
printf '\nREAD_ONLY: no files or snapshots were modified.\n'
printf 'The release, operator and model sizes above are NOT deletion approvals.\n'
