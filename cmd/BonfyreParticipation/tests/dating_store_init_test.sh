#!/bin/sh
set -eu
binary=${1:?native participation executable}
case "$binary" in /*) ;; *) binary="$(pwd)/$binary" ;; esac
temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-dating-init.XXXXXX")
trap 'rm -rf "$temp_dir"' EXIT HUP INT TERM
db="$temp_dir/owner.db"
"$binary" dating-init --db "$db" >/dev/null
[ -f "$db" ]
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM dating_request;')" = "0" ]
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM dating_person;')" = "0" ]
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM dating_verification;')" = "0" ]
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM dating_choice;')" = "0" ]
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM dating_message;')" = "0" ]
"$binary" dating-init --db "$db" >/dev/null
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM dating_person;')" = "0" ]
if "$binary" dating-init --db "../unsafe.db" >/dev/null 2>&1; then exit 1; fi
ln -s "$db" "$temp_dir/link.db"
if "$binary" dating-init --db "$temp_dir/link.db" >/dev/null 2>&1; then exit 1; fi
[ -f "$db" ]
chmod 0644 "$db"
if "$binary" dating-init --db "$db" >/dev/null 2>&1; then exit 1; fi
printf 'dating private database init: PASS (empty, idempotent, refused foreign paths/symlinks/shared files)\n'
