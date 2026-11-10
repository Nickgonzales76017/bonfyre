#!/bin/sh
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
test_dir=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-ratchet-files.XXXXXX")
trap 'rm -rf "$test_dir"' EXIT HUP INT TERM
printf '12345' > "$test_dir/bonfyre-program"
chmod 700 "$test_dir/bonfyre-program"
ln "$test_dir/bonfyre-program" "$test_dir/bonfyre-hardlink"
ln -s bonfyre-program "$test_dir/bonfyre-symlink"
printf '0123456789' > "$test_dir/bonfyre_cms.db"
chmod 600 "$test_dir/bonfyre_cms.db"
measurement=$(BONFYRE_BIN="$test_dir" sh "$root/scripts/native-ratchet.sh" measure)
case "$measurement" in *'"largest_bin_bytes":5,"installed_bonfyre_bytes":5'*) ;;
  *) echo "installed program accounting failed: $measurement" >&2; exit 1 ;;
esac
echo 'native ratchet installed files: PASS'
