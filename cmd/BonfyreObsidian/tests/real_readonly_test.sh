#!/bin/sh
# Read-only run of bonfyre-obsidian against a real vault on this machine.
#
#   sh tests/real_readonly_test.sh ./bonfyre-obsidian /path/to/vault
#
# The store, state and logs go to a temp directory; the vault itself is only
# read. Every file's size, mtime and sha256 is recorded before and after, and
# the run fails if any of them moved. Only verbs that do not write the vault
# are used: doctor, bind (a store row), vaults, status, guard, search, tasks,
# tags, properties, unresolved, backlinks, links, read, snapshot (a store row).
set -u
B=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
V=${2:-}
[ -n "$V" ] && [ -d "$V" ] || { echo "real_readonly_test: give a vault folder"; exit 2; }
V=$(cd "$V" && pwd -P)
T=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-obsidian-real.XXXXXX"); T=$(cd "$T" && pwd -P)
trap 'rm -rf "$T"' EXIT
export BONFYRE_OBSIDIAN_DB="$T/obsidian.db" BONFYRE_OBSIDIAN_STATE="$T/state" BONFYRE_OBSIDIAN_ROOTS="$(dirname "$V")"
PASS=0; FAIL=0
fingerprint() { (cd "$V" && find . -type f -print0 | sort -z | xargs -0 stat -f '%N %z %m' ; find . -type f -print0 | sort -z | xargs -0 shasum -a 256) > "$1"; }
step() { OUT=$("$B" "$@" 2>/dev/null); RC=$?; printf '%s' "$OUT" | jq -c "${JQ:-.}" 2>/dev/null | cut -c1-400; }
ok() { if [ "$RC" = "$1" ]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); echo "FAIL: $2 rc=$RC"; fi; }

fingerprint "$T/before"
echo "== doctor"; JQ='{app:.app, cli:.cli, headless:{installed:.headless.installed,version:.headless.version,node:.headless.node.version,unscoped_login:.headless.unscoped_login_present}, by_hand:[.by_hand[].why]}' step doctor --account real-test; ok 0 doctor
echo "== bind (store row only)"; JQ='.binding | {name,vault_id,local_path,transport}' step bind --account real-test --name real --path "$V"; ok 0 bind
echo "== status"; JQ='.vaults[0] | {desktop_sync_plugin, cli_usable}' step status --account real-test --name real; ok 0 status
NOTE=$(cd "$V" && find . -name '*.md' -not -path './.*' | head -1 | sed 's|^\./||')
echo "== search (first note: $NOTE)"; WORD=$(sed -n 's/[^A-Za-z]*\([A-Za-z]\{4,\}\).*/\1/p' "$V/$NOTE" | head -1)
JQ='.answer | {shape, query, files_scanned, hits:(.matches|length), first:.matches[0]}' step search --account real-test --name real --query "${WORD:-the}"; ok 0 search
echo "== tasks"; JQ='.answer.counts' step tasks --account real-test --name real; ok 0 tasks
echo "== tags"; JQ='[.answer.tags[] | .tag] | .[:12]' step tags --account real-test --name real; ok 0 tags
echo "== properties"; JQ='.answer.keys' step properties --account real-test --name real; ok 0 properties
echo "== unresolved"; JQ='.answer.count' step unresolved --account real-test --name real; ok 0 unresolved
if [ -n "$NOTE" ]; then
  echo "== read"; JQ='{via, path, bytes, sha256}' step read --account real-test --name real --path "$NOTE"; ok 0 read
  echo "== backlinks"; JQ='.answer | {target, count}' step backlinks --account real-test --name real --path "$NOTE"; ok 0 backlinks
  echo "== links"; JQ='.answer | {source, count}' step links --account real-test --name real --path "$NOTE"; ok 0 links
fi
echo "== snapshot (store rows only)"; JQ='{files, bytes, rehashed, symlinks_skipped, manifest_digest}' step snapshot --account real-test --name real; ok 0 snapshot
echo "== headless guard (a second binding would be refused; checked by rebinding as headless)"
"$B" unbind --account real-test --name real >/dev/null
JQ='.binding.transport' step bind --account real-test --name real --path "$V" --transport headless; ok 0 "bind headless"
JQ='.guard' step guard --account real-test --name real; ok 0 guard
fingerprint "$T/after"
if cmp -s "$T/before" "$T/after"; then PASS=$((PASS + 1)); echo "== vault unchanged (size, mtime and sha256 of every file)"; else FAIL=$((FAIL + 1)); echo "FAIL: the vault changed"; diff "$T/before" "$T/after" | head; fi
echo "real_readonly_test: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
