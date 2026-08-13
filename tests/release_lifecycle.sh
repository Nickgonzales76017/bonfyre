#!/bin/sh
# Proves the Fabric release lifecycle (install/upgrade/rollback/uninstall/
# reinstall/portable install/durable-state recovery) against real installed
# paths, using a real durable state directory containing genuine missions,
# events, receipts, and artifacts -- not a copy of one binary to a scratch
# directory.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
release="$root/scripts/bonfyre-release"
run_root=${BONFYRE_STATE_DIR:-/tmp/bonfyre-release-lifecycle}
prefix="$run_root/opt"
state="$run_root/state"
portable_prefix="$run_root/portable-opt"
portable_state="$run_root/portable-state"

rm -rf "$run_root"
mkdir -p "$run_root"

fail() { echo "FAIL: $*" >&2; exit 1; }

seed_state() {
    binary=$1; state_dir=$2
    BONFYRE_STATE_DIR="$state_dir" "$binary" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
        "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
        "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null \
        || fail "seed: fabric compile failed"
    fixture="$run_root/seed-input.txt"
    printf 'release lifecycle durable evidence fixture\n' >"$fixture"
    uri=$(BONFYRE_STATE_DIR="$state_dir" "$binary" artifact ingest "$fixture" text/plain | sed -n 's/^uri=//p')
    [ -n "$uri" ] || fail "seed: artifact ingest produced no uri"
    BONFYRE_STATE_DIR="$state_dir" "$binary" mission create release-lifecycle-mission >/dev/null
    BONFYRE_STATE_DIR="$state_dir" "$binary" work add release-lifecycle-mission execute command.hash "$uri" >/dev/null
    BONFYRE_STATE_DIR="$state_dir" "$binary" work run release-lifecycle-mission | grep -q 'node=execute status=complete output=bonfyre://artifact/' \
        || fail "seed: work run did not complete"
}

state_counts() {
    state_dir=$1
    db="$state_dir/fabric.db"
    [ -f "$db" ] || { echo "0 0 0"; return; }
    missions=$(sqlite3 "$db" 'SELECT count(*) FROM missions;')
    events=$(sqlite3 "$db" 'SELECT count(*) FROM events;')
    receipts=$(sqlite3 "$db" 'SELECT count(*) FROM receipts;')
    echo "$missions $events $receipts"
}

require_binary_runs() {
    binary=$1
    BONFYRE_STATE_DIR="$run_root/cli-probe-state" "$binary" >/dev/null 2>&1
    code=$?
    # A well-formed CLI must at least parse and dispatch (prints help, exits 0),
    # not crash or be missing.
    [ "$code" -eq 0 ] || fail "binary $binary did not run cleanly (exit $code)"
}

echo "== install =="
version1=$("$release" install "$prefix")
[ -x "$prefix/bin/bonfyre" ] || fail "install: bonfyre binary missing"
[ -x "$prefix/bin/bonfyred" ] || fail "install: bonfyred binary missing"
require_binary_runs "$prefix/bin/bonfyre"

seed_state "$prefix/bin/bonfyre" "$state"
before_counts=$(state_counts "$state")
set -- $before_counts
[ "$1" -ge 1 ] || fail "seed: expected at least one mission, got $before_counts"
[ "$2" -ge 1 ] || fail "seed: expected at least one event, got $before_counts"
[ "$3" -ge 1 ] || fail "seed: expected at least one receipt, got $before_counts"
echo "seeded durable state: missions=$1 events=$2 receipts=$3"

echo "== upgrade =="
version2=$("$release" upgrade "$prefix")
[ "$version2" != "$version1" ] || fail "upgrade: version did not change"
require_binary_runs "$prefix/bin/bonfyre"
after_upgrade_counts=$(state_counts "$state")
[ "$after_upgrade_counts" = "$before_counts" ] || fail "upgrade: durable state changed ($before_counts -> $after_upgrade_counts)"
BONFYRE_STATE_DIR="$state" "$prefix/bin/bonfyre" mission show release-lifecycle-mission | grep -q '^status=' \
    || fail "upgrade: seeded mission is no longer queryable through the upgraded binary"
echo "upgrade preserved durable state: $after_upgrade_counts"

echo "== rollback =="
rolled_back_to=$("$release" rollback "$prefix")
[ "$rolled_back_to" = "$version1" ] || fail "rollback: expected $version1, got $rolled_back_to"
[ "$(readlink "$prefix/bin")" = "versions/$version1" ] || fail "rollback: current symlink does not point at $version1"
require_binary_runs "$prefix/bin/bonfyre"
after_rollback_counts=$(state_counts "$state")
[ "$after_rollback_counts" = "$before_counts" ] || fail "rollback: durable state changed ($before_counts -> $after_rollback_counts)"
echo "rollback restored $version1, durable state intact: $after_rollback_counts"

echo "== uninstall (no purge) =="
"$release" uninstall "$prefix"
[ ! -e "$prefix/bin" ] || fail "uninstall: bin symlink still present"
[ ! -d "$prefix/versions" ] || fail "uninstall: versions directory still present"
[ -d "$state" ] || fail "uninstall: durable state directory was removed"
after_uninstall_counts=$(state_counts "$state")
[ "$after_uninstall_counts" = "$before_counts" ] || fail "uninstall: durable state changed ($before_counts -> $after_uninstall_counts)"
echo "uninstall removed the release, durable state intact: $after_uninstall_counts"

echo "== reinstall =="
version3=$("$release" reinstall "$prefix")
[ -x "$prefix/bin/bonfyre" ] || fail "reinstall: bonfyre binary missing"
require_binary_runs "$prefix/bin/bonfyre"
after_reinstall_counts=$(state_counts "$state")
[ "$after_reinstall_counts" = "$before_counts" ] || fail "reinstall: durable state changed ($before_counts -> $after_reinstall_counts)"
BONFYRE_STATE_DIR="$state" "$prefix/bin/bonfyre" mission show release-lifecycle-mission | grep -q 'status=complete' \
    || fail "reinstall: seeded mission is no longer reported complete through the reinstalled binary"
echo "reinstall ($version3) preserved durable state across full uninstall->reinstall: $after_reinstall_counts"

echo "== portable install (relocated prefix) =="
version_portable=$("$release" install "$portable_prefix")
[ -x "$portable_prefix/bin/bonfyre" ] || fail "portable install: binary missing"
require_binary_runs "$portable_prefix/bin/bonfyre"
seed_state "$portable_prefix/bin/bonfyre" "$portable_state"
portable_counts=$(state_counts "$portable_state")
set -- $portable_counts
[ "$1" -ge 1 ] && [ "$2" -ge 1 ] && [ "$3" -ge 1 ] || fail "portable install: seeding at relocated prefix failed"
echo "portable install ($version_portable) works standalone at $portable_prefix: $portable_counts"

echo "== portable install (source-independent operator dispatch) =="
# Unlike the check above (which still points fabric compile at this dev
# checkout's estate/ files), this proves the installed prefix's OWN bundled
# declarations and operator binaries are sufficient -- no reference to $root
# anywhere in this block. That's the actual "no dev checkout required" claim.
portable_bin="$portable_prefix/bin"
[ -d "$portable_bin/estate" ] || fail "portable install: bundled estate/ missing"
[ -x "$portable_bin/cmd/BonfyreHash/bonfyre-hash" ] || fail "portable install: bundled BonfyreHash binary missing"
dispatch_state="$run_root/portable-dispatch-state"
rm -rf "$dispatch_state"
mkdir -p "$dispatch_state"
BONFYRE_STATE_DIR="$dispatch_state" "$portable_bin/bonfyre" fabric compile \
    "$portable_bin/bonfyre.workspace.yaff" "$portable_bin/bonfyre.lock.yaff" \
    "$portable_bin/estate/catalog.yaff" "$portable_bin/estate/compositions.yaff" \
    "$portable_bin/estate/profiles.yaff" "$portable_bin/estate/legacy-operators.tsv" >/dev/null \
    || fail "portable install: fabric compile failed using only bundled declarations"
dispatch_fixture="$run_root/portable-dispatch-input.txt"
printf 'portable dispatch evidence fixture\n' >"$dispatch_fixture"
dispatch_uri=$(BONFYRE_STATE_DIR="$dispatch_state" "$portable_bin/bonfyre" artifact ingest "$dispatch_fixture" text/plain | sed -n 's/^uri=//p')
BONFYRE_STATE_DIR="$dispatch_state" "$portable_bin/bonfyre" mission create m-portable-dispatch >/dev/null
BONFYRE_STATE_DIR="$dispatch_state" "$portable_bin/bonfyre" work add m-portable-dispatch n-portable-dispatch command.hash "$dispatch_uri" >/dev/null
BONFYRE_STATE_DIR="$dispatch_state" "$portable_bin/bonfyre" work run m-portable-dispatch | grep -q 'status=complete' \
    || fail "portable install: real operator dispatch failed using only bundled binaries"
echo "portable install: real operator dispatch (command.hash) succeeded using only bundled files, no dev checkout referenced"

echo "== durable-state recovery (forced termination mid-upgrade) =="
pre_crash_version=$(readlink "$prefix/bin" | sed 's#^versions/##')
if BONFYRE_RELEASE_CRASH_BEFORE_ACTIVATE=1 "$release" upgrade "$prefix" >/tmp/bonfyre-release-crash.log 2>&1; then
    crash_status=0
else
    crash_status=$?
fi
[ "$crash_status" -ne 0 ] || fail "durable-state recovery: expected the injected crash to terminate the upgrade nonzero"
post_crash_version=$(readlink "$prefix/bin" | sed 's#^versions/##')
[ "$post_crash_version" = "$pre_crash_version" ] || fail "durable-state recovery: current symlink moved despite crash before activation ($pre_crash_version -> $post_crash_version)"
require_binary_runs "$prefix/bin/bonfyre"
post_crash_counts=$(state_counts "$state")
[ "$post_crash_counts" = "$after_reinstall_counts" ] || fail "durable-state recovery: durable state changed across crashed upgrade"
echo "crashed upgrade left prefix pinned at $pre_crash_version, durable state intact: $post_crash_counts"

echo "-- retry after crash --"
version_recovered=$("$release" upgrade "$prefix")
[ "$version_recovered" != "$pre_crash_version" ] || fail "durable-state recovery: retry did not produce a new version"
require_binary_runs "$prefix/bin/bonfyre"
final_counts=$(state_counts "$state")
[ "$final_counts" = "$after_reinstall_counts" ] || fail "durable-state recovery: durable state changed after successful retry"
echo "retry succeeded ($version_recovered), durable state intact: $final_counts"

echo ""
echo "release lifecycle: all checks passed"
