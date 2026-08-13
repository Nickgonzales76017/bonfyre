#!/bin/sh
# Requirement-runner interface for the release lifecycle: each verb is
# invoked as its own process by tests/unified_fabric_acceptance.sh (install
# depends on nothing here; upgrade depends on install; etc.), so state that
# must survive between verbs is kept under the shared acceptance-run
# directory rather than the per-requirement BONFYRE_STATE_DIR each
# invocation otherwise gets.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
release="$root/scripts/bonfyre-release"
verb=${1:?usage: release_lifecycle.sh install|upgrade|rollback|uninstall|reinstall|portable|portable_dispatch|recover}
shared=${BONFYRE_ACCEPTANCE_RUN:-${BONFYRE_STATE_DIR:?BONFYRE_STATE_DIR is required}}/shared/release
prefix="$shared/opt"
state="$shared/state"
portable_prefix="$shared/portable-opt"
portable_state="$shared/portable-state"

fail() { echo "release_lifecycle: $*" >&2; exit 1; }

seed_state() {
    binary=$1; state_dir=$2
    BONFYRE_STATE_DIR="$state_dir" "$binary" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
        "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
        "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null \
        || fail "fabric compile failed"
    fixture="$shared/seed-input-$$.txt"
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
    printf '%s %s %s\n' \
        "$(sqlite3 "$db" 'SELECT count(*) FROM missions;')" \
        "$(sqlite3 "$db" 'SELECT count(*) FROM events;')" \
        "$(sqlite3 "$db" 'SELECT count(*) FROM receipts;')"
}

require_binary_runs() {
    binary=$1
    BONFYRE_STATE_DIR="$shared/cli-probe-state" "$binary" >/dev/null 2>&1 \
        || fail "binary $binary did not run cleanly"
}

require_state_unchanged() {
    label=$1
    current=$(state_counts "$state")
    saved=$(cat "$shared/baseline-counts")
    [ "$current" = "$saved" ] || fail "$label: durable state changed ($saved -> $current)"
}

case "$verb" in
    install)
        rm -rf "$shared"
        mkdir -p "$shared"
        version1=$("$release" install "$prefix")
        [ -x "$prefix/bin/bonfyre" ] || fail "install: bonfyre binary missing"
        [ -x "$prefix/bin/bonfyred" ] || fail "install: bonfyred binary missing"
        require_binary_runs "$prefix/bin/bonfyre"
        seed_state "$prefix/bin/bonfyre" "$state"
        counts=$(state_counts "$state")
        set -- $counts
        [ "$1" -ge 1 ] && [ "$2" -ge 1 ] && [ "$3" -ge 1 ] || fail "seed produced no durable evidence: $counts"
        printf '%s\n' "$counts" >"$shared/baseline-counts"
        printf '%s\n' "$version1" >"$shared/version1"
        echo "release_install=ok version=$version1 missions=$1 events=$2 receipts=$3"
        ;;
    upgrade)
        [ -f "$shared/version1" ] || fail "upgrade: install did not run first"
        version1=$(cat "$shared/version1")
        version2=$("$release" upgrade "$prefix")
        [ "$version2" != "$version1" ] || fail "upgrade: version did not change"
        [ "$(readlink "$prefix/bin")" = "versions/$version2" ] || fail "upgrade: current symlink was not activated to $version2"
        require_binary_runs "$prefix/bin/bonfyre"
        require_state_unchanged upgrade
        BONFYRE_STATE_DIR="$state" "$prefix/bin/bonfyre" mission show release-lifecycle-mission | grep -q '^status=' \
            || fail "upgrade: seeded mission is no longer queryable"
        printf '%s\n' "$version2" >"$shared/version2"
        echo "release_upgrade=ok version=$version2"
        ;;
    rollback)
        [ -f "$shared/version1" ] && [ -f "$shared/version2" ] || fail "rollback: upgrade did not run first"
        version1=$(cat "$shared/version1")
        rolled_back_to=$("$release" rollback "$prefix")
        [ "$rolled_back_to" = "$version1" ] || fail "rollback: expected $version1, got $rolled_back_to"
        [ "$(readlink "$prefix/bin")" = "versions/$version1" ] || fail "rollback: current symlink does not point at $version1"
        require_binary_runs "$prefix/bin/bonfyre"
        require_state_unchanged rollback
        echo "release_rollback=ok version=$rolled_back_to"
        ;;
    uninstall)
        [ -f "$shared/version1" ] || fail "uninstall: install did not run first"
        "$release" uninstall "$prefix"
        [ ! -e "$prefix/bin" ] || fail "uninstall: bin symlink still present"
        [ ! -d "$prefix/versions" ] || fail "uninstall: versions directory still present"
        [ -d "$state" ] || fail "uninstall: durable state directory was removed"
        require_state_unchanged uninstall
        echo "release_uninstall=ok"
        ;;
    reinstall)
        version3=$("$release" reinstall "$prefix")
        [ -x "$prefix/bin/bonfyre" ] || fail "reinstall: bonfyre binary missing"
        require_binary_runs "$prefix/bin/bonfyre"
        require_state_unchanged reinstall
        BONFYRE_STATE_DIR="$state" "$prefix/bin/bonfyre" mission show release-lifecycle-mission | grep -q 'status=complete' \
            || fail "reinstall: seeded mission is no longer reported complete"
        printf '%s\n' "$version3" >"$shared/version3"
        echo "release_reinstall=ok version=$version3"
        ;;
    portable)
        [ -f "$shared/version3" ] || fail "portable: reinstall did not run first"
        version_portable=$("$release" install "$portable_prefix")
        [ -x "$portable_prefix/bin/bonfyre" ] || fail "portable: binary missing"
        require_binary_runs "$portable_prefix/bin/bonfyre"
        seed_state "$portable_prefix/bin/bonfyre" "$portable_state"
        counts=$(state_counts "$portable_state")
        set -- $counts
        [ "$1" -ge 1 ] && [ "$2" -ge 1 ] && [ "$3" -ge 1 ] || fail "portable: seeding at relocated prefix failed"
        echo "portable_install=ok version=$version_portable missions=$1 events=$2 receipts=$3"
        ;;
    portable_dispatch)
        [ -d "$portable_prefix/bin/estate" ] || fail "portable_dispatch: bundled estate/ missing"
        [ -x "$portable_prefix/bin/cmd/BonfyreHash/bonfyre-hash" ] || fail "portable_dispatch: bundled BonfyreHash binary missing"
        pb="$portable_prefix/bin"
        dispatch_state="$shared/portable-dispatch-state"
        BONFYRE_STATE_DIR="$dispatch_state" "$pb/bonfyre" fabric compile \
            "$pb/bonfyre.workspace.yaff" "$pb/bonfyre.lock.yaff" \
            "$pb/estate/catalog.yaff" "$pb/estate/compositions.yaff" \
            "$pb/estate/profiles.yaff" "$pb/estate/legacy-operators.tsv" >/dev/null \
            || fail "portable_dispatch: fabric compile failed using only bundled declarations"
        fixture="$shared/portable-dispatch-input.txt"
        printf 'portable dispatch requirement fixture\n' >"$fixture"
        uri=$(BONFYRE_STATE_DIR="$dispatch_state" "$pb/bonfyre" artifact ingest "$fixture" text/plain | sed -n 's/^uri=//p')
        BONFYRE_STATE_DIR="$dispatch_state" "$pb/bonfyre" mission create m-portable-dispatch >/dev/null
        BONFYRE_STATE_DIR="$dispatch_state" "$pb/bonfyre" work add m-portable-dispatch n-portable-dispatch command.hash "$uri" >/dev/null
        BONFYRE_STATE_DIR="$dispatch_state" "$pb/bonfyre" work run m-portable-dispatch | grep -q 'status=complete' \
            || fail "portable_dispatch: real operator dispatch failed using only bundled binaries"
        echo "portable_dispatch=ok"
        ;;
    recover)
        [ -f "$shared/version3" ] || fail "recover: reinstall did not run first"
        pre_crash_version=$(readlink "$prefix/bin" | sed 's#^versions/##')
        if BONFYRE_RELEASE_CRASH_BEFORE_ACTIVATE=1 "$release" upgrade "$prefix" >"$shared/crash.log" 2>&1; then
            crash_status=0
        else
            crash_status=$?
        fi
        [ "$crash_status" -ne 0 ] || fail "expected the injected crash to terminate the upgrade nonzero"
        post_crash_version=$(readlink "$prefix/bin" | sed 's#^versions/##')
        [ "$post_crash_version" = "$pre_crash_version" ] || fail "current symlink moved despite crash before activation ($pre_crash_version -> $post_crash_version)"
        require_binary_runs "$prefix/bin/bonfyre"
        require_state_unchanged "crashed upgrade"
        version_recovered=$("$release" upgrade "$prefix")
        [ "$version_recovered" != "$pre_crash_version" ] || fail "retry after crash did not produce a new version"
        require_binary_runs "$prefix/bin/bonfyre"
        require_state_unchanged "post-recovery retry"
        echo "durable_state_recovery=ok recovered_version=$version_recovered"
        ;;
    *)
        fail "unknown verb: $verb (expected install|upgrade|rollback|uninstall|reinstall|portable|recover)"
        ;;
esac
