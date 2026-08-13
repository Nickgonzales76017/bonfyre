#!/bin/sh
# Proves a real macFUSE/FSKit mount of BonfyreFS. NOT part of
# tests/unified_fabric_acceptance.sh: this needs the macFUSE FSKit system
# extension already approved via System Settings -> Privacy & Security on
# this specific machine (systemextensionsctl list must show it) -- that
# one-time interactive approval cannot be scripted, so this test cannot run
# unattended on a fresh machine/CI runner the way the core gate must.
#
# What this proves: a real `mount` table entry (not a directory generated
# under /tmp), real live POSIX reads through the mount (readdir/open/read),
# and that content ingested AFTER the mount starts appears immediately
# (proving live DB queries, not a snapshot taken at mount time), then a
# clean unmount.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_root=${BONFYRE_STATE_DIR:-/tmp/bonfyrefs-mount-lifecycle}
mountpoint="$run_root/mount"
state="$run_root/state"

fail() { echo "FAIL: $*" >&2; exit 1; }

[ -x "$root/cmd/BonfyreFS/bonfyre-fs" ] || fail "run: make -C cmd/BonfyreFS first"

rm -rf "$run_root"
mkdir -p "$mountpoint" "$state"
export BONFYRE_STATE_DIR="$state"
# `mount` reports the fully-resolved path (macOS /tmp is a symlink to
# /private/tmp), so resolve mountpoint the same way before matching against
# it below -- otherwise the match never succeeds regardless of timeout.
mountpoint=$(CDPATH= cd -- "$mountpoint" && pwd -P)

fabric="$root/programs/bonfyre/bonfyre"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
    "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
    "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null
mission="m-bonfyrefs-mount-$$"
"$fabric" mission create "$mission" >/dev/null

echo "== mount =="
"$root/cmd/BonfyreFS/bonfyre-fs" "$mountpoint" -f -o backend=fskit >"$run_root/fs.log" 2>&1 &
fs_pid=$!
# FSKit mount registration time is genuinely variable (observed anywhere
# from ~1s to 30s+ on this machine) -- give it a generous budget rather
# than a tight one that produces false failures.
attempts=0
while ! mount | grep -q "on $mountpoint "; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 300 ] || ! kill -0 "$fs_pid" 2>/dev/null; then
        cat "$run_root/fs.log" >&2
        fail "mount did not appear in the mount table (extension approved? systemextensionsctl list)"
    fi
    sleep 0.5
done
mount | grep "on $mountpoint " | grep -q "macfuse" || fail "mount table entry is not a real macfuse mount"
echo "real mount confirmed in the kernel mount table"

echo "== real POSIX reads through the mount =="
[ -d "$mountpoint/Missions" ] || fail "Missions directory not visible through the mount"
[ -d "$mountpoint/Artifacts" ] || fail "Artifacts directory not visible through the mount"
[ -f "$mountpoint/Missions/$mission.json" ] || fail "mission file not visible through the mount"
grep -q "\"mission_id\":\"$mission\"" "$mountpoint/Missions/$mission.json" || fail "mission file content is wrong"
echo "real mission file read through the mount, content correct"

echo "== live content: ingest after mount, confirm it appears without remounting =="
fixture="$run_root/fixture.txt"
printf 'bonfyrefs live mount fixture\n' >"$fixture"
uri=$("$fabric" artifact ingest "$fixture" text/plain | sed -n 's/^uri=//p')
digest=${uri#bonfyre://artifact/}
[ -f "$mountpoint/Artifacts/$digest.json" ] || fail "artifact ingested after mount start did not appear live"
grep -q "\"digest\":\"$digest\"" "$mountpoint/Artifacts/$digest.json" || fail "live artifact file content is wrong"
echo "artifact ingested after mount start appeared live, content correct -- not a static snapshot"

echo "== clean unmount =="
umount "$mountpoint" 2>&1 || diskutil unmount "$mountpoint" >/dev/null 2>&1 || fail "unmount failed"
attempts=0
while kill -0 "$fs_pid" 2>/dev/null; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 50 ] || fail "bonfyre-fs process did not exit after unmount"
    sleep 0.1
done
mount | grep -q "on $mountpoint " && fail "mount table entry still present after unmount"
echo "unmounted cleanly, process exited, mount table clear"

echo "== mission-scoped mount: an agent/tool given --mission sees only its own mission =="
scoped_mountpoint="$run_root/scoped-mount"
mkdir -p "$scoped_mountpoint"
scoped_mountpoint=$(CDPATH= cd -- "$scoped_mountpoint" && pwd -P)
own_mission="m-scoped-own-$$"
other_mission="m-scoped-other-$$"
"$fabric" mission create "$own_mission" >/dev/null
"$fabric" mission create "$other_mission" >/dev/null
own_fixture="$run_root/own.txt"; printf 'scoped mount own fixture\n' >"$own_fixture"
other_fixture="$run_root/other.txt"; printf 'scoped mount other fixture\n' >"$other_fixture"
own_uri=$("$fabric" artifact ingest "$own_fixture" text/plain | sed -n 's/^uri=//p')
other_uri=$("$fabric" artifact ingest "$other_fixture" text/plain | sed -n 's/^uri=//p')
own_digest=${own_uri#bonfyre://artifact/}
other_digest=${other_uri#bonfyre://artifact/}
"$fabric" work add "$own_mission" node-own command.hash "$own_uri" >/dev/null
"$fabric" work add "$other_mission" node-other command.hash "$other_uri" >/dev/null
"$fabric" work run "$own_mission" >/dev/null
"$fabric" work run "$other_mission" >/dev/null

"$root/cmd/BonfyreFS/bonfyre-fs" "$scoped_mountpoint" --mission "$own_mission" -f -o backend=fskit >"$run_root/scoped-fs.log" 2>&1 &
scoped_fs_pid=$!
attempts=0
while ! mount | grep -q "on $scoped_mountpoint "; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 300 ] || ! kill -0 "$scoped_fs_pid" 2>/dev/null; then
        cat "$run_root/scoped-fs.log" >&2
        fail "scoped mount did not appear in the mount table"
    fi
    sleep 0.5
done

[ -f "$scoped_mountpoint/Missions/$own_mission.json" ] || fail "scoped mount does not show its own mission"
[ ! -e "$scoped_mountpoint/Missions/$other_mission.json" ] || fail "scoped mount leaked a mission outside its scope"
[ -f "$scoped_mountpoint/Artifacts/$own_digest.json" ] || fail "scoped mount does not show its own artifact"
[ ! -e "$scoped_mountpoint/Artifacts/$other_digest.json" ] || fail "scoped mount leaked an artifact outside its scope"
missions_listed=$(ls "$scoped_mountpoint/Missions" | wc -l | tr -d ' ')
[ "$missions_listed" -eq 1 ] || fail "scoped mount readdir listed $missions_listed missions, expected exactly 1"
echo "mission-scoped mount correctly isolates one agent/tool's mission and artifacts from every other mission"

umount "$scoped_mountpoint" 2>&1 || diskutil unmount "$scoped_mountpoint" >/dev/null 2>&1 || fail "scoped unmount failed"
attempts=0
while kill -0 "$scoped_fs_pid" 2>/dev/null; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 50 ] || fail "scoped bonfyre-fs process did not exit after unmount"
    sleep 0.1
done
echo "scoped mount unmounted cleanly"

rm -rf "$run_root"
echo "bonfyrefs mount lifecycle: all checks passed"
