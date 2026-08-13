#!/bin/sh
# Proves scripts/bonfyre-supervise actually restarts bonfyred after a crash
# (SIGKILL, not a graceful stop) and stops cleanly on request -- not just
# that the script parses its own arguments.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
supervise="$root/scripts/bonfyre-supervise"
release="$root/scripts/bonfyre-release"
run_root=${BONFYRE_STATE_DIR:-/tmp/bonfyre-supervise-lifecycle}
prefix="$run_root/opt"

rm -rf "$run_root"
mkdir -p "$run_root"

fail() { echo "FAIL: $*" >&2; exit 1; }

port=$(python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
)

wait_for_health() {
    attempts=0
    while ! curl --silent --fail "http://127.0.0.1:$port/health" >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        [ "$attempts" -lt 100 ] || return 1
        sleep 0.05
    done
}

wait_for_no_health() {
    attempts=0
    while curl --silent --fail "http://127.0.0.1:$port/health" >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        [ "$attempts" -lt 100 ] || return 1
        sleep 0.05
    done
}

echo "== install a real prefix to supervise =="
"$release" install "$prefix" >/dev/null
[ -x "$prefix/bin/bonfyred" ] || fail "install: bonfyred missing from installed prefix"

# bonfyred refuses to serve against an uninitialized fabric database, and it
# resolves its own state dir the same way every other binary here does: via
# BONFYRE_STATE_DIR if set, else its own internal default. Pin it explicitly
# to run_root so the daemon the supervisor launches uses the exact database
# compiled below, regardless of what the caller's environment happens to be.
BONFYRE_STATE_DIR="$run_root/daemon-state"
export BONFYRE_STATE_DIR
"$prefix/bin/bonfyre" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
    "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
    "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null \
    || fail "fabric compile failed before starting supervision"

echo "== start supervision =="
"$supervise" start "$prefix" --port "$port" >"$run_root/supervise-start.log" 2>&1 &
supervise_fg_pid=$!
wait_for_health || { cat "$prefix/supervise.log" 2>/dev/null >&2; fail "supervised daemon never became healthy"; }
"$supervise" status "$prefix" | grep -q '^supervising=yes$' || fail "status does not report supervising=yes while running"
echo "supervised daemon healthy on port $port"

echo "== crash the daemon (SIGKILL, not graceful) and confirm real restart =="
child_pid=$(pgrep -f "bonfyred serve --port $port" | head -1)
[ -n "$child_pid" ] || fail "could not find supervised bonfyred pid"
kill -KILL "$child_pid"
wait_for_no_health || fail "daemon did not go down after being killed (test is not exercising a real crash)"
wait_for_health || { cat "$prefix/supervise.log" >&2; fail "supervisor did not restart the daemon after SIGKILL"; }
new_child_pid=$(pgrep -f "bonfyred serve --port $port" | head -1)
[ -n "$new_child_pid" ] && [ "$new_child_pid" != "$child_pid" ] || fail "restarted process has the same pid as the killed one"
grep -q "restarting in" "$prefix/supervise.log" || fail "supervise.log has no record of the restart"
echo "supervisor restarted the killed daemon (old pid=$child_pid, new pid=$new_child_pid)"

echo "== stop supervision cleanly =="
"$supervise" stop "$prefix" >/dev/null
wait_for_no_health || fail "daemon still answering health checks after stop"
attempts=0
while kill -0 "$supervise_fg_pid" 2>/dev/null; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 100 ] || fail "supervisor process did not exit after stop"
    sleep 0.05
done
"$supervise" status "$prefix" | grep -q '^supervising=no$' || fail "status does not report supervising=no after stop"
[ -f "$prefix/supervise.pid" ] && fail "supervise.pid was not cleaned up after stop"
echo "supervisor stopped cleanly, pid file removed"

echo "supervise lifecycle: all checks passed"
