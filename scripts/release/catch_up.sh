#!/bin/sh
# Bonfyre catch-up: verify native release owners, reconcile only on explicit
# sync, and offer frozen releases to the account approval workflow.
set -eu
umask 077
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
cd "$root"
mode=check
case "$#" in 0) ;; *) mode=$1; shift ;; esac
case "$mode" in check|sync|offer) ;; *)
  echo "usage: sh scripts/release/catch_up.sh [check|sync|offer RELEASE_DIR] [--offline]" >&2
  exit 64 ;;
esac
offline=0
release=""
if [ "$mode" = offer ]; then
  [ "$#" -gt 0 ] || { echo "offer requires a real accepted release directory" >&2; exit 64; }
  release=$1
  shift
fi
if [ "$#" -gt 0 ]; then
  [ "$#" -eq 1 ] && [ "$1" = --offline ] || exit 64
  offline=1
fi
bun=$HOME/.bun/bin/bun
[ -z "${BONFYRE_BUN_BINARY:-}" ] || bun=$BONFYRE_BUN_BINARY
[ -x "$bun" ] || bun=$(command -v bun || :)
[ -n "$bun" ] && [ -x "$bun" ] || { echo "BLOCKED: Bun is unavailable" >&2; exit 69; }
[ "$(git rev-parse --show-toplevel)" = "$root" ] || exit 65
head=$(git rev-parse HEAD)
stop=$HOME/.bonfyre/operator/STOP
printf 'BONFYRE_CATCH_UP %s HEAD=%s\n' "$mode" "$head"
# Cheap physical-volume admission before any fixture, compiler, or release write.
# A 2 GiB minimum for tests is not the independent release reserve.
need_kib=2097152
for volume_path in "$HOME" "${TMPDIR:-/tmp}"; do
  have_kib=$(df -Pk "$volume_path" 2>/dev/null | awk 'END {print $4}' || :)
  case "$have_kib" in ""|*[!0-9]*)
    echo "DISK_PRESSURE: cannot measure $volume_path" >&2; exit 75 ;;
  esac
  if [ "$have_kib" -lt "$need_kib" ]; then
    printf "DISK_PRESSURE: %s has %s KiB free, need >=%s KiB to run tests.\n" "$volume_path" "$have_kib" "$need_kib" >&2
    echo "Inspect actual bloat: sh scripts/release/storage_census.sh" >&2
    echo "No tests, builds, managed promotions, or publication attempted." >&2
    exit 75
  fi
done
# A checkout can be healthy while the canonical CMS grows a gigabyte WAL.
# Treat that as release pressure, not as a license to delete a SQLite sidecar.
cms_wal="$HOME/.bonfyre/estate-local/cms.db-wal"
if [ -f "$cms_wal" ] && [ ! -L "$cms_wal" ]; then
  wal_kib=$(du -sk "$cms_wal" | awk '{print $1}')
  if [ "$wal_kib" -ge 524288 ]; then
    printf "CMS_WAL_PRESSURE: %s KiB allocated; investigate long-lived reads and checkpoint starvation.\n" "$wal_kib" >&2
    echo "Owner-approved recovery: sh scripts/release/storage_census.sh --cms-wal-checkpoint --confirm" >&2
    if [ "$mode" = sync ] && [ "$wal_kib" -ge 1048576 ]; then
      echo "SYNC_HELD: canonical CMS WAL >=1 GiB; reconcile it before building managed images." >&2
      exit 75
    fi
  fi
fi
run() {
  printf '\n== %s ==\n' "$*"
  "$@"
}

# A verified release offer does not activate or resume the paused operator.
if [ "$mode" = offer ]; then
  case "$release" in "$HOME"/.bonfyre/releases/*) ;; *)
    echo "BLOCKED: select a real directory in ~/.bonfyre/releases/" >&2
    exit 65 ;;
  esac
  [ -d "$release" ] && [ ! -L "$release" ] || exit 65
  run "$bun" scripts/release/public_surface_rollout.mjs verify "$release"
  [ -f "$release/accepted.mpk" ] || { echo "BLOCKED: acceptance receipt absent" >&2; exit 73; }
  run sh scripts/release/approved_activation.sh offer "$release" \
    "Bonfyre catch-up release, acceptance rechecked; publish only after in-app approval."
  echo "OFFERED: activation remains with Aurekai approval."
  exit 0
fi

failed=0
gate() {
  printf '\n== VERIFY %s ==\n' "$1"
  shift
  if "$@"; then echo "PASS"; else
    echo "FAIL (no promotion authorized)" >&2
    failed=1
  fi
}
gate "source whitespace" git diff --check
gate "native ratchet" sh scripts/native-ratchet.sh check
gate "MCP owner tests" "$bun" test \
  integrations/mcp/tests/project_operator.test.mjs \
  integrations/mcp/tests/project_reads.test.mjs \
  integrations/mcp/tests/project_node.test.mjs \
  integrations/mcp/tests/project_service_profiles.test.mjs
gate "browser workspace" "$bun" test integrations/browser/tests/program-workspace.browser.test.mjs
gate "public release contracts" "$bun" test scripts/release/tests/public_surface_rollout.test.mjs
gate "native WebAddress build" make -C cmd/BonfyreWebAddress bonfyre-web-address

drift=0
printf '\n== MANAGED IMAGE ==\n'
if sh scripts/organ_drift.sh --verify --json >/dev/null; then
  echo "PASS: managed byte identities match"
else
  drift=1
  echo "DRIFT: digest mismatch or missing managed binaries"
fi
sh scripts/organ_drift.sh | tail -n 1

if [ "$mode" = sync ]; then
  [ "$failed" -eq 0 ] || { echo "BLOCKED: failing release checks" >&2; exit 1; }
  [ ! -e "$stop" ] && [ ! -L "$stop" ] || {
    echo "SYNC_HELD: operator STOP active. Explicit operator decision required." >&2
    exit 73
  }
  [ -z "$(git status --porcelain --untracked-files=normal)" ] || {
    echo "SYNC_HELD: dirty checkout. Review and commit existing changes first; no reset." >&2
    exit 73
  }
  lock=$HOME/.bonfyre/state/.catch-up-sync
  mkdir -p "$HOME/.bonfyre/state"
  mkdir "$lock" || { echo "SYNC_HELD: another catch-up owns the lock" >&2; exit 73; }
  trap 'rmdir "$lock" 2>/dev/null || :' EXIT HUP INT TERM
  run make all
  [ "$(git rev-parse HEAD)" = "$head" ] || { echo "SYNC_HELD: HEAD changed" >&2; exit 73; }
  [ ! -e "$stop" ] && [ ! -L "$stop" ] || { echo "SYNC_HELD: STOP appeared" >&2; exit 73; }
  run sh scripts/organ_drift.sh --sync --install-missing
  run sh scripts/organ_drift.sh --verify
  drift=0
  # MCP processes may be pinned to a source SHA; do not overwrite LaunchAgents
  # or blindly restart stale configurations.
  echo "MCP_HELD: reconcile running MCP source identity via its service owner."
fi

if [ "$offline" -eq 0 ]; then
  printf '\n== PUBLIC HTTP (availability only, not parity) ==\n'
  for host in aurekai.work aurekai.life aurekai.live aurekai.shop nick.aurekai.work; do
    code=$(curl -sS -L --connect-timeout 5 --max-time 15 -o /dev/null -w '%{http_code}' "https://$host/" 2>/dev/null || :)
    case "$code" in 200) echo "HTTP_PASS $host" ;; *)
      echo "HTTP_UNVERIFIED $host $code"; failed=1 ;;
    esac
  done
  printf '\n== EDGE / REMOTE OWNER (read-only) ==\n'
  "$bun" scripts/release/edge_reachability_audit.mjs || failed=1
fi
if [ "$failed" -eq 0 ] && [ "$drift" -eq 0 ]; then
  echo "CHECKS_PASSED (not public release acceptance)"
  exit 0
fi
echo "CATCH_UP_UNCONVERGED: checks=$failed managed_drift=$drift"
exit 1
