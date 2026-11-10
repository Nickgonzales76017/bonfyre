#!/bin/sh
# Account-owned completion gate for the 2026-10-29 Codex + Claude work.
# This never resumes agents or activates a release; approval stays in Aurekai.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
bun_bin=${BONFYRE_BUN_BINARY:-$HOME/.bun/bin/bun}
[ -x "$bun_bin" ] || bun_bin=$(command -v bun || true)
mode=${1:-audit}
release="$root/scripts/release/public_surface_rollout.mjs"
[ -n "$bun_bin" ] && [ -x "$bun_bin" ] || { echo 'Bun missing' >&2; exit 2; }
case "$mode" in
  audit)
    git diff --check
    printf 'HEAD %s\n' "$(git rev-parse --short HEAD)"
    printf 'CHANGED %s\n' "$(git status --porcelain | wc -l | tr -d ' ')"
    "$bun_bin" "$release" status || true
    if [ -x cmd/BonfyreProviderEstate/bonfyre-provider-estate ]; then
      cmd/BonfyreProviderEstate/bonfyre-provider-estate check --dir "$root/cmd/BonfyreProviderEstate"
    fi
    if [ -x cmd/BonfyreReleaseApproval/bonfyre-release-approval ]; then
      cmd/BonfyreReleaseApproval/bonfyre-release-approval list
    fi
    printf '%s\n' 'GATES: Worlds editor, media/LMS, provider authorization and authenticated rail remain independently open until real user readback.'
    ;;
  smoke)
    tmp=$(mktemp)
    trap 'rm -f "$tmp"' EXIT HUP INT TERM
    failed=0
    for host in aurekai.work aurekai.life aurekai.live aurekai.shop nick.aurekai.work; do
      got=$(curl -sS --max-time 40 -H 'Accept: text/html' -o "$tmp" -w '%{http_code}' "https://$host/" || true)
      case "$got" in 200) printf 'PASS %s\n' "$host" ;; *) printf 'FAIL %s HTTP %s\n' "$host" "$got"; failed=1 ;; esac
      if [ "$host" = nick.aurekai.work ]; then
        grep -Fq 'data-profile-cover' "$tmp" || { echo 'FAIL live Cleveland profile cover'; failed=1; }
      fi
    done
    for path in /people /board /s/prices/observed /s/models/all /o/cleveland-artworks/a47812567fb8271d725264a7; do
      got=$(curl -sS --max-time 40 -H 'Accept: text/html' -o "$tmp" -w '%{http_code}' "https://aurekai.work$path" || true)
      case "$got" in 200) printf 'PASS %s\n' "$path" ;; *) printf 'FAIL %s HTTP %s\n' "$path" "$got"; failed=1 ;; esac
    done
    got=$(curl -sS --max-time 40 -o "$tmp" -w '%{http_code}' https://aurekai.work/examples || true)
    [ "$got" = 410 ] || { echo 'FAIL retired /examples contract'; failed=1; }
    exit "$failed"
    ;;
  test)
    git diff --check
    # Check the native-retirement budget before expensive browser suites.
    sh scripts/native-ratchet.sh check
    # Bun 1.3 does not implement Node-style --check; the test imports and parses the release module.
    "$bun_bin" test scripts/release/tests/public_surface_rollout.test.mjs
    if [ -x cmd/BonfyreProviderEstate/bonfyre-provider-estate ]; then
      sh cmd/BonfyreProviderEstate/tests/provider_estate_test.sh "$root/cmd/BonfyreProviderEstate/bonfyre-provider-estate"
    fi
    make -C cmd/BonfyreCMS bonfyre-cms
    make -C cmd/BonfyreWebAddress account-world-workspace-test
    make -C cmd/BonfyreWebAddress account-activity-witness
    ;;
  prepare)
    git diff --check
    sh scripts/native-ratchet.sh check
    "$bun_bin" test scripts/release/tests/public_surface_rollout.test.mjs
    make -C cmd/BonfyreCMS bonfyre-cms
    make -C cmd/BonfyreWebAddress account-world-workspace-test
    make -C cmd/BonfyreWebAddress account-activity-witness
    lock="$HOME/.bonfyre/state/.oct29-release-gate"
    mkdir -p "$HOME/.bonfyre/state"
    mkdir "$lock" || { echo 'Another release gate is running' >&2; exit 1; }
    out=$(mktemp)
    trap 'rm -f "$out"; rmdir "$lock"' EXIT HUP INT TERM
    # Capture the stage exit code; a pipeline through tee hides a failing stage in /bin/sh.
    if "$bun_bin" "$release" stage > "$out" 2>&1; then cat "$out"; else cat "$out" >&2; exit 1; fi
    dir=$(sed -n 's/^STAGED \(.*\)$/\1/p' "$out" | tail -1)
    case "$dir" in "$HOME"/.bonfyre/releases/activity-public-*) ;; *) echo 'Invalid staged release path' >&2; exit 1 ;; esac
    [ -d "$dir" ] && [ ! -L "$dir" ] || exit 1
    "$bun_bin" "$release" verify "$dir"
    [ -f "$dir/accepted.mpk" ] || { echo 'Missing acceptance receipt' >&2; exit 1; }
    sh scripts/release/approved_activation.sh offer "$dir" 'Verified public WebAddress candidate from combined development work. Worlds/LMS and other account features remain separate acceptance targets.'
    echo 'OFFERED to Aurekai Workboard; production activation requires account approval.'
    ;;
  *)
    echo 'Usage: oct29_completion_gate.sh audit|smoke|test|prepare' >&2
    exit 2 ;;
esac
