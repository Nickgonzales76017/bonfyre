#!/usr/bin/env bash
# One-shot privacy-first Mac release cleanup (v2).
# HOLD a refused public cohort; do not block independently screened work.
# No production service, customer records, remotes, force-pushes or resets.
set -Eeuo pipefail
umask 077

REPO="${BONFYRE_RELEASE_REPO:-$HOME/Developer/Bonfyre}"
TOOLS="${BONFYRE_RELEASE_TOOLS:-$HOME/Downloads/bonfyre_release_gate}"
PREP_BASE="${BONFYRE_RELEASE_PREP_ROOT:-$HOME/Downloads/bonfyre_release_prepared_20261012}"
PRIVATE_BASE="${BONFYRE_RELEASE_PRIVATE_ROOT:-$HOME/Downloads/bonfyre_release_converged_20261012}"
EXPECTED_HEAD="${BONFYRE_RELEASE_EXPECTED_HEAD:-173d984a8e95}"
EXPECTED_BRANCH="${BONFYRE_RELEASE_EXPECTED_BRANCH:-run8-r2c-convergence-20260823}"

die() { printf 'RELEASE REFUSED: %s\n' "$*" >&2; exit 2; }
[ -d "$REPO/.git" ] || die "Expected Mac repository is unavailable"
[ -f "$REPO/scripts/release/converge.py" ] || die "Convergence owner source missing"
[ -f "$REPO/scripts/release/isolated_review.py" ] || die "Independent review owner missing"
[ -f "$REPO/scripts/release/resolve_legacy_mcp_index.py" ] || die "Guarded index recovery missing"
[ -f "$TOOLS/build_manifests.py" ] || die "Exact cohort builder missing"
SCANNER="$TOOLS/bonfyre_privacy_preflight_v2.py"
if [ ! -f "$SCANNER" ]; then
    SCANNER="$HOME/Downloads/bonfyre_privacy_preflight_v2.py"
fi
[ -f "$SCANNER" ] || die "Privacy scanner v2 not installed"
command -v git >/dev/null 2>&1 || die "Git unavailable"
command -v python3 >/dev/null 2>&1 || die "Python 3 unavailable"

HEAD_NOW="$(git -C "$REPO" rev-parse --short=12 HEAD)"
BRANCH_NOW="$(git -C "$REPO" branch --show-current)"
[ "$HEAD_NOW" = "$EXPECTED_HEAD" ] || die "HEAD changed; old checkpoint is not authoritative"
[ "$BRANCH_NOW" = "$EXPECTED_BRANCH" ] || die "Developer branch has changed"
# Refuse someone else's staged work before allocating any private output.
python3 "$REPO/scripts/release/resolve_legacy_mcp_index.py" \
    --repo "$REPO" --expected-head "$EXPECTED_HEAD" \
    --expected-branch "$EXPECTED_BRANCH"

PREP="$PREP_BASE"
PRIVATE="$PRIVATE_BASE"
if [ -e "$PREP" ] || [ -L "$PREP" ] || [ -e "$PRIVATE" ] || [ -L "$PRIVATE" ]; then
    attempt=2
    while :; do
        [ "$attempt" -le 100 ] || die "No unused private receipt destination"
        PREP="$PREP_BASE-attempt$attempt"
        PRIVATE="$PRIVATE_BASE-attempt$attempt"
        if [ ! -e "$PREP" ] && [ ! -L "$PREP" ] &&
           [ ! -e "$PRIVATE" ] && [ ! -L "$PRIVATE" ]; then
            break
        fi
        attempt=$((attempt + 1))
    done
fi

printf '\nBONFŸRE PRIVATE RELEASE CONVERGENCE\n'
printf 'Repo: %s\nPrepared audit: %s\nPrivate quarantine: %s\n' "$REPO" "$PREP" "$PRIVATE"
mkdir -m 700 -p "$PREP"
python3 "$SCANNER" --repo "$REPO" \
    --out "$PREP/privacy-preflight.json" --history --base origin/main
python3 "$TOOLS/build_manifests.py" --repo "$REPO" \
    --report "$PREP/privacy-preflight.json" --outdir "$PREP/manifests"

PLAN="$(python3 "$REPO/scripts/release/converge.py" --repo "$REPO" \
    --report "$PREP/privacy-preflight.json" --manifests "$PREP/manifests" \
    --private-out "$PRIVATE" --expected-head "$EXPECTED_HEAD" \
    --expected-branch "$EXPECTED_BRANCH")"
printf '%s\n' "$PLAN"
if [[ ! "$PLAN" =~ ([0-9]+)[[:space:]]private-review[[:space:]]source[[:space:]]files ]]; then
    die "No verifiable source-cohort count in current plan"
fi
ADMITTED="${BASH_REMATCH[1]}"
[ "$ADMITTED" -gt 0 ] || die "All source cohorts are held: no isolated release review can be constructed"
case "$PLAN" in
    *"COHORTS REFUSED: none"*)
        printf 'PUBLIC COHORT REVIEW: all current cohorts passed the independent source gate.\n' ;;
    *)
        printf 'PUBLIC COHORT HOLD: refused groups remain excluded; %s screened files can proceed.\n' "$ADMITTED" ;;
esac

# Convergence consumes only admitted cohorts. It never claims held cohorts safe.
python3 "$REPO/scripts/release/converge.py" --repo "$REPO" \
    --report "$PREP/privacy-preflight.json" --manifests "$PREP/manifests" \
    --private-out "$PRIVATE" --expected-head "$EXPECTED_HEAD" \
    --expected-branch "$EXPECTED_BRANCH" --apply

python3 "$REPO/scripts/release/isolated_review.py" \
    --repo "$REPO" --report "$PREP/privacy-preflight.json" \
    --private-out "$PRIVATE" --review-branch "bonfyre-public/source-review-20261012"

[ -z "$(git -C "$REPO" diff --cached --name-only)" ] ||
    die "Development index not empty after cleanup"
[ "$(git -C "$REPO" rev-parse --short=12 HEAD)" = "$EXPECTED_HEAD" ] ||
    die "Development HEAD changed"
[ -z "$(git -C "$PRIVATE/isolated-public-git" remote)" ] ||
    die "Independent review unexpectedly has a remote"
[ -z "$(git -C "$REPO" branch --list 'bonfyre-public/source-review-20261012')" ] ||
    die "Development repository unexpectedly gained a review branch"

printf '\nPRIVATE CONVERGENCE COMPLETE\n'
printf 'Quarantine / receipts: %s\n' "$PRIVATE"
printf 'Review Git (no remote): %s/isolated-public-git\n' "$PRIVATE"
printf 'Local source HEAD unchanged: %s\n' "$EXPECTED_HEAD"
printf 'Public push: NOT PERFORMED. Client delivery: NOT PERFORMED.\n'
