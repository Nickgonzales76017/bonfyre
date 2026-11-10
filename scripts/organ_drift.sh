#!/bin/sh
# Verify and, only when requested, atomically promote the native organs a
# managed Bonfyre runtime can actually execute.
#
# The checkout build in cmd/ and the managed copy in ~/.bonfyre/bin are two
# physical realizations. Modification time is not an identity: a restored copy
# can look newer and still be wrong. This uses content digests, and writes a
# runtime-image receipt only after a requested promotion.
#
#   scripts/organ_drift.sh                 # report; no mutation
#   scripts/organ_drift.sh --verify        # fail if a managed image drifts
#   scripts/organ_drift.sh --sync          # promote only an existing managed organ
#   scripts/organ_drift.sh --sync --install-missing  # explicitly install absent organs
#   scripts/organ_drift.sh --json          # machine-readable report
set -eu

usage() { echo "usage: organ_drift.sh [--verify] [--sync] [--install-missing] [--json] [--only TARGET] [--root DIR] [--managed-dir DIR]"; }
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
MANAGED_DIR="${HOME}/.bonfyre/bin"
MODE=report
JSON=0
INSTALL_MISSING=0
ONLY=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --verify) MODE=verify ;;
        --sync) MODE=sync ;;
        --install-missing) INSTALL_MISSING=1 ;;
        --only) shift; [ "$#" -gt 0 ] || { usage >&2; exit 64; }; ONLY=$1 ;;
        --json) JSON=1 ;;
        --root) shift; [ "$#" -gt 0 ] || { usage >&2; exit 64; }; ROOT=$1 ;;
        --managed-dir) shift; [ "$#" -gt 0 ] || { usage >&2; exit 64; }; MANAGED_DIR=$1 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 64 ;;
    esac
    shift
done
[ -d "$ROOT/cmd" ] || { echo "organ-drift: no cmd directory under $ROOT" >&2; exit 66; }
if [ -n "$ONLY" ]; then
    case "$ONLY" in bonfyre|bonfyre-*) ;; *) echo "organ-drift: invalid exact target $ONLY" >&2; exit 64;; esac
fi

digest() {
    if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print $1}'
    elif command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
    else echo "organ-drift: SHA-256 tool missing" >&2; exit 69; fi
}
json_quote() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr -d '\n'; }
target_for_dir() {
    [ -f "$1/Makefile" ] || return 1
    # Target spelling is declared by the build. Guessing it from PascalCase
    # breaks initialisms such as FPQ and MoQ.
    target=$(sed -n 's/^[[:space:]]*TARGET[[:space:]]*[:?+]*=[[:space:]]*\([^[:space:]#][^[:space:]#]*\).*/\1/p' "$1/Makefile" | head -n 1)
    case "$target" in bonfyre|bonfyre-*) printf '%s\n' "$target";; *) return 1;; esac
}

TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-organ-drift.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
ROWS="$TMP/rows"; : > "$ROWS"
ALL_ROWS="$TMP/all-rows"; : > "$ALL_ROWS"
# A non-whitespace separator retains empty digest/detail fields under POSIX read.
TAB='|'
expected=0; current=0; behind=0; missing=0; refused=0; promoted=0; installed=0

for dir in "$ROOT"/cmd/Bonfyre*/; do
    [ -d "$dir" ] || continue
    target=$(target_for_dir "$dir") || continue
selected=0
if [ -z "$ONLY" ] || [ "$ONLY" = "$target" ]; then selected=1; fi
    built="$dir/$target"; managed="$MANAGED_DIR/$target"
    [ -f "$built" ] && [ -x "$built" ] || continue
if [ "$selected" = 1 ]; then expected=$((expected + 1)); fi
source_digest=$(digest "$built")
    state=current; managed_digest=""; detail=""
    if [ -L "$managed" ]; then
state=refused_symlink; detail="managed realization is a symlink"
if [ "$selected" = 1 ]; then refused=$((refused + 1)); fi
    elif [ ! -e "$managed" ]; then
state=missing; detail="no managed realization"
if [ "$selected" = 1 ]; then missing=$((missing + 1)); fi
    elif [ ! -f "$managed" ] || [ ! -x "$managed" ]; then
state=refused_non_executable; detail="managed realization is not an executable regular file"
if [ "$selected" = 1 ]; then refused=$((refused + 1)); fi
    else
        managed_digest=$(digest "$managed")
if [ "$source_digest" = "$managed_digest" ]; then
    if [ "$selected" = 1 ]; then current=$((current + 1)); fi
else
    state=behind; detail="content digest differs"
    if [ "$selected" = 1 ]; then behind=$((behind + 1)); fi
fi
    fi
if [ "$selected" = 1 ] && [ "$MODE" = sync ] && { [ "$state" = behind ] || { [ "$INSTALL_MISSING" = 1 ] && [ "$state" = missing ]; }; }; then
        mkdir -p "$MANAGED_DIR"
        staged="$MANAGED_DIR/.${target}.stage.$$"
        # Same-directory rename leaves callers with either complete image.
        (umask 077; cp "$built" "$staged")
        chmod 755 "$staged"
        staged_digest=$(digest "$staged")
        [ "$staged_digest" = "$source_digest" ] || { rm -f "$staged"; echo "organ-drift: copy verification failed for $target" >&2; exit 74; }
        mv -f "$staged" "$managed"
        managed_digest=$source_digest
        if [ "$state" = missing ]; then
            state=installed; detail="atomically installed after digest verification"; installed=$((installed + 1))
        else
            state=promoted; detail="atomically promoted after digest verification"; promoted=$((promoted + 1))
        fi
    fi
printf '%s|%s|%s|%s|%s\n' "$target" "$state" "$source_digest" "$managed_digest" "$detail" >> "$ALL_ROWS"
if [ "$selected" = 1 ]; then
    printf '%s|%s|%s|%s|%s\n' "$target" "$state" "$source_digest" "$managed_digest" "$detail" >> "$ROWS"
fi
done
if [ -n "$ONLY" ] && [ "$expected" -ne 1 ]; then
    echo "organ-drift: selected target $ONLY has no built executable; nothing was promoted" >&2
    exit 66
fi

# A manifest is a physical realization receipt. It says what bytes were
# promoted, never that those bytes' semantic behavior is correct.
if [ "$MODE" = sync ]; then
    mkdir -p "$MANAGED_DIR"; stage="$MANAGED_DIR/.bonfyre-organ-manifest.stage.$$"
    {
        printf '{"schema":"bonfyre.organ-runtime-image.v1","root":"'; json_quote "$ROOT"
        printf '","generated_at_utc":"%s","organs":[' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        first=1
        while IFS="$TAB" read -r target state source_digest managed_digest detail; do
            [ "$first" = 1 ] || printf ','; first=0
            printf '{"target":"'; json_quote "$target"; printf '","sha256":"'; json_quote "$managed_digest"
            printf '","state":"'; json_quote "$state"; printf '"}'
done < "$ALL_ROWS"
        printf ']}\n'
    } > "$stage"
    mv -f "$stage" "$MANAGED_DIR/.bonfyre-organ-manifest.json"
fi

if [ "$JSON" = 1 ]; then
    printf '{"schema":"bonfyre.organ-drift-report.v2","mode":"%s","expected":%s,"current":%s,"behind":%s,"missing":%s,"refused":%s,"promoted":%s,"installed":%s,"organs":[' "$MODE" "$expected" "$current" "$behind" "$missing" "$refused" "$promoted" "$installed"
    first=1
    while IFS="$TAB" read -r target state source_digest managed_digest detail; do
        [ "$first" = 1 ] || printf ','; first=0
        printf '{"target":"'; json_quote "$target"; printf '","state":"'; json_quote "$state"
        printf '","checkout_sha256":"'; json_quote "$source_digest"; printf '","managed_sha256":"'; json_quote "$managed_digest"
        printf '","detail":"'; json_quote "$detail"; printf '"}'
    done < "$ROWS"
    printf ']}\n'
else
    while IFS="$TAB" read -r target state source_digest managed_digest detail; do printf '%-10s %-32s %s\n' "$state" "$target" "$detail"; done < "$ROWS"
    printf -- '---- %s expected, %s current, %s behind, %s missing, %s refused, %s promoted, %s installed\n' "$expected" "$current" "$behind" "$missing" "$refused" "$promoted" "$installed"
fi
[ "$MODE" != verify ] || [ $((behind + missing + refused)) -eq 0 ]
