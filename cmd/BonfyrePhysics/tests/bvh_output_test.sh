#!/bin/sh
# Exercise explicit output placement on a real local Embed pack.
# This does not establish semantic representation quality.
set -eu
bin=$1
pack=${BONFYRE_EMBED_PACK:-${HOME}/.local/share/bonfyre/embeds/pack.bfpack}
if [ ! -f "$pack" ]; then
    echo "physics bvh output: SKIP (no local embedding pack)"
    exit 0
fi
tmp=$(mktemp -d "${TMPDIR:-/tmp}/physics-bvh-out.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
"$bin" bvh --pack "$pack" --out "$tmp/isolated.bfvh" >"$tmp/stdout"
test -s "$tmp/isolated.bfvh"
grep -F "$tmp/isolated.bfvh" "$tmp/stdout" >/dev/null
if "$bin" bvh --pack "$pack" --out >"$tmp/no-path-out" 2>"$tmp/no-path-err"; then
    echo "physics bvh output: missing output path unexpectedly succeeded" >&2
    exit 1
fi
grep -F -- "--out requires a path" "$tmp/no-path-err" >/dev/null
if "$bin" bvh --pack "$pack" --unexpected >"$tmp/unknown-out" 2>"$tmp/unknown-err"; then
    echo "physics bvh output: unknown option unexpectedly succeeded" >&2
    exit 1
fi
grep -F "unexpected argument" "$tmp/unknown-err" >/dev/null
echo "physics bvh output: PASS (isolated actual-pack BVH; invalid options refused)"
