#!/bin/sh
# A native corpus stays with its executable or an explicitly chosen release.
# No production state, network, payment, or model is used.
set -eu
here=$(pwd -P)
bin="$here/bonfyre-experience-map"
[ -x "$bin" ] || { echo "experience-map: native executable missing" >&2; exit 1; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/experience-map-corpus.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/work" "$tmp/pkg/bin" "$tmp/pkg/share/bonfyre/experience-map/current" "$tmp/empty-home"
# The checkout's corpus is found even when the invoking process has another cwd.
(cd "$tmp/work" && "$bin" stats) > "$tmp/checkout.json"
cmp -s "$here/generated/manifest.json" "$tmp/checkout.json" || {
  echo "experience-map: cwd-dependent checkout corpus" >&2; exit 1;
}
(cd "$tmp/work" && "$bin" chain-def creator-publish) |
  grep -q 'creator-publish' || { echo "experience-map: generated chains missing" >&2; exit 1; }
# A shipped binary reads an explicitly packaged corpus relative to its own
# binary, not the developer's checkout and not an arbitrary working directory.
cp "$bin" "$tmp/pkg/bin/bonfyre-experience-map"
cp "$here/generated/manifest.json" "$tmp/pkg/share/bonfyre/experience-map/current/manifest.json"
cp "$here/generated/all_bindings.tsv" "$tmp/pkg/share/bonfyre/experience-map/current/all_bindings.tsv"
(cd "$tmp/work" && HOME="$tmp/empty-home" "$tmp/pkg/bin/bonfyre-experience-map" stats) > "$tmp/package.json"
cmp -s "$tmp/checkout.json" "$tmp/package.json" || {
  echo "experience-map: packaged manifest drift" >&2; exit 1;
}
(cd "$tmp/work" && HOME="$tmp/empty-home" PATH="$tmp/pkg/bin:$PATH" bonfyre-experience-map stats) > "$tmp/installed.json"
cmp -s "$tmp/checkout.json" "$tmp/installed.json" || {
  echo "experience-map: PATH-invoked executable failed to find its corpus" >&2; exit 1;
}
# Explicit release choice wins even if absent. There must be no silent fall
# back to the checkout or a similar-looking corpus.
mkdir -p "$tmp/selected"
printf '{"schema":"test.explicit.corpus"}\n' > "$tmp/selected/manifest.json"
(cd "$tmp/work" && BONFYRE_EXPERIENCE_MAP_ROOT="$tmp/selected" "$bin" stats) > "$tmp/explicit.json"
grep -q 'test.explicit.corpus' "$tmp/explicit.json" || {
  echo "experience-map: explicit release was not selected" >&2; exit 1;
}
if (cd "$tmp/work" && BONFYRE_EXPERIENCE_MAP_ROOT="$tmp/absent" "$bin" stats >/dev/null 2>&1); then
  echo "experience-map: invalid explicit release silently fell back" >&2; exit 1;
fi
echo 'experience-map corpus resolution: PASS (checkout, package, PATH, explicit and refusal)'
