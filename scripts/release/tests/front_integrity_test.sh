#!/bin/sh
# The rendered-front probe names each failure aurekai.* once showed by hand,
# and stays quiet on a page that has none of them.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-front-integrity.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
export BONFYRE_CHROMIUM_BINARY="${BONFYRE_CHROMIUM_BINARY:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"

cat > "$tmp/broken.html" <<'EOF'
<!doctype html><meta name=viewport content="width=device-width"><body><main>
<h1>Front</h1>
<section class="wf wf-desk"><a class="wf-story" href="#"><h3>A story with no sheet behind it</h3></a><p class="wf-by"><span class="wf-who">Scott Detrow</span><span>npr.org</span><time>6d ago</time></p></section>
<section class="wf wf-lead"><a class="wf-story" href="#"><h2>Another bare story</h2></a></section>
<section style="height:4px;overflow:hidden">A section whose words are all clipped away by a zero-height body here</section>
<div style="width:2000px">wide</div>
<a href="https://aurekai-work-samples.pages.dev/red3-planner/">Try the demo</a>
</main></body>
EOF
cat > "$tmp/clean.html" <<'EOF'
<!doctype html><meta name=viewport content="width=device-width"><style>.by{display:flex;gap:.5rem}</style><body><main>
<h1>Front</h1>
<section><a href="/o/x"><b>A story</b></a><span class="by"><span>Scott Detrow</span><span>npr.org</span><time>6d ago</time></span></section>
<p><span>Spaced</span> <span>words</span></p>
</main></body>
EOF

out=$(bun "$root/scripts/release/front_integrity.mjs" --url "file://$tmp/broken.html" || true)
for kind in unstyled run-together collapsed overflow outreach-link; do
    printf '%s\n' "$out" | grep -q "	$kind	" || { printf 'front integrity: missed %s\n%s\n' "$kind" "$out" >&2; exit 1; }
done
printf '%s\n' "$out" | grep -q 'Scott Detrow|npr.org' || { echo 'front integrity: byline pair not named' >&2; exit 1; }
clean=$(bun "$root/scripts/release/front_integrity.mjs" --url "file://$tmp/clean.html") || { printf 'front integrity: clean page failed\n%s\n' "$clean" >&2; exit 1; }
printf '%s\n' "$clean" | grep -q '^FAIL' && { printf 'front integrity: false alarm\n%s\n' "$clean" >&2; exit 1; }
echo 'front integrity: PASS'
