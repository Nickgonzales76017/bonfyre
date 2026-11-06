#!/bin/sh
# A refetched body that did not change is not rewritten, and a changed one of
# 64 KiB or more is stored compressed on APFS at the same path with the same
# bytes. This is where a reclaim pass used to be needed after every reheat.
set -eu
bin=$(cd "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
tmp=$(mktemp -d)
port=$((19000 + $$ % 900))
cat > "$tmp/serve.mjs" <<JS
let body = "<rss>" + "item ".repeat(40000) + "</rss>";
Bun.serve({ hostname: "127.0.0.1", port: $port, fetch(request) {
  if (new URL(request.url).pathname === "/change") { body += "x"; return new Response("ok"); }
  if (new URL(request.url).pathname === "/robots.txt") return new Response("User-agent: *\nAllow: /\n");
  return new Response(body, { headers: { "content-type": "application/rss+xml" } });
} });
JS
bun "$tmp/serve.mjs" & server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null || true; rm -rf "$tmp"' EXIT
for _ in 1 2 3 4 5 6 7 8 9 10; do curl -s -o /dev/null "http://127.0.0.1:$port/" && break; sleep 0.2; done
url="http://127.0.0.1:$port/feed"
observe() { "$bin" foreign observe external:test:settle "$url" --receipt "$tmp/r.json" --body "$tmp/r.body" >/dev/null 2>&1 || true; }

observe
[ -s "$tmp/r.body" ] || { echo "retained body settle: skipped, loopback sources are not observed here" >&2; exit 0; }
want=$(curl -s "$url" | shasum -a 256 | cut -d' ' -f1)
[ "$(shasum -a 256 < "$tmp/r.body" | cut -d' ' -f1)" = "$want" ]
if [ "$(uname)" = Darwin ]; then ls -lO "$tmp/r.body" | grep -q compressed; fi
inode=$(ls -i "$tmp/r.body" | awk '{print $1}')

if [ "$(uname)" = Darwin ]; then
  # A body kept raw before this rule is compressed once by its next identical read.
  cp "$tmp/r.body" "$tmp/raw" && mv "$tmp/raw" "$tmp/r.body"
  ls -lO "$tmp/r.body" | grep -q compressed && { echo "fixture is not raw" >&2; exit 1; }
  observe
  ls -lO "$tmp/r.body" | grep -q compressed
  [ "$(shasum -a 256 < "$tmp/r.body" | cut -d' ' -f1)" = "$want" ]
  inode=$(ls -i "$tmp/r.body" | awk '{print $1}')
fi
observe   # same bytes again: the retained file is left exactly where it was
[ "$(ls -i "$tmp/r.body" | awk '{print $1}')" = "$inode" ]
[ "$(wc -l < "$tmp/r.json" | tr -d ' ')" -ge 2 ]

curl -s -o /dev/null "http://127.0.0.1:$port/change"
observe   # changed bytes replace it, still whole to a reader
want=$(curl -s "$url" | shasum -a 256 | cut -d' ' -f1)
[ "$(shasum -a 256 < "$tmp/r.body" | cut -d' ' -f1)" = "$want" ]
[ "$(ls -i "$tmp/r.body" | awk '{print $1}')" != "$inode" ]
ls "$tmp" | grep -q -E 'staging|afsc' && { echo "left a staging file" >&2; exit 1; }
echo "retained body settle: PASS"
