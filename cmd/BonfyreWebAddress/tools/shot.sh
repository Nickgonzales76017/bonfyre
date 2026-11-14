#!/bin/sh
# Screenshot a preview page as a browser sees it.
#   sh tools/shot.sh HOST PATH OUT.png [WIDTH] [HEIGHT]
# WIDTH below 500 (headless Chrome's minimum window) renders the page inside an
# iframe of exactly WIDTH px. SHOT_COOKIE='name=value' fetches the page signed in
# (the preview is plain http, so a Secure cookie is sent by curl, then the HTML
# is rendered with its assets still served by the preview).
set -eu
host=$1 path=$2 out=$3 w=${4:-1280} h=${5:-2400}
port=${BONFYRE_PREVIEW_PORT:-28911}
C="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
D=$(mktemp -d); rm -f "$out"
url="http://$host$path"
if [ -n "${SHOT_COOKIE:-}" ]; then
  curl -s -H "Host: $host" -H 'Accept: text/html' -H "Cookie: $SHOT_COOKIE" "http://127.0.0.1:$port$path" \
    | sed "s#<head>#<head><base href=\"http://$host/\">#" > "$D/page.html"
  url="file://$D/page.html"
fi
win=$w; vt=--virtual-time-budget=5000
if [ "$w" -lt 500 ]; then
  printf '<!doctype html><body style="margin:0;background:#000"><iframe src="%s" style="border:0;width:%spx;height:%spx"></iframe>' "$url" "$w" "$h" > "$D/wrap.html"
  url="file://$D/wrap.html"; win=500; vt=
fi
"$C" --headless=new --user-data-dir="$D/prof" --allow-file-access-from-files --disable-web-security \
  --disable-features=HttpsUpgrades,HttpsFirstModeV2,HttpsFirstBalancedModeAutoEnable \
  --host-resolver-rules="MAP $host:80 127.0.0.1:$port" --window-size=$win,$h --hide-scrollbars \
  $vt --screenshot="$out" "$url" >/dev/null 2>&1 &
pid=$!; i=0; while [ $i -lt 45 ] && [ ! -s "$out" ]; do sleep 1; i=$((i+1)); done
sleep 1; kill $pid 2>/dev/null || true; pkill -f "user-data-dir=$D/prof" 2>/dev/null || true; rm -rf "$D"
ls -la "$out"
