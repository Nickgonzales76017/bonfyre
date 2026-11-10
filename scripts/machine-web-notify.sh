#!/bin/sh
# Tell participating search engines that URLs changed.
#
# This is the push half of machine distribution. Without it every change waits
# for a crawler to come back on its own schedule, and a *withdrawal* waits just
# as long -- which is the dangerous direction. When a source is withdrawn and a
# claim loses support, the copy still sitting in someone else's index is the
# thing that keeps asserting it. Pushing the change is how that gets corrected.
#
# Usage: machine-web-notify.sh <url> [url...]
# Requires BONFYRE_INDEXNOW_KEY, and that key served at /<key>.txt so the
# protocol can verify the notifier controls the host.
set -eu

: "${BONFYRE_INDEXNOW_KEY:?set BONFYRE_INDEXNOW_KEY}"
HOST="${BONFYRE_PUBLIC_HOST:-aurekai.life}"
[ $# -gt 0 ] || { echo "usage: $0 <url> [url...]" >&2; exit 2; }

# Verify the key is actually reachable before claiming anything. A notification
# from a host that cannot prove itself is discarded, silently, at the far end.
served=$(curl -sS -m 15 "https://$HOST/$BONFYRE_INDEXNOW_KEY.txt" 2>/dev/null | tr -d '\r\n')
if [ "$served" != "$BONFYRE_INDEXNOW_KEY" ]; then
  echo "refusing: https://$HOST/$BONFYRE_INDEXNOW_KEY.txt does not serve the key" >&2
  echo "the protocol verifies host control that way, so an unverifiable push is discarded" >&2
  exit 1
fi

list=$(for u in "$@"; do printf '"%s",' "$u"; done | sed 's/,$//')
payload=$(printf '{"host":"%s","key":"%s","keyLocation":"https://%s/%s.txt","urlList":[%s]}' \
  "$HOST" "$BONFYRE_INDEXNOW_KEY" "$HOST" "$BONFYRE_INDEXNOW_KEY" "$list")

code=$(curl -sS -m 30 -o /tmp/indexnow.out -w '%{http_code}' \
  -X POST https://api.indexnow.org/indexnow \
  -H 'Content-Type: application/json; charset=utf-8' -d "$payload")
echo "indexnow: $code for $# url(s)"
[ "$code" = "200" ] || [ "$code" = "202" ] || { cat /tmp/indexnow.out >&2; exit 1; }
