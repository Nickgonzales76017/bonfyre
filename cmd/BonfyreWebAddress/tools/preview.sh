#!/bin/sh
# Preview the tree's own bonfyre-web-address against copies of the live stores.
#
#   make preview            build, refresh store copies if missing, serve on :28911
#   make preview-refresh    re-copy the live stores, then serve
#   make preview-stop       stop the preview
#
# Nothing here writes to ~/.bonfyre/estate-local: the matrix, auth, CMS and
# projection stores are copied into $BONFYRE_PREVIEW_HOME and the server is
# pointed at the copies. The live service, its plist and its release are not
# touched; the environment is read from the plist so rooms see the same owners.
#
# A browser cannot send a Host header, and assets only answer for estate hosts,
# so open the preview through Chrome's resolver instead of 127.0.0.1:
#   --host-resolver-rules="MAP aurekai.work:80 127.0.0.1:28911, MAP aurekai.life:80 127.0.0.1:28911"
#   then http://aurekai.work/  (plain http; the Host header stays aurekai.work)
# With curl:  curl -H 'Host: aurekai.work' -H 'Accept: text/html' http://127.0.0.1:28911/
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
port=${BONFYRE_PREVIEW_PORT:-28911}
home=${BONFYRE_PREVIEW_HOME:-$HOME/.bonfyre/preview}
live=$HOME/.bonfyre/estate-local
plist=$HOME/Library/LaunchAgents/com.bonfyre.public-web-address.plist
pidfile=$home/preview.pid
bin=$here/bonfyre-web-address

stop() {
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then kill "$(cat "$pidfile")"; fi
  rm -f "$pidfile"
}

case "${1:-serve}" in
  stop) stop; echo "preview stopped"; exit 0 ;;
  refresh) rm -f "$home"/matrix.db "$home"/auth.db "$home"/cms.db "$home"/projection.db ;;
  serve) ;;
  *) echo "usage: preview.sh [serve|refresh|stop]" >&2; exit 2 ;;
esac

[ -x "$bin" ] || { echo "preview: build bonfyre-web-address first" >&2; exit 1; }
mkdir -p "$home"
for db in matrix auth cms; do
  # sqlite3 .backup takes a consistent copy while the live server keeps writing.
  [ -f "$home/$db.db" ] || sqlite3 "$live/$db.db" ".backup '$home/$db.db'"
done
[ -f "$home/projection.db" ] || : > "$home/projection.db"

stop
# Export the live service environment, then point every store it names at the copies.
eval "$(plutil -convert json -o - "$plist" | bun -e '
const env=JSON.parse(await Bun.stdin.text()).EnvironmentVariables||{};
for(const [k,v] of Object.entries(env))console.log("export "+k+"="+JSON.stringify(String(v)).replaceAll("$","\\$"));')"
export BONFYRE_CMS_DB="$home/cms.db" BONFYRE_PUBLIC_PROJECTION_DB="$home/projection.db"
nohup "$bin" serve --db "$home/matrix.db" --auth-db "$home/auth.db" --port "$port" --workers 2 \
  --site-root "$repo/site/matrix" > "$home/preview.log" 2>&1 &
echo $! > "$pidfile"
i=0; until curl -s -o /dev/null -m 1 "http://127.0.0.1:$port/" ; do i=$((i+1)); [ $i -gt 40 ] && { echo "preview: did not start; see $home/preview.log" >&2; exit 1; }; sleep 0.25; done
echo "preview: http://127.0.0.1:$port (pid $(cat "$pidfile")), stores in $home"
