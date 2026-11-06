#!/bin/sh
# A retained-item export is a finite release closure.  A dead origin must make
# publication refuse, never leave a directory that merely looks complete.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-static-items.XXXXXX")
cleanup() { [ -n "${server:-}" ] && kill "$server" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

sqlite3 "$tmp/items.db" <<'SQL'
CREATE TABLE item (source TEXT, key TEXT, at INTEGER);
INSERT INTO item VALUES ('good','one',2), ('gone','two',1);
SQL

python3 - "$tmp/port" <<'PY' &
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/o/gone/two':
            self.send_error(503); return
        body = b'<main><a href="/o/good/one">one</a></main>'
        self.send_response(200); self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *args): pass
server = HTTPServer(('127.0.0.1', 0), Handler)
open(sys.argv[1], 'w').write(str(server.server_port)); server.serve_forever()
PY
server=$!
for _ in $(seq 1 50); do [ -s "$tmp/port" ] && break; sleep 0.05; done
[ -s "$tmp/port" ] || exit 1

if "$root/scripts/export_static_estate.sh" --from "http://127.0.0.1:$(cat "$tmp/port")" \
    --out "$tmp/dist" --archive "$tmp/items.db" --items 2 >"$tmp/export.log" 2>&1; then
    echo 'static item archive: accepted an incomplete closure' >&2; exit 1
fi
grep -q '/o/gone/two did not answer after 3 attempts (503)' "$tmp/export.log"
grep -q 'item archive incomplete: expected 2 retained items, materialized 1' "$tmp/export.log"
[ -s "$tmp/dist/o/good/one.html" ]
[ ! -e "$tmp/dist/o/gone/two.html" ]
echo 'static item archive closure: PASS'
