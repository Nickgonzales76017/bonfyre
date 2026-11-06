#!/bin/sh
# A static estate must follow the page organ's own continuation beyond the
# former twelve-page exporter ceiling.  The fixture ends the route at 14, so
# this also proves that following next is bounded by the server's actual end.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-static-depth.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
port_file="$tmp/port"

python3 - "$port_file" <<'PY' &
from http.server import BaseHTTPRequestHandler, HTTPServer
import re, sys
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/feed.atom":
            encoded = b'<feed xmlns="http://www.w3.org/2005/Atom"><title>Selected public material</title></feed>'
            self.send_response(200); self.send_header('Content-Type', 'application/atom+xml')
            self.send_header('Content-Length', str(len(encoded))); self.end_headers()
            self.wfile.write(encoded)
            return
        depth = 1
        match = re.fullmatch(r"/discover/(\d+)", self.path)
        if match:
            depth = int(match.group(1))
        if self.path.startswith("/discover") and depth <= 13:
            body = ('<link rel="alternate" type="application/atom+xml" href="/feed.atom"><main><a href="/o/source-%d/item">item</a>' % depth +
                    ('<a rel="next" href="/discover/%d">more</a>' % (depth + 1)
                     if depth < 13 else '') + '</main>')
        else:
            body = '<main>nothing here</main>'
        encoded = body.encode()
        self.send_response(200); self.send_header('Content-Type', 'text/html')
        self.send_header('Content-Length', str(len(encoded))); self.end_headers()
        self.wfile.write(encoded)
    def log_message(self, *args): pass
server = HTTPServer(('127.0.0.1', 0), Handler)
open(sys.argv[1], 'w').write(str(server.server_port))
server.serve_forever()
PY
server=$!
for _ in $(seq 1 50); do [ -s "$port_file" ] && break; sleep 0.05; done
[ -s "$port_file" ] || { kill "$server" 2>/dev/null || true; exit 1; }
port=$(cat "$port_file")

"$root/scripts/export_static_estate.sh" --from "http://127.0.0.1:$port" --out "$tmp/dist" --max-depth 400 >"$tmp/export.log"
kill "$server" 2>/dev/null || true
wait "$server" 2>/dev/null || true

[ -s "$tmp/dist/discover/13.html" ] || { echo 'depth 13 was not exported' >&2; exit 1; }
[ ! -e "$tmp/dist/discover/14.html" ] || { echo 'empty terminal length was exported' >&2; exit 1; }
grep -q 'source-13' "$tmp/dist/discover/13.html"
[ -s "$tmp/dist/feed.atom" ] || { echo 'advertised Atom feed was not exported' >&2; exit 1; }
grep -q 'Selected public material' "$tmp/dist/feed.atom"
[ -s "$tmp/dist/people.html" ] || { echo 'people program was not exported' >&2; exit 1; }
[ -s "$tmp/dist/organisations.html" ] || { echo 'organisations program was not exported' >&2; exit 1; }
[ -s "$tmp/dist/search.html" ] || { echo 'search program was not exported' >&2; exit 1; }
echo 'static export depth: PASS'
