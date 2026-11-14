#!/bin/sh
# The static release carries exactly the published recording AND transcript.
# Published addresses without their exact bytes refuse export; draft assets
# are never inferred from a body present at the source.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d "/tmp/bonfyre-static-media.XXXXXX")
server=""
cleanup() { [ -z "$server" ] || { kill "$server" 2>/dev/null || :; wait "$server" 2>/dev/null || :; }; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
mkdir -p "$tmp/source/assets/twin"
printf 'Synthetic transport fixture; native decoding is tested separately.\n' > "$tmp/original.m4a"
printf 'WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nA reviewed transcript.\n' > "$tmp/original.vtt"
printf 'Unpublished fixture\n' > "$tmp/draft.m4a"
digest() {
    if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print $1}'
    else sha256sum "$1" | awk '{print $1}'; fi
}
audio=$(digest "$tmp/original.m4a")
vtt=$(digest "$tmp/original.vtt")
draft=$(digest "$tmp/draft.m4a")
cp "$tmp/original.m4a" "$tmp/source/assets/twin/$audio.m4a"
cp "$tmp/original.vtt" "$tmp/source/assets/twin/$vtt.vtt"
cp "$tmp/draft.m4a" "$tmp/source/assets/twin/$draft.m4a"
sqlite3 "$tmp/cms.db" <<SQL
CREATE TABLE media_program(media_kind TEXT,status TEXT,publication_state TEXT,asset_ref TEXT,transcript_ref TEXT);
INSERT INTO media_program VALUES('audio','published','published','/assets/twin/$audio.m4a','/assets/twin/$vtt.vtt');
INSERT INTO media_program VALUES('audio','draft','draft','/assets/twin/$draft.m4a','');
SQL
python3 - "$tmp/port" "$tmp/source" <<'PY' &
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import sys
class Source(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=sys.argv[2], **kwargs)
    def log_message(self,*args): pass
server=ThreadingHTTPServer(('127.0.0.1',0), Source)
with open(sys.argv[1],'w') as f: f.write(str(server.server_port))
server.serve_forever()
PY
server=$!
for _ in $(seq 1 50); do [ -s "$tmp/port" ] && break; sleep 0.05; done
[ -s "$tmp/port" ] || { echo 'FAIL: media source did not start' >&2; exit 1; }
port=$(cat "$tmp/port")
publish() {
    (cd "$root" && sh scripts/export_static_estate.sh --media-only \
        --from "http://127.0.0.1:$port" --host "test.localhost" \
        --cms "$tmp/cms.db" --out "$1")
}
publish "$tmp/dist" > "$tmp/ok.log" || { cat "$tmp/ok.log" >&2; echo 'FAIL: valid published media failed export' >&2; exit 1; }
cmp "$tmp/original.m4a" "$tmp/dist/assets/twin/$audio.m4a" >/dev/null ||
    { echo 'FAIL: published audio missing or altered' >&2; exit 1; }
cmp "$tmp/original.vtt" "$tmp/dist/assets/twin/$vtt.vtt" >/dev/null ||
    { echo 'FAIL: published transcript missing or altered' >&2; exit 1; }
[ ! -e "$tmp/dist/assets/twin/$draft.m4a" ] ||
    { echo 'FAIL: draft audio exported' >&2; exit 1; }
printf 'tampered' >> "$tmp/source/assets/twin/$audio.m4a"
if publish "$tmp/tampered" > "$tmp/tampered.log" 2>&1; then
    echo 'FAIL: wrong digest accepted in static release' >&2; exit 1
fi
[ ! -e "$tmp/tampered/assets/twin/$audio.m4a" ] ||
    { echo 'FAIL: tampered artifact was retained' >&2; exit 1; }
cp "$tmp/original.m4a" "$tmp/source/assets/twin/$audio.m4a"
rm "$tmp/source/assets/twin/$vtt.vtt"
if publish "$tmp/missing" > "$tmp/missing.log" 2>&1; then
    echo 'FAIL: missing published transcript accepted in static release' >&2; exit 1
fi
echo 'Bonfyre static media: published audio and transcript, draft exclusion, digest and missing-asset refusal: PASS'
