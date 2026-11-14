#!/bin/sh
# Offline plan and publish witness: a fixture item archive, fixed engines, no
# network.  Rendering needs the network and voices; it is witnessed separately.
set -eu
bin=${1:?bonfyre-broadcast binary required}
here=$(cd "$(dirname "$0")/.." && pwd)
t=$(mktemp -d); trap 'rm -rf "$t"' EXIT
now=1762560000 # 2025-11-08T00:00:00Z
day=86400

sqlite3 "$t/items.db" <<SQL
CREATE TABLE item(source TEXT NOT NULL, key TEXT NOT NULL, kind TEXT NOT NULL, family TEXT DEFAULT '', title TEXT NOT NULL, body TEXT DEFAULT '', href TEXT DEFAULT '', image TEXT DEFAULT '', by TEXT DEFAULT '', publisher TEXT DEFAULT '', ident TEXT DEFAULT '', at INTEGER DEFAULT 0, first_read TEXT NOT NULL, last_read TEXT NOT NULL, PRIMARY KEY(source, key));
INSERT INTO item VALUES('wire','a','story','news','Council approves the new library budget','','https://a.test/1','','','a.test','',$((now - day)),'x','x');
INSERT INTO item VALUES('wire','b','story','news','Second story from the same wire service','','https://a.test/2','','','a.test','',$((now - 2*day)),'x','x');
INSERT INTO item VALUES('wire','c','story','news','Third story from the same wire is capped','','https://a.test/3','','','a.test','',$((now - 3*day)),'x','x');
INSERT INTO item VALUES('other','d','story','news','A different publisher reports the weather','','https://b.test/1','','','b.test','',$((now - 4*day)),'x','x');
INSERT INTO item VALUES('other','e','story','news','A story dated in the future is not read','','https://b.test/2','','','b.test','',$((now + 9*day)),'x','x');
INSERT INTO item VALUES('other','f','story','news','A story older than the window is not read','','https://b.test/3','','','b.test','',$((now - 400*day)),'x','x');
INSERT INTO item VALUES('wire','h','story','news','The Daily Crossword: November 6, 2025','','https://a.test/9','','','a.test','',$((now - day/2)),'x','x');
INSERT INTO item VALUES('other','g','story','news','Council approves the new library budget','','https://b.test/4','','','b.test','',$((now - 5*day)),'x','x');
SQL

cat > "$t/programs.json" <<'JSON'
{"pools":[],"runtimes":[],"providers":[],"readback":[],"system_exclude":[],
 "programs":[{"id":"t","title":"Test Hour","station":"Test","says":"x","fallback":"b",
   "cast":{"a":{"prefer":["neural","system"]},"b":{"prefer":["system"],"locale":"en_GB"}},
   "clock":[{"kind":"ident","title":"Open","voice":"a","say":"Hello."},
            {"kind":"desk","title":"News","family":"news","count":5,"intro":"The news.","voices":["a","b"]},
            {"kind":"desk","title":"Empty","family":"nothing","count":3,"voices":["b"]}]}]}
JSON
cat > "$t/engines.json" <<'JSON'
[{"id":"n","kind":"neural","locale":"en","ready":false,"reason":"not here"},
 {"id":"s-us","kind":"system","voice":"US","locale":"en_US","ready":true},
 {"id":"s-gb","kind":"system","voice":"GB","locale":"en_GB","ready":true}]
JSON

"$bin" plan --programs "$t/programs.json" --program t --items "$t/items.db" --engines "$t/engines.json" --now $now --out "$t/plan.json" >/dev/null
p="$t/plan.json"
check() { if [ "$(jq -r "$1" "$p")" != "$2" ]; then echo "FAIL: $1 = $(jq -r "$1" "$p"), want $2" >&2; exit 1; fi; }
check '.cast.a' 's-us'                          # neural not ready: the next preference, distinct from b
check '.cast.b' 's-gb'                          # locale honoured
check '.segments | length' '2'                  # the empty desk is dropped ...
check '.skipped[0].family' 'nothing'            # ... and recorded
check '.segments[1].lines | length' '4'         # intro + 3 stories
check '[.segments[1].lines[1:][] | .publisher] | map(select(. == "a.test")) | length' '2'   # two per publisher
check '[.segments[1].lines[] | select(.text | test("future|older"))] | length' '0'          # outside the window
check '[.segments[1].lines[] | select(.text | test("library budget"))] | length' '1'        # one title once
check '[.segments[1].lines[] | select(.text | test("Crossword"))] | length' '0'             # recurring column
check '[.segments[1].lines[1:][] | select(.source == "" or .key == "" or .href == "")] | length' '0'  # every line cited
check '.segments[1].lines[1].role' 'a'          # roles rotate
check '.segments[1].lines[2].role' 'b'
check '.segments[1].lines[1].text' 'Council approves the new library budget. From a.test, November 7.'

# publish writes a draft and never a publication
mkdir -p "$t/out"; printf 'WEBVTT\n' > "$t/out/transcript.vtt"; printf '{"version":"1.2.0","chapters":[]}' > "$t/out/chapters.json"
printf '{"program":"t","planned_at":"2025-11-08T00:00:00Z","audio_sha256":"abcdef0123456789","duration_ms":1000,"cannot_infer":"x"}' > "$t/out/manifest.json"
sqlite3 "$t/cms.db" 'CREATE TABLE media_program(id INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT DEFAULT '"'"'root'"'"' NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, published_at TEXT, status TEXT DEFAULT '"'"'draft'"'"', program_id TEXT, subject_ref TEXT, media_kind TEXT, asset_ref TEXT, duration_ms INTEGER, poster_asset_ref TEXT, transcript_ref TEXT, chapters TEXT, captions TEXT, semantic_track_ref TEXT, publication_state TEXT, cannot_infer TEXT, waveform_peaks TEXT, waveform_audio_sha256 TEXT); CREATE UNIQUE INDEX u ON media_program(namespace, program_id);'
"$bin" publish "$t/out" --cms "$t/cms.db" >/dev/null
"$bin" publish "$t/out" --cms "$t/cms.db" >/dev/null   # idempotent
[ "$(sqlite3 "$t/cms.db" "select count(*)||status||publication_state||coalesce(published_at,'none') from media_program")" = "1draftdraftnone" ] || { echo "FAIL: publish" >&2; exit 1; }

# Waveform peaks must come from decoded, rendered audio and be associated with
# those exact bytes. A synthetic sine wave is only a deterministic test input.
cat > "$t/waveform_witness.c" <<'C'
#include <stdint.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "waveform.h"
int main(int argc, char **argv) {
    char peaks[BF_AUDIO_WAVEFORM_BINS * 4 + 1] = "";
    if (argc != 2 || !bf_waveform_from_audio(argv[1], 2.0, peaks, sizeof(peaks)) ||
        !bf_waveform_valid(peaks) || bf_waveform_valid("20,30")) return 1;
    printf("%s\n", peaks);
    return 0;
}
C
cc -std=c11 -O2 -Wall -Wextra -I "$here/src" "$t/waveform_witness.c" -lm -o "$t/waveform_witness"
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=440:duration=2" -c:a aac -b:a 96k -y "$t/out/program.m4a"
peaks=$("$t/waveform_witness" "$t/out/program.m4a") || { echo "FAIL: measured waveform" >&2; exit 1; }
[ "$(printf '%s' "$peaks" | awk -F, '{print NF}')" -eq 64 ] || { echo "FAIL: 64 amplitude bins" >&2; exit 1; }
sha=$(shasum -a 256 "$t/out/program.m4a" | awk '{print $1}')
jq -n --arg peaks "$peaks" --arg digest "$sha" \
  '{program:"t",planned_at:"2025-11-08T00:00:00Z",duration_ms:2000,cannot_infer:"measured synthetic fixture",audio_sha256:$digest,waveform_audio_sha256:$digest,waveform_peaks:$peaks}' > "$t/out/manifest.json"
"$bin" publish "$t/out" --cms "$t/cms.db" >/dev/null
[ "$(sqlite3 "$t/cms.db" 'SELECT waveform_peaks FROM media_program WHERE length(waveform_peaks) > 0')" = "$peaks" ] || { echo "FAIL: waveform not persisted in draft" >&2; exit 1; }
jq '.waveform_audio_sha256 = "deadbeef"' "$t/out/manifest.json" > "$t/out/bad.json"
mv "$t/out/bad.json" "$t/out/manifest.json"
if "$bin" publish "$t/out" --cms "$t/cms.db" >/dev/null 2>&1; then
  echo "FAIL: waveform accepted mismatched digest" >&2; exit 1
fi
[ "$(sqlite3 "$t/cms.db" 'SELECT waveform_peaks FROM media_program WHERE length(waveform_peaks) > 0')" = "$peaks" ] || { echo "FAIL: refused draft changed the record" >&2; exit 1; }
echo "BonfyreBroadcast: decoded waveform, matched digest and rejected tampering: PASS"

# Generated audio and transcript are staged as locally produced assets, not
# counterfeit ForeignTwin observations. The CMS row remains draft.
mkdir -p "$t/public-assets"
jq --arg digest "$sha" '.program="staged" | .audio_sha256=$digest | .waveform_audio_sha256=$digest' "$t/out/manifest.json" > "$t/out/valid.json"
mv "$t/out/valid.json" "$t/out/manifest.json"
result=$("$bin" publish "$t/out" --cms "$t/cms.db" --subject article:recorded-witness --asset-root "$t/public-assets") || { echo 'FAIL: local staging' >&2; exit 1; }
printf '%s' "$result" | grep -q '"status":"draft"' || { echo 'FAIL: staged audio was not a draft' >&2; exit 1; }
printf '%s' "$result" | grep -q "/assets/twin/$sha.m4a" || { echo 'FAIL: staged recording has no digest address' >&2; exit 1; }
cmp "$t/out/program.m4a" "$t/public-assets/$sha.m4a" >/dev/null || { echo 'FAIL: staged audio bytes changed' >&2; exit 1; }
vttsha=$(shasum -a 256 "$t/out/transcript.vtt" | awk '{print $1}')
cmp "$t/out/transcript.vtt" "$t/public-assets/$vttsha.vtt" >/dev/null || { echo 'FAIL: staged transcript bytes changed' >&2; exit 1; }
[ "$(sqlite3 "$t/cms.db" "SELECT subject_ref||'|'||status||'|'||publication_state||'|'||transcript_ref FROM media_program WHERE subject_ref='article:recorded-witness'")" = "article:recorded-witness|draft|draft|/assets/twin/$vttsha.vtt" ] || { echo 'FAIL: staged media CMS identity or publication state' >&2; exit 1; }
if "$bin" publish "$t/out" --cms "$t/cms.db" --subject article:another-identity --asset-root "$t/public-assets" >/dev/null 2>&1; then echo 'FAIL: staged draft was reassigned to a different subject' >&2; exit 1; fi
sqlite3 "$t/cms.db" "UPDATE media_program SET status='published',publication_state='published' WHERE subject_ref='article:recorded-witness'"
if "$bin" publish "$t/out" --cms "$t/cms.db" --subject article:recorded-witness --asset-root "$t/public-assets" >/dev/null 2>&1; then echo 'FAIL: published media was overwritten by a draft' >&2; exit 1; fi
[ "$(sqlite3 "$t/cms.db" "SELECT count(*) FROM media_program WHERE subject_ref='article:recorded-witness' AND status='published' AND publication_state='published'")" = 1 ] || { echo 'FAIL: published record changed' >&2; exit 1; }
printf 'BonfyreBroadcast: local asset staging, exact bytes, draft and published isolation: PASS\n'
jq '.program = "staged-env"' "$t/out/manifest.json" > "$t/out/manifest.new"
mv "$t/out/manifest.new" "$t/out/manifest.json"
BONFYRE_MEDIA_ASSET_ROOT="$t/public-assets" "$bin" publish "$t/out" --cms "$t/cms.db" --subject article:recorded-env >/dev/null || { echo "FAIL: service-configured media asset root was not honoured" >&2; exit 1; }
[ "$(sqlite3 "$t/cms.db" "SELECT asset_ref||'|'||status FROM media_program WHERE subject_ref='article:recorded-env'")" = "/assets/twin/$sha.m4a|draft" ] || { echo "FAIL: service-configured audio did not become a content-addressed draft" >&2; exit 1; }
printf 'BonfyreBroadcast: installed media root binds publication staging: PASS\n'
# the shipped records export and every program casts its fallback role
jq -e '.programs | all(.cast[.fallback] != null)' "$here/generated/programs.json" >/dev/null || { echo "FAIL: fallback role not cast" >&2; exit 1; }
echo "BonfyreBroadcast: plan cites, caps, windows and casts; publish drafts only: PASS"
