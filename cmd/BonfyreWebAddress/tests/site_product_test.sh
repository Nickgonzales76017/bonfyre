#!/bin/sh
# The site verbs, from an empty directory: one command makes a site whose page
# the resolver's own server answers; the network can be listed, shown and
# searched; a site leaves as one self-contained file; writing comes in from a
# saved page; and nothing half-made is left behind by a refusal.
# Everything here is made by the organs themselves. The imported page is a file
# this test writes, so what is not shown is a fetch from a live WordPress site.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
for organ in BonfyreCMS/bonfyre-cms BonfyreIngest/bonfyre-ingest BonfyreParagraph/bonfyre-paragraph BonfyreRecall/bonfyre-recall BonfyreMeter/bonfyre-meter; do
  [ -x "$root/cmd/$organ" ] || { echo "site product: SKIP ($organ not built)"; exit 0; }
done
export BONFYRE_CMS_BINARY="$root/cmd/BonfyreCMS/bonfyre-cms" BONFYRE_INGEST_BINARY="$root/cmd/BonfyreIngest/bonfyre-ingest"
export BONFYRE_PARAGRAPH_BINARY="$root/cmd/BonfyreParagraph/bonfyre-paragraph" BONFYRE_RECALL_BINARY="$root/cmd/BonfyreRecall/bonfyre-recall"
export BONFYRE_CMS_SCHEMAS="$root/cmd/BonfyreCMS/content-types" BONFYRE_SITE_ROOT="$root/site/matrix"
# This isolated product witness must use the source-owned meter, not a stale managed multi-call installation.
export BONFYRE_METER_BINARY="$root/cmd/BonfyreMeter/bonfyre-meter"
home=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-site.XXXXXX")
mkdir -p "$home/recorded-media"
export BONFYRE_MEDIA_ASSET_ROOT="$home/recorded-media"
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
cleanup() { "$web" site stop --home "$home" >/dev/null 2>&1 || true; rm -rf "$home"; }
trap cleanup EXIT INT TERM
fail() { echo "site product: $1" >&2; if [ -n "${BONFYRE_KEEP_FAILED_TEST:-}" ]; then cp -R "$home" "$BONFYRE_KEEP_FAILED_TEST"; fi; exit 1; }
site() { "$web" site "$@" --home "$home" --port "$port"; }
get() { curl -s -m 10 --resolve "$1:$port:127.0.0.1" -H 'Accept: text/html' "http://$1:$port/"; }

made=$(site new acme --title 'Acme Supply' --tagline 'Stamped steel enclosures' --accent '#0b6e4f' --no-open)
printf '%s' "$made" | grep -q '"resolved":true,"page_served":true,"http_status":"200"' || fail "new did not end in a served page: $made"
page=$(get acme.localhost)
printf '%s' "$page" | grep -q '<title>Acme Supply</title>' || fail "the site does not carry its own name"
printf '%s' "$page" | grep -q '<h1>Acme Supply</h1>' || fail "masthead is not the site's name"
printf '%s' "$page" | grep -q -- '--bf-accent:#0b6e4f' || fail "the chosen colour is not on the page"
printf '%s' "$page" | grep -q 'This site is live' || fail "the first post is not on the page"
if printf '%s' "$page" | grep -q 'AUREKAI\|this estate binds'; then fail "an operator's own site wears the estate's name"; fi

# A new site has none of the faults the check looks for; a colour nobody can
# read and a link that leads nowhere are each named.
site check acme --json | grep -q '"failed":0' || fail "a new site fails its own check: $(site check acme || true)"
site new pale --accent '#101612' --no-open >/dev/null
out=$(site check pale --json || true)
printf '%s' "$out" | grep -q '"check":"accent-visible","ok":false' || fail "an accent nobody can see passed"
# An install that left out what the page needs is caught the same way: here
# the site's own stylesheet is missing from the asset root.
thin="$home/thin-root"; mkdir -p "$thin"
cp "$root/site/matrix/client-organs.js" "$thin/"
thinport=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_SITE_ROOT="$thin" "$web" site new thin --no-open --home "$home/thin-home" --port "$thinport" >/dev/null
out=$(BONFYRE_SITE_ROOT="$thin" "$web" site check thin --json --home "$home/thin-home" --port "$thinport" || true)
"$web" site stop --home "$home/thin-home" >/dev/null 2>&1 || true
printf '%s' "$out" | grep -q '"check":"styles-load","ok":false' || fail "a page whose stylesheet is missing passed: $out"
# A published article subject selects only its own published audio edition.
# This tests an HTML binding, not audio decoding or human narration review.
"$BONFYRE_CMS_BINARY" entry create site_presentation '{"site_id":"author-editor-work","host":"editor.aurekai.work","subject_address":"person:editor","profile":"member-profile-v1","territory":"work","navigation":[],"status":"published"}' --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "author profile not admitted"
"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/essay","title":"A long-form report","heading":"A long-form report","lede":"Source-backed reporting","aperture_subject":"article:unrelated","media_subject_ref":"article:acme-report","editorial_kind":"article","author_credit":"Morgan Vale","author_subject_ref":"person:editor","editorial_published_on":"2026-11-07","editorial_updated_on":"2026-11-08","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "article page not admitted"
"$BONFYRE_CMS_BINARY" entry create site_fragment '{"coordinate":"acme-report-text","page":"/essay","slot":"body","audience":"public","kind":"prose","position":0,"heading":"The report","body":"This article follows the source material through each editorial review stage and preserves the identity of the work being discussed. [1](#source-1)","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "article content not admitted"
"$BONFYRE_CMS_BINARY" entry create media_program '{"program_id":"acme-report-audio","subject_ref":"article:acme-report","media_kind":"audio","asset_ref":"/assets/acme-report.wav","transcript_ref":"/assets/acme-report.vtt","publication_state":"published","cannot_infer":"A program does not prove playback, ownership of external material or cue review","status":"published"}' --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "recorded article program not admitted"
"$BONFYRE_CMS_BINARY" entry create media_program '{"program_id":"acme-report-draft","subject_ref":"article:acme-report","media_kind":"audio","asset_ref":"/assets/draft.wav","publication_state":"draft","cannot_infer":"draft is not public","status":"published"}' --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "draft fixture not admitted"
"$BONFYRE_CMS_BINARY" entry create media_program '{"program_id":"acme-other-audio","subject_ref":"article:unrelated","media_kind":"audio","asset_ref":"/assets/unrelated.wav","publication_state":"published","cannot_infer":"unrelated identity cannot supply this article","status":"published"}' --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "other subject fixture not admitted"
"$BONFYRE_CMS_BINARY" entry create site_fragment '{"coordinate":"acme-source-public","page":"/essay","slot":"references","audience":"public","kind":"citation","position":90,"heading":"Published source","body":"The published source backing this example.","href":"https://example.test/observed-record","source_ref":"source:fixture-001","source_digest":"sha256:fixture","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "article citation not admitted"
"$BONFYRE_CMS_BINARY" entry create site_fragment '{"coordinate":"acme-source-draft","page":"/essay","slot":"references","audience":"public","kind":"citation","position":91,"heading":"Unpublished source","body":"Never public","href":"https://example.test/draft","status":"draft"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "draft citation fixture not admitted"
essay=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/essay") || fail "article page did not respond"
printf '%s' "$essay" | grep -q 'data-article-recording' || fail "published audio was not linked to article"
printf '%s' "$essay" | grep -q 'data-play="/assets/acme-report.wav"' || fail "the exact published media address was not rendered"
printf '%s' "$essay" | grep -q 'href="/assets/acme-report.vtt"' || fail "published transcript link is absent"
printf '%s' "$essay" | grep -q 'data-deck' || fail "published edition did not use native audio controls"
printf '%s' "$essay" | grep -q 'data-article-voice-choice' || fail "recorded article has no optional device-voice alternative"
printf '%s' "$essay" | grep -q 'rel="author" href="https://editor.aurekai.work/"' || fail "verified author profile was not linked"
printf '%s' "$essay" | grep -q '<time datetime="2026-11-07">2026-11-07</time>' || fail "published article date missing"
printf '%s' "$essay" | grep -q 'Updated <time datetime="2026-11-08">' || fail "article correction date missing"
printf '%s' "$essay" | grep -q 'property="og:type" content="article"' || fail "editorial page not identified as an article"
printf '%s' "$essay" | grep -q '"@type":"Article"' || fail "structured editorial metadata missing"
printf '%s' "$essay" | grep -q '"datePublished":"2026-11-07"' || fail "structured published date missing"
printf '%s' "$essay" | grep -q 'href="#source-1"' || fail "author citation marker is not linked to its endnote"
printf '%s' "$essay" | grep -q 'id="source-1"' || fail "published source is not addressable"
printf '%s' "$essay" | grep -q 'href="https://example.test/observed-record"' || fail "source citation has no original address"
printf '%s' "$essay" | grep -q 'Identifier: <code>source:fixture-001</code>' || fail "retained citation identity is not exposed"
if printf '%s' "$essay" | grep -q 'Unpublished source'; then fail "draft citation escaped to the public article"; fi
printf 'article recorded edition, byline, and dated metadata: PASS\n'
if printf '%s' "$essay" | grep -q 'data-play="/assets/draft.wav"\|data-play="/assets/unrelated.wav"'; then fail "unrelated or draft audio was selected"; fi
printf 'article recording association: PASS (published same-subject only; HTML, not audio decoding)\n'
# A v2-bound edition is served only while all public structured editor data matches.
"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/v2-essay","title":"Revision witness","heading":"Revision witness","media_subject_ref":"article:v2-witness","editorial_kind":"article","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "v2 article not admitted"
"$BONFYRE_CMS_BINARY" entry create site_fragment '{"coordinate":"v2-story","page":"/v2-essay","slot":"showcase","audience":"public","kind":"profile-story","position":0,"heading":"Passage","body":"A complete published passage about this source-bound recording.","semantic_tags":{},"status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "v2 story not admitted"
cc -O2 -std=c11 -I"$root/lib/libbonfyre/include" -o "$home/article-revision" "$root/cmd/BonfyreBroadcast/tests/article_revision_test.c" -L"$root/lib/libbonfyre" -lbonfyre -lsqlite3
v2rev=$("$home/article-revision" "$home/cms.db" acme article:v2-witness)
"$BONFYRE_CMS_BINARY" entry create media_program "$(printf '{"program_id":"v2-audio","subject_ref":"article:v2-witness","media_kind":"audio","asset_ref":"/assets/v2-audio.m4a","narration_article_revision_sha256":"%s","publication_state":"published","cannot_infer":"fixture","status":"published"}' "$v2rev")" --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "v2 media not admitted"
v2page=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/v2-essay") || fail "v2 article failed"
printf '%s' "$v2page" | grep -q '/assets/v2-audio.m4a' || fail "current v2 audio absent"
sqlite3 "$home/cms.db" "UPDATE site_fragment SET semantic_tags=json_set('{}','$.authoring_v1.editorial',json('[]')) WHERE namespace='acme' AND coordinate='v2-story'"
v2page=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/v2-essay") || fail "updated v2 article failed"
if printf '%s' "$v2page" | grep -q '/assets/v2-audio.m4a'; then fail "stale v2 audio remained public"; fi
printf 'article structured revision invalidation: PASS\n'
# The action-aperture subject must never select audio, even when it matches a
# published recording: a different explicit media subject has no recording.
"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/plain-essay","title":"Text-only essay","heading":"Text-only essay","lede":"This page contains only reviewed text.","aperture_subject":"article:acme-report","media_subject_ref":"article:unrecorded","editorial_kind":"essay","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "text-only article not admitted"
"$BONFYRE_CMS_BINARY" entry create site_fragment '{"coordinate":"acme-text-only","page":"/plain-essay","slot":"body","audience":"public","kind":"prose","position":0,"heading":"Text alone","body":"A reviewed page should still be readable aloud when no publisher audio edition is available.","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "text-only fragment not admitted"
plain=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/plain-essay") || fail "text-only article did not respond"
printf '%s' "$plain" | grep -q 'data-article-listen' || fail "text-only article lost its browser-voice option"
if printf '%s' "$plain" | grep -q 'data-article-recording\|acme-report.wav'; then fail "aperture subject was misread as audio identity"; fi
printf '%s' "$plain" | grep -q 'rel="canonical" href="http://acme.localhost/plain-essay"' || fail "canonical URL missing from the published article"
if printf '%s' "$plain" | grep -q 'rel="author"'; then fail "text-only article invented an author"; fi
printf '%s' "$plain" | grep -q 'property="og:type" content="article"' || fail "declared text-only essay was not treated as editorial"
printf '%s' "$plain" | grep -q 'application/ld+json' || fail "structured page metadata missing"
printf '%s' "$essay" | grep -q 'rel="canonical" href="http://acme.localhost/essay"' || fail "recorded article canonical URL missing"
printf 'article text-only fallback and metadata: PASS\n'
# This fixture tests metadata-to-player wiring only. Audio decoding and SHA
# Real local M4A and WebVTT bytes remain undisclosed until the publication
# owner marks the exact media_program published. A guessed digest is not a grant.
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=440:duration=2" -c:a aac -b:a 96k -y "$home/produced.m4a" || fail "local AAC test carrier could not render"
mediahash=$(shasum -a 256 "$home/produced.m4a" | awk '{print $1}')
cp "$home/produced.m4a" "$home/recorded-media/$mediahash.m4a"
printf 'WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nSpoken audio fixture.\n' > "$home/produced.vtt"
vtthash=$(shasum -a 256 "$home/produced.vtt" | awk '{print $1}')
cp "$home/produced.vtt" "$home/recorded-media/$vtthash.vtt"
"$BONFYRE_CMS_BINARY" entry create media_program "$(printf '{"program_id":"local-unpublished-fixture","subject_ref":"article:local-staged","media_kind":"audio","asset_ref":"/assets/twin/%s.m4a","transcript_ref":"/assets/twin/%s.vtt","publication_state":"draft","cannot_infer":"staging is not publication","status":"draft"}' "$mediahash" "$vtthash")" --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "local draft fixture was not admitted"
status=$(curl -sS -m 15 -o /dev/null -w '%{http_code}' -H 'Host: acme.localhost' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 404 ] || fail "unpublished local asset was exposed ($status)"
"$BONFYRE_CMS_BINARY" entry create media_program "$(printf '{"program_id":"local-reviewed-fixture","subject_ref":"article:local-reviewed","media_kind":"audio","asset_ref":"/assets/twin/%s.m4a","transcript_ref":"/assets/twin/%s.vtt","publication_state":"published","cannot_infer":"reviewed fixture does not prove external rights","status":"published"}' "$mediahash" "$vtthash")" --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "local public fixture was not admitted"
code=$(curl -sS -m 15 -D "$home/audio-headers" -o "$home/audio-served.m4a" -w '%{http_code}' -H 'Host: acme.localhost' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$code" = 200 ] || fail "published generated recording did not answer ($code)"
grep -qi '^Content-Type: audio/mp4' "$home/audio-headers" || fail "recording was not served as audio/mp4"
cmp "$home/produced.m4a" "$home/audio-served.m4a" >/dev/null || fail "HTTP recording bytes differ from staged audio"
code=$(curl -sS -m 15 -D "$home/vtt-headers" -o "$home/vtt-served" -w '%{http_code}' -H 'Host: acme.localhost' "http://127.0.0.1:$port/assets/twin/$vtthash.vtt")
[ "$code" = 200 ] || fail "published transcript did not answer ($code)"
grep -qi '^Content-Type: text/vtt' "$home/vtt-headers" || fail "transcript was not served as text/vtt"
"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/recorded-article","title":"A narrated article","heading":"A narrated article","media_subject_ref":"article:local-reviewed","editorial_kind":"article","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "recorded article reader fixture not admitted"
recorded=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/recorded-article") || fail "recorded article reader did not respond"
printf '%s' "$recorded" | grep -q "data-transcript=\"/assets/twin/$vtthash.vtt\"" || fail "recorded transcript not bound to article audio queue"
printf '%s' "$recorded" | grep -q 'data-deck-transcript hidden' || fail "recorded article omitted the integrated read-along transcript"
printf '%s' "$recorded" | grep -q 'data-deck-transcript-cues' || fail "recorded article omitted cue seeking"
printf 'article timed transcript association and accessible read-along: PASS\n'
cmp "$home/produced.vtt" "$home/vtt-served" >/dev/null || fail "HTTP transcript bytes differ from staged transcript"
audiosize=$(wc -c < "$home/produced.m4a" | tr -d ' ')
status=$(curl -sS -m 15 -D "$home/range-headers" -o "$home/range-body" -w '%{http_code}' -H 'Host: acme.localhost' -H 'Range: bytes=0-31' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 206 ] || fail "M4A first-byte seek did not return HTTP 206 ($status)"
grep -qi "^Content-Range: bytes 0-31/$audiosize" "$home/range-headers" || fail "audio byte-range bounds incorrect"
dd if="$home/produced.m4a" of="$home/expected32" bs=32 count=1 2>/dev/null
cmp "$home/expected32" "$home/range-body" >/dev/null || fail "HTTP audio range does not match recording"
status=$(curl -sS -m 15 -D "$home/suffix-headers" -o "$home/suffix-body" -w '%{http_code}' -H 'Host: acme.localhost' -H 'Range: bytes=-16' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 206 ] || fail "M4A suffix seeking unavailable ($status)"
tail -c 16 "$home/produced.m4a" > "$home/expected-suffix"
cmp "$home/expected-suffix" "$home/suffix-body" >/dev/null || fail "audio suffix range bytes changed"
status=$(curl -sS -m 15 -D "$home/head-headers" -o /dev/null -I -w '%{http_code}' -H 'Host: acme.localhost' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 200 ] || fail "audio HEAD metadata unavailable ($status)"
grep -qi "^Content-Length: $audiosize" "$home/head-headers" || fail "audio HEAD length disagrees with produced recording"
grep -qi '^Accept-Ranges: bytes' "$home/head-headers" || fail "audio HTTP seek capability not advertised"
status=$(curl -sS -m 15 -o /dev/null -w '%{http_code}' -H 'Host: acme.localhost' -H 'Range: bytes=999999999-' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 416 ] || fail "out-of-bounds audio range was not refused ($status)"
status=$(curl -sS -m 15 -o /dev/null -w '%{http_code}' -H 'Host: acme.localhost' -H 'Range: bytes=0-2,4-6' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 416 ] || fail "unsupported multi-range audio request was not refused ($status)"
printf 'audio native seeking, suffix ranges, HEAD and bounds: PASS\n'
printf 'altered' >> "$home/recorded-media/$mediahash.m4a"
status=$(curl -sS -m 15 -o /dev/null -w '%{http_code}' -H 'Host: acme.localhost' "http://127.0.0.1:$port/assets/twin/$mediahash.m4a")
[ "$status" = 409 ] || fail "tampered published audio did not fail closed ($status)"
cp "$home/produced.m4a" "$home/recorded-media/$mediahash.m4a"
printf 'locally generated audio publication, bytes, MIME and tampering: PASS\n'
# association are independently exercised in BonfyreBroadcast's test.
wavehash=$(python3 -c "print('a'*64)")
wavebins=$(python3 -c "print(','.join(str(10+(i%64)) for i in range(64)))")
wronghash=$(python3 -c "print('b'*64)")
"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/wave-essay","title":"Measured audio article","heading":"Measured audio article","media_subject_ref":"article:measured-audio-fixture","editorial_kind":"article","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "waveform article was not admitted"
"$BONFYRE_CMS_BINARY" entry create media_program "$(printf '{"program_id":"wave-positive","subject_ref":"article:measured-audio-fixture","media_kind":"audio","asset_ref":"/assets/twin/%s.m4a","waveform_peaks":"%s","waveform_audio_sha256":"%s","publication_state":"published","cannot_infer":"fixture is not delivered audio","status":"published"}' "$wavehash" "$wavebins" "$wavehash")" --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "waveform fixture not admitted"
wavepage=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/wave-essay") || fail "waveform page did not respond"
printf '%s' "$wavepage" | grep -q 'data-deck-waveform' || fail "shared audio player has no waveform surface"
printf '%s' "$wavepage" | grep -q "data-peaks=\"$wavebins\"" || fail "digest-bound waveform was not carried into the audio deck"
"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/wave-bad","title":"Rejected mismatched waveform","heading":"Rejected mismatched waveform","media_subject_ref":"article:wrong-wave-fixture","editorial_kind":"article","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "invalid-wave page not admitted"
"$BONFYRE_CMS_BINARY" entry create media_program "$(printf '{"program_id":"wave-mismatch","subject_ref":"article:wrong-wave-fixture","media_kind":"audio","asset_ref":"/assets/twin/%s.m4a","waveform_peaks":"%s","waveform_audio_sha256":"%s","publication_state":"published","cannot_infer":"fixture must refuse mismatched peaks","status":"published"}' "$wavehash" "$wavebins" "$wronghash")" --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "mismatched-wave fixture not admitted"
badwave=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/wave-bad") || fail "mismatched waveform page did not respond"
printf '%s' "$badwave" | grep -q 'data-article-recording' || fail "invalid waveform removed the otherwise available recording"
if printf '%s' "$badwave" | grep -q 'data-peaks='; then fail "digest-mismatched waveform was sent to the reader"; fi
printf 'article waveform binding: PASS (matching asset digest required; no asset delivery claimed)\n'

"$BONFYRE_CMS_BINARY" entry create site_page '{"route":"/services","title":"Services","heading":"Services","lede":"Ordinary site information.","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "generic page not admitted"
"$BONFYRE_CMS_BINARY" entry create site_fragment '{"coordinate":"acme-service-citation","page":"/services","slot":"body","audience":"public","kind":"citation","position":0,"heading":"Service reference","body":"A generic page citation is ordinary content.","status":"published"}' --ns acme --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1 || fail "generic citation not admitted"
services=$(curl -fsS -m 15 -H 'Host: acme.localhost' "http://127.0.0.1:$port/services") || fail "generic page did not respond"
printf '%s' "$services" | grep -q 'Service reference' || fail "generic citation was wrongly hidden"
if printf '%s' "$services" | grep -q 'data-article-listen\|site-article-sources\|property="og:type" content="article"'; then fail "ordinary page inherited editorial UI"; fi
printf 'generic page isolation: PASS\n'
if [ "${BONFYRE_SITE_AUDIO_ONLY:-0}" = 1 ]; then exit 0; fi

if site new acme --no-open >/dev/null 2>&1; then fail "a second site took the same name"; fi
if site new 'Bad Name' --no-open >/dev/null 2>&1; then fail "a name that is not a slug was accepted"; fi

site list | grep -q '^acme  *acme.localhost  *ghost:acme  *independent  *composed' || fail "list does not show the site"
site list --json | grep -q '"count":3' || fail "list --json count (including the explicit author profile)"
site show acme.localhost --json | grep -q '"programs":1,"slots":1,"bindings":1,"posts":1,"resolved":true' || fail "show does not read the site back"
if site show nobody >/dev/null 2>&1; then fail "show answered for a site that does not exist"; fi

cat > "$home/saved.html" <<'PAGE'
<html><body><h1>Field notes</h1>
<h2>Freight from Haiphong</h2><p>The forwarder quoted ocean freight for forty-foot containers out of Haiphong, with a two week lead time and customs brokerage included.</p>
<h2>Night market</h2><p>The downtown market runs until four in the morning with live music, food stalls and a dance floor behind the old cinema.</p>
</body></html>
PAGE
site import "$home/saved.html" --as notes --no-open 2>/dev/null | grep -q '"site":"notes".*"page_served":true' || fail "import did not end in a served page"
notes=$(get notes.localhost)
printf '%s' "$notes" | grep -q '<h3>Freight from Haiphong</h3>' || fail "an imported section is not a post"
printf '%s' "$notes" | grep -q 'customs brokerage included' || fail "imported text is not on the page"
printf 'x' > "$home/empty.txt"
if site import "$home/empty.txt" --as hollow --no-open >/dev/null 2>&1; then fail "an empty source was imported"; fi
if site list | grep -q '^hollow'; then fail "a refused import left a site behind"; fi

found=$(site search 'container shipping quote from Vietnam' --top 3 2>/dev/null)
printf '%s' "$found" | head -1 | grep -q 'community_post' || fail "search did not put a post first: $found"
printf '%s' "$found" | head -1 | grep -q 'Haiphong' || fail "search did not find the freight post by meaning"

site export notes --out "$home/notes.html" | grep -q '"stylesheets_missing":0' || fail "export left a stylesheet out"
grep -q 'customs brokerage included' "$home/notes.html" || fail "exported file lacks the page's text"
if grep -q '<script\|rel="stylesheet"\|src="/' "$home/notes.html"; then fail "exported file still reaches for the network"; fi
grep -q '<style data-from="/assets/own-site.css">' "$home/notes.html" || fail "stylesheet was not written into the file"
[ "$(wc -c < "$home/notes.html")" -lt 60000 ] || fail "an exported page carries the estate's stylesheets"

# Bridges: a site leaves as WordPress's own export file and as Frappe documents,
# and each file read back by this estate's reader of that format carries the
# same titles and text.  What is not shown is WordPress or Frappe importing it.
text_of() { sqlite3 -readonly "$home/cms.db" "SELECT group_concat(title || char(10) || body, char(10)) FROM (SELECT title, body FROM community_post WHERE community='$1' ORDER BY title)" | tr -s ' \n\t' ' '; }
site export notes --format wordpress --out "$home/notes.xml" | grep -q '"posts":2' || fail "wordpress export"
grep -q '<wp:wxr_version>1.2</wp:wxr_version>' "$home/notes.xml" || fail "not a WordPress export file"
if command -v xmllint >/dev/null; then xmllint --noout "$home/notes.xml" || fail "WordPress export file is not well-formed"; fi
site export notes --format frappe --out "$home/notes.json" | grep -q '"posts":2' || fail "frappe export"
site import "$home/notes.xml" --as fromwp --no-open 2>/dev/null | grep -q '"page_served":true' || fail "WordPress export file did not import"
site import "$home/notes.json" --as fromfrappe --no-open 2>/dev/null | grep -q '"page_served":true' || fail "Frappe documents did not import"
[ "$(text_of notes)" = "$(text_of fromwp)" ] || fail "text changed through the WordPress format"
[ "$(text_of notes)" = "$(text_of fromfrappe)" ] || fail "text changed through the Frappe format"
# Markup characters in a post survive both formats as text, not as markup.
"$BONFYRE_CMS_BINARY" entry create community_post '{"post":"acme:sharp","community":"acme","author_subject":"institution:acme","title":"5 < 6 & \"quoted\"","body":"if a < b && c > d then ]]> done","published_at":"2026-10-10T13:00:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"test","status":"published"}' --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1
site export acme --format wordpress --out "$home/acme.xml" >/dev/null
site import "$home/acme.xml" --as acmewp --no-open >/dev/null 2>&1
[ "$(text_of acme)" = "$(text_of acmewp)" ] || fail "markup characters did not survive the WordPress format"

# The site answers assistants: the Model Context Protocol on its own host and,
# for programs that cannot look up a .localhost name, by path on loopback.
mcp() { curl -s -m 20 -X POST "http://127.0.0.1:$port/mcp/$1" -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' -d "$2"; }
mcp notes '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}' | grep -q '"protocolVersion":"2025-06-18".*"serverInfo":{"name":"notes"' || fail "mcp handshake"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/notes" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","method":"notifications/initialized"}')" = 202 ] || fail "a notification was answered"
tools=$(mcp notes '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
if printf '%s' "$tools" | grep -q 'aurekai-knight.png'; then fail "independent customer MCP tools borrowed Aurekai's knight"; fi
for tool in site_info list_pages get_page list_offers list_posts get_post search_posts show_posts send_inquiry request_booking; do printf '%s' "$tools" | grep -q "\"name\":\"$tool\"" || fail "tool $tool missing"; done
printf '%s' "$tools" | grep -q '"_meta":{"ui":{"resourceUri":"ui://site/posts"}' || fail "the view tool names no view"
if printf '%s' "$tools" | grep -q '"destructiveHint":true'; then fail "a tool that can destroy something is offered"; fi
[ "$(printf '%s' "$tools" | grep -o '"readOnlyHint":false' | wc -l | tr -d ' ')" = 2 ] || fail "tools other than the two that leave a message may write"
mcp notes '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_posts","arguments":{}}}' | grep -q '"structuredContent":{"total":2,"posts":\[{"id":' || fail "list_posts"
# The store has changed since the index was built, so search is by words and
# says so.
mcp notes '{"jsonrpc":"2.0","id":40,"method":"tools/call","params":{"name":"search_posts","arguments":{"query":"freight"}}}' | grep -q 'Freight from Haiphong.*"method":"words' || fail "search by words"
# Asking is what brings the index up to date: nobody has to run anything.
healed=0; n=0; while [ "$n" -lt 90 ]; do
  mcp notes '{"jsonrpc":"2.0","id":41,"method":"tools/call","params":{"name":"search_posts","arguments":{"query":"container shipping quote from Vietnam"}}}' | grep -q '"method":"meaning and words"' && { healed=1; break; }
  n=$((n+1)); sleep 1; done
[ "$healed" = 1 ] || fail "the search index did not bring itself up to date"
site mcp notes --json 2>/dev/null | grep -q '"tools":10,"view_tools":1,"search":"meaning and words"' || fail "site mcp did not prove the endpoint"
mcp notes '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search_posts","arguments":{"query":"container shipping quote from Vietnam"}}}' | grep -q 'Freight from Haiphong.*"method":"meaning and words"' || fail "search by meaning did not find the freight post"
# One site's endpoint never serves another site's posts.
if mcp acme '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_posts","arguments":{}}}' | grep -q 'Haiphong'; then fail "one site served another site's posts"; fi
view_response=$(mcp notes '{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"ui://site/posts"}}')
printf '%s' "$view_response" | grep -q '"mimeType":"text/html;profile=mcp-app"' || fail "the view is not served: $view_response"
mcp notes '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"drop_site"}}' | grep -q '"error":{"code":-32602' || fail "an unknown tool was not refused"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/notes" -H 'Origin: https://elsewhere.example' -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":8,"method":"ping"}')" = 403 ] || fail "another origin drove the endpoint"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/nosuchsite" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":9,"method":"ping"}')" != 200 ] || fail "a site that does not exist answered"
# A client that sends header names in lower case reaches the site by its host.
for origin in "http://notes.localhost:$port" "http://127.0.0.1:$port"; do
  [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/notes" -H "Origin: $origin" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"ping"}')" = 200 ] || fail "own origin refused"
done
for origin in "http://notes.localhost.evil:$port" "http://evil.test/notes.localhost" "http://notes.localhost@evil.test" "http://acme.localhost:$port" 'http://notes.localhost:1' 'null'; do
  for method in GET POST; do
    [ "$(curl -s -o /dev/null -w '%{http_code}' -X "$method" "http://127.0.0.1:$port/mcp/notes" -H "Origin: $origin" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"ping"}')" = 403 ] || fail "unrelated or malformed origin accepted: $method $origin"
  done
done
printf 'POST /mcp HTTP/1.1\r\nhost: notes.localhost:%s\r\ncontent-type: application/json\r\ncontent-length: 40\r\nconnection: close\r\n\r\n{"jsonrpc":"2.0","id":1,"method":"ping"}' "$port" | nc 127.0.0.1 "$port" | grep -q '"result":{}' || fail "lower-case headers were not understood"
get notes.localhost | grep -q . ; curl -s --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/.well-known/agent.json" | grep -q '"name":"Notes".*"protocol":"mcp"' || fail "the agent card does not describe the site"
if curl -s --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/.well-known/agent.json" | grep -q 'parcel'; then fail "the agent card offers the estate's skills"; fi
curl -s --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/llms.txt" | grep -q '^### Freight from Haiphong' || fail "llms.txt lacks the posts"
curl -s "http://127.0.0.1:$port/mcp/notes" | grep -q 'claude mcp add --transport http notes http://127.0.0.1' || fail "the connect page lacks the line to paste"
# The owner's side, each in one command: a standing page, a key for one
# assistant, the message it leaves, and the record of who asked what.
site page notes --title 'How we work' --summary 'Plain terms.' --text 'Quotes
A quote comes back in one working day.

Payment
Net thirty, by transfer.' --no-open | grep -q '"route":"/how-we-work".*"sections":2,"page_served":true' || fail "page was not served"
# The line at the foot of every page is the site's own: set, linked, cleared.
site set notes --footnote 'This business is made up.' --footnote-label 'See the others' --footnote-link 'https://docs.aurekai.work/three-businesses' >/dev/null 2>&1 || fail "a footnote could not be set"
get notes.localhost | grep -q '<p class="site-foot-note">This business is made up. <a href="https://docs.aurekai.work/three-businesses">See the others</a></p>' || fail "the front page lacks the footnote"
curl -s -m 10 --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/contact" | grep -q 'site-foot-note' || fail "the contact page lacks the footnote"
if site set notes --footnote 'x' --footnote-label 'L' --footnote-link 'javascript:alert(1)' >/dev/null 2>&1; then fail "a footnote link that is not https was accepted"; fi
site set notes --no-footnote >/dev/null 2>&1 || fail "a footnote could not be cleared"
if get notes.localhost | grep -q 'site-foot-note'; then fail "a cleared footnote still shows"; fi
pagehtml=$(curl -s -m 10 --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/how-we-work")
printf '%s' "$pagehtml" | grep -q '<h1>How we work</h1>.*<section id="quotes"><h2><a href="#quotes">Quotes</a></h2><p>A quote comes back in one working day.</p></section><section id="payment"><h2>' || fail "the page does not hold its sections"

# A Markdown page keeps its sections, code and tables whole, and a group makes
# the site read as a manual: every page listed, this page's outline, and the
# way on to the next.
cat > "$home/run.md" <<'PAGE'
# Run a step

Runs `one` step and <b>escapes</b> what it is given.

## Run it

```sh Terminal
notes run --input <file>   # prints the result

notes show
```

```sh Mission
notes mission run
```

## Limits

| Limit | Value |
|---|---|
| Time | 30 seconds |

> A run past its limit ends as failed.

- [Next page](/how-we-work)
- [Not a link](javascript:alert(1))
PAGE
site page notes --title 'Run a step' --group 'Reference' --file "$home/run.md" --no-open | grep -q '"route":"/run-a-step".*"sections":2,"page_served":true' || fail "the Markdown page was not cut at its headings"
manual=$(curl -s -m 10 --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/run-a-step")
printf '%s' "$manual" | grep -q 'class="site-composition site-manual"' || fail "a grouped page is not laid out as a manual"
printf '%s' "$manual" | grep -q '<p class="lede">Runs <code>one</code> step and &lt;b&gt;escapes&lt;/b&gt; what it is given.</p>' || fail "the opening paragraph is not the summary, or is not escaped"
printf '%s' "$manual" | grep -q '<div class="code-set"><figure class="code" data-lang="sh"><figcaption><span>Terminal</span>' || fail "labelled code blocks are not one set"
printf '%s' "$manual" | grep -q '<span class="typed">notes show</span>' || fail "a blank line inside a code block split the block"
printf '%s' "$manual" | grep -q '<thead><tr><th>Limit</th><th>Value</th></tr></thead><tr><td>Time</td><td>30 seconds</td></tr>' || fail "the table was not kept"
printf '%s' "$manual" | grep -q '<aside class="note"><p>A run past its limit ends as failed.</p></aside>' || fail "the note was not kept"
printf '%s' "$manual" | grep -q '<a href="/how-we-work">Next page</a>' || fail "a link to the site's own page was not made"
printf '%s' "$manual" | grep -q 'href="javascript' && fail "a link that is not an address was made into one"
printf '%s' "$manual" | grep -q '<nav class="manual-outline" aria-label="On this page"><p>On this page</p><a href="#run-it">Run it</a><a href="#limits">Limits</a>' || fail "the page has no outline"
printf '%s' "$manual" | grep -q 'aria-current="page">Run a step</a>' || fail "the list of pages does not mark this one"
curl -s -m 10 --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/assets/own-site-reading.js" | grep -q 'code-copy' || fail "the reading script is not served"
echo "site product: manual page PASS"
get notes.localhost | grep -q '<a href="/how-we-work">How we work</a>' || fail "the page is not in the site's menu"
if site page notes --title 'How we work' --text 'again' --no-open >/dev/null 2>&1; then fail "a second page took the same route"; fi
mcp notes '{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"list_pages","arguments":{}}}' | grep -q '"route":"/how-we-work","title":"How we work","summary":"Plain terms."' || fail "list_pages"
mcp notes '{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"get_page","arguments":{"route":"/how-we-work"}}}' | grep -q 'Net thirty, by transfer' || fail "get_page"
curl -s --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/llms.txt" | grep -q '^### How we work' || fail "llms.txt lacks the page"
# One site's pages are not another's.
[ "$(curl -s -o /dev/null -w '%{http_code}' --resolve "acme.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://acme.localhost:$port/how-we-work")" != 200 ] || fail "one site served another's page"

inquiry='{"jsonrpc":"2.0","id":30,"method":"tools/call","params":{"name":"send_inquiry","arguments":{"name":"Ada","contact":"ada@example.test","message":"Two pallets to Dallas, please quote."}}}'
keyed() { curl -s -m 20 -X POST "http://127.0.0.1:$port/mcp/notes" -H 'content-type: application/json' -H "Authorization: Bearer $1" -d "$2"; }
mcp notes "$inquiry" | grep -q '"isError":true' || fail "a message was taken without a key"
reader=$(site key notes --label reader --json | grep -o 'bfcms_[0-9a-f]*' | head -1)
writer=$(site key notes --label desk --write --json | grep -o 'bfcms_[0-9a-f]*' | head -1)
[ "${#writer}" = 70 ] && [ "$reader" != "$writer" ] || fail "keys are not long and distinct"
keyed "$reader" "$inquiry" | grep -q '"isError":true' || fail "a read key left a message"
keyed "$writer" "$inquiry" | grep -q '"delivered":true' || fail "a write key could not leave a message"
bad_key_status=$(curl -s -o "$home/bad-key.json" -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/notes" -H 'content-type: application/json' -H 'Authorization: Bearer bfcms_never_issued' -d '{"jsonrpc":"2.0","id":1,"method":"ping"}')
[ "$bad_key_status" = 401 ] || fail "invalid key returned $bad_key_status: $(cat "$home/bad-key.json")"
# A key for one site is not a key for another.
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/acme" -H 'content-type: application/json' -H "Authorization: Bearer $writer" -d '{"jsonrpc":"2.0","id":1,"method":"ping"}')" = 401 ] || fail "one site's key opened another site"
site inbox notes --json | grep -q '"count":1' || fail "the inbox does not hold exactly the one delivered message"
site inbox notes | grep -q 'Ada <ada@example.test>' || fail "the inbox does not show who wrote"
meter_usage=$(site usage notes --json)
if ! printf '%s' "$meter_usage" | grep -q '"caller":"desk","tool":"send_inquiry","calls":1'; then
  echo "meter diagnostic: home=$home binary=$HOME/.bonfyre/bin/bonfyre-meter" >&2
  ls -l "$home/meter.db" "$HOME/.bonfyre/bin/bonfyre-meter" >&2 || true
  sqlite3 "$home/meter.db" 'SELECT name FROM sqlite_master WHERE type="table" ORDER BY name; SELECT "events",count(*) FROM events; SELECT "rates",count(*) FROM rate;' >&2 || true
  fail "the meter did not record the keyed call: $meter_usage"
fi
site usage notes --json | grep -q '"caller":"public:[0-9a-f]\{8\}"' || fail "the meter did not record public calls under where they came from"
# An operator's own site does not offer the estate's agent skills.
[ "$(curl -s -o /dev/null -w '%{http_code}' --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/a2a")" = 404 ] || fail "the estate's agent endpoint answers on an operator's site"
# The limit is counted by the meter and enforced.
site stop >/dev/null; BONFYRE_SITE_MCP_PUBLIC_PER_MINUTE=3 "$web" site serve --home "$home" --port "$port" >/dev/null
limited=0; for n in 1 2 3 4 5 6 7 8; do [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/acme" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"site_info","arguments":{}}}')" = 429 ] && limited=$((limited+1)); done
[ "$limited" -ge 4 ] || fail "the public limit was not enforced ($limited of 8 refused)"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/notes" -H 'content-type: application/json' -H "Authorization: Bearer $reader" -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"site_info","arguments":{}}}')" = 200 ] || fail "a keyed caller was held to the public limit"
site stop >/dev/null; site serve >/dev/null

# What the site sells, a time asked for, the contact page for people, and
# taking things back: each one command, each read back.
form() { curl -s -m 15 -o "$home/form.out" -w '%{http_code}' --resolve "notes.localhost:$port:127.0.0.1" -X POST "http://notes.localhost:$port/contact" "$@"; }
site offer notes --title 'Site survey' --price '$180 per hour' --amount 180 --currency USD --unit 'per hour' --bookable --text 'One hour on site with an engineer.' | grep -q '"bookable":true,"on_page":true' || fail "offer not on its page"
site offer notes --title 'Written quote' --price 'Free' --text 'A written quote in one working day.' | grep -q '"on_page":true' || fail "second offer not on its page"
offers=$(curl -s -m 10 --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/offers")
printf '%s' "$offers" | grep -q '<h2>Site survey</h2><p class="offer-price">\$180 per hour</p>' || fail "offer and price are not shown together"
printf '%s' "$offers" | grep -q 'Request a time</a>.*Ask about this</a>' || fail "each offer does not carry its way to take it up"
listed=$(mcp notes '{"jsonrpc":"2.0","id":50,"method":"tools/call","params":{"name":"list_offers","arguments":{}}}')
printf '%s' "$listed" | grep -q '"title":"Site survey".*"price":"\$180 per hour","amount":180.00,"currency":"USD","unit":"per hour","bookable":true' || fail "list_offers lacks the price as words and as a number"
survey=$(sqlite3 -readonly "$home/cms.db" "SELECT id FROM site_fragment WHERE namespace='notes' AND heading='Site survey'")
quote=$(sqlite3 -readonly "$home/cms.db" "SELECT id FROM site_fragment WHERE namespace='notes' AND heading='Written quote'")
book() { printf '{"jsonrpc":"2.0","id":51,"method":"tools/call","params":{"name":"request_booking","arguments":{"offer_id":%s,"when":"Tuesday 2pm","name":"Ada","contact":"ada@example.test"}}}' "$1"; }
mcp notes "$(book "$survey")" | grep -q '"isError":true' || fail "a time was requested without a key"
keyed "$writer" "$(book "$quote")" | grep -q '"isError":true' || fail "a time was requested for an offer not open to it"
keyed "$writer" "$(book "$survey")" | grep -q '"requested":true,"confirmed":false' || fail "a booking request was not recorded as a request"
site inbox notes --json | grep -q 'Booking request for \\"Site survey\\" at: Tuesday 2pm' || fail "the booking request is not in the inbox"

curl -s --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/contact?about=Site%20survey" | grep -q 'About: Site survey' || fail "the contact page does not carry what it is about"
get notes.localhost | grep -q '<a href="/contact">Contact</a>' || fail "the contact page is not in the menu"
[ "$(form --data-urlencode 'name=Grace' --data-urlencode 'contact=grace@example.test' --data-urlencode 'message=Three pallets to Austin, please quote.')" = 200 ] || fail "the contact form refused a person"
grep -q 'Thank you, Grace' "$home/form.out" || fail "no confirmation shown"
site inbox notes | grep -q 'left through the site.s contact page' || fail "the form's message is not in the inbox"
before=$(site inbox notes --json | grep -o '"count":[0-9]*')
[ "$(form -d 'name=bot&contact=b@b.b&message=buy+now+buy+now&website=http://spam')" = 200 ] || fail "a program filling the hidden field was told it failed"
[ "$(site inbox notes --json | grep -o '"count":[0-9]*')" = "$before" ] || fail "a message from the hidden-field program was kept"
[ "$(form -d 'name=&contact=&message=')" = 400 ] || fail "an empty form was accepted"
[ "$(form --data-urlencode 'name=<script>alert(1)</script>' --data-urlencode 'contact=x@example.test' --data-urlencode 'message=hello there friend')" = 200 ] || fail "form with markup in a name"
if grep -q '<script>alert' "$home/form.out"; then fail "a name was written into the page as markup"; fi
refusals=0; for n in 1 2 3 4 5 6; do [ "$(form -d "name=N$n&contact=n@example.test&message=message+number+$n")" = 429 ] && refusals=$((refusals+1)); done
[ "$refusals" -ge 2 ] || fail "one address could send without limit ($refusals of 6 refused)"

site keys notes | grep -q '^desk .* yes .* works' || fail "keys does not list who may write"
site key notes --withdraw desk | grep -q '"withdrawn":"desk"' || fail "withdraw"
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$port/mcp/notes" -H 'content-type: application/json' -H "Authorization: Bearer $writer" -d '{"jsonrpc":"2.0","id":1,"method":"ping"}')" = 401 ] || fail "a withdrawn key still opens the site"
site keys notes | grep -q '^desk .* withdrawn' || fail "the withdrawn key is not shown as withdrawn"
if site key notes --withdraw nobody >/dev/null 2>&1; then fail "withdrawing a key nobody holds succeeded"; fi

site page notes --title 'How we work' --text 'Now net fifteen.' --replace --no-open | grep -q '"page_served":true' || fail "replace"
curl -s --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/how-we-work" | grep -q 'Now net fifteen' || fail "the page was not rewritten"
if curl -s --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/how-we-work" | grep -q 'Net thirty'; then fail "the old text is still on the page"; fi
site page notes --route /how-we-work --remove | grep -q '"removed":true,"now_answers":"404"' || fail "the removed page still answers"
if get notes.localhost | grep -q 'how-we-work'; then fail "a removed page is still in the menu"; fi
site post notes --title 'Rates from November' --text 'Ocean rates rise two percent on the first.' | grep -q '"on_page":true' || fail "post not on the page"
newpost=$(sqlite3 -readonly "$home/cms.db" "SELECT id FROM community_post WHERE community='notes' AND title='Rates from November'")
site post notes --withdraw "$newpost" | grep -q '"withdrawn"' || fail "post withdraw"
if get notes.localhost | grep -q 'Rates from November'; then fail "a withdrawn post is still on the page"; fi
if site post acme --withdraw "$newpost" >/dev/null 2>&1; then fail "one site withdrew another's post"; fi
site offer notes --withdraw "$quote" | grep -q '"withdrawn"' || fail "offer withdraw"
if mcp notes '{"jsonrpc":"2.0","id":52,"method":"tools/call","params":{"name":"list_offers","arguments":{}}}' | grep -q 'Written quote'; then fail "a withdrawn offer is still offered"; fi

# An address where the estate keeps its reading of someone else's publication
# answers too, read-only, and never as that publisher: no pages, offers or
# inbox, the original link in every item, and nothing written beside the store.
sqlite3 "$home/cms.db" "CREATE TABLE IF NOT EXISTS internet_source(id INTEGER PRIMARY KEY, namespace TEXT, status TEXT, source TEXT, label TEXT);
CREATE TABLE IF NOT EXISTS publication_occurrence(id INTEGER PRIMARY KEY, namespace TEXT, status TEXT, source_ref TEXT, canonical_href TEXT, title TEXT, summary TEXT, observed_at TEXT, visibility TEXT);
INSERT INTO internet_source(namespace,created_at,updated_at,status,source,label) VALUES ('root','2026-10-01','2026-10-01','published','feed-fieldwire-test-abc123','Fieldwire Journal');
INSERT INTO publication_occurrence(namespace,created_at,updated_at,status,source_ref,canonical_href,title,summary,observed_at,visibility) VALUES
 ('root','2026-10-01','2026-10-01','published','feed-fieldwire-test-abc123','https://fieldwire.test/rain','Rain changes the harvest window','Growers adjust.','2026-10-01T00:00:00Z','public'),
 ('root','2026-10-01','2026-10-01','published','feed-fieldwire-test-abc123','https://fieldwire.test/secret','Not for the public','Held back.','2026-10-02T00:00:00Z','private'),
 ('root','2026-10-01','2026-10-01','published','feed-elsewhere-test-zzz999','https://elsewhere.test/x','Another source entirely','Other.','2026-10-03T00:00:00Z','public');"
"$BONFYRE_CMS_BINARY" entry create site_presentation '{"site_id":"publisher-fieldwire-test","host":"fieldwire-test.aurekai.life","subject_address":"institution:publisher:fieldwire-test","profile":"newsroom","territory":"life","navigation":[],"status":"published"}' --ns root --db "$home/cms.db" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null 2>&1
news() { curl -s -m 20 --resolve "fieldwire-test.aurekai.life:$port:127.0.0.1" -X POST "http://fieldwire-test.aurekai.life:$port/mcp" -H 'content-type: application/json' -d "$1"; }
news '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}' | grep -q "Aurekai's reading of Fieldwire Journal.*not the publisher's own site" || fail "a reading does not say whose words it is"
readtools=$(news '{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
news '{"jsonrpc":"2.0","id":71,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{}}}' | grep -q '"icons":\[{"src":"https://aurekai.live/assets/aurekai-knight-48.png"' || fail "Aurekai newsroom MCP initialize has no knight"
printf '%s' "$readtools" | grep -q '"icons":\[{"src":"https://aurekai.live/assets/aurekai-knight-48.png"' || fail "Aurekai newsroom tool list has no knight"
curl -s -m 20 --resolve "fieldwire-test.aurekai.life:$port:127.0.0.1" "http://fieldwire-test.aurekai.life:$port/.well-known/mcp/server-card.json" | grep -q '"icons":\[{"src":"https://aurekai.live/assets/aurekai-knight-48.png"' || fail "Aurekai newsroom MCP server card has no knight"
if printf '%s' "$readtools" | grep -q 'send_inquiry\|request_booking\|list_offers\|list_pages\|"readOnlyHint":false'; then fail "a reading offers an owner's tools"; fi
listing=$(news '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_posts","arguments":{}}}')
printf '%s' "$listing" | grep -q 'Rain changes the harvest window' || fail "a reading does not list what was read"
printf '%s' "$listing" | grep -q 'Original: https://fieldwire.test/rain' || fail "an item does not carry its original link"
if printf '%s' "$listing" | grep -q 'Not for the public\|Another source entirely'; then fail "a reading shows another source's or a non-public item"; fi
news '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"send_inquiry","arguments":{"name":"a","contact":"a@b.c","message":"hello there"}}}' | grep -q '"isError":true' || fail "a reading took a message for a publisher"
stores_before=$(ls "$home" | grep -c 'rebuilding' || true)
news '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"search_posts","arguments":{"query":"harvest"}}}' | grep -q 'Rain changes the harvest window.*"method":"words"' || fail "a reading is not searched by its words"
[ "$(ls "$home" | grep -c 'rebuilding' || true)" = "$stores_before" ] || fail "searching a reading started an index build beside the store"
[ "$(curl -s -o /dev/null -w '%{http_code}' --resolve "fieldwire-test.aurekai.life:$port:127.0.0.1" -X POST "http://fieldwire-test.aurekai.life:$port/contact" -d 'name=a&contact=a@b.c&message=hello+there')" != 200 ] || fail "an estate address took a contact message"

# A request the owner accepts lands in the organs that already own each part:
# the promise in the commitment store (the same time cannot be promised twice),
# the price as an invoice Pay makes from the offer's rate in the meter, and the
# request as an opportunity in Finance.  Then paid, then delivered.
for organ in BonfyrePay/bonfyre-pay BonfyreFinance/bonfyre-finance BonfyreMeter/bonfyre-meter; do [ -x "$root/cmd/$organ" ] || { echo "site product: SKIP accept ($organ not built)"; accept_skip=1; }; done
if [ -z "${accept_skip:-}" ]; then
  export BONFYRE_PAY_BINARY="$root/cmd/BonfyrePay/bonfyre-pay" BONFYRE_FINANCE_BINARY="$root/cmd/BonfyreFinance/bonfyre-finance"
  desk=$(site key notes --label frontdesk --write --json | grep -o 'bfcms_[0-9a-f]*' | head -1)
  ask() { keyed "$desk" "$(printf '{"jsonrpc":"2.0","id":60,"method":"tools/call","params":{"name":"request_booking","arguments":{"offer_id":%s,"when":"2026-10-14 14:00","name":"%s","contact":"%s"}}}' "$survey" "$1" "$2")" | grep -o '"reference":"[^"]*"' | cut -d'"' -f4; }
  first=$(ask Ada ada@example.test); second=$(ask Grace grace@example.test)
  [ -n "$first" ] && [ -n "$second" ] && [ "$first" != "$second" ] || fail "two requests did not get two references"
  accepted=$(site accept notes --request "$first" --json)
  printf '%s' "$accepted" | grep -q '"commitment":"site:notes:offer-[0-9]*:2026-10-14-14-00".*"invoiced":true.*"finance_opportunity":"recorded"' || fail "accept did not reach all three owners: $accepted"
  if site accept notes --request "$second" >/dev/null 2>&1; then fail "the same time was promised twice"; fi
  site accept notes --request "$second" --when '2026-10-15 10:00' --json | grep -q '"invoiced":true' || fail "another time could not be promised"
  if site accept notes --request nosuchrequest >/dev/null 2>&1; then fail "a request that does not exist was accepted"; fi
  "$BONFYRE_PAY_BINARY" --db "$home/pay.db" invoices | grep -c '\$180.00 *pending' | grep -q '^2$' || fail "Pay does not hold two pending invoices at the offer's price"
  "$BONFYRE_FINANCE_BINARY" --db "$home/finance.db" opportunity rank --limit 10 | grep -q "^$first	.*	18000	" || fail "Finance does not hold the request at its price"
  site paid notes --request "$first" --reference WIRE-0091 | grep -q '"amount_cents":18000,"reference":"WIRE-0091"' || fail "payment not recorded"
  if site paid notes --request "$first" --reference WIRE-0091 >/dev/null 2>&1; then fail "one payment was recorded twice"; fi
  "$BONFYRE_PAY_BINARY" --db "$home/pay.db" invoices | grep -q '\$180.00 *paid' || fail "Pay does not show the invoice paid"
  site done notes --request "$first" | grep -q '"state":"discharged","delivered":true' || fail "the promise was not discharged"
  if site done notes --request "$first" >/dev/null 2>&1; then fail "a promise was discharged twice"; fi
  if site done notes --request "$writer" >/dev/null 2>&1; then fail "a request never accepted was marked delivered"; fi
  site inbox notes --json | grep -q '"how":"delivered; commitment site:notes' || fail "the inbox does not show the request delivered"
fi
# A site moves to a name of its own and answers under it; its old name stops.
site domain notes --to notes-freight.example | grep -q '"now":"notes-freight.example","served_under_new_name_here":true' || fail "the site did not answer under its new name"
[ "$(curl -s -o /dev/null -w '%{http_code}' --resolve "notes-freight.example:$port:127.0.0.1" -H 'Accept: text/html' "http://notes-freight.example:$port/offers")" = 200 ] || fail "its pages did not move with it"
if site domain acme --to notes-freight.example >/dev/null 2>&1; then fail "two sites took one name"; fi
site domain notes --to notes.localhost >/dev/null || fail "the site could not move back"

# Where agents look first: the server card at its well-known path, named in a
# Link header, carrying the endpoint's own tool list; and the page's script,
# which registers the read tools with a browser's assistant and no others.
card=$(curl -s -m 10 --resolve "notes.localhost:$port:127.0.0.1" "http://notes.localhost:$port/.well-known/mcp/server-card.json")
printf '%s' "$card" | grep -q '"transport":{"type":"streamable-http","endpoint":"http://127.0.0.1:' || fail "the server card does not give a reachable endpoint"
printf '%s' "$card" | grep -q '"tools":\[{"name":"site_info"' || fail "the server card does not carry the tools"
curl -s -D - -o /dev/null --resolve "notes.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://notes.localhost:$port/contact" | grep -qi '^Link: </.well-known/mcp/server-card.json>; rel="mcp"' || fail "pages do not point to the server card"
get notes.localhost | grep -q '<link rel="mcp" href="/.well-known/mcp/server-card.json"><script src="/assets/own-site-agent.js" defer></script>' || fail "the home page does not offer its tools to a browser assistant"
if command -v node >/dev/null; then
  # The browser API is stood in for by an object that records what is
  # registered; the calls it makes go to the real endpoint.
  registered=$(PORT="$port" SCRIPT="$root/site/matrix/own-site-agent.js" node -e '
    const tools = [];
    globalThis.document = { modelContext: { registerTool: (t) => { tools.push(t); } } };
    globalThis.fetch = ((real) => (url, init) => real("http://127.0.0.1:" + process.env.PORT + "/mcp/notes", init))(fetch);
    require("vm").runInThisContext(require("fs").readFileSync(process.env.SCRIPT, "utf8"));
    setTimeout(async () => {
      const names = tools.map(t => t.name).sort().join(",");
      const answer = await tools.find(t => t.name === "site_info").execute({}, {});
      console.log(names + "|" + (typeof answer) + "|" + answer.split("\n")[0] + "|" + tools.every(t => t.annotations.readOnlyHint === true));
    }, 1500);')
  printf '%s' "$registered" | grep -q '^get_page,get_post,list_offers,list_pages,list_posts,search_posts,site_info|string|Notes|true$' || fail "the page script registered the wrong tools: $registered"
fi

# The protocol's reference client, when it is installed, is the stricter judge.
if [ -n "${BONFYRE_MCP_SDK_DIR:-}" ] && [ -d "$BONFYRE_MCP_SDK_DIR/node_modules/@modelcontextprotocol/sdk" ]; then
  cp "$root/cmd/BonfyreWebAddress/tests/site_mcp_client.mjs" "$BONFYRE_MCP_SDK_DIR/.site_mcp_client.mjs"
  out=$(cd "$BONFYRE_MCP_SDK_DIR" && node .site_mcp_client.mjs "http://127.0.0.1:$port/mcp/notes" 'ocean freight') || fail "reference client: $out"
  rm -f "$BONFYRE_MCP_SDK_DIR/.site_mcp_client.mjs"
  printf '%s' "$out" | grep -q 'unknown_tool_refused	true' || fail "reference client run incomplete: $out"
  echo 'site product: reference MCP client PASS'
else echo 'site product: reference MCP client SKIP (set BONFYRE_MCP_SDK_DIR)'; fi

# A running site's name, line and colour change through the content store and
# are on the next page load; a colour that is not one is refused.
site set acme --title 'Acme Supply Co' --tagline 'Enclosures, cut to order' --accent '#22577a' 2>/dev/null | grep -q '"page_served":true' || fail "set did not read the changed page back"
page=$(get acme.localhost)
printf '%s' "$page" | grep -q '<h1>Acme Supply Co</h1>' || fail "the new name is not on the page"
printf '%s' "$page" | grep -q 'Enclosures, cut to order' || fail "the new line is not on the page"
printf '%s' "$page" | grep -q -- '--bf-accent:#22577a' || fail "the new colour is not on the page"
if site set acme --accent teal >/dev/null 2>&1; then fail "set took a colour that is not one"; fi

# One CMS presentation reaches the homepage, standing pages and contact form.
cat > "$home/presentation.json" <<'PROFILE'
{"hero_mode":"quiet-title","navigation_mode":"minimal","density":"airy","typography_mode":"optical-serif","card_mode":"soft-panel","layout_tokens":{"ground":"#fdfaf4","ink":"#28241f","display":"#28241f","accent":"#775133","panel":"#eee7dd","face_display":"Georgia,serif","face_body":"system-ui,sans-serif","display_size":"clamp(2rem,5vw,4rem)","measure":"54rem","radius":"12px"}}
PROFILE
site set acme --presentation "$home/presentation.json" >/dev/null || fail "presentation patch"
site page acme --title 'Delivery' --text 'Collect at the desk.' --no-open >/dev/null
for route in / /delivery /contact; do
  branded=$(curl -fsS --resolve "acme.localhost:$port:127.0.0.1" -H 'Accept: text/html' "http://acme.localhost:$port$route")
  printf '%s' "$branded" | grep -q 'data-hero_mode="quiet-title"' || fail "hero mode missing at $route"
  printf '%s' "$branded" | grep -q -- '--bf-face-display:Georgia,serif' || fail "typeface missing at $route"
done
if get notes.localhost | grep -q -- '--bf-face-display:Georgia,serif'; then fail "presentation leaked to another customer"; fi
before=$(sqlite3 "$home/cms.db" "SELECT layout_tokens FROM presentation_template WHERE template='acme'")
for patch in '{"layout_tokens":{"ground":"red;}body{display:none"}}' '{"layout_tokens":{"font_href":"https://fonts.googleapis.com.evil/css2?family=Test"}}' '{"navigation_mode":"javascript"}' '{"template":"notes"}' '{"layout_tokens":{"mystery":"value"}}'; do
  printf '%s' "$patch" > "$home/bad-presentation.json"
  if site set acme --presentation "$home/bad-presentation.json" >/dev/null 2>&1; then fail "unsafe presentation accepted"; fi
  [ "$before" = "$(sqlite3 "$home/cms.db" "SELECT layout_tokens FROM presentation_template WHERE template='acme'")" ] || fail "refusal changed the presentation"
done

# A price as it is written states the amount; a floor ("from $600") does not.
# Two posts in the same second each stand; the inbox shows what accept needs.
site new priced --no-open >/dev/null
site offer priced --title 'Tune-up' --price '$75 per bike' --text 'Brakes and gears adjusted.' --bookable >/dev/null
site offer priced --title 'Wheel build' --price 'from $600 per pair' --text 'Hand built wheels.' >/dev/null
site mcp priced --json >/dev/null 2>&1 || true
offers=$(curl -s -m 10 -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' "http://127.0.0.1:$port/mcp/priced" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_offers","arguments":{}}}')
printf '%s' "$offers" | grep -q 'per bike' || fail "the offer is not listed to assistants: $offers"
[ "$(sqlite3 "$home/cms.db" "SELECT json_extract(projection_contract,'$.amount')||'|'||json_extract(projection_contract,'$.currency')||'|'||json_extract(projection_contract,'$.unit') FROM site_fragment WHERE namespace='priced' AND title='Tune-up'" 2>/dev/null || sqlite3 "$home/cms.db" "SELECT json_extract(projection_contract,'$.amount')||'|'||json_extract(projection_contract,'$.currency')||'|'||json_extract(projection_contract,'$.unit') FROM site_fragment WHERE namespace='priced' AND kind='offer' ORDER BY id LIMIT 1")" = '75|USD|per bike' ] || fail "a written price did not state its amount"
[ -z "$(sqlite3 "$home/cms.db" "SELECT json_extract(projection_contract,'$.amount') FROM site_fragment WHERE namespace='priced' AND kind='offer' ORDER BY id DESC LIMIT 1")" ] || fail "a floor price was taken as an amount"
site post priced --title 'One' --text 'The first of two posts.' >/dev/null || fail "first post"
site post priced --title 'Two' --text 'The second, written at once.' >/dev/null || fail "a second post in the same second was refused"
site inbox notes | grep -q '^  reference notes-' || fail "the inbox does not show the reference accept asks for"

# A recording comes in through the same door: Ingest normalises it, Transcribe
# hears it, and its words are the post.  Needs the transcriber, its model and a
# voice to make the recording with.
if [ "${BONFYRE_SITE_SKIP_RECORDING_TEST:-0}" != 1 ] && [ -x "$root/cmd/BonfyreTranscribe/bonfyre-transcribe" ] && command -v say >/dev/null 2>&1 && [ -f "$HOME/.local/share/whisper/ggml-base.en.bin" ]; then
  export BONFYRE_TRANSCRIBE_BINARY="$root/cmd/BonfyreTranscribe/bonfyre-transcribe"
  say -o "$home/opening-hours.aiff" 'The shop is open from nine in the morning until six in the evening, Tuesday to Saturday. We are closed on Sunday and Monday.'
  site import "$home/opening-hours.aiff" --as heard --no-open 2>&1 | grep -q 'as a recording source' || fail "a recording was not imported as one"
  page=$(get heard.localhost)
  printf '%s' "$page" | grep -q 'Opening hours' || fail "the recording's post is not named after its file"
  printf '%s' "$page" | grep -qi 'closed on Sunday and Monday' || fail "the recording's words are not on the page"
  echo 'site product: recording import PASS'
else echo 'site product: recording import SKIP (explicitly disabled or transcriber, model or voice not present)'; fi

site stop | grep -q '"stopped":true' || fail "stop did not stop the server it started"
echo 'site product: PASS'
