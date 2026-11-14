#!/bin/sh
# The same native source revision drives intake, media review, and invalidation.
set -eu
bin=$1
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT INT TERM
mkdir -p "$t/render" "$t/assets"
sqlite3 "$t/cms.db" <<'SQL'
CREATE TABLE site_page(id INTEGER PRIMARY KEY,namespace TEXT,route TEXT,title TEXT,heading TEXT,lede TEXT,media_subject_ref TEXT,editorial_kind TEXT,status TEXT);
CREATE TABLE site_fragment(id INTEGER PRIMARY KEY,namespace TEXT,page TEXT,heading TEXT,body TEXT,kind TEXT,status TEXT,audience TEXT,position INTEGER);
CREATE TABLE community_post(id INTEGER PRIMARY KEY,namespace TEXT,post TEXT,title TEXT,body TEXT,visibility TEXT,moderation_state TEXT,status TEXT);
CREATE TABLE media_program(id INTEGER PRIMARY KEY,namespace TEXT,created_at TEXT,updated_at TEXT,status TEXT,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,duration_ms INTEGER,transcript_ref TEXT,chapters TEXT,captions TEXT,publication_state TEXT,cannot_infer TEXT,waveform_peaks TEXT,waveform_audio_sha256 TEXT,narration_source_sha256 TEXT);
CREATE UNIQUE INDEX media_one ON media_program(namespace,program_id);
INSERT INTO site_page VALUES(1,'acme','/essay','Native story','Native story','An independently authored report of a new public library.','article:story','article','published');
INSERT INTO site_fragment VALUES(1,'acme','/essay','First section','The original text describes a new local public library.','body','published','public',1);
INSERT INTO site_fragment VALUES(2,'acme','/essay','Source note','Not part of the narrated article.','citation','published','public',2);
INSERT INTO site_fragment VALUES(3,'acme','/essay','Profile story','A profile composer retains this complete article passage.','profile-story','published','public',3);
INSERT INTO site_fragment VALUES(4,'acme','/essay','Private draft','A draft profile passage remains private.','profile-story','draft','public',4);
INSERT INTO site_page VALUES(2,'acme','/draft','Draft story','Draft story','A private draft must never become a narrated article.','article:private','article','draft');
INSERT INTO site_page VALUES(3,'acme','/no-id','Unbound story','Unbound story','An article awaiting editorial identity.','','article','published');
INSERT INTO community_post VALUES(1,'acme','posted-1','Community update','A public library with a published opening schedule.','public','visible','published');
INSERT INTO community_post VALUES(2,'acme','private-post','Private update','A private conversation may not be narrated.','private','visible','published');
SQL
check() { [ "$1" = "$2" ] || { echo "FAIL: $3 ($1 != $2)" >&2; exit 1; }; }
rows=$("$bin" intake --cms "$t/cms.db" --namespace acme --limit 50)
printf '%s' "$rows" | grep -q 'article:story' || { echo "FAIL: article missing" >&2; exit 1; }
printf '%s' "$rows" | grep -q 'needs-identity' || { echo "FAIL: missing identity not flagged" >&2; exit 1; }
printf '%s' "$rows" | grep -q 'requires-author-review' || { echo "FAIL: community post not review gated" >&2; exit 1; }
if printf '%s' "$rows" | grep -q 'article:private\|private-post'; then echo "FAIL: private content exposed" >&2; exit 1; fi
one=$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story --out "$t/script.txt")
digest=$(printf '%s' "$one" | cut -f4)
[ "$(printf %s "$digest" | wc -c | tr -d ' ')" = 64 ] || { echo "FAIL: missing digest" >&2; exit 1; }
grep -q 'original text describes' "$t/script.txt" || { echo "FAIL: article body missing" >&2; exit 1; }
grep -q 'profile composer retains this complete article passage' "$t/script.txt" || { echo "FAIL: profile story body missing" >&2; exit 1; }
if grep -q 'draft profile passage' "$t/script.txt"; then echo "FAIL: draft profile story exposed" >&2; exit 1; fi
if grep -q 'Not part of the narrated article' "$t/script.txt"; then echo "FAIL: citation narrated" >&2; exit 1; fi
if "$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story --out "$t/script.txt" >/dev/null 2>&1; then echo "FAIL: script overwrite" >&2; exit 1; fi
if "$bin" intake --cms "$t/cms.db" --namespace acme --subject article:private >/dev/null 2>&1; then echo "FAIL: unpublished eligible" >&2; exit 1; fi
sqlite3 "$t/cms.db" "INSERT INTO site_page VALUES(9,'aaa','/other','Other story','Other story','A source outside the selected namespace.','article:other','article','published'); INSERT INTO site_page VALUES(10,'acme','/teaser','Long headline without article body','Long headline without article body','This paragraph is promotional teaser copy, not an article body.','article:teaser','article','published');"
if "$bin" intake --cms "$t/cms.db" --namespace acme --subject article:teaser >/dev/null 2>&1; then echo "FAIL: article teaser mistaken for full narrative" >&2; exit 1; fi
scoped=$("$bin" intake --cms "$t/cms.db" --namespace acme --limit 1)
printf '%s' "$scoped" | grep -q 'acme' || { echo "FAIL: namespace filtered after pagination" >&2; exit 1; }
if printf '%s' "$scoped" | grep -q 'aaa'; then echo "FAIL: wrong tenant included" >&2; exit 1; fi
scrolled=$("$bin" intake --cms "$t/cms.db" --namespace acme --after-namespace acme --after-subject article:teaser --limit 1)
printf '%s' "$scrolled" | grep -q 'post:posted-1' || { echo "FAIL: keyset cursor cannot move to next content family" >&2; exit 1; }
if "$bin" intake --cms "$t/cms.db" --after-namespace acme >/dev/null 2>&1; then echo "FAIL: incomplete keyset cursor accepted" >&2; exit 1; fi
ffmpeg -nostdin -v error -f lavfi -i "sine=frequency=440:duration=2" -c:a aac -b:a 96k -y "$t/render/program.m4a"
printf 'WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nSynthetic fixture\n' > "$t/render/transcript.vtt"
printf '{"chapters":[]}' > "$t/render/chapters.json"
audio=$(shasum -a 256 "$t/render/program.m4a" | awk '{print $1}')
printf '{"program":"narration-one","planned_at":"2026-11-09T12:00:00Z","audio_sha256":"%s","duration_ms":2000,"cannot_infer":"Synthetic sound is not approved narration"}' "$audio" > "$t/render/manifest.json"
if "$bin" publish "$t/render" --cms "$t/cms.db" --subject article:story --source-namespace acme --source-sha256 "$(printf '%064d' 0)" --asset-root "$t/assets" >/dev/null 2>&1; then echo "FAIL: wrong source admitted" >&2; exit 1; fi
[ ! -e "$t/assets/$audio.m4a" ] || { echo "FAIL: failed source staged audio" >&2; exit 1; }
"$bin" publish "$t/render" --cms "$t/cms.db" --subject article:story --source-namespace acme --source-sha256 "$digest" --asset-root "$t/assets" >/dev/null
check "$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story | cut -f5)" awaiting-review "review state"
check "$(sqlite3 "$t/cms.db" "SELECT narration_source_sha256 FROM media_program WHERE subject_ref='article:story'")" "$digest" "saved revision"
sqlite3 "$t/cms.db" "UPDATE site_fragment SET body='The revised article reports that the library opens with two reading rooms.' WHERE id=1"
changed=$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story)
[ "$(printf '%s' "$changed" | cut -f4)" != "$digest" ] || { echo "FAIL: source digest unchanged" >&2; exit 1; }
check "$(printf '%s' "$changed" | cut -f5)" stale-draft "revision invalidation"
if "$bin" publish "$t/render" --cms "$t/cms.db" --subject article:story --source-namespace acme --source-sha256 "$digest" --asset-root "$t/assets" >/dev/null 2>&1; then echo "FAIL: stale republished" >&2; exit 1; fi
sqlite3 "$t/cms.db" "UPDATE media_program SET status='published',publication_state='published' WHERE subject_ref='article:story'"
check "$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story | cut -f5)" stale-published "stale approved edition"
sqlite3 "$t/cms.db" "UPDATE site_fragment SET body='The original text describes a new local public library.' WHERE id=1"
check "$("$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story | cut -f5)" published-current "fresh approved edition"
sqlite3 "$t/cms.db" "INSERT INTO site_page VALUES(4,'acme','/duplicate','Duplicate','Duplicate','This duplicate must not be allowed to share a narration identity.','article:story','essay','published')"
if "$bin" intake --cms "$t/cms.db" --namespace acme --subject article:story >/dev/null 2>&1; then echo "FAIL: duplicate identity accepted" >&2; exit 1; fi
echo "Bonfyre native narration: source discovery, review, staged media, and revision invalidation: PASS"
