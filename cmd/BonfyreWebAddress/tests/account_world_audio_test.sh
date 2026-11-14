#!/bin/sh
# Stations and shows in account spaces, end to end over native HTTP: an owner
# sets up a station with a weekly schedule and a live stream and a show with
# dated episodes, chapters, people, cited sources, clips, tracklists and a
# reviewed transcript; listeners see only the released version, hear only
# recordings whose own source states open terms, follow a strictly valid RSS
# feed and add the schedule to a calendar.
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/world-audio.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -n "${KEEP_WORLD_AUDIO:-}" ] || rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ printf 'account world audio: FAIL %s\n' "$1" >&2; exit 1; }
mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world site_presentation site_page internet_source; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"; done
export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS='' BONFYRE_FOREIGN_ROOT="$tmp/foreign"
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns "${3:-root}" --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null; }
key(){ printf '%s\n#%s' "$1" "$2" | shasum -a 256 | cut -c1-24; }
retain(){ printf '%s' "$2" >"$tmp/foreign/bodies/$1.body"; printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/$1.body" >"$tmp/foreign/receipts/$1.json"; }

# --- recordings, each read through PageComposition with the terms its source states
sound='{"rows":[
 {"id":"e1","title":"The levee at night","maker":"Avery","about":"Walking the levee after dark with a river historian.","audio":"https://example.test/levee-1.mp3","page":"https://example.test/levee-1","terms":"CC BY 4.0","deed":"https://creativecommons.org/licenses/by/4.0/"},
 {"id":"e2","title":"Floodgates","maker":"Avery","about":"How the 1908 flood rebuilt the city.","audio":"https://example.test/levee-2.m4a","page":"https://example.test/levee-2","terms":"CC BY-SA 4.0","deed":"https://creativecommons.org/licenses/by-sa/4.0/"},
 {"id":"e3","title":"A held interview","maker":"Someone","about":"Rights are reserved.","audio":"https://example.test/held.mp3","page":"https://example.test/held","terms":"All rights reserved"},
 {"id":"a1","title":"Night Shift, 24 October","maker":"Avery","about":"Two hours of field recordings and slow songs.","audio":"https://example.test/night-1024.mp3","page":"https://example.test/night-1024","terms":"CC0 1.0","deed":"https://creativecommons.org/publicdomain/zero/1.0/"},
 {"id":"a2","title":"Night Shift, 17 October","maker":"Avery","about":"Rain on the river.","audio":"https://example.test/night-1017.mp3","page":"https://example.test/night-1017","terms":"CC BY 4.0","deed":"https://creativecommons.org/licenses/by/4.0/"},
 {"id":"live","title":"Night Station live","maker":"Avery","about":"The live stream.","audio":"https://example.test/live.mp3","page":"https://example.test/live","terms":"CC0 1.0","deed":"https://creativecommons.org/publicdomain/zero/1.0/"}]}'
retain fixture-sound "$sound"
entry internet_source '{"family":"sound","source":"fixture-sound","label":"sound archive","says":"recordings","url":"https://example.test/sound","twin":"fixture-sound","state":"observed","extract":"rows","title_field":"title","link_field":"page","maker_field":"maker","summary_field":"about","rights_field":"terms","rights_link_field":"deed","media_field":"audio","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"audio"}'
e1="fixture-sound/$(key fixture-sound e1)"; e2="fixture-sound/$(key fixture-sound e2)"; e3="fixture-sound/$(key fixture-sound e3)"
a1="fixture-sound/$(key fixture-sound a1)"; a2="fixture-sound/$(key fixture-sound a2)"; live="fixture-sound/$(key fixture-sound live)"

# --- accounts, address and profile site -------------------------------------
"$auth" --db "$tmp/auth.db" signup --email owner@example.test --name Owner --password 'fixture-ownership-pass' >/dev/null
"$auth" --db "$tmp/auth.db" signup --email other@example.test --name Other --password 'fixture-ownership-pass' >/dev/null
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
other=$("$auth" --db "$tmp/auth.db" login --email other@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
"$auth" --db "$tmp/auth.db" actor-bind --user-id 1 --actor person:avery >/dev/null
"$auth" --db "$tmp/auth.db" actor-bind --user-id 2 --actor person:other >/dev/null
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$web" subject --db "$BONFYRE_MATRIX_DB" --id person:avery --kind Person --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --evidence fixture:profile >/dev/null
"$web" institution --db "$BONFYRE_MATRIX_DB" --id inst:person-avery --kind Person --world live --evidence fixture:profile >/dev/null
"$web" domain --db "$BONFYRE_MATRIX_DB" --host avery.aurekai.work --institution inst:person-avery --territory work --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
"$web" presence --db "$BONFYRE_MATRIX_DB" --id presence:avery:work --subject person:avery --territory work --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
entry site_presentation '{"site_id":"profile-avery-work","host":"avery.aurekai.work","subject_address":"person:avery","territory":"work","profile":"member-profile-v1","navigation":[],"status":"published"}'
port=$((20000 + ($$ + 11) % 20000))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -fsS "$base/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep .1; done
enc(){ jq -rn --arg v "$1" '$v|@uri'; }
post(){ token=$1; path=$2; data=$3; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://aurekai.work' -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' --data "$data" "$base$path"; }
get(){ token=$1; path=$2; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -H 'Host: aurekai.work' -H "Authorization: Bearer $token" "$base$path"; }
public(){ curl -sS -o "$tmp/page" -D "$tmp/headers" -w '%{http_code}' -H "Host: ${2:-avery.aurekai.work}" "$base$1"; }
W='site=profile-avery-work'
rev(){ get "$owner" "/workspace/world?operation=read&$W&world=1&space=$1" >/dev/null; jq -r .revision "$tmp/response"; }
sound(){ sp=$1; shift; code=$(post "$owner" /workspace/world-audio "$W&world=1&space=$sp&revision=$(rev "$sp")&$*"); printf '%s' "$code"; }
must(){ code=$(sound "$@"); [ "$code" = 200 ] || fail "$* ($code: $(cat "$tmp/response"))"; }

# --- spaces from the foundation ------------------------------------------------
[ "$(post "$owner" /workspace/world "operation=create&$W")" = 200 ] || fail 'create'
for sp in station:Night+Station:radio:ticker levee:The+Levee+Hour:podcast:journal; do
 id=${sp%%:*}; rest=${sp#*:}; title=${rest%%:*}; rest=${rest#*:}; medium=${rest%%:*}; look=${rest#*:}
 [ "$(post "$owner" /workspace/world "operation=put&$W&world=1&space=$id&title=$title&renderer=2d&template=template%3Aworld-character&layout=layout%3Asequence&revision=0")" = 200 ] || fail "put $id"
 [ "$(post "$owner" /workspace/world "operation=style&$W&world=1&space=$id&medium=$medium&character=$look&lede=Kept+by+Avery+on+the+river&revision=1")" = 200 ] || fail "style $id"
done
item(){ sp=$1 pos=$2 ref=$3 role=$4; [ "$(post "$owner" /workspace/world "operation=item&$W&world=1&space=$sp&position=$pos&ref=$(enc "$ref")&role=$role&revision=$(rev "$sp")")" = 200 ] || fail "item $sp $pos"; }
item station 1 "$a1" archive; item station 2 "$a2" archive
item levee 1 "$e1" episode; item levee 2 "$e2" episode; item levee 3 "$e3" episode

# --- the owner sets up a station ----------------------------------------------
[ "$(sound station operation=sound\&kind=station\&timezone=Mars%2FOlympus\&language=en\&author=Avery\&category=Music)" = 409 ] || fail 'unknown zone refused'
[ "$(sound station operation=sound\&kind=station\&timezone=America%2FChicago\&language=en\&author=Avery\&category=Music\&stream=$(enc https://example.test/live.mp3))" = 409 ] || fail 'a stream is a reference, not a URL'
must station "operation=sound&kind=station&timezone=America%2FChicago&language=en&author=Avery&category=Music&stream=$(enc "$live")"
[ "$(post "$other" /workspace/world-audio "$W&world=1&space=station&revision=$(rev station)&operation=sound&kind=show&timezone=UTC&language=en&author=X&category=Music")" = 404 ] || fail 'another account cannot change the station'
[ "$(post "$owner" /workspace/world-audio "$W&world=1&space=station&revision=1&operation=sound&kind=show&timezone=UTC&language=en&author=X&category=Music")" = 409 ] || fail 'stale revision'
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://attacker.test' -H "Authorization: Bearer $owner" -H 'Content-Type: application/x-www-form-urlencoded' --data "$W&world=1&space=station&revision=$(rev station)&operation=unslot&slot=1" "$base/workspace/world-audio")" = 403 ] || fail 'foreign origin'
# Schedule around the station's own clock: one programme on now, one later.
now=$(TZ=America/Chicago date '+%u %H %M'); set -- $now; h=${2#0}; mi=${3#0}; wk=$(( ($1-1)*1440 + h*60 + mi ))
on=$(( (wk - 30 + 10080) % 10080 )); later=$(( (wk + 180) % 10080 )); nextday=$(( later / 1440 + 1 ))
hm(){ printf '%02d%%3A%02d' $(( ($1 % 1440) / 60 )) $(( $1 % 60 )); }
must station "operation=slot&slot=1&weekday=$((on/1440+1))&start=$(hm $on)&minutes=120&title=Night+Shift&host=Avery"
must station "operation=slot&slot=2&weekday=$nextday&start=$(hm $later)&minutes=60&title=Morning+Ritual&host=Jo"
must station "operation=episode&ref=$(enc "$a1")&season=0&number=0&published=2026-10-24"
must station "operation=episode&ref=$(enc "$a2")&season=0&number=0&published=2026-10-17"
must station "operation=mark&ref=$(enc "$a1")&type=track&at=0%3A00&title=Dawn+chorus&credit=Field+recordist&checked=1"
must station "operation=mark&ref=$(enc "$a1")&type=track&at=12%3A40&title=Slow+river&credit=Unknown+band"
must station "operation=mark&ref=$(enc "$a2")&type=track&at=1%3A02%3A03&title=Rain+song&credit=Jo&checked=1"
[ "$(sound station "operation=mark&ref=$(enc "$e1")&type=track&at=0%3A00&title=x")" = 409 ] || fail 'marks only on items the space holds'

# --- and a show ----------------------------------------------------------------
must levee "operation=sound&kind=show&timezone=America%2FChicago&language=en-US&author=Avery+%26+Jordan&category=History"
must levee "operation=episode&ref=$(enc "$e1")&season=1&number=1&published=2026-10-01"
must levee "operation=episode&ref=$(enc "$e2")&season=1&number=2&published=2026-10-15"
must levee "operation=episode&ref=$(enc "$e3")&season=1&number=3&published=2026-10-22"
[ "$(sound levee "operation=episode&ref=$(enc "$e2")&season=1&number=2&published=2026-13-01")" = 409 ] || fail 'invalid date'
m(){ must levee "operation=mark&ref=$(enc "$e1")&$1"; }
m "type=chapter&at=0%3A00&title=Cold+open"
m "type=chapter&at=1%3A30&title=The+river+historian"
m "type=person&at=0&title=Avery&credit=host"
m "type=person&at=0&title=Jordan+Reyes&credit=guest&href=$(enc https://example.test/jordan)"
m "type=source&at=1%3A35&title=Levee+district+records%2C+1910&href=$(enc https://example.test/levee-records)"
m "type=clip&at=1%3A30&until=2%3A00&title=Why+the+levee+bends&checked=1"
m "type=clip&at=3%3A00&until=3%3A20&title=Unreviewed+clip"
[ "$(sound levee "operation=mark&ref=$(enc "$e1")&type=clip&at=3%3A00&title=No+end")" = 409 ] || fail 'a clip needs an end'
[ "$(sound levee "operation=mark&ref=$(enc "$e1")&type=source&at=0&title=x&href=javascript%3Aalert(1)")" = 409 ] || fail 'only https or piece links'
vtt='WEBVTT

00:00:01.000 --> 00:00:04.500
<v Avery>Good evening from the levee.

00:01:30.000 --> 00:01:35.250
<v Jordan Reyes>The river carries everything downstream.

00:02:00.000 --> 00:02:03.000
<v Jordan Reyes>Speling misteak the reviewer has not seen.
'
must levee "operation=transcript&ref=$(enc "$e1")&vtt=$(enc "$vtt")"
[ "$(jq -r .imported "$tmp/response")" = 3 ] || fail 'three cues imported'
[ "$(get "$owner" "/workspace/world-audio?operation=draft&$W&world=1&space=levee")" = 200 ] || fail 'draft readback'
[ "$(jq -r '[.marks[]|select(.kind=="cue" and .checked==0)]|length' "$tmp/response")" = 3 ] || fail 'cues arrive unreviewed'
[ "$(jq -r '.sound[0].kind+"/"+.sound[0].timezone+"/"+(.episodes|length|tostring)' "$tmp/response")" = 'show/America/Chicago/3' ] || fail 'draft programme'
for at in 1000 90000; do
 id=$(jq -r ".marks[]|select(.kind==\"cue\" and .start_ms==$at)|.id" "$tmp/response")
 must levee "operation=check&id=$id&checked=1"
 get "$owner" "/workspace/world-audio?operation=draft&$W&world=1&space=levee" >/dev/null
done
[ "$(get "$other" "/workspace/world-audio?operation=draft&$W&world=1&space=levee")" = 404 ] || fail 'another account cannot read the draft'

# --- studio --------------------------------------------------------------------
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=levee")" = 200 ] || fail 'studio'
grep -q 'data-world-sound-studio' "$tmp/response" || fail 'studio has the station or show section'
grep -q 'href="/assets/aurekai-native.css' "$tmp/response" || fail 'audio editor links its generated stylesheet'
! grep -q '<style>\.aw-sound-studio' "$tmp/response" || fail 'audio editor styles are emitted inline'
grep -q 'Needs review' "$tmp/response" && grep -q '>Reviewed<' "$tmp/response" || fail 'studio flags review state'
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=station")" = 200 ] || fail 'station studio'
grep -q '>Unconfirmed<' "$tmp/response" && grep -q 'data-sound-schedule' "$tmp/response" || fail 'studio shows the schedule and unconfirmed tracks'
cp "$tmp/response" "$tmp/studio-station.html"

# --- nothing is public before release -----------------------------------------
[ "$(public /worlds/levee/feed.xml)" = 404 ] || fail 'no feed before release'
for sp in station levee; do [ "$(post "$owner" /workspace/world "operation=release&$W&world=1&space=$sp&revision=$(rev $sp)")" = 200 ] || fail "release $sp"; done

# --- the station, as a listener -------------------------------------------------
[ "$(public /worlds/station)" = 200 ] || fail 'station page'
grep -q 'data-world-sound="station"' "$tmp/page" || fail 'station block'
grep -q 'data-live="playing"><p class="aw-role">Live</p><audio controls preload="none" src="https://example.test/live.mp3">' "$tmp/page" || fail 'CC0 live stream plays on demand'
grep -q '<p data-now><span class="aw-role">On air now</span> <strong>Night Shift</strong> with Avery' "$tmp/page" || fail 'on air now'
grep -q '<p data-next><span class="aw-role">Next</span> <time datetime="[0-9T:Z-]*">[A-Za-z]* [0-9:]*</time> &middot; Morning Ritual with Jo' "$tmp/page" || fail 'next'
grep -q 'America/Chicago' "$tmp/page" || fail 'the station clock is named'
grep -q 'data-world-schedule' "$tmp/page" || fail 'weekly schedule'
grep -q 'Unknown band &mdash; Slow river <span class="aw-flag" data-unconfirmed>not yet confirmed</span>' "$tmp/page" || fail 'unconfirmed track is flagged'
grep -q 'Field recordist &mdash; Dawn chorus</li>' "$tmp/page" || fail 'confirmed track'
grep -q 'data-seek="3723000"' "$tmp/page" || fail 'offsets seek'
if grep -q 'autoplay' "$tmp/page"; then fail 'nothing plays by itself'; fi
[ "$(grep -o 'data-episode-for="piece-[0-9]*"' "$tmp/page" | wc -l | tr -d ' ')" = 2 ] || fail 'two archive episodes'
cp "$tmp/page" "$tmp/station.html"
[ "$(public /worlds/station/schedule.ics)" = 200 ] || fail 'calendar'
grep -qi '^Content-Type: text/calendar' "$tmp/headers" || fail 'calendar type'
[ "$(grep -c '^BEGIN:VEVENT' "$tmp/page")" = 2 ] && grep -q '^RRULE:FREQ=WEEKLY' "$tmp/page" && grep -q '^DTSTART;TZID=America/Chicago:' "$tmp/page" && grep -q '^SUMMARY:Night Shift' "$tmp/page" || fail 'calendar events'
[ "$(public /worlds/station/feed.xml)" = 200 ] || fail 'station archive feed'
xmllint --noout "$tmp/page" || fail 'station feed is XML'

# --- the show, as a listener ----------------------------------------------------
[ "$(public /worlds/levee)" = 200 ] || fail 'show page'
cp "$tmp/page" "$tmp/levee.html"
grep -q 'data-world-sound="show"' "$tmp/page" || fail 'show block'
grep -q 'Season 1 &middot; Episode 1 &middot; <time datetime="2026-10-01">1 October 2026</time>' "$tmp/page" || fail 'episode meta'
grep -q '<button type="button" class="aw-at" data-seek="90000" aria-label="Play from 1:30">1:30</button> The river historian' "$tmp/page" || fail 'chapter jump'
grep -q 'With Avery <span>(host)</span>, <a rel="noopener" href="https://example.test/jordan">Jordan Reyes</a> <span>(guest)</span>' "$tmp/page" || fail 'people'
grep -q 'href="https://example.test/levee-records">Levee district records, 1910</a>' "$tmp/page" || fail 'cited source'
grep -q 'data-until="120000"' "$tmp/page" && grep -q 'Why the levee bends' "$tmp/page" || fail 'reviewed clip'
if grep -q 'Unreviewed clip' "$tmp/page"; then fail 'unreviewed clip must stay private'; fi
grep -q 'data-cue-search' "$tmp/page" && grep -q 'Good evening from the levee.' "$tmp/page" || fail 'searchable transcript'
if grep -q 'Speling misteak' "$tmp/page"; then fail 'unreviewed transcript line must stay private'; fi
grep -q '1 more line is still being checked' "$tmp/page" || fail 'pending lines are counted'
grep -q "data-world-piece=\"$e3\" data-shown=\"link\"" "$tmp/page" || fail 'rights-reserved episode is linked'
if grep -q '<audio[^>]*held.mp3' "$tmp/page"; then fail 'rights-reserved audio must not play'; fi
# the reserved episode has no seek buttons: its marks would be spans
[ "$(public /worlds/levee/transcript/1.vtt)" = 200 ] || fail 'transcript file'
head -1 "$tmp/page" | grep -q '^WEBVTT' && grep -q '<v Avery>Good evening from the levee.' "$tmp/page" || fail 'vtt'
if grep -q 'Speling' "$tmp/page"; then fail 'vtt carries only reviewed lines'; fi
[ "$(public /worlds/levee/chapters/1.json)" = 200 ] || fail 'chapters'
[ "$(jq -r '.version+"|"+(.chapters|length|tostring)+"|"+(.chapters[1].startTime|tostring)+"|"+.chapters[1].title' "$tmp/page")" = '1.2.0|2|90|The river historian' ] || fail 'chapters json'
[ "$(public /worlds/levee/chapters/3.json)" = 404 ] || fail 'no chapters for episode 3'

# --- the feed: strict XML, valid RSS shape --------------------------------------
[ "$(public /worlds/levee/feed.xml)" = 200 ] || fail 'feed'
grep -qi '^Content-Type: application/rss+xml' "$tmp/headers" || fail 'feed type'
cp "$tmp/page" "$tmp/feed.xml"
xmllint --noout --nonet "$tmp/feed.xml" || fail 'feed is not well-formed XML'
x(){ xmllint --xpath "$1" "$tmp/feed.xml" 2>/dev/null; }
[ "$(x 'string(/rss/@version)')" = 2.0 ] || fail 'rss 2.0'
[ "$(x 'string(/rss/channel/title)')" = 'The Levee Hour' ] || fail 'channel title'
[ "$(x 'string(/rss/channel/link)')" = 'https://avery.aurekai.work/worlds/levee' ] || fail 'channel link'
[ -n "$(x 'string(/rss/channel/description)')" ] || fail 'channel description'
[ "$(x 'string(/rss/channel/language)')" = en-US ] || fail 'language'
[ "$(x "string(/rss/channel/*[local-name()='author'])")" = 'Avery & Jordan' ] || fail 'itunes author escaped and read back'
[ "$(x "string(/rss/channel/*[local-name()='category']/@text)")" = History ] || fail 'itunes category'
[ "$(x "string(/rss/channel/*[local-name()='link' and namespace-uri()='http://www.w3.org/2005/Atom']/@href)")" = 'https://avery.aurekai.work/worlds/levee/feed.xml' ] || fail 'atom self link'
[ "$(x 'count(/rss/channel/item)')" = 2 ] || fail 'two playable episodes; the reserved one is left out'
[ "$(x 'string(/rss/channel/item[1]/title)')" = 'Floodgates' ] || fail 'newest first'
[ "$(x 'string(/rss/channel/item[1]/enclosure/@type)')" = 'audio/mp4' ] || fail 'm4a type'
[ "$(x 'string(/rss/channel/item[2]/enclosure/@url)')" = 'https://example.test/levee-1.mp3' ] || fail 'enclosure'
[ "$(x 'string(/rss/channel/item[2]/pubDate)')" = 'Thu, 01 Oct 2026 12:00:00 GMT' ] || fail 'pubDate'
[ "$(x 'count(/rss/channel/item/guid)')" = 2 ] || fail 'guids'
[ "$(x "string(/rss/channel/item[2]/*[local-name()='chapters']/@url)")" = 'https://avery.aurekai.work/worlds/levee/chapters/1.json' ] || fail 'podcast:chapters'
[ "$(x "string(/rss/channel/item[2]/*[local-name()='transcript']/@type)")" = 'text/vtt' ] || fail 'podcast:transcript'
[ "$(x "string(/rss/channel/item[2]/*[local-name()='person' and @role='guest']/@href)")" = 'https://example.test/jordan' ] || fail 'podcast:person'
[ "$(x "count(/rss/channel/item[2]/*[local-name()='soundbite'])")" = 1 ] || fail 'only the reviewed clip is a soundbite'
[ "$(x "string(/rss/channel/item[2]/*[local-name()='soundbite']/@startTime)")" = '90.000' ] || fail 'soundbite start'
if grep -q 'held.mp3' "$tmp/feed.xml"; then fail 'reserved audio never enters the feed'; fi

# --- corrections stay private until released ---------------------------------------
m "type=chapter&at=4%3A00&title=Late+correction"
public /worlds/levee >/dev/null; if grep -q 'Late correction' "$tmp/page"; then fail 'draft correction leaked'; fi
[ "$(post "$owner" /workspace/world "operation=release&$W&world=1&space=levee&revision=$(rev levee)")" = 200 ] || fail 'release correction'
public /worlds/levee >/dev/null; grep -q 'Late correction' "$tmp/page" || fail 'released correction'
get "$owner" "/workspace/world-audio?operation=draft&$W&world=1&space=levee" >/dev/null
id=$(jq -r '.marks[]|select(.title=="Late correction")|.id' "$tmp/response")
must levee "operation=unmark&id=$id"
[ "$(post "$owner" /workspace/world "operation=release&$W&world=1&space=levee&revision=$(rev levee)")" = 200 ] || fail 'release removal'
public /worlds/levee >/dev/null; if grep -q 'Late correction' "$tmp/page"; then fail 'removed chapter'; fi

# --- the source decides, on every request ---------------------------------------
retain fixture-sound "$(printf '%s' "$sound" | sed 's/"terms":"CC BY 4.0","deed":"https:\/\/creativecommons.org\/licenses\/by\/4.0\/"},$/"terms":"All rights reserved"},/' | sed 's/"CC0 1.0","deed":"https:\/\/creativecommons.org\/publicdomain\/zero\/1.0\/"}]}/"All rights reserved"}]}/')"
public /worlds/levee/feed.xml >/dev/null; xmllint --noout "$tmp/page" || fail 'feed stays XML'
[ "$(xmllint --xpath 'count(/rss/channel/item)' "$tmp/page")" = 1 ] || fail 'an episode whose terms closed leaves the feed'
public /worlds/levee >/dev/null; if grep -q 'levee-1.mp3"' "$tmp/page" && grep -q '<audio[^>]*levee-1.mp3' "$tmp/page"; then fail 'closed terms stop playback'; fi
grep -q '<span class="aw-at">1:30</span> The river historian' "$tmp/page" || fail 'chapters without playback are plain times'
public /worlds/station >/dev/null; grep -q 'data-live="linked"' "$tmp/page" || fail 'a live stream with closed terms is linked'
retain fixture-sound "$sound"

# --- nothing scheduled now -------------------------------------------------------
must station "operation=unslot&slot=1"
[ "$(post "$owner" /workspace/world "operation=release&$W&world=1&space=station&revision=$(rev station)")" = 200 ] || fail 'release schedule change'
public /worlds/station >/dev/null; grep -q 'Nothing is scheduled right now.' "$tmp/page" || fail 'nothing on now'

# --- take-down ------------------------------------------------------------------------
[ "$(post "$owner" /workspace/world "operation=withdraw&$W&world=1&space=levee")" = 200 ] || fail 'withdraw'
for p in /worlds/levee/feed.xml /worlds/levee/chapters/1.json /worlds/levee/transcript/1.vtt; do [ "$(public $p)" = 410 ] || fail "withdrawn $p"; done
[ "$(sqlite3 "$BONFYRE_CMS_DB" "SELECT count(*) FROM account_world_mark WHERE released=1 AND space_id='levee'")" = 0 ] || fail 'withdrawal drops the public copy'
[ "$(public /worlds/station/feed.xml avery.aurekai.life)" = 404 ] || fail 'another host does not carry it'
printf 'account world audio: ok\n'
