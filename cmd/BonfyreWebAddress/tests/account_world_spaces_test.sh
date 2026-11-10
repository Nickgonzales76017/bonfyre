#!/bin/sh
# Account spaces end to end: an owner starts spaces on their profile site,
# styles each one in a World character, places pieces by reference, publishes
# and takes down; anonymous readers see only released versions on the member
# host, media plays only under open terms its own source states, and a piece
# that leaves its source leaves the page.
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/world-spaces.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -n "${KEEP_WORLD_SPACES:-}" ] || rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ printf 'account spaces: FAIL %s\n' "$1" >&2; exit 1; }
mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world site_presentation site_page internet_source saved_item; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"; done
export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS='' BONFYRE_FOREIGN_ROOT="$tmp/foreign"
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns "${3:-root}" --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null; }
key(){ printf '%s\n#%s' "$1" "$2" | shasum -a 256 | cut -c1-24; }

# --- material: four retained sources, each read through PageComposition ----
retain(){ printf '%s' "$2" >"$tmp/foreign/bodies/$1.body"; printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/$1.body" >"$tmp/foreign/receipts/$1.json"; }
cp "$root/cmd/BonfyreWebAddress/tests/fixtures/cleveland-cc0-artworks.json" "$tmp/foreign/bodies/cleveland-artworks.body"
printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/cleveland-artworks.body" >"$tmp/foreign/receipts/cleveland-artworks.json"
entry internet_source '{"family":"museums","source":"cleveland-artworks","label":"Cleveland Museum of Art","says":"open-access artworks","url":"https://openaccess-api.clevelandart.org/api/artworks/","twin":"cleveland-artworks","state":"observed","extract":"data","title_field":"title","link_field":"url","image_field":"images.web.url","maker_field":"creators.0.description","summary_field":"tombstone","rights_field":"share_license_status","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"image"}'
films='{"rows":[{"id":"Q623051","title":"Night of the Living Dead","maker":"George A. Romero","about":"1968 American independent horror film.","video":"https://commons.wikimedia.org/wiki/Special:FilePath/Night%20of%20the%20Living%20Dead%20%281968%29.webm","page":"https://en.wikipedia.org/wiki/Night_of_the_Living_Dead","terms":"Public domain"},{"id":"Q999","title":"A held film","maker":"Someone","about":"Rights are reserved by its maker.","video":"https://example.test/held.webm","page":"https://example.test/held","terms":"All rights reserved"}]}'
retain fixture-films "$films"
entry internet_source '{"family":"films","source":"fixture-films","label":"film archive","says":"films","url":"https://example.test/films","twin":"fixture-films","state":"observed","extract":"rows","title_field":"title","link_field":"page","maker_field":"maker","summary_field":"about","rights_field":"terms","media_field":"video","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"video"}'
sound='{"rows":[{"id":"s1","title":"Dawn chorus, Trinity River","maker":"Field recordist","about":"Twelve minutes recorded at first light.","audio":"https://example.test/dawn.mp3","page":"https://example.test/dawn","terms":"CC BY 4.0","deed":"https://creativecommons.org/licenses/by/4.0/"}]}'
retain fixture-sound "$sound"
entry internet_source '{"family":"sound","source":"fixture-sound","label":"sound archive","says":"recordings","url":"https://example.test/sound","twin":"fixture-sound","state":"observed","extract":"rows","title_field":"title","link_field":"page","maker_field":"maker","summary_field":"about","rights_field":"terms","rights_link_field":"deed","media_field":"audio","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"audio"}'
retain fixture-press '{"rows":[{"id":"p1","title":"The river at night","about":"An essay on walking the levee after dark, and what the water carries.","page":"https://example.test/river-essay"}]}'
entry internet_source '{"family":"press","source":"fixture-press","label":"press","says":"essays","url":"https://example.test/press","twin":"fixture-press","state":"observed","extract":"rows","title_field":"title","link_field":"page","summary_field":"about","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"text"}'
art="cleveland-artworks/$(key cleveland-artworks 92937)"
film="fixture-films/$(key fixture-films Q623051)"
held="fixture-films/$(key fixture-films Q999)"
song="fixture-sound/$(key fixture-sound s1)"
essay="fixture-press/$(key fixture-press p1)"

# --- accounts, addresses and profile sites --------------------------------
"$auth" --db "$tmp/auth.db" signup --email owner@example.test --name Owner --password 'fixture-ownership-pass' >/dev/null
"$auth" --db "$tmp/auth.db" signup --email other@example.test --name Other --password 'fixture-ownership-pass' >/dev/null
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
other=$("$auth" --db "$tmp/auth.db" login --email other@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
"$auth" --db "$tmp/auth.db" actor-bind --user-id 1 --actor person:avery >/dev/null
"$auth" --db "$tmp/auth.db" actor-bind --user-id 2 --actor person:other >/dev/null
entry saved_item "{\"saved\":\"avery-art\",\"actor\":\"user:1\",\"item_ref\":\"$art\",\"title\":\"Avery saved artwork\",\"publisher\":\"Cleveland Museum of Art\",\"saved_at\":\"2026-11-05T00:00:00Z\",\"collection\":\"World research\",\"cannot_infer\":\"A save never grants rights.\",\"status\":\"published\"}"
entry saved_item "{\"saved\":\"other-private\",\"actor\":\"user:2\",\"item_ref\":\"$held\",\"title\":\"Other private selection\",\"saved_at\":\"2026-11-05T00:00:00Z\",\"collection\":\"Other research\",\"cannot_infer\":\"A save never grants rights.\",\"status\":\"published\"}"
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$web" subject --db "$BONFYRE_MATRIX_DB" --id person:avery --kind Person --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --evidence fixture:profile >/dev/null
"$web" institution --db "$BONFYRE_MATRIX_DB" --id inst:person-avery --kind Person --world live --evidence fixture:profile >/dev/null
for territory in work life; do
 "$web" domain --db "$BONFYRE_MATRIX_DB" --host "avery.aurekai.$territory" --institution inst:person-avery --territory "$territory" --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
 "$web" presence --db "$BONFYRE_MATRIX_DB" --id "presence:avery:$territory" --subject person:avery --territory "$territory" --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
 entry site_presentation "{\"site_id\":\"profile-avery-$territory\",\"host\":\"avery.aurekai.$territory\",\"subject_address\":\"person:avery\",\"territory\":\"$territory\",\"profile\":\"member-profile-v1\",\"navigation\":[],\"status\":\"published\"}"
done
port=$((20000 + ($$ + 7) % 20000))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -fsS "$base/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep .1; done
post(){ token=$1; data=$2; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://aurekai.work' -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' --data "$data" "$base/workspace/world"; }
get(){ token=$1; path=$2; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -H 'Host: aurekai.work' -H "Authorization: Bearer $token" "$base$path"; }
public(){ curl -sS -o "$tmp/page" -D "$tmp/headers" -w '%{http_code}' -H "Host: ${2:-avery.aurekai.work}" "$base$1"; }
catalogue(){ curl -fsS -H "Host: aurekai.live" "$base$1" >"$tmp/catalogue.html"; }
W='site=profile-avery-work'

# --- the studio starts spaces; nobody else can open or change them ---------
[ "$(get "$owner" "/workspace/worlds?$W")" = 200 ] || fail 'studio opens'
grep -q 'Start spaces on this site' "$tmp/response" || fail 'studio offers a start'
grep -qi '^Cache-Control: private, no-store' "$tmp/headers" || fail 'studio is private'
[ "$(get "$other" "/workspace/worlds?$W")" = 404 ] || fail 'another account cannot open the studio'
[ "$(get '' "/workspace/worlds?$W")" = 404 ] || fail 'anonymous cannot open the studio'
[ "$(post "$owner" "operation=create&$W&return=studio")" = 303 ] || fail 'create'
grep -qi '^Location: /workspace/worlds?site=profile-avery-work&world=1' "$tmp/headers" || fail 'create returns to studio'
[ "$(sqlite3 "$BONFYRE_CMS_DB" "SELECT status||'|'||subject||'|'||sources FROM world WHERE id=1")" = 'draft|user:1|none' ] || fail 'the World row stays a draft with no sources'
[ "$(post "$other" "operation=create&site=profile-avery-work")" = 404 ] || fail 'another account cannot start spaces here'
space(){ id=$1 title=$2 renderer=$3 medium=$4 look=$5
 [ "$(post "$owner" "operation=put&$W&world=1&space=$id&title=$title&renderer=$renderer&template=template%3Aworld-character&layout=layout%3Asequence&revision=0")" = 200 ] || fail "put $id"
 [ "$(post "$owner" "operation=style&$W&world=1&space=$id&medium=$medium&character=$look&lede=Kept+by+Avery&revision=1")" = 200 ] || fail "style $id"; }
space station 'Night+Station' 2d radio ticker
space screen 'Small+Screen' 2d film almanac
space lounge 'Listening+Lounge' 3d mixed zine
[ "$(post "$owner" "operation=style&$W&world=1&space=station&medium=radio&character=default&lede=x&revision=2")" = 400 ] || fail 'the fallback look is not a choice'
[ "$(post "$owner" "operation=style&$W&world=1&space=station&medium=radio&character=ticker&lede=x&revision=1")" = 409 ] || fail 'stale style'
[ "$(post "$other" "operation=style&$W&world=1&space=station&medium=radio&character=zine&lede=x&revision=2")" = 404 ] || fail 'another account cannot restyle'
item(){ sp=$1 pos=$2 ref=$3 role=$4 rev=$5 note=${6:-}
 code=$(post "$owner" "operation=item&$W&world=1&space=$sp&position=$pos&ref=$(printf '%s' "$ref" | sed 's|/|%2F|g;s|:|%3A|g')&role=$role&note=$note&revision=$rev&return=studio")
 [ "$code" = 303 ] || fail "item $sp $pos ($code)"; }
item station 1 "$song" track 2 'Opens+every+show'
item station 2 "$held" feature 3
item station 3 space:lounge doorway 4
item screen 1 "$film" screening 2 'Saturday+matinee'
item screen 2 "$art" still 3
item lounge 1 "$essay" essay 2
item lounge 2 space:station doorway 3
item lounge 3 space:screen doorway 4
[ "$(post "$owner" "operation=item&$W&world=1&space=station&position=4&ref=https%3A%2F%2Fexample.test%2Fx&role=link&revision=5")" = 409 ] || fail 'a URL is not a reference'
[ "$(get "$owner" "/workspace/world?operation=items&$W&world=1&space=station")" = 200 ] || fail 'items'
[ "$(jq -r '[.items[].role]|join(",")' "$tmp/response")" = 'track,feature,doorway' ] || fail 'items in order'
[ "$(jq -r '.medium+"/"+.character+"/"+.release' "$tmp/response")" = 'radio/ticker/' ] || fail 'detail before release'

# Native World picker: one account, retained identity, optimistic draft revision, no writes on browse.
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=screen")" = 200 ] || fail "picker owner studio opens"
grep -q 'data-world-source-picker' "$tmp/response" || fail "source picker exists in native studio"
grep -q 'Avery saved artwork' "$tmp/response" || fail "picker reads saved references from self account"
if grep -q 'Other private selection' "$tmp/response"; then fail "picker leaks another account saved items"; fi
grep -Fq "name=\"ref\" value=\"$art\"" "$tmp/response" || fail "saved item has exact source ref for revision-controlled draft"
[ "$(get "$other" "/workspace/worlds?$W&world=1&space=screen&source=fixture-films")" = 404 ] || fail "other actor cannot open source picker"
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=screen&source=fixture-films")" = 200 ] || fail "source picker accepts observed film source"
grep -q 'data-world-source-picks' "$tmp/response" || fail "source picker shows actual retained PageComposition records"
grep -Fq "name=\"ref\" value=\"$film\"" "$tmp/response" || fail "film reference is offered to World draft"
grep -q 'name="role" value="screening"' "$tmp/response" || fail "picker carries film kind into role"
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=screen&q=absent-publisher")" = 200 ] || fail "source search is read-only"
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=screen&source=unobserved")" = 200 ] || fail "unknown source is held safely"
grep -q 'not currently available for selection' "$tmp/response" || fail "unobserved source cannot supply inferred items"
[ "$(sqlite3 "$BONFYRE_CMS_DB" "SELECT revision FROM account_world_space WHERE world_id=1 AND space_id='screen'")" = 4 ] || fail "picker browse changed the draft"
# --- nothing is public before release --------------------------------------
[ "$(public /)" = 200 ] || fail 'profile before spaces release'
[ "$(curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -H 'Accept: text/html' -H 'Host: aurekai.work' -H "Authorization: Bearer $owner" "$base/profile?$W")" = 200 ] || fail 'owner profile'
grep -q 'href="/workspace/worlds?site=profile-avery-work"' "$tmp/response" || fail 'profile opens the exact site World studio'
[ "$(public /worlds/station)" = 404 ] || fail 'unreleased space is not public'
[ "$(public /worlds)" = 404 ] || fail 'empty shelf is not public'
catalogue /watch
if grep -q 'href="https://avery.aurekai.work/worlds/screen"' "$tmp/catalogue.html"; then fail 'unreleased cinema was discovered'; fi
catalogue /listen
if grep -q 'href="https://avery.aurekai.work/worlds/station"' "$tmp/catalogue.html"; then fail 'unreleased station was discovered'; fi
for sp in station:5 screen:4 lounge:5; do
 [ "$(post "$owner" "operation=release&$W&world=1&space=${sp%%:*}&revision=${sp##*:}&return=studio")" = 303 ] || fail "release $sp"
done

[ "$(public /)" = 200 ] || fail 'published profile'
grep -q 'data-profile-worlds' "$tmp/page" || fail 'profile reveals its released spaces'
grep -q 'href="https://avery.aurekai.work/worlds/station"' "$tmp/page" || fail 'profile links to its real station'
grep -q 'Night Station' "$tmp/page" || fail 'profile shows the actual released title'
grep -q 'href="https://avery.aurekai.work/worlds/lounge"' "$tmp/page" || fail 'profile links to its real lounge'
[ "$(public / avery.aurekai.life)" = 200 ] || fail 'second profile site'
if grep -q 'data-profile-worlds' "$tmp/page"; then fail 'another site cannot inherit the first site World shelf'; fi
# Public cinema and radio must surface once through the existing LIVE catalogue.
catalogue /watch
grep -q 'data-media-space-catalogue' "$tmp/catalogue.html" || fail 'released cinema is not in Watch'
grep -q 'href="https://avery.aurekai.work/worlds/screen"' "$tmp/catalogue.html" || fail 'watch misses canonical cinema URL'
if grep -q 'href="https://avery.aurekai.work/worlds/station"' "$tmp/catalogue.html"; then fail 'radio was misfiled as film'; fi
catalogue /listen
grep -q 'href="https://avery.aurekai.work/worlds/station"' "$tmp/catalogue.html" || fail 'released station is not in Listen'
if grep -q 'href="https://avery.aurekai.work/worlds/screen"' "$tmp/catalogue.html"; then fail 'film was misfiled as audio'; fi
catalogue /whats-on
if grep -q 'data-media-space-catalogue' "$tmp/catalogue.html"; then fail 'unannounced screenings became events'; fi
catalogue '/watch?spaces-page=1'
if grep -q 'href="https://avery.aurekai.work/worlds/screen"' "$tmp/catalogue.html"; then fail 'paginated cinema repeats page zero'; fi
# --- the public space, in its own look, on the member host only ------------
[ "$(public /worlds/station)" = 200 ] || fail 'station is public'
grep -qi '^Cache-Control: public, no-cache' "$tmp/headers" || fail 'public page revalidates'
grep -q 'data-world-look="ticker"' "$tmp/page" || fail 'station wears its look'
if grep -q 'data-character=' "$tmp/page"; then fail 'a member space does not take the estate page dress'; fi
grep -q 'href="/assets/world.css' "$tmp/page" || fail 'world.css dresses it'
grep -q '<audio controls preload="none" src="https://example.test/dawn.mp3">' "$tmp/page" || fail 'CC BY audio plays without autoplay'
grep -q 'rel="license noopener" href="https://creativecommons.org/licenses/by/4.0/">CC BY 4.0</a>' "$tmp/page" || fail 'terms and deed are stated'
grep -q 'Opens every show' "$tmp/page" || fail 'owner note'
if grep -q 'held.webm"' "$tmp/page" && grep -q '<video[^>]*held.webm' "$tmp/page"; then fail 'reserved film must not play'; fi
grep -q "data-world-piece=\"$held\" data-shown=\"link\"" "$tmp/page" || fail 'reserved film is linked instead'
grep -q 'href="/worlds/lounge">Listening Lounge</a>' "$tmp/page" || fail 'portal to lounge'
grep -q "href=\"/o/$song\"" "$tmp/page" || fail 'deep record link'
if grep -q 'autoplay' "$tmp/page"; then fail 'nothing autoplays'; fi
[ "$(public /worlds/screen)" = 200 ] || fail 'screen is public'
grep -q 'data-world-look="almanac"' "$tmp/page" || fail 'screen wears a different look'
grep -q '<video controls preload="none" playsinline src="https://commons.wikimedia.org/wiki/Special:FilePath/Night%20of%20the%20Living%20Dead%20%281968%29.webm">' "$tmp/page" || fail 'public-domain film plays on demand'
grep -q 'src="https://openaccess-cdn.clevelandart.org/1922.1133/1922.1133_web.jpg"' "$tmp/page" || fail 'CC0 still shows'
[ "$(public /worlds/lounge)" = 200 ] || fail 'lounge is public'
grep -q 'data-renderer="3d"' "$tmp/page" && grep -q 'data-world-room' "$tmp/page" || fail 'published room has the native 3D scene'
grep -q 'data-room-items' "$tmp/page" && grep -q 'data-room-jump' "$tmp/page" || fail 'room has accessible selection and readable item navigation'
grep -q 'id="world-room-readable"' "$tmp/page" || fail '3D room retains the exact published page fallback'
if grep -q 'data-world-flat\|Rooms are not open yet' "$tmp/page"; then fail 'spatial rooms still use the placeholder'; fi
grep -q 'The river at night' "$tmp/page" && grep -q 'href="https://example.test/river-essay">Where it is published' "$tmp/page" || fail 'essay links to its source'
[ "$(public /worlds)" = 200 ] || fail 'shelf'
[ "$(grep -o 'class="cover c-[a-z]*" href="/worlds/[a-z]*"' "$tmp/page" | tr '\n' ' ')" = 'class="cover c-zine" href="/worlds/lounge" class="cover c-almanac" href="/worlds/screen" class="cover c-ticker" href="/worlds/station" ' ] || fail 'each cover in its own look'
[ "$(public /worlds/station avery.aurekai.life)" = 404 ] || fail 'another site of the same person does not carry it'
[ "$(public /worlds/station aurekai.work)" != 200 ] || fail 'the territory apex does not carry it'
[ "$(public /worlds/nope)" = 404 ] || fail 'unknown space'

# --- draft edits stay private until the next release ------------------------
item station 1 "$song" track 5 'Changed+note'
public /worlds/station >/dev/null; grep -q 'Opens every show' "$tmp/page" && ! grep -q 'Changed note' "$tmp/page" || fail 'draft edit leaked'
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=station")" = 200 ] || fail 'studio space'
grep -q 'data-world-state="changed"' "$tmp/response" || fail 'studio shows unpublished changes'
grep -q 'Publish changes' "$tmp/response" || fail 'studio offers publishing changes'
[ "$(post "$owner" "operation=release&$W&world=1&space=station&revision=6")" = 200 ] || fail 'publish changes'
public /worlds/station >/dev/null; grep -q 'Changed note' "$tmp/page" || fail 'released edit'

# --- the source decides: rights and withdrawal are re-read on every request -
retain fixture-sound "$(printf '%s' "$sound" | sed 's/CC BY 4.0/All rights reserved/')"
public /worlds/station >/dev/null; if grep -q '<audio' "$tmp/page"; then fail 'audio must stop when terms close'; fi
retain fixture-sound '{"rows":[]}'
public /worlds/station >/dev/null; grep -q 'no longer available from where it was published' "$tmp/page" || fail 'a removed piece leaves the page'
retain fixture-sound "$sound"

# --- origin and take-down ----------------------------------------------------
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://attacker.test' -H "Authorization: Bearer $owner" -H 'Content-Type: application/x-www-form-urlencoded' --data "operation=withdraw&$W&world=1&space=station" "$base/workspace/world")" = 403 ] || fail 'foreign origin'
[ "$(post "$other" "operation=withdraw&$W&world=1&space=station")" = 404 ] || fail 'another account cannot take it down'
[ "$(post "$owner" "operation=withdraw&$W&world=1&space=station")" = 200 ] || fail 'withdraw'
[ "$(public /worlds/station)" = 410 ] || fail 'a withdrawn space says so'
public / >/dev/null
if grep -q 'href="https://avery.aurekai.work/worlds/station"' "$tmp/page"; then fail 'withdrawn space remains on profile'; fi
public /worlds >/dev/null; if grep -q '/worlds/station' "$tmp/page"; then fail 'shelf drops a withdrawn space'; fi
catalogue /listen
if grep -q 'href="https://avery.aurekai.work/worlds/station"' "$tmp/catalogue.html"; then fail 'withdrawn station remains in Listen'; fi
public /worlds/lounge >/dev/null; if grep -q 'href="/worlds/station"' "$tmp/page"; then fail 'portals drop a withdrawn space'; fi
[ "$(post "$owner" "operation=withdraw&$W&world=1&space=station")" = 409 ] || fail 'second withdraw'
# Ending the World row takes every space off the address.
sqlite3 "$BONFYRE_CMS_DB" "UPDATE world SET status='ended' WHERE id=1"
[ "$(public /worlds/screen)" = 404 ] || fail 'ended World'
catalogue /watch
if grep -q 'href="https://avery.aurekai.work/worlds/screen"' "$tmp/catalogue.html"; then fail 'ended World remains in Watch'; fi
public / >/dev/null
if grep -q 'data-profile-worlds' "$tmp/page"; then fail 'ended World remains on profile'; fi
printf 'account spaces: ok\n'
