#!/bin/sh
# Cinema on account spaces, end to end over native HTTP: an owner programmes
# screenings, opens a film club and keeps a watch diary on a space; only
# announced screenings of a public space are public; a screening plays only
# when its own source states public domain or CC0; club talk is for members,
# the host moderates, a removed member stays out; private diary notes never
# leave the studio; lists page; withdrawal takes it all off the address.
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/world-cinema.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -n "${KEEP_WORLD_CINEMA:-}" ] || rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ printf 'account cinema: FAIL %s\n' "$1" >&2; exit 1; }
mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world site_presentation site_page internet_source; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"; done
export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS='' BONFYRE_FOREIGN_ROOT="$tmp/foreign"
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns "${3:-root}" --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null; }
key(){ printf '%s\n#%s' "$1" "$2" | shasum -a 256 | cut -c1-24; }
retain(){ printf '%s' "$2" >"$tmp/foreign/bodies/$1.body"; printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/$1.body" >"$tmp/foreign/receipts/$1.json"; }

# --- three films as their source states them: public domain, CC BY, reserved
films='{"rows":[
{"id":"Q623051","title":"Night of the Living Dead","maker":"George A. Romero","about":"1968 American independent horror film.","video":"https://commons.wikimedia.org/wiki/Special:FilePath/Night%20of%20the%20Living%20Dead%20%281968%29.webm","page":"https://en.wikipedia.org/wiki/Night_of_the_Living_Dead","terms":"Public domain"},
{"id":"Q36812","title":"Big Buck Bunny","maker":"Blender Foundation","about":"2008 open animated short.","video":"https://commons.wikimedia.org/wiki/Special:FilePath/Big_Buck_Bunny_4K.webm","page":"https://peach.blender.org/","terms":"CC BY 3.0","deed":"https://creativecommons.org/licenses/by/3.0/"},
{"id":"Q999","title":"A held film","maker":"Someone","about":"Rights are reserved by its maker.","video":"https://example.test/held.webm","page":"https://example.test/held","terms":"All rights reserved"}]}'
retain fixture-films "$films"
entry internet_source '{"family":"films","source":"fixture-films","label":"film archive","says":"films","url":"https://example.test/films","twin":"fixture-films","state":"observed","extract":"rows","title_field":"title","link_field":"page","maker_field":"maker","summary_field":"about","rights_field":"terms","rights_link_field":"deed","media_field":"video","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"video"}'
pd="fixture-films/$(key fixture-films Q623051)"
by="fixture-films/$(key fixture-films Q36812)"
held="fixture-films/$(key fixture-films Q999)"
enc(){ printf '%s' "$1" | sed 's|/|%2F|g;s|:|%3A|g'; }

# --- accounts and the owner's profile site ---------------------------------
for who in owner:Avery other:Robin third:Sam; do
 "$auth" --db "$tmp/auth.db" signup --email "${who%%:*}@example.test" --name "${who##*:}" --password 'fixture-ownership-pass' >/dev/null
done
login(){ "$auth" --db "$tmp/auth.db" login --email "$1@example.test" --password 'fixture-ownership-pass' | jq -r .session.token; }
owner=$(login owner); other=$(login other); third=$(login third)
"$auth" --db "$tmp/auth.db" actor-bind --user-id 1 --actor person:avery >/dev/null
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
send(){ token=$1; path=$2; data=$3; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://aurekai.work' -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' --data "$data" "$base$path"; }
post(){ send "$1" /workspace/world "$2"; }
film(){ send "$1" /workspace/world/film "$2"; }
get(){ curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -H 'Host: aurekai.work' ${1:+-H "Authorization: Bearer $1"} "$base$2"; }
public(){ curl -sS -o "$tmp/page" -D "$tmp/headers" -w '%{http_code}' -H "Host: avery.aurekai.work" "$base$1"; }
catalogue(){ curl -fsS -H "Host: aurekai.live" "$base$1" >"$tmp/catalogue.html"; }
W='site=profile-avery-work'; S="$W&world=1&space=cinema"

# --- a released film space -------------------------------------------------
[ "$(post "$owner" "operation=create&$W")" = 200 ] || fail 'create'
[ "$(post "$owner" "operation=put&$S&title=Saturday+Matinee&renderer=2d&template=template%3Aworld-character&layout=layout%3Asequence&revision=0")" = 200 ] || fail 'put'
[ "$(post "$owner" "operation=style&$S&medium=film&character=almanac&lede=Free+films+on+Saturdays&revision=1")" = 200 ] || fail 'style'
rev=2; for ref in "$pd" "$by" "$held"; do pos=$((rev-1))
 [ "$(post "$owner" "operation=item&$S&position=$pos&ref=$(enc "$ref")&role=feature&revision=$rev")" = 200 ] || fail "item $pos"; rev=$((rev+1)); done
[ "$(post "$owner" "operation=release&$S&revision=$rev")" = 200 ] || fail 'release'

catalogue /watch
grep -q 'href="https://avery.aurekai.work/worlds/cinema"' "$tmp/catalogue.html" || fail 'Watch does not discover the released cinema'
catalogue /whats-on
if grep -q 'data-media-space-catalogue' "$tmp/catalogue.html"; then fail 'draft screenings leaked into What’s on'; fi
# --- programme: drafts private, announced public, public domain plays ------
screen(){ film "$owner" "operation=screening&$S&screening=$1&starts=$2&venue=$3&ref=$(enc "$4")&note=${5:-}"; }
# Screenings are dated from today so the programme never ages into the past:
# the matinee is the Saturday at least a week ahead, the rest follow weekly.
day(){ date -u -v+"$1"d "+$2" 2>/dev/null || date -u -d "+$1 day" "+$2"; }
sat=$(( (6 - $(date -u +%u) + 7) % 7 + 7 ))
D0=$(day $sat %Y-%m-%d); D1=$(day $((sat+7)) %Y-%m-%d); D2=$(day $((sat+14)) %Y-%m-%d); D4=$(day $((sat+28)) %Y-%m-%d); D5=$(day $((sat+35)) %Y-%m-%d)
D0H="Sat $((10#$(day $sat %d))) $(day $sat %b) $(day $sat %Y), 14:00"
[ "$(screen matinee ${D0}T14%3A00 Main+room "$pd" 'Bring+a+friend')" = 200 ] || fail 'screening'
[ "$(screen shorts ${D1}T19%3A30 Roof "$by")" = 200 ] || fail 'cc by screening'
[ "$(screen held ${D2}T19%3A30 Hall "$held")" = 200 ] || fail 'reserved screening'
[ "$(screen secret ${D4}T19%3A30 Hall "$pd")" = 200 ] || fail 'draft screening'
[ "$(screen rained ${D5}T19%3A30 Yard "$pd")" = 200 ] || fail 'to cancel'
[ "$(screen bad 2026-02-30T14%3A00 Main+room "$pd")" = 409 ] || fail 'impossible date refused'
[ "$(film "$owner" "operation=screening&$S&screening=u&starts=${D0}T14%3A00&venue=x&ref=https%3A%2F%2Fexample.test%2Fx.webm")" = 409 ] || fail 'a URL is not a film'
[ "$(film "$other" "operation=screening&$S&screening=x&starts=${D0}T14%3A00&venue=x&ref=$(enc "$pd")")" = 404 ] || fail 'another account cannot programme'
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://attacker.test' -H "Authorization: Bearer $owner" -H 'Content-Type: application/x-www-form-urlencoded' --data "operation=announce&$S&screening=matinee" "$base/workspace/world/film")" = 403 ] || fail 'foreign origin'
public /worlds/cinema >/dev/null; if grep -q 'data-world-programme' "$tmp/page"; then fail 'nothing public before announcing'; fi
for s in matinee shorts held rained; do [ "$(film "$owner" "operation=announce&$S&screening=$s")" = 200 ] || fail "announce $s"; done
[ "$(film "$owner" "operation=announce&$S&screening=matinee")" = 409 ] || fail 'second announce'
[ "$(film "$owner" "operation=cancel&$S&screening=rained&return=studio")" = 303 ] || fail 'cancel announced'
grep -qi '^Location: /workspace/worlds?site=profile-avery-work&world=1&space=cinema' "$tmp/headers" || fail 'cancel returns to studio'
[ "$(get "$owner" "/workspace/world/film?operation=programme&$S")" = 200 ] || fail 'programme read'
[ "$(jq -r '[.screenings[]|.screening+":"+.state]|join(",")' "$tmp/response")" = 'matinee:announced,shorts:announced,held:announced,secret:draft,rained:cancelled' ] || fail 'programme in date order with states'
[ "$(get "$other" "/workspace/world/film?operation=programme&$S")" = 404 ] || fail 'programme is the owner'"'"'s'
catalogue /whats-on
grep -q 'data-media-space-catalogue' "$tmp/catalogue.html" || fail 'announced screening not in What’s on'
grep -q 'href="https://avery.aurekai.work/worlds/cinema#screening-matinee"' "$tmp/catalogue.html" || fail 'announced screening lacks exact venue anchor'
grep -q 'href="https://avery.aurekai.work/worlds/cinema#screening-shorts"' "$tmp/catalogue.html" || fail 'second scheduled film missing'
if grep -q '#screening-secret\|#screening-rained' "$tmp/catalogue.html"; then fail 'cancelled or draft screening leaked'; fi
grep -q 'data-media-space-search' "$tmp/catalogue.html" || fail 'screening search missing'
grep -q "data-media-source href=\"/o/$pd\"" "$tmp/catalogue.html" || fail 'What’s on does not open the retained film item'
if grep -q "data-media-source href=\"/o/$held\".*autoplay" "$tmp/catalogue.html"; then fail 'catalogue invented a playable film'; fi
grep -q 'data-media-space-filter="weekend"' "$tmp/catalogue.html" || fail 'screening weekend control missing'
[ "$(public /worlds/cinema)" = 200 ] || fail 'public space'
grep -q 'data-world-programme' "$tmp/page" || fail 'programme shows'
grep -q '<h3 class="aw-month">November 2026</h3>' "$tmp/page" && grep -q '<h3 class="aw-month">December 2026</h3>' "$tmp/page" || fail 'grouped by month'
grep -q "<time datetime=\"${D0}T14:00\">${D0H}</time> &middot; Main room" "$tmp/page" || fail 'weekday, date, time and place'
grep -q 'data-screening="matinee" data-state="announced" data-shown="media"' "$tmp/page" || fail 'public-domain screening plays'
grep -q '<details class="aw-watch"><summary>Watch it here</summary><video controls preload="none" playsinline src="https://commons.wikimedia.org/wiki/Special:FilePath/Night%20of%20the%20Living%20Dead%20%281968%29.webm">' "$tmp/page" || fail 'player waits behind a disclosure'
grep -q 'data-screening="shorts" data-state="announced" data-shown="link"' "$tmp/page" || fail 'a CC BY film is linked, not screened'
grep -q 'creativecommons.org/licenses/by/3.0/">CC BY 3.0</a> &middot; not played here' "$tmp/page" || fail 'its terms are stated'
grep -q 'data-screening="held" data-state="announced" data-shown="link"' "$tmp/page" || fail 'a reserved film is linked'
if grep -q '<video[^>]*held.webm' "$tmp/page"; then fail 'reserved film must not play'; fi
if grep -q 'data-screening="secret"' "$tmp/page"; then fail 'a draft screening leaked'; fi
grep -q 'data-screening="rained" data-state="cancelled" data-shown="link"' "$tmp/page" && grep -q 'Yard &middot; <strong>Cancelled</strong>' "$tmp/page" || fail 'a cancelled screening says so'
grep -q "<video controls preload=\"none\" playsinline src=\"https://commons.wikimedia.org/wiki/Special:FilePath/Big_Buck_Bunny_4K.webm\">" "$tmp/page" || fail 'the CC BY film still plays as a piece of the space'
if grep -q 'autoplay' "$tmp/page"; then fail 'nothing autoplays'; fi
[ "$(get "$owner" "/workspace/worlds?$S")" = 200 ] || fail 'studio'
grep -q 'data-world-cine' "$tmp/response" && grep -q 'data-cine-screening="secret" data-state="draft"' "$tmp/response" || fail 'studio programme'
grep -q 'Night of the Living Dead &middot; plays here\|plays here' "$tmp/response" || fail 'studio says what plays'

# --- the source decides: closed terms stop the screening -------------------
retain fixture-films "$(printf '%s' "$films" | sed 's/"terms":"Public domain"/"terms":"All rights reserved"/')"
public /worlds/cinema >/dev/null; grep -q 'data-screening="matinee" data-state="announced" data-shown="link"' "$tmp/page" || fail 'closed terms stop the screening'
retain fixture-films "$films"

# --- diary: private stays in the studio -------------------------------------
note(){ film "$owner" "operation=note&$W&world=1&ref=$(enc "$1")&watched=$2&rating=$3&visibility=$4&body=$5${6:+&rewatch=1}"; }
[ "$(note "$pd" 2026-10-30 9 public 'Better+on+the+third+watch.' again)" = 200 ] || fail 'public note'
[ "$(note "$pd" 2026-10-02 6 private 'Only+for+me.')" = 200 ] || fail 'private note'
[ "$(note "$held" 2026-10-03 8 public 'Seen+at+a+festival.')" = 200 ] || fail 'note on a linked film'
[ "$(note "$pd" 2026-10-02 11 private 'x')" = 400 ] || fail 'rating out of range'
[ "$(film "$other" "operation=note&$W&world=1&ref=$(enc "$pd")&watched=2026-10-02&visibility=public&body=mine")" = 404 ] || fail 'another account cannot write the diary'
public /worlds/cinema >/dev/null
grep -q 'data-world-diary' "$tmp/page" && grep -q 'Better on the third watch.' "$tmp/page" || fail 'public note shows'
grep -q 'watched again &middot; <span class="aw-stars" role="img" aria-label="Rated 4 and a half out of 5">' "$tmp/page" || fail 'rating is spoken'
grep -q 'Seen at a festival.' "$tmp/page" || fail 'public note beside a released film'
if grep -q 'Only for me' "$tmp/page"; then fail 'a private note leaked'; fi
for n in $(seq 1 24); do [ "$(note "$by" 2026-09-$(printf '%02d' $(( (n % 28) + 1 ))) 0 private 'Filler')" = 200 ] || fail "filler note $n"; done
[ "$(get "$owner" "/workspace/world/film?operation=notes&$W&world=1")" = 200 ] || fail 'notes'
[ "$(jq '.notes|length' "$tmp/response")" = 20 ] && [ "$(jq -r '.notes[0].watched' "$tmp/response")" = 2026-10-30 ] || fail 'first page of the diary, newest first'
nb=$(jq -r .next.before "$tmp/response"); nd=$(jq -r .next.before_day "$tmp/response")
[ "$(get "$owner" "/workspace/world/film?operation=notes&$W&world=1&before=$nb&before_day=$nd")" = 200 ] || fail 'second page'
[ "$(jq '.notes|length' "$tmp/response")" = 7 ] && [ "$(jq 'has("next")' "$tmp/response")" = false ] || fail 'the diary ends'

# --- club: members only, host moderates ------------------------------------
[ "$(get "$other" "/workspace/worlds/club?$W&space=cinema")" = 404 ] || fail 'no club before opening'
public /worlds/cinema >/dev/null; if grep -q 'data-world-club' "$tmp/page"; then fail 'no club door before opening'; fi
[ "$(film "$other" "operation=club&$S&state=open")" = 404 ] || fail 'only the owner opens a club'
[ "$(film "$owner" "operation=club&$S&state=open&return=studio")" = 303 ] || fail 'open club'
public /worlds/cinema >/dev/null
grep -q 'href="https://aurekai.work/workspace/worlds/club?site=profile-avery-work&amp;space=cinema">Join or open the club</a>' "$tmp/page" || fail 'club door'
[ "$(get '' "/workspace/worlds/club?$W&space=cinema")" = 404 ] || fail 'anonymous cannot open the club'
[ "$(get "$other" "/workspace/worlds/club?$W&space=cinema")" = 200 ] || fail 'a signed-in guest sees the door'
grep -qi '^Cache-Control: private, no-store' "$tmp/headers" || fail 'the club room is private'
grep -q 'data-club-role="guest"' "$tmp/response" && grep -q 'Join the club' "$tmp/response" || fail 'join offered'
if grep -q 'aw-talk' "$tmp/response"; then fail 'a guest cannot read the talk'; fi
[ "$(get "$other" "/workspace/world/film?operation=posts&$W&space=cinema")" = 404 ] || fail 'a guest cannot read posts'
[ "$(film "$other" "operation=say&$W&space=cinema&body=hi")" = 409 ] || fail 'a guest cannot post'
[ "$(film "$other" "operation=join&$W&space=cinema&return=club")" = 303 ] || fail 'join'
grep -qi '^Location: /workspace/worlds/club?site=profile-avery-work&space=cinema' "$tmp/headers" || fail 'join returns to the club'
[ "$(film "$third" "operation=join&$W&space=cinema")" = 200 ] || fail 'second member'
[ "$(film "$other" "operation=join&$W&space=cinema")" = 409 ] || fail 'joined once'
[ "$(film "$other" "operation=pick&$W&space=cinema&ref=$(enc "$pd")&note=For+Halloween")" = 200 ] || fail 'pick'
[ "$(film "$third" "operation=pick&$W&space=cinema&ref=$(enc "$by")")" = 200 ] || fail 'second pick'
[ "$(film "$third" "operation=pick&$W&space=cinema&ref=$(enc "$pd")")" = 409 ] || fail 'a film is proposed once'
[ "$(film "$other" "operation=vote&$W&space=cinema&item=1")" = 200 ] || fail 'vote'
[ "$(film "$other" "operation=vote&$W&space=cinema&item=1")" = 409 ] || fail 'one vote each'
[ "$(film "$third" "operation=vote&$W&space=cinema&item=1")" = 200 ] || fail 'second vote'
[ "$(film "$owner" "operation=vote&$W&space=cinema&item=2")" = 200 ] || fail 'host votes'
[ "$(get "$other" "/workspace/world/film?operation=picks&$W&space=cinema")" = 200 ] || fail 'picks'
[ "$(jq -r '[.picks[]|(.pick|tostring)+":"+(.votes|tostring)+":"+(.voted|tostring)]|join(",")' "$tmp/response")" = '1:2:1,2:1:0' ] || fail 'picks ranked by votes'
[ "$(film "$other" "operation=say&$W&space=cinema&ref=$(enc "$pd")&body=The+barn+scene+still+works.")" = 200 ] || fail 'say'
[ "$(film "$third" "operation=say&$W&space=cinema&body=Buy+cheap+tickets+here")" = 200 ] || fail 'spam'
[ "$(film "$other" "operation=hide&$S&kind=post&item=2")" = 404 ] || fail 'a member cannot moderate'
[ "$(film "$owner" "operation=hide&$S&kind=post&item=2&return=club")" = 303 ] || fail 'host hides'
[ "$(get "$other" "/workspace/worlds/club?$W&space=cinema")" = 200 ] || fail 'member room'
grep -q 'data-club-role="member"' "$tmp/response" && grep -q 'The barn scene still works.' "$tmp/response" || fail 'member reads talk'
grep -q 'Proposed by you &middot; 2 votes' "$tmp/response" && grep -q 'You voted' "$tmp/response" || fail 'member sees picks'
grep -q '<p class="by">Sam</p>\|Proposed by Sam' "$tmp/response" || fail 'members are named as their accounts state'
if grep -q 'Buy cheap tickets' "$tmp/response"; then fail 'a hidden post reached a member'; fi
if grep -q 'user:[0-9]' "$tmp/response"; then fail 'no account numbers in the room'; fi
[ "$(get "$owner" "/workspace/worlds/club?$W&space=cinema")" = 200 ] || fail 'host room'
grep -q 'data-club-role="host"' "$tmp/response" && grep -q 'hidden from members' "$tmp/response" && grep -q 'Take out of the club' "$tmp/response" || fail 'host sees moderation'
[ "$(film "$third" "operation=retract&$W&space=cinema&item=1")" = 409 ] || fail 'only the author withdraws'
[ "$(film "$other" "operation=retract&$W&space=cinema&item=1")" = 200 ] || fail 'retract'
[ "$(get "$third" "/workspace/world/film?operation=posts&$W&space=cinema")" = 200 ] || fail 'posts'
[ "$(jq -r '.posts[0].state+"|"+.posts[0].body' "$tmp/response")" = 'retracted|' ] || fail 'a withdrawn post keeps no words'
for n in $(seq 1 24); do [ "$(film "$third" "operation=say&$W&space=cinema&body=Thought+$n")" = 200 ] || fail "post $n"; done
[ "$(get "$third" "/workspace/world/film?operation=posts&$W&space=cinema")" = 200 ] || fail 'posts page'
[ "$(jq '.posts|length' "$tmp/response")" = 20 ] && [ "$(jq -r '.posts[0].body' "$tmp/response")" = 'Thought 24' ] || fail 'newest twenty'
pb=$(jq -r .next.before "$tmp/response")
[ "$(get "$third" "/workspace/world/film?operation=posts&$W&space=cinema&before=$pb")" = 200 ] || fail 'older posts'
[ "$(jq '.posts|length' "$tmp/response")" = 5 ] || fail 'older page excludes the hidden post'
[ "$(get "$third" "/workspace/worlds/club?$W&space=cinema")" = 200 ] && grep -q "rel=\"next\" href=\"/workspace/worlds/club?site=profile-avery-work&amp;space=cinema&amp;before=$pb\">Older posts" "$tmp/response" || fail 'room pages'
[ "$(film "$owner" "operation=remove&$S&member=user%3A3")" = 200 ] || fail 'remove member'
[ "$(get "$third" "/workspace/worlds/club?$W&space=cinema")" = 200 ] && grep -q 'data-club-role="removed"' "$tmp/response" || fail 'removed member is told'
[ "$(film "$third" "operation=join&$W&space=cinema")" = 409 ] || fail 'a removed member stays out'
[ "$(get "$third" "/workspace/world/film?operation=posts&$W&space=cinema")" = 404 ] || fail 'a removed member cannot read'
[ "$(film "$other" "operation=leave&$W&space=cinema")" = 200 ] || fail 'leave'
[ "$(film "$other" "operation=join&$W&space=cinema")" = 200 ] || fail 'leaving is not removal'

# --- withdrawal takes the programme, the diary and the club away ------------
[ "$(post "$owner" "operation=withdraw&$S")" = 200 ] || fail 'withdraw'
catalogue /whats-on
if grep -q 'href="https://avery.aurekai.work/worlds/cinema#screening-matinee"' "$tmp/catalogue.html"; then fail 'withdrawn screening survives public catalogue'; fi
catalogue /watch
if grep -q 'href="https://avery.aurekai.work/worlds/cinema"' "$tmp/catalogue.html"; then fail 'withdrawn cinema survives Watch'; fi
[ "$(public /worlds/cinema)" = 410 ] || fail 'withdrawn space'
[ "$(get "$other" "/workspace/worlds/club?$W&space=cinema")" = 404 ] || fail 'club closes with the space'
[ "$(film "$other" "operation=say&$W&space=cinema&body=still+here")" = 409 ] || fail 'no posting after withdrawal'
printf 'account cinema: ok\n'
