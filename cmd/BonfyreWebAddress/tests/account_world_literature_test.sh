#!/bin/sh
# Writing in account spaces, end to end over native HTTP: an owner keeps their
# own poem with indents and stanza breaks, a public-domain poem with the page
# it came from, a translation, a serial with a chapter still to come and a
# story readers steer by two choices; glosses become numbered notes; an edit
# keeps the text it replaced for comparison; readers see only the released
# version, line for line, and hear a recitation only when its own source
# states open terms; the same poem shows in a second space without a copy; a
# signed-in reader keeps their exact place, including the path taken through
# the story; the space's club becomes a reading circle; withdrawal answers 410.
set -eu
root=$(CDPATH='' cd -- "$(dirname "$0")/../../.." && pwd)
web=${BONFYRE_TEST_WEB_BINARY:-"$root/cmd/BonfyreWebAddress/bonfyre-web-address"}
cms=${BONFYRE_TEST_CMS_BINARY:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
auth=${BONFYRE_TEST_AUTH_BINARY:-"$root/cmd/BonfyreAuth/bonfyre-auth"}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/world-literature.XXXXXX")
pid=''
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -n "${KEEP_WORLD_LITERATURE:-}" ] || rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ printf 'account world literature: FAIL %s\n' "$1" >&2; exit 1; }
mkdir -p "$tmp/schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world site_presentation site_page internet_source; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$tmp/schemas/$type.json"; done
export BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$tmp/schemas" BONFYRE_MATRIX_DB="$tmp/matrix.db" BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_FACT_DELTAS='' BONFYRE_FOREIGN_ROOT="$tmp/foreign"
"$cms" schema migrate --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns "${3:-root}" --db "$BONFYRE_CMS_DB" --schemas "$BONFYRE_CMS_SCHEMAS" >/dev/null; }
key(){ printf '%s\n#%s' "$1" "$2" | shasum -a 256 | cut -c1-24; }
retain(){ printf '%s' "$2" >"$tmp/foreign/bodies/$1.body"; printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/$1.body" >"$tmp/foreign/receipts/$1.json"; }

# --- recordings of the poems read aloud, each with the terms its source states
retain fixture-voice '{"rows":[
 {"id":"r1","title":"The river, read by the author","maker":"Avery Lane","about":"A reading.","audio":"https://example.test/river.mp3","page":"https://example.test/river","terms":"CC0 1.0","deed":"https://creativecommons.org/publicdomain/zero/1.0/"},
 {"id":"r2","title":"A held reading","maker":"Someone","about":"Rights are reserved.","audio":"https://example.test/held.mp3","page":"https://example.test/held","terms":"All rights reserved"}]}'
entry internet_source '{"family":"sound","source":"fixture-voice","label":"readings","says":"recordings","url":"https://example.test/voice","twin":"fixture-voice","state":"observed","extract":"rows","title_field":"title","link_field":"page","maker_field":"maker","summary_field":"about","rights_field":"terms","rights_link_field":"deed","media_field":"audio","key_field":"id","body_regime":"REMOTE","status":"published","tier":"COLD","kind":"audio"}'
r1="fixture-voice/$(key fixture-voice r1)"; r2="fixture-voice/$(key fixture-voice r2)"

# --- accounts, address and profile site -------------------------------------
for who in owner other third; do "$auth" --db "$tmp/auth.db" signup --email "$who@example.test" --name "$(printf '%s' "$who" | sed 's/./\U&/')" --password 'fixture-ownership-pass' >/dev/null; done
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
other=$("$auth" --db "$tmp/auth.db" login --email other@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
third=$("$auth" --db "$tmp/auth.db" login --email third@example.test --password 'fixture-ownership-pass' | jq -r .session.token)
"$auth" --db "$tmp/auth.db" actor-bind --user-id 1 --actor person:avery >/dev/null
"$auth" --db "$tmp/auth.db" actor-bind --user-id 2 --actor person:other >/dev/null
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$web" subject --db "$BONFYRE_MATRIX_DB" --id person:avery --kind Person --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --evidence fixture:profile >/dev/null
"$web" institution --db "$BONFYRE_MATRIX_DB" --id inst:person-avery --kind Person --world live --evidence fixture:profile >/dev/null
"$web" domain --db "$BONFYRE_MATRIX_DB" --host avery.aurekai.work --institution inst:person-avery --territory work --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
"$web" presence --db "$BONFYRE_MATRIX_DB" --id presence:avery:work --subject person:avery --territory work --scope public --world live --repr HTML --evidence fixture:profile >/dev/null
entry site_presentation '{"site_id":"profile-avery-work","host":"avery.aurekai.work","subject_address":"person:avery","territory":"work","profile":"member-profile-v1","navigation":[],"status":"published"}'
port=${WORLD_LITERATURE_PORT:-$((20000 + ($$ + 23) % 20000))}
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --port "$port" --site-root "${WORLD_LITERATURE_SITE_ROOT:-$root/site/matrix}" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0; until curl -fsS "$base/healthz" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 100 ] || { cat "$tmp/server.log"; exit 1; }; sleep .1; done
enc(){ jq -rn --arg v "$1" '$v|@uri'; }
post(){ token=$1; path=$2; data=$3; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://aurekai.work' -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' --data "$data" "$base$path"; }
get(){ token=$1; path=$2; curl -sS -o "$tmp/response" -D "$tmp/headers" -w '%{http_code}' -H 'Host: aurekai.work' -H "Authorization: Bearer $token" "$base$path"; }
public(){ curl -sS -o "$tmp/page" -D "$tmp/headers" -w '%{http_code}' -H "Host: ${2:-avery.aurekai.work}" "$base$1"; }
W='site=profile-avery-work'
rev(){ get "$owner" "/workspace/world?operation=read&$W&world=1&space=$1" >/dev/null; jq -r .revision "$tmp/response"; }
lit(){ sp=$1; shift; post "$owner" /workspace/world-literature "$W&world=1&space=$sp&revision=$(rev "$sp")&$*"; }
must(){ code=$(lit "$@"); [ "$code" = 200 ] || fail "$* ($code: $(cat "$tmp/response"))"; }
work(){ sp=$1 id=$2 kind=$3 title=$4 rights=$5 licence=$6; shift 6; extra=${1:+&$1}; must "$sp" "operation=work&work=$id&kind=$kind&title=$(enc "$title")&author=Avery+Lane&language=en&rights=$rights&licence=$(enc "$licence")&position=1$extra"; }
passage(){ sp=$1 w=$2 p=$3 n=$4 title=$5 body=$6; shift 6; extra=${1:+&$1}; must "$sp" "operation=passage&work=$w&passage=$p&number=$n&title=$(enc "$title")&body=$(enc "$body")$extra"; }

# --- spaces from the foundation ------------------------------------------------
[ "$(post "$owner" /workspace/world "operation=create&$W")" = 200 ] || fail 'create'
for sp in journal:The+Night+Journal:poetry:journal:2d room:Reading+room:reading:almanac:3d; do
 id=${sp%%:*}; rest=${sp#*:}; title=${rest%%:*}; rest=${rest#*:}; medium=${rest%%:*}; rest=${rest#*:}; look=${rest%%:*}; shape=${rest#*:}
 [ "$(post "$owner" /workspace/world "operation=put&$W&world=1&space=$id&title=$title&renderer=$shape&template=template%3Aworld-character&layout=layout%3Asequence&revision=0")" = 200 ] || fail "put $id"
 [ "$(post "$owner" /workspace/world "operation=style&$W&world=1&space=$id&medium=$medium&character=$look&lede=Poems%2C+a+serial+and+a+story+you+steer&revision=1")" = 200 ] || fail "style $id"
done

# --- works, with refusals ------------------------------------------------------
[ "$(lit journal "operation=work&work=chariot&kind=poem&title=The+Chariot&author=Emily+Dickinson&language=en&rights=public-domain&licence=Public+domain&position=2")" = 409 ] || fail 'a public-domain text needs where it came from'
[ "$(lit journal "operation=work&work=chariot&kind=poem&title=The+Chariot&author=Emily+Dickinson&language=en&rights=public-domain&licence=CC+BY+4.0&provenance=$(enc https://en.wikisource.org/wiki/Poems_\(Dickinson\)/The_Chariot)&position=2")" = 409 ] || fail 'and says it is public domain'
[ "$(post "$other" /workspace/world-literature "$W&world=1&space=journal&revision=$(rev journal)&operation=work&work=x&kind=poem&title=X&author=X&language=en&rights=own&licence=x&position=1")" = 404 ] || fail 'another account cannot write here'
[ "$(post "$owner" /workspace/world-literature "$W&world=1&space=journal&revision=1&operation=work&work=x&kind=poem&title=X&author=X&language=en&rights=own&licence=x&position=1")" = 409 ] || fail 'stale revision'
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.work' -H 'Origin: https://attacker.test' -H "Authorization: Bearer $owner" -H 'Content-Type: application/x-www-form-urlencoded' --data "$W&world=1&space=journal&revision=$(rev journal)&operation=unwork&work=river" "$base/workspace/world-literature")" = 403 ] || fail 'foreign origin'
work journal river poem 'The River Keeps Its Counsel' own 'All rights reserved' "collection=Night+poems&recitation=$(enc "$r1")"
must journal "operation=work&work=chariot&kind=poem&title=The+Chariot&author=Emily+Dickinson&language=en&rights=public-domain&licence=$(enc 'Public domain in the United States')&provenance=$(enc 'https://en.wikisource.org/wiki/Poems_(Dickinson)/The_Chariot')&source_label=Wikisource&collection=Night+poems&position=2&recitation=$(enc "$r2")"
must journal "operation=work&work=rio&kind=poem&title=$(enc 'El río guarda su consejo')&author=Avery+Lane&translator=Avery+Lane&language=es&original=river&rights=own&licence=$(enc 'All rights reserved')&collection=Night+poems&position=3"
work journal ferry serial 'The Last Ferry' own 'CC BY 4.0' 'collection=Serials'
work journal door interactive 'Two Doors' own 'CC BY-SA 4.0' 'collection=Stories+you+steer'

poem='The river keeps its counsel,
    folding light
	into the reeds.

At the ferry
        no one _waits_.'
passage journal river text 1 '' "$poem"
chariot='Because I could not stop for Death,
He kindly stopped for me;
The carriage held but just ourselves
And Immortality.

We slowly drove, he knew no haste,
And I had put away
My labor, and my leisure too,
For his civility.

We passed the school where children played,
Their lessons scarcely done;
We passed the fields of gazing grain,
We passed the setting sun.

We paused before a house that seemed
A swelling of the ground;
The roof was scarcely visible,
The cornice but a mound.

Since then '"'"'t is centuries; but each
Feels shorter than the day
I first surmised the horses'"'"' heads
Were toward eternity.'
passage journal chariot text 1 '' "$chariot"
passage journal rio texto 1 '' 'El río guarda su consejo,
    doblando la luz
	entre los juncos.'
past=$(date -u -v-10d '+%Y-%m-%d' 2>/dev/null || date -u -d '10 days ago' '+%Y-%m-%d')
later=$(date -u -v+30d '+%Y-%m-%d' 2>/dev/null || date -u -d '30 days' '+%Y-%m-%d')
passage journal ferry one 1 'The landing' 'The ferry came at dusk, as it always had,
with its lamp swinging.

Nobody on the landing said a word.' "release_on=$past"
passage journal ferry two 2 'Crossing' 'Halfway over, the pilot cut the engine.

The river took them the rest of the way.'
passage journal ferry three 3 'The far bank' 'Not yet.' "release_on=$later"
[ "$(lit journal "operation=passage&work=ferry&passage=four&number=4&body=x&release_on=2026-02-30")" = 409 ] || fail 'impossible release date'
passage journal door hall 1 'The hall' 'Two doors stand open at the end of the hall.' 'start=1'
passage journal door garden 2 'The garden' 'Rain on the roses. You stay until it stops.'
passage journal door cellar 3 'The cellar' 'Dark stairs. <script>alert(1)</script> Somebody left a lamp.'
passage journal door attic 4 'The attic' 'Nobody can reach this yet.'
[ "$(lit journal "operation=choice&work=door&passage=hall&number=1&label=Garden&target=nowhere")" = 409 ] || fail 'a choice must lead to a passage'
[ "$(lit journal "operation=choice&work=ferry&passage=one&number=1&label=Skip&target=two")" = 409 ] || fail 'only steered stories branch'
must journal "operation=choice&work=door&passage=hall&number=1&label=$(enc 'Step out into the garden')&target=garden"
must journal "operation=choice&work=door&passage=hall&number=2&label=$(enc 'Take the cellar stairs')&target=cellar"
[ "$(lit journal "operation=unpassage&work=door&passage=garden")" = 409 ] || fail 'a passage a choice leads to stays'
must journal "operation=gloss&work=river&passage=text&unit=2&note_kind=footnote&label=$(enc 'Written on the Natchez ferry.')"
must journal "operation=gloss&work=chariot&passage=text&unit=1&note_kind=note&label=$(enc 'First published in 1890 as The Chariot.')"
[ "$(lit journal "operation=gloss&work=river&passage=text&unit=9&note_kind=note&label=x")" = 409 ] || fail 'a gloss sits on a line that exists'
# an edit keeps the text it replaced
poem2='The river keeps its counsel,
    folding the light
	into the reeds.

At the ferry
        no one _waits_.'
passage journal river text 1 '' "$(printf '%s' "$poem2" | sed 's/$/\r/')"   # browsers send CRLF; the text keeps LF
[ "$(get "$owner" "/workspace/world-literature?operation=draft&$W&world=1&space=journal")" = 200 ] || fail 'draft readback'
[ "$(jq -r '.works|length' "$tmp/response")" = 5 ] || fail 'five works'
[ "$(jq -r '.passages[]|select(.work=="river")|.body' "$tmp/response")" = "$poem2" ] || fail 'text kept exactly, CRLF folded to LF'
[ "$(jq -r '[.choices[]|.passage+">"+.target]|join(",")' "$tmp/response")" = 'hall>garden,hall>cellar' ] || fail 'declared edges'
river_key=$(jq -r '.works[]|select(.work=="river")|.key' "$tmp/response"); chariot_key=$(jq -r '.works[]|select(.work=="chariot")|.key' "$tmp/response")
[ "${#river_key}" = 24 ] || fail 'stable key'
[ "$(get "$other" "/workspace/world-literature?operation=draft&$W&world=1&space=journal")" = 404 ] || fail 'another account cannot read the draft'

# --- studio --------------------------------------------------------------------
[ "$(get "$owner" "/workspace/worlds?$W&world=1&space=journal")" = 200 ] || fail 'studio'
cp "$tmp/response" "$tmp/studio.html"
grep -q 'data-world-lit-studio' "$tmp/response" || fail 'writing desk'
grep -q 'data-lit-diff' "$tmp/response" && grep -q '<li class="del"><del>    folding light</del></li>' "$tmp/response" && grep -q '<li class="ins"><ins>    folding the light</ins></li>' "$tmp/response" || fail 'earlier version compared line by line'
grep -q 'readers cannot reach this yet' "$tmp/response" || fail 'unreachable passage flagged'
grep -q 'data-lit-structure>2 endings readers can reach' "$tmp/response" || fail 'endings counted'
grep -q 'Open a reading circle' "$tmp/response" || fail 'circle offered'

# --- nothing is public before release -----------------------------------------
[ "$(public /worlds/journal/read/river)" = 404 ] || fail 'no reader before release'
item(){ sp=$1 pos=$2 ref=$3 role=$4; [ "$(post "$owner" /workspace/world "operation=item&$W&world=1&space=$sp&position=$pos&ref=$(enc "$ref")&role=$role&revision=$(rev "$sp")")" = 200 ] || fail "item $sp $pos"; }
item journal 1 "works/$river_key" poem
item room 1 "works/$river_key" poem
for sp in journal room; do [ "$(post "$owner" /workspace/world "operation=release&$W&world=1&space=$sp&revision=$(rev $sp)")" = 200 ] || fail "release $sp"; done

# --- the space page: the poem itself and the bibliography ---------------------
[ "$(public /worlds/journal)" = 200 ] || fail 'journal page'
cp "$tmp/page" "$tmp/journal.html"
grep -q "data-world-piece=\"works/$river_key\" data-work-kind=\"poem\" data-shown=\"text\"" "$tmp/page" || fail 'the poem shown on its page'
grep -q 'data-world-works' "$tmp/page" || fail 'bibliography'
for t in 'The River Keeps Its Counsel' 'The Chariot' 'The Last Ferry' 'Two Doors'; do grep -q "\">$t</a>" "$tmp/page" || fail "listed: $t"; done
grep -q 'Spanish</span><span class="by">Avery Lane, translated by Avery Lane' "$tmp/page" || fail 'translation listed with its language'

# --- the reader: line for line ----------------------------------------------------
[ "$(public /worlds/journal/read/river)" = 200 ] || fail 'river reader'
cp "$tmp/page" "$tmp/river.html"
perl -0ne 'while(/<span class="aw-line[^"]*" id="l-\d+" data-unit="\d+">(.*?)<\/span>\n/g){my $l=$1;$l=~s/<span class="aw-ln"[^>]*>\d+<\/span>//;$l=~s/<sup.*?<\/sup>//g;$l=~s/<\/?em>/_/g;$l=~s/&#39;/\x27/g;$l=~s/&amp;/&/g;$l=~s/&lt;/</g;$l=~s/&gt;/>/g;$l=~s/&quot;/"/g;print "$l\n"}' "$tmp/page" >"$tmp/lines"
[ "$(cat "$tmp/lines")" = "$poem2" ] || { diff "$tmp/lines" - <<EOF
$poem2
EOF
fail 'the poem reads back line for line'; }
grep -q '<span class="aw-line aw-gap" id="l-4" data-unit="4"></span>' "$tmp/page" || fail 'stanza break kept'
grep -q 'no one <em>waits</em>.' "$tmp/page" || fail 'emphasis'
grep -q '<sup class="aw-fn"><a href="#n-[0-9]*" id="r-[0-9]*" aria-label="Footnote 1">1</a></sup>' "$tmp/page" && grep -q 'Line 2</span> Written on the Natchez ferry.' "$tmp/page" || fail 'footnote'
grep -q 'data-recitation="playing"><p class="aw-role">Hear it read</p><audio controls preload="none" src="https://example.test/river.mp3">' "$tmp/page" || fail 'CC0 recitation plays'
grep -q 'data-world-rights="own">Text &copy; Avery Lane. All rights reserved' "$tmp/page" || fail 'author credit and terms'
grep -q 'hreflang="es">El río guarda su consejo</a> <span>Spanish, translated by Avery Lane' "$tmp/page" || fail 'other edition'
grep -q 'href="https://aurekai.work/workspace/worlds/reading?site=profile-avery-work&amp;space=journal&amp;work=river&amp;passage=text&amp;unit=0">Keep my place' "$tmp/page" || fail 'keep my place'
if grep -q 'autoplay' "$tmp/page"; then fail 'nothing plays by itself'; fi
[ "$(public /worlds/journal/read/chariot)" = 200 ] || fail 'chariot'
cp "$tmp/page" "$tmp/chariot.html"
grep -q 'data-world-rights="public-domain">Public domain in the United States &middot; <a rel="noopener" href="https://en.wikisource.org/wiki/Poems_(Dickinson)/The_Chariot">Wikisource</a>' "$tmp/page" || fail 'public-domain provenance'
[ "$(grep -c 'class="aw-line' "$tmp/page")" = 24 ] || fail 'twenty-four lines with four stanza breaks'
grep -q 'data-recitation="linked"' "$tmp/page" || fail 'a reserved recording is linked, not played'
if grep -q '<audio[^>]*held.mp3' "$tmp/page"; then fail 'reserved recording must not play'; fi
[ "$(public /worlds/journal/read/rio)" = 200 ] && grep -q 'lang="es"' "$tmp/page" && grep -q 'the original' "$tmp/page" || fail 'translation links its original'

# --- the serial: contents, release days ---------------------------------------
[ "$(public /worlds/journal/read/ferry)" = 200 ] || fail 'contents'
cp "$tmp/page" "$tmp/ferry.html"
grep -q 'data-reader-page="contents"' "$tmp/page" && grep -q 'href="/worlds/journal/read/ferry/one">The landing' "$tmp/page" && grep -q 'href="/worlds/journal/read/ferry/two">Crossing' "$tmp/page" || fail 'two chapters out'
grep -q 'data-passage="three" data-open="0"' "$tmp/page" && grep -q 'The far bank <span class="aw-when">Arrives' "$tmp/page" || fail 'third chapter announced, not linked'
if grep -q 'read/ferry/three' "$tmp/page"; then fail 'an unreleased chapter is not linked'; fi
[ "$(public /worlds/journal/read/ferry/one)" = 200 ] && grep -q 'rel="next" href="/worlds/journal/read/ferry/two"' "$tmp/page" || fail 'chapter one turns to two'
grep -q '<p id="p-1" data-unit="1">The ferry came at dusk, as it always had, with its lamp swinging.</p>' "$tmp/page" || fail 'prose paragraphs'
[ "$(public /worlds/journal/read/ferry/two)" = 200 ] && grep -q 'data-next-release>The next chapter arrives' "$tmp/page" || fail 'next release day'
[ "$(public /worlds/journal/read/ferry/three)" = 404 ] || fail 'chapter three before its day'

# --- the story readers steer -------------------------------------------------------
[ "$(public /worlds/journal/read/door)" = 200 ] || fail 'story'
cp "$tmp/page" "$tmp/door.html"
grep -q 'data-world-choices><ol><li><a href="/worlds/journal/read/door/garden?path=hall.garden">Step out into the garden</a></li><li><a href="/worlds/journal/read/door/cellar?path=hall.cellar">Take the cellar stairs</a></li></ol>' "$tmp/page" || fail 'two choices'
[ "$(public '/worlds/journal/read/door/cellar?path=hall.cellar')" = 200 ] || fail 'cellar'
grep -q 'data-world-ending' "$tmp/page" && grep -q 'data-world-path="hall.cellar"' "$tmp/page" || fail 'an ending with its path'
grep -q 'Dark stairs. &lt;script&gt;alert(1)&lt;/script&gt; Somebody' "$tmp/page" || fail 'story text is escaped'
if grep -q '<script>alert' "$tmp/page"; then fail 'no story script runs'; fi
[ "$(public /worlds/journal/read/door/garden)" = 200 ] && grep -q 'data-world-path="hall.garden"' "$tmp/page" || fail 'a missing path is the shortest way'
[ "$(public '/worlds/journal/read/door/garden?path=attic.garden')" = 200 ] && grep -q 'data-world-path="hall.garden"' "$tmp/page" || fail 'a false path is replaced'
[ "$(public /worlds/journal/read/door/attic)" = 404 ] || fail 'an unreachable passage is not served'

# --- the same poem in a second space, without a copy ------------------------------
[ "$(public /worlds/room)" = 200 ] || fail 'room'
cp "$tmp/page" "$tmp/room.html"
grep -q 'data-world-flat' "$tmp/page" && grep -q 'data-world-verse' "$tmp/page" && grep -q 'href="/worlds/journal/read/river">Open it in the reader' "$tmp/page" || fail 'room text from the journal'
if grep -q 'data-world-works' "$tmp/page"; then fail 'the room holds no writing of its own'; fi
[ "$(sqlite3 "$BONFYRE_CMS_DB" "select count(*) from account_world_passage where space_id='room'")" = 0 ] || fail 'no copy'

# --- private draft edits after release -----------------------------------------------
passage journal river text 1 '' "$poem2
And a line nobody has seen."
public /worlds/journal/read/river >/dev/null; if grep -q 'nobody has seen' "$tmp/page"; then fail 'drafts stay private'; fi

# --- a reader keeps their place ----------------------------------------------------------
R="$W&space=journal"
[ "$(get '' "/workspace/worlds/reading?$R")" = 404 ] || fail 'anonymous readers keep places in their browser only'
[ "$(get "$third" "/workspace/worlds/reading?$R&work=door&passage=garden&path=hall.garden&unit=1")" = 200 ] || fail 'reading room'
grep -qi '^Cache-Control: private, no-store' "$tmp/headers" && grep -q 'data-reading-keep' "$tmp/response" || fail 'keep offered privately'
cp "$tmp/response" "$tmp/reading.html"
place(){ post "$third" /workspace/world-literature "$R&operation=place&$1"; }
[ "$(place 'work=door&passage=garden&unit=1&path=hall.cellar')" = 409 ] || fail 'a path must end where the reader is'
[ "$(place 'work=door&passage=attic&unit=0&path=hall.attic')" = 409 ] || fail 'no edge, no place'
[ "$(place 'work=ferry&passage=three&unit=0')" = 409 ] || fail 'not before release day'
[ "$(place 'work=river&passage=text&unit=99')" = 409 ] || fail 'past the last line'
[ "$(place 'work=door&passage=garden&unit=1&path=hall.garden&return=reading')" = 303 ] || fail 'keep the garden'
grep -qi '^Location: /workspace/worlds/reading?site=profile-avery-work&space=journal' "$tmp/headers" || fail 'back to the reading room'
[ "$(place 'work=river&passage=text&unit=5')" = 200 ] || fail 'keep a line of the poem'
[ "$(get "$third" "/workspace/world-literature?operation=places&$R")" = 200 ] || fail 'places'
[ "$(jq -r '[.places[]|.work+":"+.passage+":"+(.unit|tostring)+":"+.path]|sort|join(",")' "$tmp/response")" = 'door:garden:1:hall.garden,river:text:5:' ] || fail 'exact places'
[ "$(get "$owner" "/workspace/world-literature?operation=places&$R")" = 200 ] && [ "$(jq -r '.places|length' "$tmp/response")" = 0 ] || fail 'places are per reader'
[ "$(get "$third" "/workspace/worlds/reading?$R")" = 200 ] || fail 'reading room list'
grep -q 'href="https://avery.aurekai.work/worlds/journal/read/door/garden?path=hall.garden#p-1">Two Doors</a>' "$tmp/response" && grep -q 'href="https://avery.aurekai.work/worlds/journal/read/river/text#l-5"' "$tmp/response" || fail 'continue links to the exact place'
if grep -q 'user:' "$tmp/response"; then fail 'no account numbers'; fi

# --- the club is a reading circle --------------------------------------------------------
[ "$(post "$owner" /workspace/world/film "operation=club&$W&world=1&space=journal&state=open&return=studio")" = 303 ] || fail 'open the circle'
public /worlds/journal >/dev/null
grep -q 'data-club-word="reading circle"' "$tmp/page" && grep -q 'Join or open the circle</a>' "$tmp/page" || fail 'circle door'
[ "$(post "$third" /workspace/world/film "operation=join&$W&space=journal")" = 200 ] || fail 'join the circle'
[ "$(post "$third" /workspace/world/film "operation=pick&$W&space=journal&ref=works%2F$chariot_key&note=$(enc 'For the first frost')")" = 200 ] || fail 'propose a poem'
[ "$(post "$third" /workspace/world/film "operation=say&$W&space=journal&ref=works%2F$chariot_key&body=$(enc 'The carriage holds three, not two.')")" = 200 ] || fail 'talk about it'
[ "$(get "$third" "/workspace/worlds/club?$W&space=journal")" = 200 ] || fail 'circle room'
cp "$tmp/response" "$tmp/circle.html"
grep -q '<title>The Night Journal · reading circle</title>' "$tmp/response" && grep -q ': the circle</h1>' "$tmp/response" || fail 'it is called a reading circle'
grep -q '<a href="https://avery.aurekai.work/worlds/journal/read/chariot">The Chariot</a>' "$tmp/response" || fail 'the proposed poem by title'
grep -q "option value=\"works/$river_key\">The River Keeps Its Counsel, Avery Lane</option>" "$tmp/response" || fail 'works offered to propose'
grep -q '<p class="by">You on The Chariot</p>' "$tmp/response" || fail 'talk names the poem'

# --- withdrawal --------------------------------------------------------------------------
[ "$(post "$owner" /workspace/world "operation=withdraw&$W&world=1&space=journal")" = 200 ] || fail 'withdraw'
[ "$(public /worlds/journal/read/river)" = 410 ] || fail 'reader answers 410'
[ "$(public /worlds/room)" = 200 ] && grep -q 'This writing is not published right now.' "$tmp/page" || fail 'the room no longer shows the poem'
[ "$(get "$third" "/workspace/world-literature?operation=places&$R")" = 200 ] && [ "$(jq -r '.places|length' "$tmp/response")" = 0 ] || fail 'places stop resolving'
printf 'account world literature: ok\n'
