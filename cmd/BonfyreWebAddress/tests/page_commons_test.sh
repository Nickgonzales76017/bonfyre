#!/bin/sh
# Wikimedia's free knowledge reaches the pages through the census contract,
# not through source-specific code: today's article leads by its own words,
# the picture of the day carries its author and terms, a Wikipedia upload that
# is not free is never shown, a file's name becomes words, films and
# recordings play in place with where they are kept and their terms, places
# close together sit on a street map that credits its tiles, one identity
# reported by two sources is one thing on a page and "also about this" on the
# other, and a picture offers the credit line its terms ask for.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page commons: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-commons.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null

# The declared rows, written the way production writes them.
bun "$root/cmd/BonfyreWebAddress/tools/apply_census_sources.mjs" --cms "$cms" --db "$cmsdb" --schemas "$schemas" >/dev/null

body() { printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/$1.body\"}" >"$tmp/foreign/receipts/$1.json"; cat >"$tmp/foreign/bodies/$1.body"; }

body wikipedia-featured-today <<'JSON'
{"tfa":{"normalizedtitle":"Eternal Blue (album)","extract":"Eternal Blue is the debut studio album by the Canadian heavy metal band Spiritbox.","wikibase_item":"Q107000001","thumbnail":{"source":"https://upload.wikimedia.org/wikipedia/en/thumb/1/1a/Eternal_Blue.jpg/330px-Eternal_Blue.jpg"},"content_urls":{"desktop":{"page":"https://en.wikipedia.org/wiki/Eternal_Blue_(album)"}}},
 "image":{"title":"File:Eurasian_coot_(Fulica_atra)_with_chicks.jpg","thumbnail":{"source":"https://thumb.wikimedia.org/wikipedia/commons/thumb/e/ea/Eurasian_coot_%28Fulica_atra%29_with_chicks.jpg/640px-Eurasian_coot_%28Fulica_atra%29_with_chicks.jpg"},"file_page":"https://commons.wikimedia.org/wiki/File:Eurasian_coot_(Fulica_atra)_with_chicks.jpg","artist":{"html":"<bdi><a href=\"https://www.wikidata.org/wiki/Q54800218\">Charles J. Sharp</a></bdi>"},"license":{"type":"CC BY-SA 4.0","code":"cc-by-sa-4.0","url":"https://creativecommons.org/licenses/by-sa/4.0"},"description":{"html":"A <a href=\"x\">Eurasian coot</a> with chicks, &amp; a nest"}},
 "onthisday":[
  {"text":"A lone gunman shot twelve people at the Washington Navy Yard.","year":2013,"pages":[{"wikibase_item":"Q14918135","extract":"The Washington Navy Yard shooting occurred on September 16, 2013.","thumbnail":{"source":"https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Navy_Yard.jpg/320px-Navy_Yard.jpg"},"content_urls":{"desktop":{"page":"https://en.wikipedia.org/wiki/Washington_Navy_Yard_shooting"}},"coordinates":{"lat":38.8747,"lon":-76.9947}}]},
  {"text":"The first issue of a newspaper was printed.","year":1847,"pages":[{"wikibase_item":"Q55","extract":"A newspaper.","content_urls":{"desktop":{"page":"https://en.wikipedia.org/wiki/Newspaper"}}}]}
 ]}
JSON

body wikipedia-most-read <<'JSON'
{"mostread":{"articles":[
 {"normalizedtitle":"Washington Navy Yard shooting","views":90210,"wikibase_item":"Q14918135","extract":"Most read copy of the same page.","content_urls":{"desktop":{"page":"https://en.wikipedia.org/wiki/Washington_Navy_Yard_shooting"}}},
 {"normalizedtitle":"Grace Coolidge","views":526535,"wikibase_item":"Q234287","extract":"Grace Anna Coolidge was first lady of the United States.","content_urls":{"desktop":{"page":"https://en.wikipedia.org/wiki/Grace_Coolidge"}}}
]}}
JSON

body commons-featured-pictures <<'JSON'
{"head":{"vars":["file","caption","name","picture","page","author","user","license","deed"]},"results":{"bindings":[
 {"file":{"type":"uri","value":"https://commons.wikimedia.org/entity/M1"},"caption":{"type":"literal","value":"Buildings with neon signs at blue hour, Shinjuku"},"name":{"type":"literal","value":"Neon_Shinjuku.jpg"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Neon_Shinjuku.jpg?width=500"},"page":{"type":"literal","value":"https://commons.wikimedia.org/wiki/File:Neon_Shinjuku.jpg"},"author":{"type":"literal","value":"Basile Morin"},"license":{"type":"literal","value":"CC BY-SA 4.0"},"deed":{"type":"literal","value":"https://creativecommons.org/licenses/by-sa/4.0/"}},
 {"file":{"type":"uri","value":"https://commons.wikimedia.org/entity/M2"},"name":{"type":"literal","value":"Hunter_baby_chameleon%28young%29.jpg"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Hunter_baby_chameleon%28young%29.jpg?width=500"},"page":{"type":"literal","value":"https://commons.wikimedia.org/wiki/File:Hunter_baby_chameleon%28young%29.jpg"},"user":{"type":"literal","value":"Mkrc85"}}
]}}
JSON

body commons-dfw-rated <<'JSON'
{"head":{"vars":["file","caption","name","picture","page","author","license","deed","geometry","taken"]},"results":{"bindings":[
 {"file":{"type":"uri","value":"https://commons.wikimedia.org/entity/M3"},"caption":{"type":"literal","value":"The Margaret McDermott Bridge in Dallas"},"name":{"type":"literal","value":"McDermott_Bridge.jpg"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/McDermott_Bridge.jpg?width=500"},"page":{"type":"literal","value":"https://commons.wikimedia.org/wiki/File:McDermott_Bridge.jpg"},"author":{"type":"literal","value":"Fkbowen"},"license":{"type":"literal","value":"CC BY-SA 4.0"},"deed":{"type":"literal","value":"https://creativecommons.org/licenses/by-sa/4.0/"},"geometry":{"type":"literal","value":"POINT(-96.8120 32.7705)"},"taken":{"type":"literal","value":"2020-03-09T00:00:00Z"}}
]}}
JSON

body wikidata-places-dfw <<'JSON'
{"head":{"vars":["place","name","about","picture","geometry","page"]},"results":{"bindings":[
 {"place":{"type":"uri","value":"http://www.wikidata.org/entity/Q16557"},"name":{"type":"literal","value":"Dallas"},"about":{"type":"literal","value":"city in the state of Texas, United States"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Dallas%20Skyline.jpg?width=500"},"geometry":{"type":"literal","value":"POINT(-96.808889 32.779167)"},"page":{"type":"literal","value":"https://en.wikipedia.org/wiki/Dallas"}},
 {"place":{"type":"uri","value":"http://www.wikidata.org/entity/Q16558"},"name":{"type":"literal","value":"Fort Worth"},"about":{"type":"literal","value":"city and county seat of Tarrant County, Texas"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Fort%20Worth%20Skyline.jpg?width=500"},"geometry":{"type":"literal","value":"POINT(-97.3325 32.756389)"},"page":{"type":"literal","value":"https://en.wikipedia.org/wiki/Fort_Worth,_Texas"}},
 {"place":{"type":"uri","value":"http://www.wikidata.org/entity/Q17943"},"name":{"type":"literal","value":"Arlington"},"about":{"type":"literal","value":"suburban city in Tarrant County, Texas"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Arlington.jpg?width=500"},"geometry":{"type":"literal","value":"POINT(-97.122839 32.705033)"},"page":{"type":"literal","value":"https://en.wikipedia.org/wiki/Arlington,_Texas"}}
]}}
JSON

body wikidata-public-domain-films <<'JSON'
{"head":{"vars":["film","title","maker","about","video","picture","page","terms"]},"results":{"bindings":[
 {"film":{"type":"uri","value":"http://www.wikidata.org/entity/Q208910"},"title":{"type":"literal","value":"Night of the Living Dead"},"maker":{"type":"literal","value":"George A. Romero"},"about":{"type":"literal","value":"1968 American independent horror film"},"video":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Night%20of%20the%20Living%20Dead%20(1968).webm"},"picture":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Night%20of%20the%20Living%20Dead%20(1968).webm?width=500"},"page":{"type":"literal","value":"https://en.wikipedia.org/wiki/Night_of_the_Living_Dead"},"terms":{"type":"literal","value":"Public domain"}}
]}}
JSON

body wikidata-recorded-music <<'JSON'
{"head":{"vars":["work","title","composer","audio","page"]},"results":{"bindings":[
 {"work":{"type":"uri","value":"http://www.wikidata.org/entity/Q1142"},"title":{"type":"literal","value":"Symphony No. 5"},"composer":{"type":"literal","value":"Ludwig van Beethoven"},"audio":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Ludwig_van_Beethoven_-_symphony_no._5.ogg"},"page":{"type":"literal","value":"https://en.wikipedia.org/wiki/Symphony_No._5_(Beethoven)"}},
 {"work":{"type":"uri","value":"http://www.wikidata.org/entity/Q183"},"title":{"type":"literal","value":"Für Elise"},"composer":{"type":"literal","value":"Ludwig van Beethoven"},"audio":{"type":"literal","value":"https://commons.wikimedia.org/wiki/Special:FilePath/Fur_Elise.ogg"},"page":{"type":"literal","value":"https://en.wikipedia.org/wiki/F%C3%BCr_Elise"}}
]}}
JSON

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(bun -e 'const net = require("node:net"); const server = net.createServer(); server.listen(0, "127.0.0.1", () => { console.log(server.address().port); server.close(); });')
BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.life' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page commons: server did not start' >&2; exit 1; fi
  sleep 0.05
done

bun "$root/cmd/BonfyreWebAddress/tests/page_commons_check.mjs" "$port"
