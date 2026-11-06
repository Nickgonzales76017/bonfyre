#!/bin/sh
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
project="$root/cmd/BonfyreProject/bonfyre-project"
collector="$root/cmd/BonfyreExternalEstate/tools/collect_public_carrier.mjs"
resource_promoter="$root/cmd/BonfyreExternalEstate/tools/promote_external_resource.mjs"
ingest="$root/cmd/BonfyreExternalEstate/tools/ingest_incumbent_record.mjs"
compiler="$root/cmd/BonfyreCMS/tools/compile_foundry_behavior.mjs"
spec="$root/schemas/control/foundry-behavior-specs.json"
[ -x "$cms" ] || { echo 'github foundry closure: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'github foundry closure: SKIP (auth not built)'; exit 0; }
[ -x "$project" ] || { echo 'github foundry closure: SKIP (project not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-github-foundry.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

observer="$tmp/fixture-foreign-observer"
printf '%s\n' '#!/bin/sh' \
  'set -eu' \
  '[ "$1" = foreign ] && [ "$2" = observe ]' \
  'ident=$3; url=$4' \
  'shift 4' \
  'body=""; receipt=""; right=""' \
  'while [ "$#" -gt 0 ]; do' \
  '  case "$1" in' \
  '    --body) body=$2; shift 2 ;;' \
  '    --receipt) receipt=$2; shift 2 ;;' \
  '    --observe-right) right=$2; shift 2 ;;' \
  '    *) shift ;;' \
  '  esac' \
  'done' \
  '[ -n "$body" ] && [ -n "$receipt" ] && [ "$right" = public ]' \
  'case "$ident:$url" in' \
  '  github-fixture-org-foundry-project:https://api.github.com/repos/fixture-org/foundry-project) payload="{\"id\":42,\"full_name\":\"fixture-org/foundry-project\",\"html_url\":\"https://github.com/fixture-org/foundry-project\",\"description\":\"Retained public project fixture.\",\"created_at\":\"2026-09-28T00:00:00Z\"}" ;;' \
  '  reddit-fixture_community:https://www.reddit.com/r/fixture_community/.json?limit=1) payload="{\"data\":{\"children\":[{\"data\":{\"subreddit\":\"fixture_community\",\"subreddit_url\":\"https://www.reddit.com/r/fixture_community/\"}}]}}" ;;' \
  '  discord-42:https://discord.com/api/guilds/42/widget.json) payload="{\"id\":\"42\",\"name\":\"Fixture Discord\",\"instant_invite\":\"https://discord.gg/fixture\"}" ;;' \
  '  meetup-event-*:https://events.fixture/meetup) payload="<html><script type=\"application/ld+json\">{\"@type\":\"Event\",\"name\":\"Fixture Meetup\",\"url\":\"https://events.fixture/meetup\",\"description\":\"Retained public event fixture.\",\"startDate\":\"2026-10-01T18:00:00Z\"}</script></html>" ;;' \
  '  211-resource-*:https://211.fixture/resources/food) payload="{\"id\":\"food-1\",\"name\":\"Fixture Food Help\",\"url\":\"https://211.fixture/resources/food\",\"description\":\"Retained public food support.\",\"services\":[\"food\",\"transport\"],\"private_enrollment\":\"discarded\"}" ;;' \
  '  *) exit 64 ;;' \
  'esac' \
  'printf "%s\n" "$payload" > "$body"' \
  'printf "%s\n" "{\"receipt\":\"fixture-public-observation\"}" > "$receipt"' > "$observer"
chmod 755 "$observer"

foreign="$tmp/foreign"
envelope="$tmp/github.json"
bun "$collector" github --repository fixture-org/foundry-project --source-ref source:github:fixture-org/foundry-project \
  --project "$observer" --foreign-root "$foreign" --observed-at 2026-09-28T00:00:00Z --out "$envelope" >/dev/null
[ -s "$foreign/bodies/github-fixture-org-foundry-project.body" ]
[ -s "$foreign/receipts/github-fixture-org-foundry-project.json" ]
reddit_envelope="$tmp/reddit.json"
discord_envelope="$tmp/discord.json"
bun "$collector" reddit --subreddit fixture_community --source-ref source:reddit:fixture_community \
  --project "$observer" --foreign-root "$foreign" --observed-at 2026-09-28T00:00:00Z --out "$reddit_envelope" >/dev/null
bun "$collector" discord --guild-id 42 --source-ref source:discord:42 \
  --project "$observer" --foreign-root "$foreign" --observed-at 2026-09-28T00:00:00Z --out "$discord_envelope" >/dev/null
[ -s "$foreign/bodies/reddit-fixture_community.body" ]
[ -s "$foreign/receipts/reddit-fixture_community.json" ]
[ -s "$foreign/bodies/discord-42.body" ]
[ -s "$foreign/receipts/discord-42.json" ]
event_envelope="$tmp/event.json"
bun "$collector" event --provider meetup --url https://events.fixture/meetup --source-ref source:meetup:fixture \
  --project "$observer" --foreign-root "$foreign" --observed-at 2026-09-28T00:00:00Z --out "$event_envelope" >/dev/null
resource_envelope="$tmp/resource.json"
promoted_resource="$tmp/promoted-resource.json"
bun "$collector" 211 --url https://211.fixture/resources/food --source-ref source:211:fixture \
  --project "$observer" --foreign-root "$foreign" --observed-at 2026-09-28T00:00:00Z --out "$resource_envelope" >/dev/null
bun "$resource_promoter" --input "$resource_envelope" --out "$promoted_resource" >/dev/null

schemas="$tmp/schemas"
mkdir -p "$schemas"
cp "$root/cmd/BonfyreCMS/content-types/object_contract.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/project.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/project_support.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_body.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_membership.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/publication_occurrence.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/occurrence_follow.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/external_resource.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/household_opportunity.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/household_opportunity_save.json" "$schemas/"
cmsdb="$tmp/cms.db"
bun "$ingest" --input "$envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'created:project'
bun "$ingest" --input "$envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'updated:project'
bun "$ingest" --input "$reddit_envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'created:community_body'
bun "$ingest" --input "$reddit_envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'updated:community_body'
bun "$ingest" --input "$discord_envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'created:community_body'
bun "$ingest" --input "$event_envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'created:publication_occurrence'
bun "$ingest" --input "$event_envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'updated:publication_occurrence'
bun "$ingest" --input "$resource_envelope" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'created:external_resource'
bun "$ingest" --input "$promoted_resource" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'created:household_opportunity'
bun "$ingest" --input "$promoted_resource" --cms "$cms" --db "$cmsdb" --schemas "$schemas" | grep -q 'updated:household_opportunity'
project_id=$(sqlite3 "$cmsdb" "SELECT project_id FROM project WHERE subject_ref='external:github:42';")
[ -n "$project_id" ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM project WHERE project_id='$project_id';")" = 1 ]
reddit_community=$(sqlite3 "$cmsdb" "SELECT community FROM community_body WHERE subject_address='external:reddit:r/fixture_community';")
discord_community=$(sqlite3 "$cmsdb" "SELECT community FROM community_body WHERE subject_address='external:discord:42';")
[ -n "$reddit_community" ] && [ -n "$discord_community" ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM community_body;")" = 2 ]
occurrence_id=$(sqlite3 "$cmsdb" "SELECT occurrence_id FROM publication_occurrence WHERE source_ref='source:meetup:fixture';")
[ -n "$occurrence_id" ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM publication_occurrence WHERE occurrence_id='$occurrence_id';")" = 1 ]
opportunity_id=$(sqlite3 "$cmsdb" "SELECT opportunity_id FROM household_opportunity WHERE source_ref='source:211:fixture';")
[ -n "$opportunity_id" ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM external_resource WHERE grouping='211_resource';")" = 1 ]

catalog="$tmp/catalog.json"
bun "$compiler" --spec "$spec" --out "$catalog" >/dev/null
bun "$root/cmd/BonfyreWebAddress/tests/foundry_fixture_ops.mjs" seed-contracts "$cms" "$cmsdb" "$schemas" "$catalog"

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(bun "$root/cmd/BonfyreWebAddress/tests/foundry_fixture_ops.mjs" port)
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
host='Host: aurekai.life'
attempt=0
until curl -sS -o /dev/null -H "$host" "$base/_aperture?subject=project:$project_id"; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then
    cat "$tmp/server.log" >&2
    exit 1
  fi
  sleep 0.05
done

[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/s/project/$project_id")" = 200 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/s/publication_occurrence/$occurrence_id")" = 200 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/s/household_opportunity/$opportunity_id")" = 200 ]
form=$(curl -fsS -H "$host" "$base/_aperture/form?subject=project:$project_id&operation=Project.support")
printf '%s' "$form" | grep -q 'action="/govern/support"'
printf '%s' "$form" | grep -q "name=\"id\" value=\"$project_id\""
printf '%s' "$form" | grep -q 'name="amount"'
reddit_form=$(curl -fsS -H "$host" "$base/_aperture/form?subject=community_body:$reddit_community&operation=Community.join")
discord_form=$(curl -fsS -H "$host" "$base/_aperture/form?subject=community_body:$discord_community&operation=Community.join")
printf '%s' "$reddit_form" | grep -q 'action="/govern/community-join"'
printf '%s' "$reddit_form" | grep -q "name=\"id\" value=\"$reddit_community\""
printf '%s' "$discord_form" | grep -q 'action="/govern/community-join"'
printf '%s' "$discord_form" | grep -q "name=\"id\" value=\"$discord_community\""
occurrence_form=$(curl -fsS -H "$host" "$base/_aperture/form?subject=publication_occurrence:$occurrence_id&operation=Occurrence.follow")
printf '%s' "$occurrence_form" | grep -q 'action="/govern/occurrence-follow"'
printf '%s' "$occurrence_form" | grep -q "name=\"id\" value=\"$occurrence_id\""
opportunity_form=$(curl -fsS -H "$host" "$base/_aperture/form?subject=household_opportunity:$opportunity_id&operation=HouseholdOpportunity.save")
printf '%s' "$opportunity_form" | grep -q 'action="/govern/opportunity-save"'
printf '%s' "$opportunity_form" | grep -q "name=\"id\" value=\"$opportunity_id\""

curl -fsS -X POST -H "$host" -d '{"email":"github-fixture@foundry.test","name":"GitHub Fixture","password":"a-long-enough-passphrase"}' "$base/signup" >/dev/null
session=$(curl -fsS -X POST -H "$host" -d '{"email":"github-fixture@foundry.test","password":"a-long-enough-passphrase"}' "$base/login" | bun "$root/cmd/BonfyreWebAddress/tests/foundry_fixture_ops.mjs" session-token)
[ -n "$session" ]
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
  -d "id=$project_id&amount=20&return=/s/project/$project_id" "$base/govern/support")
[ "$code" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM project_support WHERE project_id='$project_id' AND mode='stated';")" = 1 ]
for community in "$reddit_community" "$discord_community"; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
    -d "id=$community&return=/s/community/$community" "$base/govern/community-join")
  [ "$code" = 303 ]
done
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM community_membership WHERE state='requested';")" = 2 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
  -d "id=$occurrence_id&return=/s/publication_occurrence/$occurrence_id" "$base/govern/occurrence-follow")" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM occurrence_follow WHERE occurrence_id='$occurrence_id' AND follower<>'';")" = 1 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
  -d "id=$opportunity_id&return=/s/household_opportunity/$opportunity_id" "$base/govern/opportunity-save")" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM household_opportunity_save WHERE opportunity_id='$opportunity_id' AND saver<>'';")" = 1 ]

echo 'carrier foundry closure: PASS'
