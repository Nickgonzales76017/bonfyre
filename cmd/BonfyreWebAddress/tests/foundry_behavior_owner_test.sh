#!/bin/sh
# A Foundry behavior family expands before runtime. The live lookup stays exact
# and AtomicForm carries an owner-declared identifier to the existing handler.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
compiler="$root/cmd/BonfyreCMS/tools/compile_foundry_behavior.mjs"
spec="$root/schemas/control/foundry-behavior-specs.json"
[ -x "$cms" ] || { echo 'foundry behavior owner: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'foundry behavior owner: SKIP (auth not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-foundry-behavior.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas"
cp "$root/cmd/BonfyreCMS/content-types/object_contract.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/project.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/project_support.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_body.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_membership.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/publication_occurrence.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/occurrence_follow.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/household_opportunity.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/household_opportunity_save.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/governance_proposal.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/governance_vote.json" "$schemas/"
cmsdb="$tmp/cms.db"
matrix="$tmp/matrix.db"
catalog="$tmp/catalog.json"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
bun "$compiler" --spec "$spec" --out "$catalog" >/dev/null
python3 - "$cms" "$cmsdb" "$schemas" "$catalog" <<'PY'
import json, subprocess, sys
cms, db, schemas, catalog = sys.argv[1:]
for contract in json.load(open(catalog))["contracts"]:
    subprocess.run([cms, "entry", "create", "object_contract", json.dumps(contract),
                    "--ns", "root", "--db", db, "--schemas", schemas], check=True,
                   stdout=subprocess.DEVNULL)
PY
"$cms" entry create project '{"project_id":"river-garden","subject_ref":"program:river-garden","title":"River Garden","pitch":"A fixture project.","stage":"open","opened_at":"2026-09-28T00:00:00Z","cannot_infer":"fixture project is not a payment","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_body '{"community":"fixture-community","subject_address":"external:reddit:fixture-community","title":"Fixture Community","description":"A public community fixture.","visibility":"public_observed","cannot_infer":"fixture community is not provider membership","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create publication_occurrence '{"occurrence_id":"fixture-occurrence","source_twin":"source:fixture:event","source_ref":"source:fixture:event","canonical_href":"https://events.fixture/fixture","title":"Fixture Occurrence","summary":"A retained public event fixture.","published_at":"2026-10-01T18:00:00Z","observed_at":"2026-09-28T00:00:00Z","receipt_ref":"foreign:fixture-event","rights_basis":"public_observation","retention":"metadata","visibility":"public","assertion_state":"observed","cannot_infer":"fixture event is not attendance","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create household_opportunity '{"opportunity_id":"fixture-opportunity","resource_ref":"resource-fixture","title":"Fixture Food Resource","description":"A retained public service fixture.","canonical_href":"https://211.fixture/food","service_kinds":"food,transport","source_ref":"source:211:fixture","receipt_ref":"foreign:fixture-211","observed_at":"2026-09-28T00:00:00Z","cannot_infer":"fixture resource is not eligibility","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create governance_proposal '{"proposal_id":"fixture-vote","verb":"Vote","subject_ref":"program:river-garden","asks":"Should the fixture proceed?","backing":"human_approval","opened_at":"2026-09-28T00:00:00Z","state":"open","derived_from":"fixture","cannot_infer":"a proposal is not a decision","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
host='Host: aurekai.life'
attempt=0
until curl -sS -o /dev/null -H "$host" "$base/_aperture?subject=project:river-garden"; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then
    cat "$tmp/server.log" >&2
    exit 1
  fi
  sleep 0.05
done

form=$(curl -fsS -H "$host" "$base/_aperture/form?subject=project:river-garden&operation=Project.support")
printf '%s' "$form" | grep -q 'action="/govern/support"'
printf '%s' "$form" | grep -q 'name="id" value="river-garden"'
printf '%s' "$form" | grep -q 'name="return" value="/s/project/river-garden"'
printf '%s' "$form" | grep -q 'name="amount"'
printf '%s' "$form" | grep -q 'State my support'

json=$(curl -fsS -H "$host" -H 'Accept: application/json' "$base/_aperture/form?subject=project:river-garden&operation=Project.join")
printf '%s' "$json" | grep -q '"submit_to":"/govern/join"'
printf '%s' "$json" | grep -q '"birth_mode":"INVOKE"'

community=$(curl -fsS -H "$host" "$base/_aperture/form?subject=community_body:fixture-community&operation=Community.join")
printf '%s' "$community" | grep -q 'action="/govern/community-join"'
printf '%s' "$community" | grep -q 'name="id" value="fixture-community"'

occurrence=$(curl -fsS -H "$host" "$base/_aperture/form?subject=publication_occurrence:fixture-occurrence&operation=Occurrence.follow")
printf '%s' "$occurrence" | grep -q 'action="/govern/occurrence-follow"'
printf '%s' "$occurrence" | grep -q 'name="id" value="fixture-occurrence"'

opportunity=$(curl -fsS -H "$host" "$base/_aperture/form?subject=household_opportunity:fixture-opportunity&operation=HouseholdOpportunity.save")
printf '%s' "$opportunity" | grep -q 'action="/govern/opportunity-save"'
printf '%s' "$opportunity" | grep -q 'name="id" value="fixture-opportunity"'

connect=$(curl -fsS -H "$host" "$base/_aperture/form?subject=institution:fixture-school.test&operation=Institution.connect")
printf '%s' "$connect" | grep -q 'action="/govern/connect"'
printf '%s' "$connect" | grep -q 'name="institution" value="fixture-school.test"'
printf '%s' "$connect" | grep -q 'name="plane"'

vote=$(curl -fsS -H "$host" "$base/_aperture/form?subject=governance_proposal:fixture-vote&operation=GovernanceProposal.vote")
printf '%s' "$vote" | grep -q 'action="/govern/vote"'
printf '%s' "$vote" | grep -q 'name="id" value="fixture-vote"'
printf '%s' "$vote" | grep -q 'name="choice"'

code=$(curl -sS -o "$tmp/owner-refusal" -w '%{http_code}' -X POST -H "$host" \
  -d 'id=river-garden&amount=20&return=/s/project/river-garden' "$base/govern/support")
[ "$code" = 401 ]
grep -q 'Sign in first' "$tmp/owner-refusal"

signup() { curl -fsS -X POST -H "$host" -d "{\"email\":\"$1\",\"name\":\"Fixture\",\"password\":\"a-long-enough-passphrase\"}" "$base/signup" >/dev/null; }
token() { curl -fsS -X POST -H "$host" -d "{\"email\":\"$1\",\"password\":\"a-long-enough-passphrase\"}" "$base/login" | python3 -c 'import json,sys; print(json.load(sys.stdin)["session"]["token"])'; }
signup 'fixture@foundry.test'
session=$(token 'fixture@foundry.test')
[ -n "$session" ]
post() { curl -sS -o "$tmp/post" -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: https://aurekai.life' -d "$1" "$base$2"; }
[ "$(post 'id=river-garden&amount=20&return=/s/project/river-garden' /govern/support)" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM project_support WHERE project_id='river-garden' AND mode='stated';")" = 1 ]
[ "$(post 'id=fixture-community&return=/subjects' /govern/community-join)" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM community_membership WHERE community='fixture-community' AND state='requested' AND member_subject<>'';")" = 1 ]
[ "$(post 'id=fixture-occurrence&return=/subjects' /govern/occurrence-follow)" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM occurrence_follow WHERE occurrence_id='fixture-occurrence' AND follower<>'';")" = 1 ]
[ "$(post 'id=fixture-opportunity&return=/subjects' /govern/opportunity-save)" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM household_opportunity_save WHERE opportunity_id='fixture-opportunity' AND saver<>'';")" = 1 ]
[ "$(post 'id=fixture-vote&choice=for&return=/subjects' /govern/vote)" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM governance_vote WHERE proposal_id='fixture-vote' AND choice='for';")" = 1 ]
[ "$(post 'id=not-a-project&amount=20&return=/subjects' /govern/support)" = 404 ]
[ "$(post 'id=not-a-community&return=/subjects' /govern/community-join)" = 404 ]
[ "$(post 'id=not-an-occurrence&return=/subjects' /govern/occurrence-follow)" = 404 ]
[ "$(post 'id=not-an-opportunity&return=/subjects' /govern/opportunity-save)" = 404 ]
[ "$(post 'id=not-a-proposal&choice=for&return=/subjects' /govern/vote)" = 404 ]

code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" \
  "$base/_aperture/form?subject=project:river-garden&operation=Project.vote")
[ "$code" = 404 ]

echo 'foundry behavior owner: PASS'
