#!/bin/sh
# Local SurfacePattern algebra witness. No production CMS or network state.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'surface pattern contract: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-surface-pattern.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas"
cp "$root/cmd/BonfyreCMS/content-types/surface_pattern.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/world.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/world_fragment.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_body.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_membership.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_post.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_reaction.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_presence.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/object_contract.json" "$schemas/"
cat >"$schemas/slice_fixture.json" <<'JSON'
{
  "name":"slice_fixture",
  "displayName":"Slice Fixture",
  "attributes":{
    "kind":{"type":"string","required":true},
    "region":{"type":"string","required":true},
    "audience":{"type":"string","required":true},
    "template":{"type":"string","required":true},
    "title":{"type":"string","required":true},
    "status":{"type":"string","required":true}
  }
}
JSON

cmsdb="$tmp/cms.db"
matrix="$tmp/matrix.db"
authdb="$tmp/auth.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null

entry() {
  "$cms" entry create slice_fixture "$1" --ns fixture --db "$cmsdb" --schemas "$schemas" >/dev/null
}
entry '{"kind":"A","region":"north","audience":"public","template":"ghost-one","title":"Alpha","status":"published"}'
entry '{"kind":"A","region":"south","audience":"private","template":"ghost-two","title":"Beta","status":"published"}'
entry '{"kind":"B","region":"north","audience":"public","template":"ghost-one","title":"Gamma","status":"published"}'
"$cms" entry create slice_fixture '{"kind":"A","region":"north","audience":"public","template":"ghost-one","title":"Root duplicate","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

"$cms" entry create community_body '{"community":"proof-club","subject_address":"subject:test:community","title":"Proof Club","description":"Humans around one addressed subject.","visibility":"public","governance_scope":"member-moderated","moderation_policy":"accepted public posts only","cannot_infer":"membership is not global effect authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_membership '{"membership":"proof-club-alice","community":"proof-club","member_subject":"subject-actor-alice","role":"member","state":"active","cannot_infer":"membership is not global authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_post '{"post":"proof-post-1","community":"proof-club","author_subject":"subject-actor-alice","subject_address":"subject:test:community","title":"First post","body":"A human post, not a federation edge.","published_at":"2026-09-08T01:00:00-05:00","visibility":"public","moderation_state":"accepted","cannot_infer":"published post is not endorsement","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_post '{"post":"proof-post-2","community":"proof-club","author_subject":"subject-actor-bob","subject_address":"subject:test:community","body":"A reply in the same human body.","reply_to":"proof-post-1","published_at":"2026-09-08T01:05:00-05:00","visibility":"public","moderation_state":"accepted","cannot_infer":"reply visibility is not mutation authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_membership '{"membership":"proof-club-expired","community":"proof-club","member_subject":"subject-actor-expired","role":"member","state":"active","expires_at":"2020-01-01T00:00:00Z","cannot_infer":"retained membership history is not current membership","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_presence '{"presence":"proof-club-alice-here","community":"proof-club","actor_subject":"subject-actor-alice","surface":"community","disclosure":"public","availability":"available","attention":"following","observed_at":"2020-01-01T00:00:00Z","expires_at":"2099-01-01T00:00:00Z","cannot_infer":"presence is not membership or authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_presence '{"presence":"proof-club-expired-here","community":"proof-club","actor_subject":"subject-actor-expired","surface":"community","disclosure":"public","availability":"available","observed_at":"2020-01-01T00:00:00Z","expires_at":"2099-01-01T00:00:00Z","cannot_infer":"expired membership cannot keep a presence current","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_reaction '{"reaction":"proof-reaction-1","post":"proof-post-1","actor_subject":"subject-actor-alice","kind":"acknowledge","state":"active","created_at":"2026-09-08T01:06:00-05:00","cannot_infer":"reaction is not authority or endorsement","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_reaction '{"reaction":"proof-reaction-inactive","post":"proof-post-1","actor_subject":"subject-actor-bob","kind":"acknowledge","state":"withdrawn","created_at":"2026-09-08T01:07:00-05:00","cannot_infer":"withdrawn reaction is retained history, not current reaction","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

"$cms" entry create world '{"world":"mixed-proof","title":"Mixed proof world","character":"zine","template":"ghost-wire","lede":"Seven grammars, one addressed World.","subject":"institution:nightgrid","sources":"fixture-source","grammars":"discover,stage,channels,board,spatial,listings,studio","cannot_infer":"publication plan is not subject truth","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create object_contract '{"object_name":"Fixture Event","applies_to_kind":"institution","birth_operation":"Event.declare","birth_mode":"DECLARE","requires_authority":"","initial_state":"draft","identity_allocation":"matrix","body_content_type":"slice_fixture","collection_lens":"fixture-events","slug_field":"title","closed_fields":"institution","human_fields":"title","owner_note":"fixture native owner","submit_to":"/_aperture/submit","submit_method":"POST","action_label":"Declare fixture event","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

fragment() {
  "$cms" entry create world_fragment "$1" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
}
fragment '{"fragment_id":"mixed-01-clip","world":"mixed-proof","ordinal":10,"subject_address":"subject:test:clip","surface_grammar":"discover","renderer_family":"vertical","renderer_variant":"faceted_stream","extent":"page","template":"ghost-wire","source_projection":"/_surface/slice-test?kind=A","media_posture":"full-bleed","temporal_posture":"recent","continuity_group":"mixed-proof-media","visible_sections":"candidate_material,why_here","action_aperture":"save,watch,share","provenance":"fixture:vertical","cannot_infer":"ranked clip is not intent or authority","status":"published"}'
fragment '{"fragment_id":"mixed-02-show","world":"mixed-proof","ordinal":20,"subject_address":"subject:test:show","surface_grammar":"stage","renderer_family":"stage","renderer_variant":"timeline","extent":"page","template":"ghost-wire","source_projection":"/_surface/slice-test?region=north","media_posture":"theatre","temporal_posture":"now","continuity_group":"mixed-proof-show","visible_sections":"timeline,participants,context","action_aperture":"observe,participate,share","provenance":"fixture:stage","cannot_infer":"stage is not a stream or commitment","status":"published"}'
fragment '{"fragment_id":"mixed-03-thread","world":"mixed-proof","ordinal":30,"subject_address":"subject:test:thread","surface_grammar":"channels","renderer_family":"thread","renderer_variant":"threaded_stream","extent":"section","template":"ghost-wire","source_projection":"/_surface/slice-test?kind=A","media_posture":"thumbnail","temporal_posture":"recent","continuity_group":"mixed-proof-show","visible_sections":"topics,participants,context","action_aperture":"read,reply","provenance":"fixture:thread","cannot_infer":"thread visibility is not permission to send","status":"published"}'
fragment '{"fragment_id":"mixed-04-board","world":"mixed-proof","ordinal":40,"subject_address":"subject:test:board","surface_grammar":"board","renderer_family":"board","renderer_variant":"masonry_board","extent":"page","template":"ghost-wire","source_projection":"/_surface/slice-test","media_posture":"masonry","temporal_posture":"evergreen","continuity_group":"mixed-proof-collection","visible_sections":"arrangement,references,context","action_aperture":"save,organize","provenance":"fixture:board","cannot_infer":"arrangement is not ownership","status":"published"}'
fragment '{"fragment_id":"mixed-05-route","world":"mixed-proof","ordinal":50,"subject_address":"subject:test:route","surface_grammar":"spatial","renderer_family":"spatial","renderer_variant":"map","extent":"page","template":"ghost-wire","source_projection":"/_surface/slice-test?region=north","media_posture":"contain","temporal_posture":"upcoming","continuity_group":"mixed-proof-route","visible_sections":"map,layers,place_explanation","action_aperture":"locate,route","provenance":"fixture:spatial","cannot_infer":"map placement is not presence or capacity","status":"published"}'
fragment '{"fragment_id":"mixed-06-market","world":"mixed-proof","ordinal":60,"subject_address":"subject:test:offer","surface_grammar":"listings","renderer_family":"market","renderer_variant":"listing_grid","extent":"page","template":"ghost-wire","source_projection":"/_surface/slice-test?audience=public","media_posture":"cover","temporal_posture":"upcoming","continuity_group":"mixed-proof-commerce","visible_sections":"availability,terms,comparison","action_aperture":"quote,buy,save","provenance":"fixture:market","cannot_infer":"listed terms are not a commitment or settlement","status":"published"}'
fragment '{"fragment_id":"mixed-07-work","world":"mixed-proof","ordinal":70,"subject_address":"subject:test:work","surface_grammar":"studio","renderer_family":"work","renderer_variant":"studio_canvas","extent":"workspace","template":"ghost-wire","source_projection":"/_surface/slice-test?kind=B","media_posture":"thumbnail","temporal_posture":"now","continuity_group":"mixed-proof-work","visible_sections":"composition,preview,owner_doors","action_aperture":"compose,review,publish_request","provenance":"fixture:work","cannot_infer":"workspace affordance is not effect authority","status":"published"}'


pattern() {
  "$cms" entry create surface_pattern "$1" --ns "$2" --db "$cmsdb" --schemas "$schemas" >/dev/null
}
pattern '{"pattern":"slice-test","reads_type":"slice_fixture","record_namespace":"fixture","selector_field":"kind","order_by":"title","item_fields":"kind,region,audience,template,title","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"","time_mode":"any","title":"Slice test","caption":"","extent":"page","lineage":"contract","status":"published"}' root
pattern '{"pattern":"hidden-template","reads_type":"slice_fixture","selector_field":"template","order_by":"title","item_fields":"title,region","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"","time_mode":"any","title":"Hidden selector","caption":"","extent":"page","lineage":"contract","status":"published"}' root
pattern '{"pattern":"namespace-bound","reads_type":"slice_fixture","record_namespace":"fixture","selector_field":"kind","order_by":"title","item_fields":"kind,title","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"","time_mode":"any","title":"Namespace boundary","caption":"","extent":"page","lineage":"contract","status":"published"}' root
pattern '{"pattern":"bad-control","reads_type":"slice_fixture","selector_field":"extent","order_by":"title","item_fields":"title","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"","time_mode":"any","title":"Bad control","caption":"","extent":"page","lineage":"contract","status":"published"}' root
pattern '{"pattern":"ambiguous","reads_type":"slice_fixture","selector_field":"kind","order_by":"title","item_fields":"kind,title","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"","time_mode":"any","title":"Ambiguous A","caption":"","extent":"page","lineage":"contract","status":"published"}' root
pattern '{"pattern":"ambiguous","reads_type":"slice_fixture","selector_field":"kind","order_by":"title","item_fields":"kind,title","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"","time_mode":"any","title":"Ambiguous B","caption":"","extent":"page","lineage":"contract","status":"published"}' other

"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
  "$web" serve --db "$matrix" --auth-db "$authdb" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
host='Host: aurekai.life'

attempt=0
until curl -sS -o /dev/null -H "$host" "$base/_surface/slice-test"; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then
    echo 'surface pattern contract: server did not become ready' >&2
    cat "$tmp/server.log" >&2
    exit 1
  fi
  sleep 0.05
done

get() { curl -sS -H "$host" -H 'Accept: application/json' "$base$1"; }
count() { python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])'; }
titles() { python3 -c 'import json,sys; print(",".join(r["title"] for r in json.load(sys.stdin)["records"]))'; }

j=$(get '/_surface/slice-test?kind=A')
[ "$(printf '%s' "$j" | count)" = 2 ]
[ "$(printf '%s' "$j" | titles)" = 'Alpha,Beta' ]

j=$(get '/_surface/slice-test?kind=A&region=north')
[ "$(printf '%s' "$j" | count)" = 1 ]
[ "$(printf '%s' "$j" | titles)" = 'Alpha' ]

j=$(get '/_surface/slice-test?region=~north')
[ "$(printf '%s' "$j" | count)" = 2 ]

j=$(get '/_surface/slice-test?template=ghost-one')
[ "$(printf '%s' "$j" | count)" = 2 ]
j=$(get '/_surface/slice-test?world=outside-this-pattern')
[ "$(printf '%s' "$j" | count)" = 3 ]

j=$(get '/_surface/hidden-template?template=ghost-two')
[ "$(printf '%s' "$j" | count)" = 1 ]
printf '%s' "$j" | grep -q '"title":"Beta"'
if printf '%s' "$j" | grep -q '"template"'; then
  echo 'surface pattern contract: hidden selector leaked into projected records' >&2
  exit 1
fi

j=$(get '/_surface/namespace-bound?kind=A')
[ "$(printf '%s' "$j" | count)" = 2 ]
if printf '%s' "$j" | grep -q 'Root duplicate'; then
  echo 'surface pattern contract: descriptor-owned record namespace leaked a duplicate seed row' >&2
  exit 1
fi

code=$(curl -sS -o "$tmp/unknown.json" -w '%{http_code}' -H "$host" -H 'Accept: application/json' "$base/_surface/slice-test?bogus=x")
[ "$code" = 400 ]
grep -q '"error":"unknown_slice"' "$tmp/unknown.json"

code=$(curl -sS -o "$tmp/control.json" -w '%{http_code}' -H "$host" -H 'Accept: application/json' "$base/_surface/bad-control")
[ "$code" = 500 ]
grep -q '"error":"unsafe_pattern_descriptor"' "$tmp/control.json"

code=$(curl -sS -o "$tmp/ambiguous.json" -w '%{http_code}' -H "$host" -H 'Accept: application/json' "$base/_surface/ambiguous?kind=A")
[ "$code" = 500 ]
grep -q '"error":"ambiguous_pattern_descriptor"' "$tmp/ambiguous.json"

j=$(get '/_surface/world-composition?world=mixed-proof')
# This asserts the CUE 15 × 4 cell data, rather than preserving the former C
# fallback that flattened every grammar into a page. Discover is a section;
# Board and Studio are workspaces.
printf '%s' "$j" | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["schema_version"]=="bonfyre.surface_composition.v1"; assert x["presentation_class"]=="anonymous"; assert x["interaction_density"]=="low"; assert x["subject_address"]=="institution:nightgrid"; assert x["fragment_count"]==7; assert [f["renderer_family"] for f in x["ordered_fragments"]]==["vertical","stage","thread","board","spatial","market","work"]; assert [f["renderer_variant"] for f in x["ordered_fragments"]]==["faceted_stream","timeline","threaded_stream","masonry_board","map","listing_grid","studio_canvas"]; assert [f["extent"] for f in x["ordered_fragments"]]==["section","page","page","workspace","page","page","workspace"]'

j=$(get '/_surface/world-fragments?world=mixed-proof')
printf '%s' "$j" | python3 -c 'import json,sys; x=json.load(sys.stdin); fs=x["ordered_fragments"]; assert x["schema_version"]=="bonfyre.world_fragments.v1"; assert x["presentation_class"]=="anonymous" and x["interaction_density"]=="low"; assert x["fragment_count"]==7; assert [f["fragment_id"] for f in fs]==["mixed-01-clip","mixed-02-show","mixed-03-thread","mixed-04-board","mixed-05-route","mixed-06-market","mixed-07-work"]; assert [f["subject_address"] for f in fs]==["subject:test:clip","subject:test:show","subject:test:thread","subject:test:board","subject:test:route","subject:test:offer","subject:test:work"]; assert [f["renderer_family"] for f in fs]==["vertical","stage","thread","board","spatial","market","work"]; assert fs[0]["source_projection"]=="/_surface/slice-test?kind=A"; assert fs[0]["media_posture"]=="full-bleed"; assert fs[1]["temporal_posture"]=="now"; assert fs[2]["continuity_group"]=="mixed-proof-show"; assert fs[5]["action_aperture"]=="quote,buy,save"; assert fs[6]["extent"]=="workspace"'

j=$(get '/_surface/community-body?community=proof-club')
printf '%s' "$j" | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["schema_version"]=="bonfyre.human_community.v1"; assert x["community"]=="proof-club"; assert x["subject_address"]=="subject:test:community"; assert x["member_count"]==1; assert x["public_presence_count"]==1; assert [p["post"] for p in x["records"]]==["proof-post-2","proof-post-1"]; assert x["records"][0]["reply_to"]=="proof-post-1"; assert x["records"][1]["reaction_count"]==1; assert x["federation"]=="projected elsewhere"; assert x["truth_level"]=="authored_human_social_body"'
community_page=$(curl -sS -H "$host" -H 'Accept: text/html' "$base/community?community=proof-club")
printf '%s' "$community_page" | grep -q '1 active members · 1 publicly here now'
printf '%s' "$community_page" | grep -q '1 accepted reactions'

code=$(curl -sS -o "$tmp/spoof.json" -w '%{http_code}' -H "$host" -H 'Accept: application/json' "$base/_surface/world-composition?world=mixed-proof&actor=somebody")
[ "$code" = 400 ]
grep -q '"error":"forbidden_presentation_coordinate"' "$tmp/spoof.json"

page=$(curl -sS -H "$host" -H 'Accept: text/html' "$base/w/mixed-proof")
# A World document may link a machine-readable fragment plan, but it must not
# ship a browser-resident second renderer that fetches JSON and invents the
# visual body. Native/HTMX lowering owns that transition.
! printf '%s' "$page" | grep -q '/_surface/world-fragments?world='
! printf '%s' "$page" | grep -q "nativeWorldFragments\|bfHumanCommunity"

stage=$(curl -sS -H "$host" -H 'Accept: text/html' "$base/w/mixed-proof/stage")
printf '%s' "$stage" | grep -q 'Declare fixture event' || { echo 'surface pattern contract: no compiled aperture action'; exit 1; }
printf '%s' "$stage" | grep -q 'hx-get="/_aperture/form?subject=institution:nightgrid&amp;operation=Event.declare"' || { echo 'surface pattern contract: aperture was not subject-bound'; exit 1; }
form=$(curl -sS -H "$host" -H 'Accept: text/html' "$base/_aperture/form?subject=institution:nightgrid&operation=Event.declare")
printf '%s' "$form" | grep -q 'action="/_aperture/submit"' || { echo 'surface pattern contract: compiled aperture did not reach native submit owner'; exit 1; }

echo 'surface pattern contract: PASS'
