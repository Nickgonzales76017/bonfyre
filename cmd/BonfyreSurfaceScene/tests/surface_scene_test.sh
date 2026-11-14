#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
scene="$root/cmd/BonfyreSurfaceScene/bonfyre-surface-scene"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
source_schemas="$root/cmd/BonfyreCMS/content-types"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"
# Limit this isolated fixture to the schema families it actually composes.
# The production schema count is verified separately; this is not a release snapshot.
schemas="$tmp/schemas"
mkdir -p "$schemas"
for type in world site_presentation site_program capability_program semantic_slot application_binding experience_stage surface_renderer surface_pattern world_fragment presentation_template surface_interaction_profile object_contract proposal_contract surface_scene; do
  cp "$source_schemas/$type.json" "$schemas/$type.json"
done
export BONFYRE_CMS_BINARY="$cms"
export BONFYRE_CMS_SCHEMAS="$schemas"
export BONFYRE_SURFACE_SCENE_SCHEMA="$schemas/surface_scene.json"
export BONFYRE_FACT_DELTAS="$tmp/deltas"
mkdir -p "$BONFYRE_FACT_DELTAS"

"$cms" schema migrate --db "$db" --schemas "$schemas" >/dev/null

create() { type=$1; body=$2; "$cms" entry create "$type" "$body" --ns root --db "$db" --schemas "$schemas" >/dev/null || { printf 'Native CMS fixture create failed: %s\\n' "$type" >&2; return 1; }; }

create world '{"world":"night-proof","title":"Night Proof","character":"live-pulse","template":"ghost:night","lede":"One subject, many lenses.","subject":"world:night-proof","sources":"night-sources","grammars":"now,community,spatial","cannot_infer":"world publication is not subject truth","status":"published"}'
create site_presentation '{"site_id":"night-proof","host":"night-proof.aurekai.live","subject_address":"world:night-proof","profile":"ghost:night","territory":"live","navigation":["NOW","MAP"],"status":"published"}'
create site_program '{"program_id":"night-proof:v1","site_id":"night-proof","experience_programs":["Night"],"application_bindings":["night:feed","night:talk"],"renderer_preferences":{"profile":"ghost:night"},"source_collections":["night-sources"],"capability_programs":["night-capabilities"],"actor_presentations":{"public":"public"},"status":"published"}'
create capability_program '{"program_id":"night-capabilities","application_kind":"PublicationFeed","action":"feed-intake","consumes":["publication"],"produces":{"mode":"source-library"},"carrier_candidates":[],"capability_requirements":{},"authority_requirement":"current destination write authority","transition_conditions":[],"failure_projection":"No writable destination.","status":"published"}'
create semantic_slot '{"slot_id":"night:primary","site_id":"night-proof","slot_name":"primary","ordinal":10,"extent":"page","accepts":["PublicationFeed","Community"],"min_material_quality":"observed","max_items":50,"htmx_boundary":"scene:primary","status":"published"}'
create application_binding '{"binding_id":"night:feed","site_id":"night-proof","slot_id":"night:primary","application_kind":"PublicationFeed","material_query":{"kind":"PublicationOccurrence"},"source_collection":"night-sources","action_aperture":"save,share","continuity_key":"night:feed","status":"published"}'
create application_binding '{"binding_id":"night:talk","site_id":"night-proof","slot_id":"night:primary","application_kind":"Community","material_query":{"community":"night-talk"},"action_aperture":"read,reply","continuity_key":"night:talk","status":"published"}'
create experience_stage '{"stage_id":"night:stage","experience":"Night","application_kind":"Stage","consumes":["EventOccurrence"],"produces":["ParticipationIntent"],"material_queries":[{"kind":"EventOccurrence"}],"renderer":"stage","extent":"page","actions":["observe","participate"],"next_stage_rules":[],"empty_projection":"No published event.","refused_projection":"Stage unavailable.","status":"published"}'
create surface_renderer '{"renderer":"vertical","canonical_name":"Vertical Stream","donor":"Pixelfed","grammar":"vertical-social-media","seam":"SurfaceIR","renders":"PublicationFeed","extents":"page,section","cannot_infer":"renderer is not source truth","status":"published"}'
create surface_renderer '{"renderer":"spatial","canonical_name":"Spatial Lens","donor":"MapLibre","grammar":"spatial-manipulation","seam":"SurfaceIR","renders":"SpatialObservation","extents":"page","cannot_infer":"map placement is not presence","status":"published"}'
create surface_renderer '{"renderer":"stage","canonical_name":"Timeline Stage","donor":"Mobilizon","grammar":"event-time","seam":"SurfaceIR","renders":"EventOccurrence","extents":"page","cannot_infer":"stage is not reservation","status":"published"}'
create surface_pattern '{"pattern":"night-feed","reads_type":"publication_occurrence","record_namespace":"root","selector_field":"status","order_by":"published_at","item_fields":"occurrence_id,title,published_at","shape":"record","effect_class":"QUERY","requires_authority":"","visible_when":"","status_filter":"published","time_field":"published_at","time_mode":"recent","title":"Night feed","caption":"","extent":"page","lineage":"test","status":"published"}'
create world_fragment '{"fragment_id":"night:now","world":"night-proof","ordinal":10,"subject_address":"world:night-proof","surface_grammar":"NOW","renderer_family":"vertical","renderer_variant":"faceted_stream","extent":"page","template":"ghost:night","source_projection":"/_surface/night-feed?status=published","media_posture":"full-bleed","temporal_posture":"now","continuity_group":"night:world","visible_sections":"material,why_here","action_aperture":"save,watch,share","provenance":"test","cannot_infer":"fragment is not intent or authority","status":"published"}'
create world_fragment '{"fragment_id":"night:map","world":"night-proof","ordinal":20,"subject_address":"world:night-proof","surface_grammar":"MAP","renderer_family":"spatial","renderer_variant":"map","extent":"page","template":"ghost:night","source_projection":"","media_posture":"contain","temporal_posture":"now","continuity_group":"night:world","visible_sections":"map,layers","action_aperture":"locate,route","provenance":"test","cannot_infer":"map is not presence","status":"published"}'
create presentation_template '{"template":"ghost:night","reference_source":"test","reference_name":"Night","reference_url":"https://example.test/night","reference_categories":"live","family":"LivePulse","renderer_mix":"vertical,spatial,stage","density":"immersive","hero_mode":"media","navigation_mode":"contextual","media_mode":"primary","typography_mode":"display","card_mode":"edge-to-edge","chrome_mode":"receding","semantic_roles":{"primary":"world"},"layout_tokens":{"gap":"fluid"},"acceptance_contract":"presentation only","cannot_infer":"template is not semantics","status":"published"}'
create surface_interaction_profile '{"profile_id":"night:interaction","site_id":"night-proof","world":"night-proof","default_lens":"NOW","lens_order":["NOW","MAP","TALK"],"semantic_zoom":[{"resolution":0,"name":"world"},{"resolution":2,"name":"subject"},{"resolution":4,"name":"occurrence"}],"interaction_depths":["peek","expand","focus"],"context_rail":{"mode":"floating"},"relation_halo":{"enabled":true,"max":8},"command_aperture":{"shortcut":"Meta+K","mode":"contextual"},"transition_policy":{"object_permanence":true,"view_transition":"auto"},"mobile_posture":{"primary":"vertical","lens_gesture":"swipe"},"desktop_posture":{"inspector":"persistent","keyboard":"primary"},"live_update_policy":{"mode":"SurfaceDelta","transport":"htmx"},"status":"published"}'
create object_contract '{"object_name":"NightPlan","applies_to_kind":"world","birth_operation":"NightPlan.propose","birth_mode":"PROPOSE","requires_authority":"","initial_state":"draft","identity_allocation":"none","body_content_type":"","collection_lens":"plans","slug_field":"","closed_fields":"subject","human_fields":"note","owner_note":"Persistent purpose only after TargetObligation lowering.","submit_to":"/_proposal","submit_method":"POST","action_label":"Make a plan","status":"published"}'
create proposal_contract '{"operation":"NightPlan.propose","semantic_contract":"night-plan","preference_policy":"resilient","predicate_id":"night-ready","owner":"ForeignTwin","predicate":"delivery","expected_state":"observed","evidence_requirement":"","authority_requirement":"","status":"published"}'

compiled=$($scene compile --db "$db" --site night-proof --actor-scope public --frontier frontier:one --intent explore)
printf '%s' "$compiled" | python3 -c '
import json,sys
s=json.load(sys.stdin)
assert s["schema"]=="bonfyre-surface-scene.v1"
assert s["focus"]["subject_address"]=="world:night-proof"
assert {"NOW","MAP","PublicationFeed","Community","Stage"}.issubset(set(s["lenses"]))
assert any(p["pattern"]=="night-feed" for p in s["patterns"])
assert any(x["fragment_id"]=="night:now" for x in s["fragments"])
assert s["interaction"]["profile_id"]=="night:interaction"
assert s["interaction"]["interaction_depths"]==["peek","expand","focus"]
assert "NightPlan.propose" in [x["birth_operation"] for x in s["operations"]["object_contracts"]]
assert "watch" in s["operations"]["apertures"]
assert "feed-intake" in s["operations"]["apertures"]
assert "cms:application_binding:night:feed" in s["dependencies"]
assert s["runtime_queries"]["source_collections"]==["night-sources"]
'

sync1=$($scene sync --db "$db" --site night-proof --actor-scope public)
id1=$(printf '%s' "$sync1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
digest1=$(printf '%s' "$sync1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["source_digest"])')
[ "$(sqlite3 "$db" 'SELECT count(*) FROM surface_scene;')" = 1 ]

binding_id=$(sqlite3 "$db" "SELECT id FROM application_binding WHERE namespace='root' AND binding_id='night:feed' LIMIT 1;")
"$cms" entry update application_binding "$binding_id" '{"application_kind":"LivePulseFeed","action_aperture":"save,share,follow","status":"published"}' --ns root --db "$db" --schemas "$schemas" >/dev/null 2>/dev/null
cat > "$tmp/handoff.json" <<'JSON'
{"schema":"bonfyre-lowering-handoff.v1","delta_id":"delta:scene-test","identity":"cms:application_binding:night:feed","candidate_kind":"application_binding","owner_ref":"CMS:application_binding:night:feed","lowerer_ref":"SurfaceIR:application","truth":"dependency_cut_only"}
JSON
lower=$($scene lower --db "$db" --handoff "$tmp/handoff.json" --actor-scope public)
printf '%s' "$lower" | grep -q '"resynced":1'
[ "$(sqlite3 "$db" 'SELECT count(*) FROM surface_scene;')" = 1 ]
id2=$(sqlite3 "$db" 'SELECT id FROM surface_scene LIMIT 1;')
digest2=$(sqlite3 "$db" 'SELECT source_digest FROM surface_scene LIMIT 1;')
[ "$id1" = "$id2" ]
[ "$digest1" != "$digest2" ]
scene_after=$(sqlite3 "$db" 'SELECT lenses FROM surface_scene LIMIT 1;')
printf '%s' "$scene_after" | grep -q 'LivePulseFeed'
ops_after=$(sqlite3 "$db" 'SELECT operations FROM surface_scene LIMIT 1;')
printf '%s' "$ops_after" | grep -q 'follow'
[ -n "$(find "$BONFYRE_FACT_DELTAS" -type f -name '*.json' -print -quit)" ]

# The durable AddressPlane outbox partitions by lowerer. SurfaceScene consumes
# only SurfaceIR envelopes, rematerializes the dependent scene, then acks by
# rename; unrelated lowerer families remain untouched for their own workers.
mkdir -p "$tmp/lowerer-inbox/SurfaceIR-application"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"delta:scene-drain","identity":"cms:application_binding:night:feed","candidate_kind":"application_binding","owner_ref":"CMS:application_binding:night:feed","lowerer_ref":"SurfaceIR:application","truth":"dependency_cut_only"}' > "$tmp/lowerer-inbox/SurfaceIR-application/scene.json.handled"
drain=$($scene drain --db "$db" --lowerer-dir "$tmp/lowerer-inbox" --actor-scope public)
printf '%s' "$drain" | grep -q '"handled":1'
[ -f "$tmp/lowerer-inbox/SurfaceIR-application/scene.json.handled.surface-scene.handled" ]

echo "surface_scene_test: PASS"
