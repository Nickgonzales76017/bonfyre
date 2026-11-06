#!/bin/sh
# A SiteProgram is not deployable while it merely returns its composition JSON.
# This witness proves CMS-authored slots/bindings lower server-side into a human
# document over admitted PublicationOccurrence material, with no global-feed
# fallback and no browser interpretation of source/protocol descriptors.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'native SiteProgram: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-site-program.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in internet_source publication_occurrence source_collection presentation_template site_presentation site_program semantic_slot application_binding community_body community_membership community_post music_recording media_program world world_fragment; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db" matrix="$tmp/matrix.db" authdb="$tmp/auth.db" projection_cache="$tmp/public-projections.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null

"$cms" entry create internet_source '{"family":"publication","source":"fieldwire","label":"Fieldwire Journal","says":"Field reporting","url":"https://example.test/field.xml","twin":"fieldwire-twin","state":"observed","extract":"rss:item","title_field":"title","meta_field":"pubDate","link_field":"link","body_regime":"retained-public","status":"published","tier":"warm","kind":"publication"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"publication","source":"unrelated","label":"Unrelated","says":"Unrelated reporting","url":"https://example.test/no.xml","twin":"unrelated-twin","state":"observed","extract":"rss:item","title_field":"title","meta_field":"pubDate","link_field":"link","body_regime":"retained-public","status":"published","tier":"warm","kind":"publication"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
printf '%s\n' '<rss><channel><item><title>Rain changes the harvest window</title><link>https://example.test/rain</link><pubDate>Wed, 09 Sep 2026 12:00:00 GMT</pubDate></item><item><title>Growers meet at the field school</title><link>https://example.test/school</link><pubDate>Wed, 09 Sep 2026 11:00:00 GMT</pubDate></item></channel></rss>' >"$tmp/foreign/bodies/field.body"
printf '%s\n' '<rss><channel><item><title>Unrelated global-feed item</title><link>https://example.test/unrelated</link><pubDate>Wed, 09 Sep 2026 13:00:00 GMT</pubDate></item></channel></rss>' >"$tmp/foreign/bodies/unrelated.body"
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/field.body\"}" >"$tmp/foreign/receipts/fieldwire-twin.json"
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/unrelated.body\"}" >"$tmp/foreign/receipts/unrelated-twin.json"
BONFYRE_FOREIGN_ROOT="$tmp/foreign" "$cms" publication promote-rss --source fieldwire --limit 8 --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
BONFYRE_FOREIGN_ROOT="$tmp/foreign" "$cms" publication promote-rss --source unrelated --limit 8 --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

"$cms" entry create source_collection '{"collection_id":"agriculture-journal","subject_address":"topic:agriculture","source_selector":{"domains":["agriculture"],"source_refs":["fieldwire"]},"material_kind":["PublicationOccurrence"],"refresh_policy":"demand:agriculture","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create presentation_template '{"template":"field-notes","reference_source":"measured-local","reference_name":"Field Notes","reference_url":"https://example.test/field-notes","reference_categories":"agriculture","family":"editorial","renderer_mix":"vertical thread","density":"standard","hero_mode":"lead","navigation_mode":"header","media_mode":"editorial","typography_mode":"mixed","card_mode":"borderless","chrome_mode":"quiet","semantic_roles":{"accepts":["title"]},"layout_tokens":{"ground":"#101610","ink":"#f4f0e9","display":"#fff8e8","accent":"#dcaa56","muted":"#b8bba8","rule":"rgba(244,240,233,.16)","panel":"#1b2619","measure":"76rem","gap":"3rem","radius":"0px","section_space":"3rem"},"acceptance_contract":"measured profile fixture","cannot_infer":"profile is presentation only","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create site_presentation '{"site_id":"agriculture","host":"agriculture.aurekai.life","subject_address":"topic:agriculture","profile":"ghost:field-notes","territory":"life","navigation":["discover","community"],"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:lead","site_id":"agriculture","slot_name":"lead","ordinal":0,"extent":"page","accepts":["PublicationFeed"],"min_material_quality":"standard","max_items":8,"htmx_boundary":"agriculture-lead","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:weather","site_id":"agriculture","slot_name":"sidebar","ordinal":1,"extent":"section","accepts":["Weather"],"min_material_quality":"standard","max_items":1,"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:community","site_id":"agriculture","slot_name":"body","ordinal":2,"extent":"section","accepts":["Community"],"min_material_quality":"standard","max_items":8,"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:listen","site_id":"agriculture","slot_name":"media","ordinal":3,"extent":"section","accepts":["Listen"],"min_material_quality":"standard","max_items":1,"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:watch","site_id":"agriculture","slot_name":"feature","ordinal":4,"extent":"page","accepts":["Watch"],"min_material_quality":"standard","max_items":1,"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:journal","site_id":"agriculture","slot_id":"agriculture:lead","application_kind":"PublicationFeed","material_query":{"kind":"PublicationOccurrence"},"source_collection":"agriculture-journal","continuity_key":"agriculture:journal","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:weather","site_id":"agriculture","slot_id":"agriculture:weather","application_kind":"Weather","material_query":{"kind":"WeatherObservation"},"continuity_key":"agriculture:weather","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_body '{"community":"field-notes","subject_address":"topic:agriculture","title":"Field Notes","description":"Growers, field workers and neighbours in one human body.","visibility":"public","governance_scope":"public","moderation_policy":"accepted posts only","cannot_infer":"federation is not a human post","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_membership '{"membership":"field-notes:ada","community":"field-notes","member_subject":"person:ada","role":"member","state":"active","cannot_infer":"membership is not effect authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_post '{"post":"field-notes:rain","community":"field-notes","author_subject":"person:ada","title":"Rain watch","body":"Bring the soil samples before noon.","published_at":"2026-09-09T12:30:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"a human post is not a foreign publication","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:community","site_id":"agriculture","slot_id":"agriculture:community","application_kind":"Community","material_query":{"community":"field-notes"},"continuity_key":"agriculture:community","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create music_recording '{"recording_id":"field-song","work_ref":"work:field-song","title":"Before the Rain","primary_artist_ref":"artist:field-notes","cannot_infer":"a recording is not a release, right or listener session","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create media_program '{"program_id":"field-song:audio","subject_ref":"recording:field-song","media_kind":"audio","asset_ref":"/assets/field-song.mp3","publication_state":"published","cannot_infer":"a media program is not recording identity or playback state","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:listen","site_id":"agriculture","slot_id":"agriculture:listen","application_kind":"Listen","material_query":{"recording":"field-song"},"continuity_key":"agriculture:field-song","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create media_program '{"program_id":"field-report:watch","subject_ref":"report:field-rain","media_kind":"video","asset_ref":"/assets/field-report.mp4","poster_asset_ref":"/assets/field-report.jpg","transcript_ref":"/assets/field-report.vtt","chapters":[{"start_ms":0,"title":"Opening"}],"publication_state":"published","cannot_infer":"a media program is not subject identity or viewing authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:watch","site_id":"agriculture","slot_id":"agriculture:watch","application_kind":"Watch","material_query":{"program":"field-report:watch"},"continuity_key":"agriculture:field-report","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world '{"world":"field-notes","title":"Field Notes","character":"seasonal field exchange","lede":"A temporary public World over field reporting and shared observations.","sources":"fieldwire","grammars":"board|community|stage","cannot_infer":"a World is not a provider, offer or authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world_fragment '{"fragment_id":"field-notes:board","world":"field-notes","ordinal":0,"subject_address":"topic:agriculture","surface_grammar":"board","renderer_family":"pinry","renderer_variant":"masonry","extent":"section","source_projection":"published board relation","provenance":"Field references arranged by the World publisher.","cannot_infer":"a Board pin is not subject truth or action authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world_fragment '{"fragment_id":"field-notes:community","world":"field-notes","ordinal":1,"subject_address":"community:field-notes","surface_grammar":"community","renderer_family":"lemmy","renderer_variant":"thread","extent":"section","source_projection":"canonical human community","provenance":"Public human discussion admitted by the Community owner.","cannot_infer":"a community fragment is not federation or effect authority","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:board","site_id":"agriculture","slot_name":"gallery","ordinal":5,"extent":"section","accepts":["Board"],"min_material_quality":"standard","max_items":8,"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:board","site_id":"agriculture","slot_id":"agriculture:board","application_kind":"Board","material_query":{"world":"field-notes"},"continuity_key":"field-notes:board","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create semantic_slot '{"slot_id":"agriculture:board-missing-coordinate","site_id":"agriculture","slot_name":"footer_feature","ordinal":6,"extent":"section","accepts":["Board"],"min_material_quality":"standard","max_items":8,"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create application_binding '{"binding_id":"agriculture:board-missing-coordinate","site_id":"agriculture","slot_id":"agriculture:board-missing-coordinate","application_kind":"Board","material_query":{"not_world":"field-notes"},"continuity_key":"field-notes:board-missing-coordinate","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create site_program '{"program_id":"agriculture:v1","site_id":"agriculture","experience_programs":["Agriculture","Publication","Community","Board","Listen","Watch"],"application_bindings":["agriculture:journal","agriculture:weather","agriculture:community","agriculture:listen","agriculture:watch","agriculture:board","agriculture:board-missing-coordinate"],"renderer_preferences":{"profile":"ghost:field-notes"},"source_collections":["agriculture-journal"],"capability_programs":[],"actor_presentations":{},"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_PUBLIC_PROJECTION_DB="$projection_cache" "$web" serve --db "$matrix" --auth-db "$authdb" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"; attempt=0
until curl -sS -o /dev/null -H 'Host: agriculture.aurekai.life' "$base/healthz"; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 80 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; exit 1; }; sleep 0.05
done
page=$(curl -sS -H 'Host: agriculture.aurekai.life' -H 'Accept: text/html' "$base/_surface/site-program?site=agriculture")
printf '%s' "$page" | grep -q 'site-program-masthead'
printf '%s' "$page" | grep -q 'aurekai-site-program.css'
printf '%s' "$page" | grep -q 'data-bf-source="presentation_template"'
printf '%s' "$page" | grep -q -- '--bf-ground:#101610'
printf '%s' "$page" | grep -q 'site-program-nav'
printf '%s' "$page" | grep -q 'href="/discover">discover</a>'
printf '%s' "$page" | grep -q 'href="/community">community</a>'
printf '%s' "$page" | grep -q 'href="/assets/donor-pixelfed.css"'
printf '%s' "$page" | grep -q 'href="/assets/donor-lemmy.css"'
printf '%s' "$page" | grep -q 'href="/assets/donor-pinry.css"'
! printf '%s' "$page" | grep -q 'donor-mobilizon.css\|donor-zulip.css\|donor-nextcommerce.css'
printf '%s' "$page" | grep -q 'application-publication'
printf '%s' "$page" | grep -q 'data-application-kind="PublicationFeed"'
printf '%s' "$page" | grep -q 'Rain changes the harvest window'
printf '%s' "$page" | grep -q 'Growers meet at the field school'
printf '%s' "$page" | grep -q 'id="agriculture-lead"'
printf '%s' "$page" | grep -q 'No admitted weather observation'
printf '%s' "$page" | grep -q 'Field Notes\|Rain watch\|Bring the soil samples before noon\|1 active members'
printf '%s' "$page" | grep -q 'Before the Rain\|artist:field-notes\|<audio controls preload="metadata" src="/assets/field-song.mp3"'
printf '%s' "$page" | grep -q 'watch · published program\|report:field-rain\|<video controls preload="metadata" src="/assets/field-report.mp4" poster="/assets/field-report.jpg"\|Transcript'
printf '%s' "$page" | grep -q 'topic:agriculture\|community:field-notes\|Field references arranged by the World publisher\|/w/field-notes/board\|/w/field-notes/community'
printf '%s' "$page" | grep -q 'No published Board arrangement is available'
! printf '%s' "$page" | grep -q 'Unrelated global-feed item\|external-estate\|MCP\|Beckn\|<script'
! printf '%s' "$page" | grep -q 'card shadow-sm\|timeline-status-component'
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-site-program|agriculture|/_surface/site-program|site=agriculture';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-site-program|agriculture|/_surface/site-program|site=agriculture' AND dependency='cms:application_binding:agriculture:watch';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-site-program|agriculture|/_surface/site-program|site=agriculture' AND dependency='cms:source_collection:agriculture-journal';")" = 1 ]
cat > "$tmp/site-handoff.json" <<'JSON'
{"schema":"bonfyre-lowering-handoff.v1","delta_id":"delta:site:watch","identity":"cms:application_binding:agriculture:watch","candidate_kind":"application_binding","owner_ref":"CMS:application_binding:agriculture:watch","lowerer_ref":"SurfaceIR:application","truth":"dependency_cut_only"}
JSON
site_cut=$($web public-projection-lower --cache-db "$projection_cache" --handoff "$tmp/site-handoff.json")
printf '%s' "$site_cut" | grep -q '"invalidated":1'
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-site-program|agriculture|/_surface/site-program|site=agriculture';")" = 0 ]
page_after_cut=$(curl -sS -H 'Host: agriculture.aurekai.life' -H 'Accept: text/html' "$base/_surface/site-program?site=agriculture")
printf '%s' "$page_after_cut" | grep -q 'watch · published program\|report:field-rain'
apex=$(curl -sS -H 'Host: agriculture.aurekai.life' -H 'Accept: text/html' "$base/")
printf '%s' "$apex" | grep -q 'Rain changes the harvest window'
! printf '%s' "$apex" | grep -q 'Find the signals behind the moment\|<script'
board=$(curl -sS -H 'Host: agriculture.aurekai.life' -H 'Accept: text/html' "$base/w/field-notes/board")
printf '%s' "$board" | grep -q 'topic:agriculture\|field-notes'
! printf '%s' "$board" | grep -q 'This World has not published that interaction grammar'
machine=$(curl -sS -H 'Host: agriculture.aurekai.life' -H 'Accept: application/json' "$base/_surface/site-program?site=agriculture")
printf '%s' "$machine" | grep -q '"schema_version":"bonfyre.site_program.v1"'
! printf '%s' "$machine" | grep -q 'Rain changes the harvest window'
echo 'native SiteProgram: PASS'
