#!/bin/sh
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
compress="$root/cmd/BonfyreCompress/bonfyre-compress"
cd "$root"
tmp=$(mktemp -d)
mcp_pid=''
trap '[ -z "$mcp_pid" ] || kill "$mcp_pid" 2>/dev/null || true; [ -z "$mcp_pid" ] || wait "$mcp_pid" 2>/dev/null || true; rm -rf "$tmp"' EXIT
cue export ./schemas/control -e surfacePresentationCellSet --out json > "$tmp/cells.json"
"$bin" import --db "$tmp/estate.db" --occurrences "$root/estate/generated/external-estate-v4/observed-external-estate.jsonl" --cells "$tmp/cells.json" \
  --bindings "$root/cmd/BonfyreExperienceMap/generated/external_bindings.jsonl" \
  --stages "$root/cmd/BonfyreExperienceMap/generated/stage_bindings.jsonl" \
  --chains "$root/cmd/BonfyreExperienceMap/generated/chains.jsonl" | grep -q '4891'
# Temporal classification becomes an explicit forwarding dimension. The two
# corpus counts are owner-state receipts; `not_evident` contributes no
# temporal coordinate and can never be reinterpreted as static=false.
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM occurrence_temporal WHERE state='bearing';")" = "1156" ]
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM occurrence_temporal WHERE state='not_evident';")" = "3735" ]
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM occurrence_temporal t JOIN occurrence_term a ON a.occurrence_id=t.occurrence_id WHERE t.state='not_evident' AND a.axis='temporal';")" = "0" ]
"$bin" address --db "$tmp/estate.db" --need temporal:feed --limit 64 > "$tmp/address-temporal-feed.json"
grep -q '"truth":"eligible_for_further_consideration"' "$tmp/address-temporal-feed.json"
feed_returned=$(sed -n 's/.*"returned":\([0-9][0-9]*\).*/\1/p' "$tmp/address-temporal-feed.json")
[ "$feed_returned" -gt 0 ]
[ "$feed_returned" -lt 4891 ]
"$bin" address --db "$tmp/estate.db" --need temporal:forecast --limit 64 > "$tmp/address-temporal-forecast.json"
forecast_returned=$(sed -n 's/.*"returned":\([0-9][0-9]*\).*/\1/p' "$tmp/address-temporal-forecast.json")
[ "$forecast_returned" -gt 0 ]
! cmp -s "$tmp/address-temporal-feed.json" "$tmp/address-temporal-forecast.json"
# Broad reuse is compiler-planning material, not a human query index. This
# witness pins the collision that previously made a health API appear under a
# weather address merely because the saturated candidate graph could reuse it.
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM occurrence o JOIN occurrence_term t ON t.occurrence_id=o.id WHERE o.observed_name='Cure Cancer With AI' AND t.axis='domain' AND t.term='weather';")" = "0" ]
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM occurrence o JOIN occurrence_term t ON t.occurrence_id=o.id WHERE o.observed_name='Open-Meteo' AND t.axis='domain' AND t.term='weather';")" -ge 1 ]
# Every imported occurrence participates in the same native forwarding index.
# This is a deterministic prefilter only: it cannot turn a weather candidate
# into evidence/provider/authority, and a prohibited coordinate wins over a
# positive mask match.
"$bin" address --db "$tmp/estate.db" --need domain:weather --limit 64 > "$tmp/address.json"
grep -q '"considered":4891' "$tmp/address.json"
grep -q '"mask_words":1' "$tmp/address.json"
grep -q '"truth":"eligible_for_further_consideration"' "$tmp/address.json"
grep -q 'Open-Meteo' "$tmp/address.json"
! grep -q 'Cure Cancer With AI' "$tmp/address.json"
# ExternalEstate's 4,891 canonical rows also compile into the shared typed
# candidate table, without copying their body or changing their candidate-only
# truth level.
"$bin" forward --db "$tmp/estate.db" --kind external_occurrence --need domain:weather --limit 64 > "$tmp/forward-external.json"
grep -q '"considered":4891' "$tmp/forward-external.json"
grep -q 'Open-Meteo' "$tmp/forward-external.json" || grep -q 'external:ext-' "$tmp/forward-external.json"
"$bin" address --db "$tmp/estate.db" --need domain:weather --forbid source_kind:public_api --limit 64 > "$tmp/address-forbid.json"
! grep -q 'Open-Meteo' "$tmp/address-forbid.json"
"$bin" address --db "$tmp/estate.db" --need domain:imaginary-universe > "$tmp/address-none.json"
grep -q '"reason":"unknown_coordinate"' "$tmp/address-none.json"
# The generic forwarding plane is shared physically but remains type-partitioned.
# These are compiler-intake fixtures, not production state: they prove that an
# overlapping address cannot let a renderer leak into a provider request.
printf '%s\n' \
  '{"identity":"cms:deep-ellum","candidate_kind":"site_program","owner_ref":"CMS:site_presentation:deep-ellum","exact_verifier":"CMS:publication","lowerer_ref":"SurfaceIR:site_program","validity_frontier":"cms:deep-ellum:published","terms":["world:deep-ellum","territory:live","extent:page"],"dependencies":["cms:deep-ellum","world:deep-ellum"]}' \
  '{"identity":"provider:deep-ellum-media","candidate_kind":"provider","owner_ref":"ProviderState:deep-ellum-media","exact_verifier":"ProviderState:availability","lowerer_ref":"ResourceAdmission:provider","validity_frontier":"provider:deep-ellum-media:weather","terms":["world:deep-ellum","territory:live","operation:transcode"],"dependencies":["provider:deep-ellum-media:weather"]}' \
  > "$tmp/advertisements.jsonl"
sed -n '1p' "$tmp/advertisements.jsonl" > "$tmp/candidate.json"
cue vet "$root/schemas/control/address-advertisement.cue" "$tmp/candidate.json" -d '#CandidateAdvertisement'
"$bin" import-advertisements --db "$tmp/estate.db" --advertisements "$tmp/advertisements.jsonl" | grep -q '"imported_candidate_advertisements":2'
"$bin" forward --db "$tmp/estate.db" --kind site_program --need world:deep-ellum --need extent:page > "$tmp/forward-site.json"
grep -q 'cms:deep-ellum' "$tmp/forward-site.json"
! grep -q 'provider:deep-ellum-media' "$tmp/forward-site.json"
"$bin" forward --db "$tmp/estate.db" --kind provider --need world:deep-ellum > "$tmp/forward-provider.json"
grep -q 'provider:deep-ellum-media' "$tmp/forward-provider.json"
! grep -q 'cms:deep-ellum' "$tmp/forward-provider.json"
"$bin" affected --db "$tmp/estate.db" --write world:deep-ellum > "$tmp/affected-world.json"
grep -q 'cms:deep-ellum' "$tmp/affected-world.json"
! grep -q 'provider:deep-ellum-media' "$tmp/affected-world.json"
"$bin" affected --db "$tmp/estate.db" --write provider:deep-ellum-media:weather > "$tmp/affected-provider.json"
grep -q 'provider:deep-ellum-media' "$tmp/affected-provider.json"
! grep -q 'cms:deep-ellum' "$tmp/affected-provider.json"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:deep-ellum","writes":["world:deep-ellum","unrelated:write"]}' > "$tmp/fact-delta.json"
"$bin" fact-delta --db "$tmp/estate.db" --delta "$tmp/fact-delta.json" > "$tmp/fact-cut.json"
grep -q '"delta_id":"delta:deep-ellum"' "$tmp/fact-cut.json"
grep -q 'cms:deep-ellum' "$tmp/fact-cut.json"
! grep -q 'provider:deep-ellum-media' "$tmp/fact-cut.json"
grep -q '"lowering_state":"queued"' "$tmp/fact-cut.json"
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:deep-ellum';")" = "1" ]
"$bin" fact-delta --db "$tmp/estate.db" --delta "$tmp/fact-delta.json" > "$tmp/fact-dedup.json"
grep -q '"state":"already_processed"' "$tmp/fact-dedup.json"
mkdir "$tmp/deltas"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:provider","writes":["provider:deep-ellum-media:weather"]}' > "$tmp/deltas/provider.json"
"$bin" drain-fact-deltas --db "$tmp/estate.db" --dir "$tmp/deltas" --limit 8 > "$tmp/drain.json"
grep -q '"fact_delta_files_seen":1' "$tmp/drain.json"
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:provider';")" = "1" ]
# ForeignTwin’s durable re-observation delta names the observed topic.  The
# external population carries that declared topic dependency alongside its
# exact ExternalEstate identity, so one changed source wakes only its own
# material compiler residue.
foreign_id=$(sqlite3 "$tmp/estate.db" "SELECT id FROM occurrence ORDER BY id LIMIT 1")
foreign_topic=$(sqlite3 "$tmp/estate.db" "SELECT primary_url FROM occurrence WHERE id='$foreign_id'")
printf '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:foreign","writes":["ForeignTwin:%s"]}\n' "$foreign_topic" > "$tmp/foreign-delta.json"
"$bin" fact-delta --db "$tmp/estate.db" --delta "$tmp/foreign-delta.json" > "$tmp/foreign-cut.json"
grep -q "\"identity\":\"$foreign_id\"" "$tmp/foreign-cut.json"
[ "$(sqlite3 "$tmp/estate.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:foreign' AND identity='$foreign_id' AND lowerer_ref='MaterialCompiler:external';")" = "1" ]
! "$bin" forward --db "$tmp/estate.db" --kind not_a_kind --need world:deep-ellum >/dev/null 2>&1
# Owner emitters compile actual owner rows into the one interchange format;
# the following tiny databases stand in for their independently owned stores.
# No emitter receives a semantic selection from this test.
sqlite3 "$tmp/cms.db" "CREATE TABLE site_presentation(site_id TEXT,subject_address TEXT,territory TEXT,profile TEXT,status TEXT); CREATE TABLE semantic_slot(slot_id TEXT,site_id TEXT,slot_name TEXT,extent TEXT,status TEXT); CREATE TABLE application_binding(binding_id TEXT,site_id TEXT,slot_id TEXT,application_kind TEXT,source_collection TEXT,status TEXT); CREATE TABLE source_collection(collection_id TEXT,subject_address TEXT,status TEXT); CREATE TABLE world(world TEXT,subject TEXT,character TEXT,grammars TEXT,sources TEXT,status TEXT); CREATE TABLE internet_source(id INTEGER PRIMARY KEY,source TEXT,family TEXT,kind TEXT,tier TEXT,url TEXT,status TEXT); CREATE TABLE surface_renderer(renderer TEXT,canonical_name TEXT,grammar TEXT,extents TEXT,status TEXT); INSERT INTO site_presentation VALUES('deep-ellum','world:deep-ellum','live','ghost:edition','published'); INSERT INTO semantic_slot VALUES('hero','deep-ellum','hero','page','published'); INSERT INTO application_binding VALUES('watch-main','deep-ellum','hero','Watch','deep-ellum-media','published'); INSERT INTO source_collection VALUES('deep-ellum-media','world:deep-ellum','published'); INSERT INTO world VALUES('deep-ellum','world:deep-ellum','nightlife','discover','deep-ellum-feed','published'); INSERT INTO internet_source VALUES(1,'deep-ellum-feed','publication','rss','warm','https://example.test/deep-ellum.rss','published'); INSERT INTO surface_renderer VALUES('pixelfed-vertical','Pixelfed Vertical','temporal','section','published');"
sqlite3 "$tmp/targets.db" "CREATE TABLE target_obligations(target_id TEXT,source_frontier TEXT,semantic_contract TEXT,state TEXT); INSERT INTO target_obligations VALUES('reserve:deep-ellum','proof:venue','reservation','OPEN');"
sqlite3 "$tmp/providers.db" "CREATE TABLE provider_materializations(id INTEGER,provider TEXT,kind TEXT,purpose TEXT,scope TEXT,expires_at TEXT,released_at TEXT); CREATE TABLE provider_embedding_run(run_id TEXT,provider TEXT,model TEXT,representation_id TEXT,input_digest TEXT,requested_count INTEGER,stored_count INTEGER,dimensions INTEGER,status TEXT,completed_at TEXT); INSERT INTO provider_materializations VALUES(7,'media-edge','video','transcode','world:deep-ellum','2030-01-01T00:00:00Z',NULL); INSERT INTO provider_embedding_run VALUES('embedding:mistral:fixture','mistral','mistral-embed','mistral-embed-v1','sha256:provider-fixture',2,2,1024,'complete','2026-09-15T00:00:00Z');"
sqlite3 "$tmp/models.db" "CREATE TABLE models(id TEXT,format TEXT,geometry TEXT,transform_family TEXT,fpq_sha256 TEXT); CREATE TABLE model_representation_passport(representation_id TEXT,model_id TEXT,representation_family TEXT,representation_version TEXT,exactness_class TEXT,measurement_state TEXT,admission_state TEXT,admission_scope TEXT,receipt_digest TEXT); INSERT INTO models VALUES('bitnet-embed','gguf','short-form','T16','abc123'); INSERT INTO model_representation_passport VALUES('qwen-native-f16','bitnet-embed','native-f16-passthrough','FPQ0-v10-runtime-baseline','exact','measured','DEFER','generation','sha256:receipt-qwen');"
printf '%s\n' '{"tools":[{"name":"world_events"}],"resources":[{"uri":"bonfyre://world/deep-ellum/events"}],"resourceTemplates":[{"uriTemplate":"bonfyre://world/{world}/events"}],"prompts":[{"name":"operate_world"}]}' > "$tmp/mcp-closure.json"
# The dynamic MCP path reads only the authenticated protocol aperture.  It
# neither imports gateway code nor sees a bearer secret in its artifact.
cat > "$tmp/mcp_gateway.py" <<'PY'
import http.server, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
class Gateway(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_): pass
    def do_POST(self):
        if self.headers.get('Authorization') != 'Bearer test-mcp-token':
            self.send_response(401); self.end_headers(); return
        if self.headers.get('MCP-Protocol-Version') != '2026-07-28':
            self.send_response(400); self.end_headers(); return
        n = int(self.headers.get('Content-Length', '0'))
        req = json.loads(self.rfile.read(n))
        if self.headers.get('Mcp-Method') != req.get('method') or self.headers.get('Mcp-Name') != req.get('method'):
            self.send_response(400); self.end_headers(); return
        if not req.get('params', {}).get('_meta', {}).get('io.modelcontextprotocol/clientInfo'):
            self.send_response(400); self.end_headers(); return
        values = {
          'tools/list': {'tools':[{'name':'live_world_events'}]},
          'resources/list': {'resources':[{'uri':'bonfyre://world/live/events'}]},
          'resources/templates/list': {'resourceTemplates':[{'uriTemplate':'bonfyre://world/{world}/events'}]},
          'prompts/list': {'prompts':[{'name':'operate_live_world'}]},
        }
        body = json.dumps({'jsonrpc':'2.0','id':req.get('id'),'result':values[req['method']]}).encode()
        self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
with http.server.ThreadingHTTPServer(('127.0.0.1',0), Gateway) as server:
    (root/'mcp-port').write_text(str(server.server_address[1]))
    server.serve_forever()
PY
python3 "$tmp/mcp_gateway.py" "$tmp" &
mcp_pid=$!
for _ in $(seq 1 50); do [ -s "$tmp/mcp-port" ] && break; sleep 0.05; done
[ -s "$tmp/mcp-port" ]
printf '%s\n' 'test-mcp-token' > "$tmp/mcp-token"
mcp_url="http://127.0.0.1:$(cat "$tmp/mcp-port")/mcp"
"$bin" refresh-mcp-closure --mcp-url "$mcp_url" --mcp-token-file "$tmp/mcp-token" --out "$tmp/live-mcp-closure.json" > "$tmp/mcp-refresh.json"
grep -q '"mcp_closure":"refreshed"' "$tmp/mcp-refresh.json"
grep -q 'live_world_events' "$tmp/live-mcp-closure.json"
"$bin" compile-advertisements --db "$tmp/live-mcp-index.db" --mcp-url "$mcp_url" --mcp-token-file "$tmp/mcp-token" > "$tmp/live-mcp-compile.json"
"$bin" forward --db "$tmp/live-mcp-index.db" --kind mcp_projection --need mcp:tool > "$tmp/live-mcp-forward.json"
grep -q 'mcp:tool:live_world_events' "$tmp/live-mcp-forward.json"
# A published SourceCollection, CapabilityProgram and SiteProgram each declare
# more than an identity and a label: which domains and sources a collection
# draws on and how often it refreshes, what a capability consumes and produces,
# which carriers may carry it and what authority it demands, and which parts a
# site program composes.  The forwarding plane is asked those questions
# directly, so flattening them to id-plus-label made the plane unable to answer
# what these rows exist to say.  Cannot infer: a preserved carrier term is a
# declared candidate, never a running protocol; a preserved authority term is a
# stated requirement, never a grant; a preserved consumes term is a declared
# input, never an admitted resource.
sqlite3 "$tmp/cms.db" "ALTER TABLE source_collection ADD COLUMN source_selector TEXT; ALTER TABLE source_collection ADD COLUMN material_kind TEXT; ALTER TABLE source_collection ADD COLUMN refresh_policy TEXT; UPDATE source_collection SET source_selector='{\"domains\":[\"nightlife\"],\"source_refs\":[\"deep-ellum-feed\"]}', material_kind='[\"Watch\",\"Article\"]', refresh_policy='cms:refresh:hourly' WHERE collection_id='deep-ellum-media'; CREATE TABLE capability_program(program_id TEXT,application_kind TEXT,action TEXT,consumes TEXT,produces TEXT,carrier_candidates TEXT,capability_requirements TEXT,authority_requirement TEXT,transition_conditions TEXT,status TEXT); INSERT INTO capability_program VALUES('book-room','Shop','reserve','[\"subject:venue:foundry-room\"]','[\"commitment:event-booking\"]','[\"mcp\",\"a2a\",\"beckn\"]','[\"resource:room-admission\"]','venue.booking','[\"evidence:deposit-received\"]','published'); CREATE TABLE site_program(program_id TEXT,site_id TEXT,experience_programs TEXT,application_bindings TEXT,source_collections TEXT,capability_programs TEXT,status TEXT); INSERT INTO site_program VALUES('deep-ellum-live','deep-ellum','[\"discover\"]','[\"watch-main\"]','[\"deep-ellum-media\"]','[\"book-room\"]','published');"
# PackGraph and RelationGraph are owner populations, not surface decoration.
# A closure is built from activations alone, so an activation is what is
# compiled; a pack declares demand and does not fetch, so the demand is
# compiled as a declaration.  A membership names a relationship and settles
# nothing about disclosure or authority, so its authority reference is carried
# as a dependency edge and never as a term.
sqlite3 "$tmp/cms.db" "CREATE TABLE pack_head(pack TEXT,channel TEXT,version TEXT,advanced_by TEXT,advanced_at TEXT,status TEXT); CREATE TABLE pack_dependency(pack TEXT,depends_on TEXT,pinned_version TEXT,channel TEXT,why TEXT,status TEXT); CREATE TABLE pack_activation(pack TEXT,activated_for TEXT,scope TEXT,activated_by TEXT,conditions TEXT,status TEXT); CREATE TABLE pack_demand(pack TEXT,demand_kind TEXT,needs TEXT,preferred_sources TEXT,fields TEXT,body_regime TEXT,reheat_when TEXT,optional TEXT,status TEXT); CREATE TABLE community_body(community TEXT,subject_address TEXT,title TEXT,visibility TEXT,cannot_infer TEXT,status TEXT); CREATE TABLE community_membership(membership TEXT,community TEXT,member_subject TEXT,role TEXT,state TEXT,authority_ref TEXT,cannot_infer TEXT,status TEXT); CREATE TABLE community_presence(presence TEXT,community TEXT,actor_subject TEXT,surface TEXT,disclosure TEXT,availability TEXT,attention TEXT,observed_at TEXT,expires_at TEXT,cannot_infer TEXT,status TEXT); INSERT INTO pack_head VALUES('deep-ellum-nightlife','official','3','curator','2026-09-01','published'); INSERT INTO pack_head VALUES('nightlife-base','official','2','curator','2026-08-01','published'); INSERT INTO pack_dependency VALUES('deep-ellum-nightlife','nightlife-base','2','official','the fork keeps its parent rather than copying it','published'); INSERT INTO pack_activation VALUES('deep-ellum-nightlife','world:deep-ellum','world:deep-ellum','curator','[\"venue-open\"]','published'); INSERT INTO pack_demand VALUES('deep-ellum-nightlife','internet','stage-times','[\"deep-ellum-feed\"]','[\"set_time\"]','remote','cms:refresh:hourly','0','published'); INSERT INTO community_body VALUES('deep-ellum-crew','world:deep-ellum','Deep Ellum Crew','group','a community is not a pack','published'); INSERT INTO community_membership VALUES('crew-001','deep-ellum-crew','actor:ron','promoter','active','authority:venue.booking','membership is not authority','published'); INSERT INTO community_presence VALUES('crew-ron-here','deep-ellum-crew','actor:ron','community','public','available','following','2026-09-12T21:00:00Z','2026-09-12T23:00:00Z','presence is not attendance or authority','published');"
# Space is another exact coordinate on an existing subject. Two real fixes and
# the display position computed between them: the interpolated one is a
# legitimate record and is not a third fix, and it names the observations it was
# derived from so the two stay distinguishable after the fact. A camera's
# coverage is a separate claim about reach, carrying how it was arrived at,
# because a published specification and a measured footprint are not the same.
sqlite3 "$tmp/cms.db" "CREATE TABLE spatial_observation(observation_id TEXT,subject_address TEXT,geometry_kind TEXT,geometry TEXT,crs TEXT,horizontal_datum TEXT,observed_at TEXT,source_occurrence TEXT,observation_mode TEXT,derived_from TEXT,mobility TEXT,scale TEXT,region TEXT,cell TEXT,provenance TEXT,status TEXT); CREATE TABLE coverage_volume(coverage_id TEXT,owner_subject TEXT,origin TEXT,heading TEXT,range_m TEXT,scale TEXT,region TEXT,cell TEXT,valid_frontier TEXT,quality TEXT,derivation TEXT,status TEXT); INSERT INTO spatial_observation VALUES('fix-a','subject:aircraft:abc123','point','POINT(-96.78 32.78)','EPSG:4326','WGS84','2026-09-12T22:00:00Z','external:ext-00001','observed','[]','moving','city','dfw','dfw-0912','opensky','published'); INSERT INTO spatial_observation VALUES('fix-b','subject:aircraft:abc123','point','POINT(-96.77 32.79)','EPSG:4326','WGS84','2026-09-12T22:00:30Z','external:ext-00001','observed','[]','moving','city','dfw','dfw-0912','opensky','published'); INSERT INTO spatial_observation VALUES('display-1','subject:aircraft:abc123','point','POINT(-96.775 32.785)','EPSG:4326','WGS84','2026-09-12T22:00:17Z','external:ext-00001','interpolated','[\"fix-a\",\"fix-b\"]','moving','city','dfw','dfw-0912','bonfyre-dead-reckoning','published'); INSERT INTO coverage_volume VALUES('cam-foundry','subject:camera:foundry-entrance','POINT(-96.78 32.78)','215','120','street','dfw','dfw-0912','cms:coverage_volume:cam-foundry','degraded','specified','published');"
"$bin" emit-cms --cms-db "$tmp/cms.db" > "$tmp/cms-ads.jsonl"
"$bin" emit-targets --target-db "$tmp/targets.db" > "$tmp/target-ads.jsonl"
"$bin" emit-providers --provider-db "$tmp/providers.db" > "$tmp/provider-ads.jsonl"
"$bin" emit-compute --model-db "$tmp/models.db" > "$tmp/compute-ads.jsonl"
"$bin" emit-mcp --mcp-closure "$tmp/mcp-closure.json" > "$tmp/mcp-ads.jsonl"
"$bin" emit-protocols --protocols "$root/cmd/BonfyreExperienceMap/generated/protocols.jsonl" > "$tmp/protocol-ads.jsonl"
"$bin" emit-policy --policy-json "$root/cmd/BonfyreWebAddress/generated/native-domain-profiles.json" > "$tmp/policy-ads.jsonl"
grep -q 'CMS:site_presentation:deep-ellum' "$tmp/cms-ads.jsonl"
grep -q 'CMS:semantic_slot:hero' "$tmp/cms-ads.jsonl"
grep -q 'cms:source_collection:deep-ellum-media' "$tmp/cms-ads.jsonl"
grep -q 'cms:capability_program:book-room' "$tmp/cms-ads.jsonl"
grep -q 'cms:site_program:deep-ellum-live' "$tmp/cms-ads.jsonl"
grep -q '"domain:nightlife"' "$tmp/cms-ads.jsonl"
grep -q '"material:watch"' "$tmp/cms-ads.jsonl"
grep -q '"refresh:cms-refresh-hourly"' "$tmp/cms-ads.jsonl"
grep -q '"consumes:subject-venue-foundry-room"' "$tmp/cms-ads.jsonl"
grep -q '"produces:commitment-event-booking"' "$tmp/cms-ads.jsonl"
grep -q '"carrier:beckn"' "$tmp/cms-ads.jsonl"
grep -q '"requirement:resource-room-admission"' "$tmp/cms-ads.jsonl"
grep -q '"authority:venue-booking"' "$tmp/cms-ads.jsonl"
grep -q '"transition:evidence-deposit-received"' "$tmp/cms-ads.jsonl"
grep -q '"capability_program:book-room"' "$tmp/cms-ads.jsonl"
grep -q 'ForeignTwin:https://example.test/deep-ellum.rss' "$tmp/cms-ads.jsonl"
grep -q 'cms:publication_source:deep-ellum-feed' "$tmp/cms-ads.jsonl"
grep -q 'CMS:surface_renderer:pixelfed-vertical' "$tmp/cms-ads.jsonl"
grep -q 'TargetObligation:reserve:deep-ellum' "$tmp/target-ads.jsonl"
grep -q 'ProviderMaterialization:7' "$tmp/provider-ads.jsonl"
grep -q 'ProviderEmbeddingRun:embedding:mistral:fixture' "$tmp/provider-ads.jsonl"
grep -q '"operation:embedding"' "$tmp/provider-ads.jsonl"
grep -q 'ModelCatalog:bitnet-embed' "$tmp/compute-ads.jsonl"
grep -q 'ModelCatalog:representation:qwen-native-f16' "$tmp/compute-ads.jsonl"
grep -q '"operation:generation"' "$tmp/compute-ads.jsonl"
grep -q '"admission:defer"' "$tmp/compute-ads.jsonl"
grep -q 'MCPClosure:tool:world_events' "$tmp/mcp-ads.jsonl"
grep -q 'ProtocolCommons:x402' "$tmp/protocol-ads.jsonl"
grep -q 'CMS:pack_activation:deep-ellum-nightlife' "$tmp/cms-ads.jsonl"
grep -q '"condition:venue-open"' "$tmp/cms-ads.jsonl"
grep -q 'cms:pack_head:nightlife-base' "$tmp/cms-ads.jsonl"
grep -q 'CMS:pack_demand:deep-ellum-nightlife' "$tmp/cms-ads.jsonl"
grep -q '"regime:remote"' "$tmp/cms-ads.jsonl"
grep -q 'CMS:community_membership:crew-001' "$tmp/cms-ads.jsonl"
grep -q 'CMS:community_presence:crew-ron-here' "$tmp/cms-ads.jsonl"
grep '"identity":"cms:community_presence:crew-ron-here"' "$tmp/cms-ads.jsonl" | grep -q '"community:deep-ellum-crew"'
grep '"identity":"cms:community_presence:crew-ron-here"' "$tmp/cms-ads.jsonl" | grep -q '"surface:community"'
grep '"identity":"cms:community_presence:crew-ron-here"' "$tmp/cms-ads.jsonl" | grep -q 'cms:community_membership:crew-001'
! grep '"identity":"cms:community_presence:crew-ron-here"' "$tmp/cms-ads.jsonl" | grep -q 'actor-ron\|attention\|availability'
grep -q 'CMS:spatial_observation:fix-a' "$tmp/cms-ads.jsonl"
grep -q 'CMS:coverage_volume:cam-foundry' "$tmp/cms-ads.jsonl"
grep -q '"cell:dfw-0912"' "$tmp/cms-ads.jsonl"
grep -q '"mobility:moving"' "$tmp/cms-ads.jsonl"
grep -q '"derivation:specified"' "$tmp/cms-ads.jsonl"
# Exact geometry, CRS and datum never become coordinates. An exact spatial
# question is answered by the spatial owner on whatever survives the cut; a
# plane carrying geometry would answer it cheaply and badly.
! grep -q 'POINT(' "$tmp/cms-ads.jsonl"
! grep -qi '"crs:\|epsg\|wgs84' "$tmp/cms-ads.jsonl"
# A derived position names the fixes it was computed from, so the two remain
# distinguishable after the fact rather than only at the moment of writing.
grep '"identity":"cms:spatial_observation:display-1"' "$tmp/cms-ads.jsonl" | grep -q 'cms:spatial_observation:fix-a'
grep -q '"role:promoter"' "$tmp/cms-ads.jsonl"
# The membership's authority reference is a dependency edge and never a term:
# a term would let the forwarding plane read a relation as a grant.
grep -q '"dependencies":\[[^]]*"authority:venue.booking"' "$tmp/cms-ads.jsonl"
! grep '"identity":"cms:community_membership:crew-001"' "$tmp/cms-ads.jsonl" | grep -q '"authority:'
# InformationPolicy compiles the shape of a declaration, audience by audience.
# The parcel owner mailing address is declared to self and institution only, so
# it is absent from the market and public candidates rather than marked there:
# the forwarding cut is default-deny in the same direction as the evaluator.
grep -q 'InformationPolicy:parcel' "$tmp/policy-ads.jsonl"
grep -q '"audience:public"' "$tmp/policy-ads.jsonl"
grep -q '"evidence:assessment-roll-receipt"' "$tmp/policy-ads.jsonl"
grep '"identity":"policy:parcel:institution"' "$tmp/policy-ads.jsonl" | grep -q '"field:owner-mailing-address"'
! grep '"identity":"policy:parcel:public"' "$tmp/policy-ads.jsonl" | grep -q '"field:owner-mailing-address"'
! grep '"identity":"policy:parcel:market"' "$tmp/policy-ads.jsonl" | grep -q '"field:owner-mailing-address"'
# Every emitted advertisement in the real population is held to the declared
# interchange schema, not one hand-written sample beside it.
cat "$tmp/cms-ads.jsonl" "$tmp/target-ads.jsonl" "$tmp/provider-ads.jsonl" "$tmp/compute-ads.jsonl" "$tmp/mcp-ads.jsonl" "$tmp/protocol-ads.jsonl" "$tmp/policy-ads.jsonl" \
  | awk 'BEGIN{printf "["} NF{if(c++)printf ","; printf "%s",$0} END{printf "]\n"}' > "$tmp/population.json"
cue vet "$root/schemas/control/address-advertisement.cue" "$tmp/population.json" -d '#CandidateAdvertisementSet'
# The kind and axis vocabularies exist in two places -- the CUE declaration and
# the native plane.  Two copies that are never compared is how a plane starts
# answering with terms no owner meant, so they are compared.
"$bin" kinds > "$tmp/vocabulary.txt"
awk '$1=="kind"{print $2}' "$tmp/vocabulary.txt" | sort > "$tmp/native-kinds.txt"
awk '$1=="axis"{print $2}' "$tmp/vocabulary.txt" | sort > "$tmp/native-axes.txt"
grep '^#CandidateKind:' "$root/schemas/control/address-advertisement.cue" | sed 's/^#CandidateKind://' | tr '|' '\n' | tr -d ' "' | grep -v '^$' | sort > "$tmp/cue-kinds.txt"
sed -n 's/^#AddressTerm: =~"^(\([^)]*\)):.*/\1/p' "$root/schemas/control/address-advertisement.cue" | tr '|' '\n' | grep -v '^$' | sort > "$tmp/cue-axes.txt"
cmp -s "$tmp/native-kinds.txt" "$tmp/cue-kinds.txt"
cmp -s "$tmp/native-axes.txt" "$tmp/cue-axes.txt"
# The axis vocabulary is declared twice in CUE -- once as #AddressAxis and once
# inside the #AddressTerm pattern -- and only the pattern was ever compared with
# the native plane. Two declarations of one vocabulary with one of them
# unchecked is the same drift this comparison exists to prevent, one level in.
# The enum is authored across several lines with comments between them, so it
# is read through cue's own normaliser rather than by pattern-matching source.
cue def ./schemas/control 2>/dev/null | grep '^#AddressAxis:' \
  | sed 's/^#AddressAxis://' | tr '|' '\n' | tr -d ' "' | grep -v '^$' | sort \
  > "$tmp/cue-axis-enum.txt"
cmp -s "$tmp/cue-axis-enum.txt" "$tmp/cue-axes.txt"
# The native generation compiler preflights all owner output, then swaps the
# selected populations together.  A later empty CMS generation retires its
# stale rows without touching MCP/protocol/provider populations.
"$bin" compile-advertisements --db "$tmp/compiled-index.db" --cms-db "$tmp/cms.db" --target-db "$tmp/targets.db" --provider-db "$tmp/providers.db" --model-db "$tmp/models.db" --protocols "$root/cmd/BonfyreExperienceMap/generated/protocols.jsonl" --mcp-closure "$tmp/mcp-closure.json" --policy-json "$root/cmd/BonfyreWebAddress/generated/native-domain-profiles.json" > "$tmp/compiled-generation.json"
grep -q '"compiled_owner_populations":7' "$tmp/compiled-generation.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind site_presentation --need subject:world-deep-ellum > "$tmp/compiled-cms.json"
grep -q 'cms:site_presentation:deep-ellum' "$tmp/compiled-cms.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind mcp_projection --need mcp:tool > "$tmp/compiled-mcp.json"
grep -q 'mcp:tool:world_events' "$tmp/compiled-mcp.json"
# The preserved terms are what the plane cuts on.  A carrier question, an
# authority question and a composition question each reach the one published
# row that declared the answer, and a carrier nobody declared selects nothing.
"$bin" forward --db "$tmp/compiled-index.db" --kind capability_program --need carrier:mcp --need authority:venue-booking > "$tmp/compiled-capability.json"
grep -q 'cms:capability_program:book-room' "$tmp/compiled-capability.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind capability_program --need consumes:subject-venue-foundry-room --need produces:commitment-event-booking > "$tmp/compiled-capability-flow.json"
grep -q 'cms:capability_program:book-room' "$tmp/compiled-capability-flow.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind capability_program --need carrier:x402 > "$tmp/compiled-capability-x402.json"
! grep -q 'cms:capability_program:book-room' "$tmp/compiled-capability-x402.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind site_program --need capability_program:book-room --need source_collection:deep-ellum-media > "$tmp/compiled-site-program.json"
grep -q 'cms:site_program:deep-ellum-live' "$tmp/compiled-site-program.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind source_collection --need domain:nightlife --need material:watch > "$tmp/compiled-collection.json"
grep -q 'cms:source_collection:deep-ellum-media' "$tmp/compiled-collection.json"
# A forwarding generation may be published only through a current
# OwnerContractImage. This test fixture is deliberately minimal and is used
# only to exercise the freshness gate; it is not owner state or an invocation
# authorization. A malformed image must leave the published generation alone.
printf '%s\n' '{"schema":"bonfyre-owner-contract-image.v1","state":"compiled_reference_image","interactions":{"unresolved":1},"capability_registry":{"realized_unmapped":0}}' > "$tmp/owner-image-invalid.json"
! "$bin" address-worker --db "$tmp/compiled-index.db" --model-db "$tmp/models.db" --owner-contract-image "$tmp/owner-image-invalid.json" > "$tmp/owner-image-invalid.out"
printf '%s\n' '{"schema":"bonfyre-owner-contract-image.v1","state":"compiled_reference_image","interactions":{"unresolved":0},"capability_registry":{"realized_unmapped":0}}' > "$tmp/owner-image-valid.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --model-db "$tmp/models.db" --owner-contract-image "$tmp/owner-image-valid.json" > "$tmp/owner-image-valid.out"
grep -q '"owner_contract_image":true' "$tmp/owner-image-valid.out"
# Owner-generation receipts are emitted before the worker captures its
# RepresentationPassport snapshot.  They must not leak through the forked
# stdio buffer into that JSONL snapshot, or PhysicalCompiler correctly rejects
# the receipt as a non-passport declaration.
"$bin" address-worker --db "$tmp/compiled-index.db" --model-db "$tmp/models.db" --physical-db "$tmp/owner-worker-physical.sqlite" --physical-compiler "$compress" --limit 8 > "$tmp/owner-worker-physical.out"
grep -q '"published_generation":true' "$tmp/owner-worker-physical.out"
grep -q '"physical_registry":true' "$tmp/owner-worker-physical.out"
[ "$(sqlite3 "$tmp/owner-worker-physical.sqlite" 'SELECT count(*) FROM representation_passport;')" -gt 0 ]
# PackGraph, RelationGraph and InformationPolicy now answer from the same cut.
"$bin" forward --db "$tmp/compiled-index.db" --kind pack --need pack:deep-ellum-nightlife --need scope:world-deep-ellum > "$tmp/compiled-pack.json"
grep -q 'cms:pack_activation:deep-ellum-nightlife' "$tmp/compiled-pack.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind pack_demand --need regime:remote --need demand:internet > "$tmp/compiled-pack-demand.json"
grep -q 'cms:pack_demand:deep-ellum-nightlife' "$tmp/compiled-pack-demand.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind relation --need community:deep-ellum-crew --need role:promoter > "$tmp/compiled-relation.json"
grep -q 'cms:community_membership:crew-001' "$tmp/compiled-relation.json"
# A relation is not a grant: no authority coordinate reaches the membership.
"$bin" forward --db "$tmp/compiled-index.db" --kind relation --need authority:venue-booking > "$tmp/compiled-relation-authority.json"
! grep -q 'cms:community_membership:crew-001' "$tmp/compiled-relation-authority.json"
# Default-deny survives the cut: the field the policy declares only to self and
# institution is not reachable by asking as the public.
"$bin" forward --db "$tmp/compiled-index.db" --kind information_policy --need audience:public --need field:owner > "$tmp/compiled-policy-public.json"
grep -q 'policy:parcel:public' "$tmp/compiled-policy-public.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind information_policy --need audience:public --need field:owner-mailing-address > "$tmp/compiled-policy-withheld.json"
! grep -q 'policy:parcel' "$tmp/compiled-policy-withheld.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind information_policy --need audience:institution --need field:owner-mailing-address > "$tmp/compiled-policy-institution.json"
grep -q 'policy:parcel:institution' "$tmp/compiled-policy-institution.json"
# The distinction the whole record exists for: a display position computed
# between two fixes is not a third fix. Asking the plane for what a source
# observed cannot return what this estate interpolated, and vice versa.
"$bin" forward --db "$tmp/compiled-index.db" --kind spatial_observation --need subject:subject-aircraft-abc123 --need mode:observed > "$tmp/compiled-spatial-observed.json"
grep -q 'cms:spatial_observation:fix-a' "$tmp/compiled-spatial-observed.json"
! grep -q 'cms:spatial_observation:display-1' "$tmp/compiled-spatial-observed.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind spatial_observation --need mode:interpolated > "$tmp/compiled-spatial-derived.json"
grep -q 'cms:spatial_observation:display-1' "$tmp/compiled-spatial-derived.json"
! grep -q 'cms:spatial_observation:fix-a' "$tmp/compiled-spatial-derived.json"
# Coarse addressing narrows and stops. A cell selects the population an exact
# spatial closure then runs on; it is not itself a location.
"$bin" forward --db "$tmp/compiled-index.db" --kind spatial_observation --need cell:dfw-0912 --need mobility:moving > "$tmp/compiled-spatial-cell.json"
grep -q 'cms:spatial_observation:fix-b' "$tmp/compiled-spatial-cell.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind coverage_volume --need derivation:specified --need scale:street > "$tmp/compiled-coverage.json"
grep -q 'cms:coverage_volume:cam-foundry' "$tmp/compiled-coverage.json"
# A specification is not a measurement.
"$bin" forward --db "$tmp/compiled-index.db" --kind coverage_volume --need derivation:measured > "$tmp/compiled-coverage-measured.json"
! grep -q 'cms:coverage_volume:cam-foundry' "$tmp/compiled-coverage-measured.json"
# Coverage reaching a subject is not permission to observe or disclose it: no
# authority or audience coordinate is carried on a coverage volume.
! grep '"identity":"cms:coverage_volume:cam-foundry"' "$tmp/cms-ads.jsonl" | grep -qE '"(authority|audience|field):'
# A fix retires behind the occurrence that produced it.
mkdir "$tmp/spatial-deltas"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:spatial","writes":["cms:spatial_observation:fix-a"]}' > "$tmp/spatial-deltas/spatial.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/spatial-deltas" --limit 8 > "$tmp/worker-spatial.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:spatial' AND identity='cms:spatial_observation:display-1';")" = "1" ]
# A pack's declared dependency retires the closure that stood on it.
mkdir "$tmp/pack-deltas"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:pack-base","writes":["cms:pack_head:nightlife-base"]}' > "$tmp/pack-deltas/pack.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/pack-deltas" --limit 8 > "$tmp/worker-pack.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:pack-base' AND lowerer_ref='PackGraph:activation';")" = "1" ]
# A published CapabilityProgram retires through its own frontier, and its
# SiteProgram composition cut retires with it.
mkdir "$tmp/capability-deltas"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:capability","writes":["cms:capability_program:book-room"]}' > "$tmp/capability-deltas/capability.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/capability-deltas" --limit 8 > "$tmp/worker-capability.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:capability' AND lowerer_ref='ProtocolContract:capability';")" = "1" ]
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:capability' AND identity='cms:site_program:deep-ellum-live';")" = "1" ]
# A machine session sees a closure-scoped aperture derived from the same
# CandidateAdvertisement index, rather than a hand-maintained static tool
# catalogue.  The AddressPlane cut can narrow it before any later gateway
# scope/authority recheck; the result carries no bearer or authority fact.
"$bin" mcp-aperture --db "$tmp/compiled-index.db" --actor-closure sha256:actor --address-closure sha256:world-deep-ellum --target-frontier sha256:target-open --policy-generation policy:7 --need mcp:tool > "$tmp/mcp-aperture-tools.json"
grep -q '"schema":"bonfyre-mcp-aperture.v1"' "$tmp/mcp-aperture-tools.json"
grep -q '"mcp_protocol_version":"2026-07-28"' "$tmp/mcp-aperture-tools.json"
grep -q 'mcp:tool:world_events' "$tmp/mcp-aperture-tools.json"
! grep -q 'mcp:resource:bonfyre://world/deep-ellum/events' "$tmp/mcp-aperture-tools.json"
grep -q '"truth":"advertisement_only_exact_owner_recheck_required"' "$tmp/mcp-aperture-tools.json"
"$bin" mcp-aperture --db "$tmp/compiled-index.db" --actor-closure sha256:actor --address-closure sha256:world-deep-ellum --target-frontier sha256:target-open --policy-generation policy:7 > "$tmp/mcp-aperture-all.json"
grep -q 'mcp:tool:world_events' "$tmp/mcp-aperture-all.json"
grep -q 'mcp:resource:bonfyre://world/deep-ellum/events' "$tmp/mcp-aperture-all.json"
grep -q 'mcp:resource_template:bonfyre://world/{world}/events' "$tmp/mcp-aperture-all.json"
grep -q 'mcp:prompt:operate_world' "$tmp/mcp-aperture-all.json"
# WebAddress publishes body-family writes such as cms:site_presentation. The
# native worker must see that broad declared publish dependency, atomically
# retain its lowerer handoff, and not need a route-local CMS ladder.
mkdir "$tmp/worker-deltas"
mkdir "$tmp/lowerer-inbox"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:cms-publish","writes":["cms:site_presentation"]}' > "$tmp/worker-deltas/cms.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/worker-deltas" --physical-db "$tmp/worker-physical.sqlite" --physical-compiler "$compress" --limit 8 > "$tmp/worker.json"
grep -q '"worker":"address-plane"' "$tmp/worker.json"
grep -q '"physical_registry":true' "$tmp/worker.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:cms-publish' AND lowerer_ref='SurfaceIR:site_program';")" = "1" ]
# The source collection is material-owner input, but its declared binding edge
# must also retire the SiteProgram projection cut.  This proves the cache's
# cross-owner dependency is carried through the shared forwarding index rather
# than a route-local WebAddress special case.
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:source-collection","writes":["cms:source_collection:deep-ellum-media"]}' > "$tmp/worker-deltas/source-collection.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/worker-deltas" --limit 8 > "$tmp/worker-source-collection.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:source-collection' AND lowerer_ref='SurfaceIR:application';")" = "1" ]
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:source-collection' AND lowerer_ref='MaterialCompiler:source_collection';")" = "1" ]
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:foreign-topic","writes":["ForeignTwin:https://example.test/deep-ellum.rss"]}' > "$tmp/worker-deltas/foreign-topic.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/worker-deltas" --limit 8 > "$tmp/worker-foreign-topic.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:foreign-topic' AND identity='cms:world:deep-ellum' AND lowerer_ref='SurfaceIR:world';")" = "1" ]
# This is the source-specific coordinate emitted after a committed CMS
# publication promotion. It reaches only the World that declared this source.
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:publication-source","writes":["cms:publication_source:deep-ellum-feed"]}' > "$tmp/worker-deltas/publication-source.json"
"$bin" address-worker --db "$tmp/compiled-index.db" --delta-dir "$tmp/worker-deltas" --limit 8 > "$tmp/worker-publication-source.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:publication-source' AND identity='cms:world:deep-ellum' AND lowerer_ref='SurfaceIR:world';")" = "1" ]
"$compress" plan --db "$tmp/worker-physical.sqlite" --subject-family mcp_projection --operation filtering --loss-budget 0 --requires-exact > "$tmp/worker-physical-plan.json"
grep -q 'addressplane:advertisement:mcp:tool:world_events:sqlite-v1' "$tmp/worker-physical-plan.json"
# Physical registry replacement consumes a measured snapshot build aperture.
# ResourceAdmission may defer or reject it; neither state replaces the last
# registry nor pretends that a representation was selected.
"$bin" address-worker --db "$tmp/compiled-index.db" --physical-db "$tmp/worker-physical.sqlite" --physical-compiler "$compress" --admission-db "$tmp/physical-admission.sqlite" --admission-volume "$tmp" --admission-plane physical-compiler --admission-free-bytes 1000000000 --admission-protected-floor-bytes -1 --limit 8 > "$tmp/worker-physical-admitted.json"
grep -q '"physical_admission":"ADMIT"' "$tmp/worker-physical-admitted.json"
[ "$(sqlite3 "$tmp/physical-admission.sqlite" "SELECT count(*) FROM resource_grants WHERE released_at IS NOT NULL;")" = "1" ]
"$bin" address-worker --db "$tmp/compiled-index.db" --physical-db "$tmp/rejected-physical.sqlite" --physical-compiler "$compress" --admission-db "$tmp/rejected-admission.sqlite" --admission-volume "$tmp" --admission-free-bytes 1 --admission-protected-floor-bytes -1 --limit 8 > "$tmp/worker-physical-rejected.json"
grep -q '"physical_registry":false' "$tmp/worker-physical-rejected.json"
grep -q '"physical_admission":"REJECT"' "$tmp/worker-physical-rejected.json"
[ ! -e "$tmp/rejected-physical.sqlite" ]
# Re-running an unchanged address generation preserves the physical registry;
# it hashes the fsync'd source snapshot and avoids a second full replacement.
"$bin" address-worker --db "$tmp/compiled-index.db" --physical-db "$tmp/worker-physical.sqlite" --physical-compiler "$compress" --limit 8 > "$tmp/worker-physical-unchanged.json"
grep -q '"state":"unchanged"' "$tmp/worker-physical-unchanged.json"
# Failed source validation is preflight only: a malformed authenticated MCP
# snapshot may not erase the last valid MCP closure generation.
printf '%s\n' '{"tools":[' > "$tmp/bad-mcp-closure.json"
! "$bin" compile-advertisements --db "$tmp/compiled-index.db" --mcp-closure "$tmp/bad-mcp-closure.json" >/dev/null 2>&1
"$bin" forward --db "$tmp/compiled-index.db" --kind mcp_projection --need mcp:tool > "$tmp/compiled-mcp-still-atomic.json"
grep -q 'mcp:tool:world_events' "$tmp/compiled-mcp-still-atomic.json"
sqlite3 "$tmp/cms.db" "DELETE FROM site_presentation;"
"$bin" compile-advertisements --db "$tmp/compiled-index.db" --cms-db "$tmp/cms.db" > "$tmp/compiled-retire.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind site_presentation --need subject:world-deep-ellum > "$tmp/compiled-retired.json"
! grep -q 'cms:site_presentation:deep-ellum' "$tmp/compiled-retired.json"
"$bin" forward --db "$tmp/compiled-index.db" --kind mcp_projection --need mcp:tool > "$tmp/compiled-mcp-survives.json"
grep -q 'mcp:tool:world_events' "$tmp/compiled-mcp-survives.json"
{ sed -n '1,20p' "$tmp/cms-ads.jsonl"; sed -n '1,20p' "$tmp/target-ads.jsonl"; sed -n '1,20p' "$tmp/provider-ads.jsonl"; sed -n '1,20p' "$tmp/compute-ads.jsonl"; sed -n '1,20p' "$tmp/mcp-ads.jsonl"; sed -n '1,200p' "$tmp/protocol-ads.jsonl"; } > "$tmp/owner-ads.jsonl"
"$bin" import-advertisements --db "$tmp/estate.db" --advertisements "$tmp/owner-ads.jsonl" >/dev/null
"$bin" forward --db "$tmp/estate.db" --kind target_obligation --need operation:desired-state > "$tmp/forward-target.json"
grep -q 'target:reserve:deep-ellum' "$tmp/forward-target.json"
"$bin" forward --db "$tmp/estate.db" --kind protocol_chain --need protocol:x402 > "$tmp/forward-protocol.json"
grep -q 'protocol:x402' "$tmp/forward-protocol.json"
"$bin" forward --db "$tmp/estate.db" --kind compute --need compute:bitnet-embed > "$tmp/forward-compute.json"
grep -q 'compute:bitnet-embed' "$tmp/forward-compute.json"
"$bin" forward --db "$tmp/estate.db" --kind compute --need operation:generation --need admission:defer > "$tmp/forward-compute-representation.json"
grep -q 'compute-representation:qwen-native-f16' "$tmp/forward-compute-representation.json"
"$bin" forward --db "$tmp/estate.db" --kind mcp_projection --need mcp:tool > "$tmp/forward-mcp.json"
grep -q 'mcp:tool:world_events' "$tmp/forward-mcp.json"
# The physical compiler receives live AddressPlane populations through a
# generic kind exporter.  It is not a parallel hand-maintained registry: all
# present owner kinds and the canonical ExternalEstate reservoir emit the same
# RepresentationPassport vocabulary, marked declared until runtime receipts
# exist.
"$bin" emit-representation-passports --db "$tmp/estate.db" > "$tmp/address-passports.jsonl"
grep -q '"identity":"addressplane:advertisement:external:ext-00001:sqlite-v1"' "$tmp/address-passports.jsonl"
grep -q '"subject_family":"mcp_projection"' "$tmp/address-passports.jsonl"
grep -q '"measurement_state":"declared"' "$tmp/address-passports.jsonl"
grep -q '"semantic_digest":"sha256:' "$tmp/address-passports.jsonl"
sed -n '1p' "$tmp/address-passports.jsonl" > "$tmp/address-passport.json"
cue vet "$root/schemas/control/representation-passport.cue" "$tmp/address-passport.json" -d '#RepresentationPassport'
"$compress" compile-passports --db "$tmp/address-physical.sqlite" --input "$tmp/address-passports.jsonl" > "$tmp/address-physical-compile.json"
grep -q '"runtime":"dense_handles"' "$tmp/address-physical-compile.json"
[ "$(sqlite3 "$tmp/address-physical.sqlite" 'SELECT count(*) FROM representation_passport;')" -ge 4891 ]
"$compress" plan --db "$tmp/address-physical.sqlite" --subject-family mcp_projection --operation filtering --loss-budget 0 --requires-exact > "$tmp/address-physical-plan.json"
grep -q 'addressplane:advertisement:mcp:tool:world_events:sqlite-v1' "$tmp/address-physical-plan.json"
grep -q '"selected":false' "$tmp/address-physical-plan.json"
"$bin" fragment --db "$tmp/estate.db" --grammar spatial --class anonymous --domain weather --limit 3 > "$tmp/fragment.html"
grep -q 'data-grammar="spatial"' "$tmp/fragment.html"
grep -q 'Human paths' "$tmp/fragment.html"
grep -q 'class="surface-act"' "$tmp/fragment.html"
grep -q '/_surface/intake?occurrence=' "$tmp/fragment.html"
grep -q '&url=https%3A%2F%2F' "$tmp/fragment.html"
grep -q 'Protocol program' "$tmp/fragment.html"
"$bin" route --db "$tmp/estate.db" --occurrence external:ext-00001 --url https://github.com/coinbase/x402 > "$tmp/route.html"
grep -q 'data-occurrence="external:ext-00001"' "$tmp/route.html"
grep -q 'x402 Protocol Specification' "$tmp/route.html"
grep -q 'Protocol programs' "$tmp/route.html"
grep -qi 'no source admission' "$tmp/route.html"
! "$bin" route --db "$tmp/estate.db" --occurrence external:ext-00001 --url https://example.org/not-the-source >/dev/null 2>&1
grep -qi 'candidate material is not an admitted provider' "$tmp/fragment.html"
! grep -q '<script' "$tmp/fragment.html"
"$bin" fragment --db "$tmp/estate.db" --grammar discover --class anonymous --domain weather --limit 3 > "$tmp/discover.html"
grep -q 'External execution material' "$tmp/discover.html"
! grep -q 'human program\|Deep chain\|Cure Cancer With AI' "$tmp/discover.html"
"$bin" drain-lowering-queue --db "$tmp/compiled-index.db" --dir "$tmp/lowerer-inbox" --limit 16 > "$tmp/lowerer-drain.json"
grep -q '"lowering_handoffs_dispatched":13' "$tmp/lowerer-drain.json"
[ "$(sqlite3 "$tmp/compiled-index.db" "SELECT count(*) FROM candidate_lowering_queue WHERE delta_id='delta:cms-publish' AND lowerer_ref='SurfaceIR:site_program' AND state='dispatched';")" = "1" ]
# Three deltas now reach the SurfaceIR site_program lowerer: the site
# presentation publish, and the SiteProgram composition retiring behind both
# its source collection and its capability program.  Cannot infer: a handoff
# is a dispatched lowering request, never a rendered surface.
[ "$(find "$tmp/lowerer-inbox/SurfaceIR-site_program" -type f -name '*.json' | wc -l | tr -d ' ')" = "3" ]
grep -q '"lowerer_ref":"SurfaceIR:site_program"' "$tmp/lowerer-inbox/SurfaceIR-site_program"/*.json
[ "$(find "$tmp/lowerer-inbox/SurfaceIR-application" -type f -name '*.json' | wc -l | tr -d ' ')" = "1" ]
grep -q '"identity":"cms:application_binding:watch-main"' "$tmp/lowerer-inbox/SurfaceIR-application"/*.json
[ "$(find "$tmp/lowerer-inbox/SurfaceIR-world" -type f -name '*.json' | wc -l | tr -d ' ')" = "2" ]
grep -q '"identity":"cms:world:deep-ellum"' "$tmp/lowerer-inbox/SurfaceIR-world"/*.json
echo "native external estate: ok"
