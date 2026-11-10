#!/bin/sh
set -eu
bin=${1:?binary}; root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
mkdir -p "$root/inbox/MCP-tool" "$root/inbox/ProtocolCompiler-late_carrier" "$root/inbox/ProviderCompiler-embedding" "$root/inbox/InteractionGrammarCompiler-surface" "$root/inbox/MaterialCompiler-external" "$root/inbox/MaterialCompiler-source" "$root/inbox/MaterialCompiler-source_collection" "$root/inbox/MaterialCompiler-pack_demand" "$root/demands"
printf '%s\n' '{"tools":[{"name":"observe.world"}],"resources":[],"resourceTemplates":[],"prompts":[]}' > "$root/closure.json"
printf '%s\n' '{"id":"x402","stages":["request","pay","receipt"]}' > "$root/protocols.jsonl"
sqlite3 "$root/cms.db" "CREATE TABLE internet_source(id INTEGER PRIMARY KEY,source TEXT,url TEXT,status TEXT);CREATE TABLE source_collection(collection_id TEXT,subject_address TEXT,status TEXT);CREATE TABLE pack_demand(pack TEXT,demand_kind TEXT,needs TEXT,status TEXT);INSERT INTO internet_source VALUES(1,'feed','https://example.test/feed','published');INSERT INTO source_collection VALUES('night','world:night','published');INSERT INTO pack_demand VALUES('weather-pack','observation','weather','published');"
sqlite3 "$root/external.db" "CREATE TABLE occurrence(id TEXT PRIMARY KEY,primary_url TEXT);INSERT INTO occurrence VALUES('external:ext-02596','https://www.weather.gov/documentation/services-web-api');"
sqlite3 "$root/provider.db" "CREATE TABLE provider_embedding_run(run_id TEXT PRIMARY KEY,provider TEXT,model TEXT,representation_id TEXT,requested_count INTEGER,stored_count INTEGER,dimensions INTEGER,status TEXT);INSERT INTO provider_embedding_run VALUES('embedding:mistral:fixture','mistral','mistral-embed','mistral-embed-v1',2,2,1024,'complete');"
sqlite3 "$root/grammar.db" "CREATE TABLE interaction_grammar_catalog(grammar_id TEXT PRIMARY KEY,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL);CREATE TABLE interaction_grammar_binding(binding_id TEXT PRIMARY KEY,grammar_id TEXT NOT NULL,subject_ref TEXT NOT NULL,institution_ref TEXT NOT NULL,owner_ref TEXT NOT NULL,visibility TEXT NOT NULL,state TEXT NOT NULL);INSERT INTO interaction_grammar_catalog VALUES('discord-community','Community:conversation_visibility','CommunitySurfaceCompiler:conversation');INSERT INTO interaction_grammar_binding VALUES('deep-ellum-discord','discord-community','world:deep-ellum','institution:deep-ellum','Community:deep-ellum','member','active');"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d1","identity":"mcp:tool:observe.world","candidate_kind":"mcp_projection","owner_ref":"MCPClosure:tool:observe.world","lowerer_ref":"MCP:tool","truth":"dependency_cut_only"}' > "$root/inbox/MCP-tool/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d2","identity":"protocol:x402","candidate_kind":"protocol_chain","owner_ref":"ProtocolCommons:x402","lowerer_ref":"ProtocolCompiler:late_carrier","truth":"dependency_cut_only"}' > "$root/inbox/ProtocolCompiler-late_carrier/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d2p","identity":"provider-embedding:embedding:mistral:fixture","candidate_kind":"provider","owner_ref":"ProviderEmbeddingRun:embedding:mistral:fixture","lowerer_ref":"ProviderCompiler:embedding","truth":"dependency_cut_only"}' > "$root/inbox/ProviderCompiler-embedding/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d2a","identity":"interaction-grammar:deep-ellum-discord","candidate_kind":"interaction_grammar","owner_ref":"Community:deep-ellum","lowerer_ref":"InteractionGrammarCompiler:surface","truth":"dependency_cut_only"}' > "$root/inbox/InteractionGrammarCompiler-surface/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d2b","identity":"external:ext-02596","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:ext-02596","lowerer_ref":"MaterialCompiler:external","truth":"dependency_cut_only"}' > "$root/inbox/MaterialCompiler-external/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d3","identity":"cms:internet_source:feed","candidate_kind":"external_source","owner_ref":"CMS:internet_source:feed","lowerer_ref":"MaterialCompiler:source","truth":"dependency_cut_only"}' > "$root/inbox/MaterialCompiler-source/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d4","identity":"cms:source_collection:night","candidate_kind":"source_collection","owner_ref":"CMS:source_collection:night","lowerer_ref":"MaterialCompiler:source_collection","truth":"dependency_cut_only"}' > "$root/inbox/MaterialCompiler-source_collection/a.json"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d5","identity":"cms:pack_demand:weather-pack:observation:weather","candidate_kind":"pack_demand","owner_ref":"CMS:pack_demand:weather-pack","lowerer_ref":"MaterialCompiler:pack_demand","truth":"dependency_cut_only"}' > "$root/inbox/MaterialCompiler-pack_demand/a.json"
"$bin" drain --db "$root/derived.db" --lowerer-dir "$root/inbox" --mcp-closure "$root/closure.json" --protocols "$root/protocols.jsonl" --cms "$root/cms.db" --provider-db "$root/provider.db" --interaction-grammar-db "$root/grammar.db" --external-estate "$root/external.db" --demand-dir "$root/demands" > "$root/out.json"
grep -q '"handled":8' "$root/out.json"
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM mcp_projection_artifact;')" = 1 ]
[ "$(sqlite3 "$root/derived.db" "SELECT closure_digest LIKE 'sha256:%' FROM mcp_projection_artifact;")" = 1 ]
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM protocol_compilation_artifact;')" = 1 ]
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM provider_embedding_artifact WHERE identity='provider-embedding:embedding:mistral:fixture' AND provider='mistral' AND model='mistral-embed' AND representation_id='mistral-embed-v1' AND dimensions=1024 AND admission_state='unrequested';")" = 1 ]
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM interaction_surface_artifact WHERE grammar_id='discord-community' AND state='pending_exact_owner';")" = 1 ]
[ "$(sqlite3 "$root/derived.db" "SELECT composition_digest LIKE 'sha256:%' AND exact_verifier_state='unqueried' AND json_extract(composition_json,'$.state')='pending_exact_owner' FROM interaction_surface_composition WHERE identity='interaction-grammar:deep-ellum-discord';")" = 1 ]
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM material_observation_demand;')" = 2 ]
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM material_collection_artifact;')" = 1 ]
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM material_pack_demand_artifact;')" = 1 ]
grep -q observation_requested_not_performed "$root/demands/cms-internet_source-feed.json"
grep -q 'https://www.weather.gov/documentation/services-web-api' "$root/demands/external-ext-02596.json"
[ "$(find "$root/inbox" -name '*.handled' | wc -l | tr -d ' ')" = 8 ]
# A current MCP closure is also the validity frontier for derived aperture
# artifacts. Withdrawal retires only that disposable projection; it does not
# touch a tool owner, an OAuth token, or an already recorded call.
printf '%s\n' '{"tools":[],"resources":[],"resourceTemplates":[],"prompts":[]}' > "$root/closure.json"
mkdir "$root/retire"
"$bin" drain --db "$root/derived.db" --lowerer-dir "$root/retire" --mcp-closure "$root/closure.json" --protocols "$root/protocols.jsonl" --cms "$root/cms.db" --external-estate "$root/external.db" --demand-dir "$root/demands" > "$root/retire.json"
grep -q '"mcp_artifacts_retired":1' "$root/retire.json"
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM mcp_projection_artifact;')" = 0 ]
printf '%s\n' '{"tools":[{"name":"observe.world"}],"resources":[],"resourceTemplates":[],"prompts":[]}' > "$root/closure.json"
# A stale advertisement is not an MCP aperture.  The consumer must retain the
# handoff for retry/inspection rather than compiling a tool that disappeared.
mkdir -p "$root/bad/MCP-tool"
printf '%s\n' '{"schema":"bonfyre-lowering-handoff.v1","delta_id":"d5","identity":"mcp:tool:not.in.snapshot","candidate_kind":"mcp_projection","owner_ref":"MCPClosure:tool:not.in.snapshot","lowerer_ref":"MCP:tool","truth":"dependency_cut_only"}' > "$root/bad/MCP-tool/a.json"
if "$bin" drain --db "$root/derived.db" --lowerer-dir "$root/bad" --mcp-closure "$root/closure.json" --protocols "$root/protocols.jsonl" --cms "$root/cms.db" --external-estate "$root/external.db" --demand-dir "$root/demands"; then
  echo 'stale MCP aperture was accepted' >&2; exit 1
fi
[ -f "$root/bad/MCP-tool/a.json" ]
[ "$(sqlite3 "$root/derived.db" 'SELECT count(*) FROM mcp_projection_artifact;')" = 0 ]

# MaterialCompiler may turn only a retained ForeignTwin receipt that intersects
# a persisted BitNet frontier into ContextCut input. SHADOW needs an explicit
# diagnostic flag and still produces a material manifest, not an executable
# lowerer handoff or an owner-query claim.
printf 'retained foreign body\n' > "$root/retained.body"
body_sha=$(shasum -a 256 "$root/retained.body" | awk '{print $1}')
sqlite3 "$root/rank.db" "CREATE TABLE bitnet_rank_run(run_id TEXT PRIMARY KEY,gate_state TEXT);CREATE TABLE embedding_projection(run_id TEXT,candidate_identity TEXT);INSERT INTO bitnet_rank_run VALUES('shadow-run','SHADOW');INSERT INTO embedding_projection VALUES('shadow-run','external:ext-02596');"
printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:ext-02596","content_sha256":"%s","body_path":"%s","observed_at":"2026-09-12T00:00:00Z","observation_kind":"source_observed"}\n' "$body_sha" "$root/retained.body" > "$root/foreign.jsonl"
! "$bin" context-manifest --rank-db "$root/rank.db" --run-id shadow-run --foreign-evidence "$root/foreign.jsonl" --policy public --out "$root/context-material.jsonl" 2>/dev/null
"$bin" context-manifest --rank-db "$root/rank.db" --run-id shadow-run --foreign-evidence "$root/foreign.jsonl" --policy public --out "$root/context-material.jsonl" --shadow-diagnostic > "$root/context-material.json"
grep -q '"retained_materials":1' "$root/context-material.json"
grep -q '"candidate_identity":"external:ext-02596"' "$root/context-material.jsonl"
grep -q '"content_sha256":"sha256:' "$root/context-material.jsonl"

# BitNet's stored verifier reference becomes an exact-owner query only here.
# The retained receipt is admitted through the native ForeignTwin ABI, every
# persisted rank candidate is queried, and the output distinguishes an absent
# observation from an observed one.  SHADOW needs an explicit evaluation flag;
# this worker cannot turn the measured representation into an ADMIT passport.
sqlite3 "$root/verify-rank.db" "CREATE TABLE bitnet_rank_run(run_id TEXT PRIMARY KEY,representation_id TEXT,gate_state TEXT); CREATE TABLE ranked_candidate_envelope(run_id TEXT,rank INTEGER,candidate_identity TEXT,exact_verifier TEXT); CREATE TABLE embedding_projection(run_id TEXT,candidate_identity TEXT,source_digest TEXT,vector_digest TEXT); INSERT INTO bitnet_rank_run VALUES('shadow-verify','bitnet-test','SHADOW'); INSERT INTO ranked_candidate_envelope VALUES('shadow-verify',0,'external:ext-02596','ForeignTwin:admission'); INSERT INTO ranked_candidate_envelope VALUES('shadow-verify',1,'external:missing','ForeignTwin:admission'); INSERT INTO embedding_projection VALUES('shadow-verify','external:ext-02596','source-a','vector-a'); INSERT INTO embedding_projection VALUES('shadow-verify','external:missing','source-b','vector-b');"
printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"external:ext-02596","source_url":"https://example.test/weather","observed_at":"2026-09-12T00:00:00Z","content_sha256":"%s","body_path":"%s","policy_decision":"allow","observation_kind":"source_observed"}\n' "$body_sha" "$root/retained.body" > "$root/verify-foreign.jsonl"
! "$bin" foreign-verify --rank-db "$root/verify-rank.db" --run-id shadow-verify --foreign-evidence "$root/verify-foreign.jsonl" --foreign-db "$root/foreign-twin.db" --out "$root/exact-owner.jsonl" 2>/dev/null
"$bin" foreign-verify --rank-db "$root/verify-rank.db" --run-id shadow-verify --foreign-evidence "$root/verify-foreign.jsonl" --foreign-db "$root/foreign-twin.db" --out "$root/exact-owner.jsonl" --shadow-evaluation > "$root/exact-owner.json"
grep -q '"exact_owner_queries":2' "$root/exact-owner.json"
grep -q '"retained_observations":1' "$root/exact-owner.json"
grep -q '"execution_mode":"shadow_owner_verification"' "$root/exact-owner.json"
grep '"candidate_identity":"external:ext-02596"' "$root/exact-owner.jsonl" | grep -q '"outcome":"observed"'
grep '"candidate_identity":"external:missing"' "$root/exact-owner.jsonl" | grep -q '"outcome":"unobserved"'
[ "$(sqlite3 "$root/foreign-twin.db" 'SELECT count(*) FROM foreign_twin_observations;')" = 1 ]
# Context material can now require the exact-owner feedback artifact as an
# input.  A raw receipt by itself is insufficient once this stronger path is
# requested; the accepted body must have both a native owner query and a fresh
# local digest check.
"$bin" context-manifest --rank-db "$root/verify-rank.db" --run-id shadow-verify --foreign-evidence "$root/verify-foreign.jsonl" --foreign-query-evidence "$root/exact-owner.jsonl" --policy public --out "$root/verified-context.jsonl" --shadow-diagnostic > "$root/verified-context.json"
grep -q '"retained_materials":1' "$root/verified-context.json"
grep -q '"candidate_identity":"external:ext-02596"' "$root/verified-context.jsonl"
echo 'BonfyreLoweringWorkers: PASS'
