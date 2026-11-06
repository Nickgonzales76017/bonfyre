#!/bin/sh
set -eu
bin=${1:?binary}; root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
db="$root/grammar.db"; estate="${BONFYRE_ESTATE_BIN:-$(cd "$(dirname "$bin")/../BonfyreExternalEstate" && pwd)/bonfyre-external-estate}"
workers="${BONFYRE_LOWERING_WORKERS_BIN:-$(cd "$(dirname "$bin")/../BonfyreLoweringWorkers" && pwd)/bonfyre-lowering-workers}"
"$bin" catalog >"$root/catalog.json"
grep -q '"grammar_count":20' "$root/catalog.json"
grep -q 'discord-community' "$root/catalog.json"
grep -q 'microsoft-teams-workspace' "$root/catalog.json"
grep -q 'facebook-social-body' "$root/catalog.json"
"$bin" describe --grammar discord-community >"$root/discord.json"
grep -q 'Community:conversation_visibility' "$root/discord.json"
if "$bin" bind --db "$db" --id grammar:wrong-owner --grammar discord-community --subject world:deep-ellum --owner Offer:not-a-community --visibility member --by Offer:not-a-community; then
  echo 'cross-family grammar binding was accepted' >&2; exit 1
fi
"$bin" bind --db "$db" --id grammar:deep-ellum-discord --grammar discord-community --subject world:deep-ellum --institution institution:deep-ellum --owner Community:deep-ellum --visibility member --by Community:deep-ellum
"$bin" bind --db "$db" --id grammar:deep-ellum-channel --grammar youtube-creator-channel --subject channel:deep-ellum --owner CreatorChannel:channel:deep-ellum --visibility public --by CreatorChannel:channel:deep-ellum
"$bin" surface --db "$db" --binding grammar:deep-ellum-discord >"$root/surface.json"
grep -q 'CommunitySurfaceCompiler:conversation' "$root/surface.json"
grep -q '"exact_verifier_state":"UNQUERIED"' "$root/surface.json"
# The portfolio is not a decorative donor list: every declared grammar has a
# compact owner-family-bound route through the same AddressPlane and native
# consumer. These are synthetic references, explicitly not claims about a
# live owner record, donor account, authority, message, entitlement, or
# deployed target lowerer.
n=0
while IFS='|' read -r grammar operation owner_family; do
  n=$((n + 1))
  "$bin" bind --db "$db" --id "grammar:portfolio-$n" --grammar "$grammar" --subject "subject:portfolio-$n" --owner "$owner_family:portfolio-$n" --visibility private --by "$owner_family:portfolio-$n" >/dev/null
done <<'EOF'
youtube-creator-channel|watch|CreatorChannel
discord-community|communicate|Community
microsoft-teams-workspace|collaborate|WorkGraph
slack-workstream|collaborate|WorkGraph
facebook-social-body|discover|RelationshipGraph
reddit-topic-community|communicate|Community
twitch-live-stage|stream|Stage
tiktok-short-stream|watch|CreatorChannel
instagram-identity-media|discover|RelationshipGraph
whatsapp-private-correspondence|communicate|Correspondence
telegram-community-broadcast|communicate|Community
github-collaborative-code|review|WorkGraph
notion-knowledge-workspace|compose|KnowledgeGraph
linear-project-flow|plan|WorkGraph
jira-project-service|triage|WorkGraph
figma-design-collaboration|review|ArtifactGraph
spotify-audio-habitat|listen|MediaLibrary
pinterest-visual-board|collect|WorldPublication
shopify-merchant-commerce|purchase|Offer
airbnb-place-reservation|reserve|ProtocolContract
EOF
"$bin" emit-advertisements --db "$db" >"$root/ads.jsonl"
test "$(wc -l < "$root/ads.jsonl" | tr -d ' ')" = 22
grep -q '"candidate_kind":"interaction_grammar"' "$root/ads.jsonl"
if [ -x "$estate" ]; then
  "$estate" import-advertisements --db "$root/direct-address.db" --advertisements "$root/ads.jsonl"
fi
if [ -x "$estate" ]; then
  "$estate" compile-advertisements --db "$root/address.db" --interaction-grammar-db "$db"
  "$estate" forward --db "$root/address.db" --kind interaction_grammar --need grammar:discord-community --need operation:communicate --need subject:world-deep-ellum >"$root/forward.json"
  grep -q '"owner_ref":"Community:deep-ellum"' "$root/forward.json"
  grep -q '"exact_verifier":"Community:conversation_visibility"' "$root/forward.json"
  grep -q '"lowerer_ref":"InteractionGrammarCompiler:surface"' "$root/forward.json"
  while IFS='|' read -r grammar operation owner_family; do
    "$estate" forward --db "$root/address.db" --kind interaction_grammar --need "grammar:$grammar" --need "operation:$operation" > "$root/forward-$grammar.json"
    grep -q '"lowerer_ref":"InteractionGrammarCompiler:surface"' "$root/forward-$grammar.json"
  done <<'EOF'
youtube-creator-channel|watch|CreatorChannel
discord-community|communicate|Community
microsoft-teams-workspace|collaborate|WorkGraph
slack-workstream|collaborate|WorkGraph
facebook-social-body|discover|RelationshipGraph
reddit-topic-community|communicate|Community
twitch-live-stage|stream|Stage
tiktok-short-stream|watch|CreatorChannel
instagram-identity-media|discover|RelationshipGraph
whatsapp-private-correspondence|communicate|Correspondence
telegram-community-broadcast|communicate|Community
github-collaborative-code|review|WorkGraph
notion-knowledge-workspace|compose|KnowledgeGraph
linear-project-flow|plan|WorkGraph
jira-project-service|triage|WorkGraph
figma-design-collaboration|review|ArtifactGraph
spotify-audio-habitat|listen|MediaLibrary
pinterest-visual-board|collect|WorldPublication
shopify-merchant-commerce|purchase|Offer
airbnb-place-reservation|reserve|ProtocolContract
EOF
  # The real path is AddressPlane FactDelta -> durable handoff -> native
  # consumer.  The consumer reopens the grammar DB and refuses to claim that
  # its named Community verifier ran; its artifact stays pending that owner.
  mkdir "$root/lowerer-inbox"
  printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:discord-binding","writes":["interaction-grammar:grammar:deep-ellum-discord"]}' > "$root/discord-delta.json"
  "$estate" fact-delta --db "$root/address.db" --delta "$root/discord-delta.json" > "$root/discord-cut.json"
  grep -q '"lowering_state":"queued"' "$root/discord-cut.json"
  "$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/lowerer-inbox" --limit 8 > "$root/discord-handoff.json"
  grep -q '"lowering_handoffs_dispatched":1' "$root/discord-handoff.json"
  "$workers" drain --db "$root/derived.db" --lowerer-dir "$root/lowerer-inbox" --interaction-grammar-db "$db" > "$root/discord-consumed.json"
  grep -q '"handled":1' "$root/discord-consumed.json"
  [ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM interaction_surface_artifact WHERE grammar_id='discord-community' AND exact_verifier='Community:conversation_visibility' AND target_lowerer_ref='CommunitySurfaceCompiler:conversation' AND state='pending_exact_owner';")" = 1 ]
  [ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM interaction_surface_composition WHERE identity='interaction-grammar:grammar:deep-ellum-discord' AND composition_digest LIKE 'sha256:%' AND exact_verifier_state='unqueried' AND json_extract(composition_json,'$.target_lowerer_ref')='CommunitySurfaceCompiler:conversation' AND json_extract(composition_json,'$.truth')='composition_not_render_or_authority';")" = 1 ]
  # Exercise the whole market-researched population through one incremental
  # cut.  The worker must ignore the already-handled Discord file and consume
  # exactly the 20 new portfolio handoffs, proving it is a batch compiler and
  # not an example-only command.
  sqlite3 "$db" "SELECT binding_id FROM interaction_grammar_binding WHERE binding_id LIKE 'grammar:portfolio-%' ORDER BY binding_id;" > "$root/portfolio-bindings.txt"
  {
    printf '%s' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:portfolio","writes":['
    first=1
    while IFS= read -r binding; do
      [ -n "$binding" ] || continue
      [ "$first" = 1 ] || printf ','
      first=0
      printf '"interaction-grammar:%s"' "$binding"
    done < "$root/portfolio-bindings.txt"
    printf '%s\n' ']}'
  } > "$root/portfolio-delta.json"
  "$estate" fact-delta --db "$root/address.db" --delta "$root/portfolio-delta.json" > "$root/portfolio-cut.json"
  grep -q '"affected_count":20' "$root/portfolio-cut.json"
  "$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/lowerer-inbox" --limit 32 > "$root/portfolio-handoff.json"
  grep -q '"lowering_handoffs_dispatched":20' "$root/portfolio-handoff.json"
  "$workers" drain --db "$root/derived.db" --lowerer-dir "$root/lowerer-inbox" --interaction-grammar-db "$db" --limit 32 > "$root/portfolio-consumed.json"
  grep -q '"handled":20' "$root/portfolio-consumed.json"
  [ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM interaction_surface_composition WHERE json_extract(composition_json,'$.subject_ref') LIKE 'subject:portfolio-%';")" = 20 ]
  "$bin" retire --db "$db" --id grammar:deep-ellum-discord --by Community:deep-ellum
  "$estate" compile-advertisements --db "$root/address.db" --interaction-grammar-db "$db"
  "$estate" forward --db "$root/address.db" --kind interaction_grammar --need grammar:discord-community --need operation:communicate --need subject:world-deep-ellum >"$root/retired.json"
  grep -q '"survivors":\[\]' "$root/retired.json"
fi
# Owner-derived grammar bindings use the existing Channel and MediaLibrary
# organs, not a manually supplied owner string.  The worker reopens those
# owners and requires current published/membership state before retaining a
# derived interaction composition.
channel_bin="$(cd "$(dirname "$bin")/../BonfyreChannel" && pwd)/bonfyre-channel"
media_bin="$(cd "$(dirname "$bin")/../BonfyreMediaLibrary" && pwd)/bonfyre-media-library"
channel_db="$root/channel.db"; media_db="$root/media.db"; cms_db="$root/cms.db"
sqlite3 "$cms_db" "CREATE TABLE media_program(id INTEGER PRIMARY KEY,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,publication_state TEXT,status TEXT); INSERT INTO media_program VALUES(1,'video:opening','video:opening','video','/assets/opening.mp4','published','published'); INSERT INTO media_program VALUES(2,'short:opening','short:opening','video','/assets/short.mp4','published','published');"
"$channel_bin" put --db "$channel_db" --id channel:deep-ellum --owner actor:creator --name 'Deep Ellum' --handle deepellum --visibility public --community community:deep-ellum --by actor:creator >/dev/null
"$channel_bin" publish --db "$channel_db" --cms-db "$cms_db" --channel channel:deep-ellum --subject video:opening --program video:opening --kind video --by actor:creator >/dev/null
"$channel_bin" publish --db "$channel_db" --cms-db "$cms_db" --channel channel:deep-ellum --subject short:opening --program short:opening --kind short --by actor:creator >/dev/null
"$media_bin" library-put --db "$media_db" --id audio-club --mode community --owner community:audio --actor community:audio --title 'Audio Club' --visibility community >/dev/null
"$media_bin" bind --db "$media_db" --library audio-club --subject recording:opening --program audio:opening --collection live --state active --by community:audio >/dev/null
"$bin" bind-creator-channel --db "$db" --id grammar:owner-youtube --grammar youtube-creator-channel --creator-channel-db "$channel_db" --channel channel:deep-ellum --by actor:creator >"$root/owner-youtube.json"
"$bin" bind-creator-channel --db "$db" --id grammar:owner-tiktok --grammar tiktok-short-stream --creator-channel-db "$channel_db" --channel channel:deep-ellum --by actor:creator >"$root/owner-tiktok.json"
"$bin" bind-media-library --db "$db" --id grammar:owner-spotify --grammar spotify-audio-habitat --media-library-db "$media_db" --library audio-club --by community:audio >"$root/owner-spotify.json"
grep -q 'active_creator_channel_owner_row_bound' "$root/owner-youtube.json"
grep -q 'active_media_library_owner_row_bound' "$root/owner-spotify.json"
if "$bin" bind-creator-channel --db "$db" --id grammar:forged-channel --grammar youtube-creator-channel --creator-channel-db "$channel_db" --channel channel:deep-ellum --by actor:other >/dev/null 2>&1; then
  echo 'creator grammar accepted a non-owner source bind' >&2; exit 1
fi
"$estate" compile-advertisements --db "$root/address.db" --interaction-grammar-db "$db" >/dev/null
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:owner-bound-grammars","writes":["interaction-grammar:grammar:owner-youtube","interaction-grammar:grammar:owner-tiktok","interaction-grammar:grammar:owner-spotify"]}' > "$root/owner-grammar-delta.json"
"$estate" fact-delta --db "$root/address.db" --delta "$root/owner-grammar-delta.json" > "$root/owner-grammar-cut.json"
grep -q '"affected_count":3' "$root/owner-grammar-cut.json"
"$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/lowerer-inbox" --limit 8 > "$root/owner-grammar-handoff.json"
grep -q '"lowering_handoffs_dispatched":3' "$root/owner-grammar-handoff.json"
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/lowerer-inbox" --interaction-grammar-db "$db" --creator-channel-db "$channel_db" --creator-channel-bin "$channel_bin" --cms "$cms_db" --media-library "$media_db" --limit 8 > "$root/owner-grammar-consumed.json"
grep -q '"handled":3' "$root/owner-grammar-consumed.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM interaction_surface_composition WHERE json_extract(composition_json,'$.owner_binding_state')='current_native_owner_row' AND json_extract(composition_json,'$.binding_source_kind') IN ('creator_channel','media_library');")" = 3 ]
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM channel_surface_projection WHERE json_extract(projection_json,'$.schema')='bonfyre-channel-surface.v1';")" = 2 ]
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM interaction_surface_composition WHERE json_extract(composition_json,'$.target_lowerer_state')='executed_public_channel_surface';")" = 2 ]
printf '%s\n' 'interaction grammar: PASS'
