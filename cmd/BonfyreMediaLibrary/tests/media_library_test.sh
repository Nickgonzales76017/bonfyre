#!/bin/sh
set -eu

bin=${1:?binary}; root=$(mktemp -d); trap 'rm -rf "$root"' EXIT
db="$root/media.db"; cms="$root/cms.db"
repo=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
estate="$repo/cmd/BonfyreExternalEstate/bonfyre-external-estate"
workers="$repo/cmd/BonfyreLoweringWorkers/bonfyre-lowering-workers"
auth="$repo/cmd/BonfyreAuth/bonfyre-auth"
auth_db="$root/auth.db"

# The CMS MediaProgram is the representation owner.  The MediaLibrary only
# binds it into personal/community shelves and never copies the media object.
sqlite3 "$cms" "CREATE TABLE media_program(id INTEGER PRIMARY KEY,program_id TEXT,subject_ref TEXT,media_kind TEXT,asset_ref TEXT,representations TEXT,publication_state TEXT,status TEXT); INSERT INTO media_program VALUES(1,'blade-runner:watch','movie:blade-runner','video','/media/blade-master.mkv','[{\"id\":\"lan-h265\",\"asset_ref\":\"/media/blade-4k.mkv\",\"codec\":\"h265\",\"bitrate_kbps\":24000,\"delivery\":\"direct\"},{\"id\":\"mobile-h264\",\"asset_ref\":\"/media/blade-1080.mp4\",\"codec\":\"h264\",\"bitrate_kbps\":4000,\"delivery\":\"direct\"}]','published','published'); INSERT INTO media_program VALUES(2,'after-hours:listen','recording:after-hours','audio','/media/after-hours-master.flac','[{\"id\":\"stream-opus\",\"asset_ref\":\"/media/after-hours.opus\",\"codec\":\"opus\",\"bitrate_kbps\":192,\"delivery\":\"direct\"}]','published','published');"

"$bin" library-put --db "$db" --id nick-library --mode personal --owner actor:nick --actor actor:nick --title 'Nick media' --visibility private >/dev/null
"$bin" bind --db "$db" --library nick-library --subject movie:blade-runner --program blade-runner:watch --collection favorites --state active --by actor:nick >/dev/null
"$bin" bind --db "$db" --library nick-library --subject recording:after-hours --program after-hours:listen --collection favorites --state active --by actor:nick >/dev/null

# A personal library is not accidentally public.
! "$bin" catalog --db "$db" --library nick-library --actor actor:other >/dev/null 2>&1
"$bin" catalog --db "$db" --library nick-library --actor actor:nick > "$root/personal.json"
grep -q '"mode":"personal"' "$root/personal.json"
grep -q 'blade-runner:watch' "$root/personal.json"
"$bin" surface --db "$db" --cms-db "$cms" --library nick-library --actor actor:nick > "$root/personal-surface.json"
grep -q '"grammar":"personal_media_habitat"' "$root/personal-surface.json"
grep -q '"ContinueWatching"' "$root/personal-surface.json"
grep -q '"media_kind":"video"' "$root/personal-surface.json"
grep -q '"id":"videos","items":1' "$root/personal-surface.json"
grep -q '"id":"music","items":1' "$root/personal-surface.json"
! "$bin" surface --db "$db" --cms-db "$cms" --library nick-library --actor actor:other >/dev/null 2>&1

# Public ingress receives no caller-chosen actor.  BonfyreAuth exposes an
# explicit, revocable ActorClosure only after an administrator binds the login
# principal to the canonical coordinate.  The media owner asks Auth; it does
# not inspect Auth tables or treat an account as an Actor identity.
make -C "$repo/cmd/BonfyreAuth" all >/dev/null
"$auth" --db "$auth_db" signup --email nick-media@example.test --name 'Nick Media' --password fixture-secret --tier free >/dev/null
auth_token=$("$auth" --db "$auth_db" login --email nick-media@example.test --password fixture-secret | python3 -c 'import json,sys; print(json.load(sys.stdin)["session"]["token"])')
! BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-catalog --db "$db" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library >/dev/null 2>&1
"$auth" --db "$auth_db" actor-bind --user-id 1 --actor actor:nick >/dev/null
BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-catalog --db "$db" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library > "$root/authenticated-catalog.json"
grep -q 'blade-runner:watch' "$root/authenticated-catalog.json"
printf '%s\n' "$auth_token" | BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-catalog --db "$db" --auth-db "$auth_db" --auth-token-stdin --library nick-library > "$root/authenticated-catalog-stdin.json"
cmp "$root/authenticated-catalog.json" "$root/authenticated-catalog-stdin.json"
! BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-catalog --db "$db" --auth-db "$auth_db" --auth-token "$auth_token" --actor actor:other --library nick-library >/dev/null 2>&1
BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-surface --db "$db" --cms-db "$cms" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library > "$root/authenticated-surface.json"
grep -q '"grammar":"personal_media_habitat"' "$root/authenticated-surface.json"
BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-playback-plan --db "$db" --cms-db "$cms" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library --subject movie:blade-runner --capabilities '{"codecs":["h264"],"max_bitrate_kbps":7000}' > "$root/authenticated-plan.json"
grep -q '"state":"DIRECT_PLAYABLE"' "$root/authenticated-plan.json"
BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-session-open --db "$db" --cms-db "$cms" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library --subject movie:blade-runner --capabilities '{"codecs":["h264"],"max_bitrate_kbps":7000}' > "$root/authenticated-session.json"
auth_session=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["session_id"])' "$root/authenticated-session.json")
BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-progress --db "$db" --auth-db "$auth_db" --auth-token "$auth_token" --session "$auth_session" --position-ms 61000 --state paused > "$root/authenticated-progress.json"
grep -q 'actor_reported_continuity_not_watch_proof' "$root/authenticated-progress.json"
BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-continue --db "$db" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library > "$root/authenticated-continue.json"
grep -q '"position_ms":61000' "$root/authenticated-continue.json"
"$auth" --db "$auth_db" actor-revoke --user-id 1 >/dev/null
! BONFYRE_AUTH_BIN="$auth" "$bin" authenticated-playback-plan --db "$db" --cms-db "$cms" --auth-db "$auth_db" --auth-token "$auth_token" --library nick-library --subject movie:blade-runner --capabilities '{"codecs":["h264"],"max_bitrate_kbps":7000}' >/dev/null 2>&1

# Listen is the same canonical MediaProgram / representation path, not a
# second audio-only library.  The plan remains a direct representation choice,
# never an audio stream, licence, or listening proof.
"$bin" playback-plan --db "$db" --cms-db "$cms" --library nick-library --actor actor:nick --subject recording:after-hours --capabilities '{"codecs":["opus"],"max_bitrate_kbps":256}' > "$root/audio-plan.json"
grep -q '"representation_id":"stream-opus"' "$root/audio-plan.json"
grep -q 'no_transport_or_watch_claim' "$root/audio-plan.json"

# Device capabilities choose an existing direct representation.  They do not
# authorize a stream, invoke a transcoder, or claim the item was watched.
"$bin" playback-plan --db "$db" --cms-db "$cms" --library nick-library --actor actor:nick --subject movie:blade-runner --capabilities '{"codecs":["h264"],"max_bitrate_kbps":7000}' > "$root/plan.json"
grep -q '"state":"DIRECT_PLAYABLE"' "$root/plan.json"
grep -q '"representation_id":"mobile-h264"' "$root/plan.json"
! "$bin" playback-plan --db "$db" --cms-db "$cms" --library nick-library --actor actor:nick --subject movie:blade-runner --capabilities '{"codecs":["av1"],"max_bitrate_kbps":7000}' > "$root/refused.json" 2>&1
grep -q 'REFUSED_TRANSCODE_REQUIRED' "$root/refused.json"

"$bin" session-open --db "$db" --cms-db "$cms" --library nick-library --actor actor:nick --subject movie:blade-runner --capabilities '{"codecs":["h264"],"max_bitrate_kbps":7000}' > "$root/session.json"
session=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["session_id"])' "$root/session.json")
"$bin" progress --db "$db" --session "$session" --actor actor:nick --position-ms 124000 --state paused > "$root/progress.json"
grep -q 'actor_reported_continuity_not_watch_proof' "$root/progress.json"
"$bin" continue --db "$db" --library nick-library --actor actor:nick > "$root/continue.json"
grep -q '"position_ms":124000' "$root/continue.json"

# Community is Jellyfin-like sharing, not public-by-default.  A viewer must be
# granted by the community owner before the same canonical program can appear.
"$bin" library-put --db "$db" --id film-club --mode community --owner community:film-club --actor community:film-club --title 'Film Club' --visibility community >/dev/null
"$bin" bind --db "$db" --library film-club --subject movie:blade-runner --program blade-runner:watch --collection neo-noir --state active --by community:film-club >/dev/null
! "$bin" catalog --db "$db" --library film-club --actor actor:nick >/dev/null 2>&1
"$bin" access-put --db "$db" --library film-club --actor actor:nick --role viewer --state active --by community:film-club >/dev/null
"$bin" catalog --db "$db" --library film-club --actor actor:nick > "$root/community.json"
grep -q '"mode":"community"' "$root/community.json"
"$bin" surface --db "$db" --cms-db "$cms" --library film-club --actor actor:nick > "$root/community-surface.json"
grep -q '"grammar":"community_media_channel"' "$root/community-surface.json"
grep -q '"Community"' "$root/community-surface.json"

# One shared program becomes two library bindings, never two media identities.
"$bin" emit-advertisements --db "$db" > "$root/ads.jsonl"
[ "$(grep -c '"candidate_kind":"media_library"' "$root/ads.jsonl")" = 3 ]
grep -q '"exact_verifier":"MediaLibrary:access"' "$root/ads.jsonl"
grep -q '"lowerer_ref":"MediaLibraryCompiler:surface"' "$root/ads.jsonl"
# Media owner identities use their own canonical syntax.  Address terms are
# compact forwarding atoms, so raw `movie:blade-runner` cannot leak into the
# shared coordinate vocabulary.
grep -q '"subject:movie-blade-runner"' "$root/ads.jsonl"
grep -q '"library:nick-library"' "$root/ads.jsonl"
jq -s . "$root/ads.jsonl" > "$root/ads.json"
cue vet "$repo/schemas/control/address-advertisement.cue" "$root/ads.json" -d '#CandidateAdvertisementSet'

# The same advertisements enter the shared forwarding plane.  A kind
# partition prevents media library candidates from becoming provider/source
# candidates, and a MediaLibrary FactDelta reaches only the matching library
# membership lowerer.
make -C "$repo/cmd/BonfyreExternalEstate" all >/dev/null
make -C "$repo/cmd/BonfyreLoweringWorkers" all >/dev/null
"$estate" emit-media-library --media-library-db "$db" > "$root/native-ads.jsonl"
sort "$root/ads.jsonl" > "$root/ads.sorted"
sort "$root/native-ads.jsonl" > "$root/native-ads.sorted"
cmp "$root/ads.sorted" "$root/native-ads.sorted"
"$estate" compile-advertisements --db "$root/address.db" --media-library-db "$db" > "$root/compiled.json"
grep -q '"compiled_owner_populations":1' "$root/compiled.json"
"$estate" forward --db "$root/address.db" --kind media_library --need subject:movie-blade-runner --need operation:playback > "$root/forward.json"
grep -q 'media:library:nick-library:blade-runner:watch' "$root/forward.json"
grep -q 'media:library:film-club:blade-runner:watch' "$root/forward.json"
! "$estate" forward --db "$root/address.db" --kind provider --need subject:movie-blade-runner >/dev/null 2>&1
# The same typed media advertisements become a scoped MCP template/tool
# aperture.  This is only a forwarding advertisement: the authenticated media
# owner still rechecks access before the catalog or plan body is exposed.
"$estate" mcp-aperture --db "$root/address.db" --actor-closure sha256:actor-fixture --address-closure sha256:address-fixture --target-frontier sha256:target-fixture --policy-generation sha256:policy-fixture > "$root/media-aperture.json"
grep -q 'bonfyre://media/library/{library}/catalog' "$root/media-aperture.json"
grep -q '"identity":"media_library_catalog"' "$root/media-aperture.json"
grep -q '"identity":"media_playback_plan"' "$root/media-aperture.json"
grep -q '"exact_verifier":"MediaLibrary:access"' "$root/media-aperture.json"
grep -q '"identity":"media_transport_lease"' "$root/media-aperture.json"
grep -q '"exact_verifier":"MediaDelivery:right"' "$root/media-aperture.json"
printf '%s\n' '{"schema":"bonfyre-fact-delta.v1","delta_id":"delta:personal-library","writes":["MediaLibrary:nick-library"]}' > "$root/library-delta.json"
"$estate" fact-delta --db "$root/address.db" --delta "$root/library-delta.json" > "$root/library-cut.json"
grep -q 'media:library:nick-library:blade-runner:watch' "$root/library-cut.json"
grep -q 'media:library:nick-library:after-hours:listen' "$root/library-cut.json"
! grep -q 'media:library:film-club:blade-runner:watch' "$root/library-cut.json"
grep -q '"lowering_state":"queued"' "$root/library-cut.json"
mkdir "$root/lowerer-inbox"
"$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/lowerer-inbox" --limit 8 > "$root/handoffs.json"
grep -q '"lowering_handoffs_dispatched":2' "$root/handoffs.json"
"$workers" drain --db "$root/lowerer-artifacts.db" --lowerer-dir "$root/lowerer-inbox" --media-library "$db" --limit 8 > "$root/lowerer.json"
grep -q '"handled":2' "$root/lowerer.json"
[ "$(sqlite3 "$root/lowerer-artifacts.db" "SELECT count(*) FROM media_library_surface_artifact WHERE library_id='nick-library' AND grammar='personal_media_habitat';")" = 2 ]

# A new complete MediaLibrary generation atomically retires only its own
# stale advertisements; the unrelated community binding remains visible.
"$bin" bind --db "$db" --library nick-library --subject movie:blade-runner --program blade-runner:watch --collection favorites --state removed --by actor:nick >/dev/null
"$bin" bind --db "$db" --library nick-library --subject recording:after-hours --program after-hours:listen --collection favorites --state removed --by actor:nick >/dev/null
"$estate" compile-advertisements --db "$root/address.db" --media-library-db "$db" > "$root/retired.json"
"$estate" forward --db "$root/address.db" --kind media_library --need library:nick-library > "$root/retired-forward.json"
! grep -q 'media:library:nick-library:blade-runner:watch' "$root/retired-forward.json"
"$estate" forward --db "$root/address.db" --kind media_library --need library:film-club > "$root/community-forward.json"
grep -q 'media:library:film-club:blade-runner:watch' "$root/community-forward.json"
echo 'BonfyreMediaLibrary: PASS'
