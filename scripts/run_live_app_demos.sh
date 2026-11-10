#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEMOS_DIR="$ROOT/site/demos"
BIN_DIR="$ROOT/cmd"
mkdir -p "$DEMOS_DIR"
TEST_WAV="$DEMOS_DIR/test_a.wav"
if [ ! -f "$TEST_WAV" ]; then
  echo "Missing test WAV: $TEST_WAV" >&2
  exit 1
fi

log() { printf '[demo] %s\n' "$*"; }
run_bin() {
  local name="$1"; shift
  local outdir="$DEMOS_DIR/${name}"
  mkdir -p "$outdir"
  {
    printf -- '--- running %q' "$name"
    printf ' %q' "$@"
    printf ' ---\n'
  } > "$outdir/run.log"
  if [ -x "$BIN_DIR/${name}/${name}" ]; then
    (cd "$BIN_DIR/${name}" && ./"${name}" "$@") >> "$outdir/run.log" 2>&1 || echo "ERROR: $name failed (see run.log)" >> "$outdir/run.log"
  else
    echo "MISSING BINARY: $name" > "$outdir/run.log"
  fi
}

# App-specific demo flows (best-effort, non-blocking)
# Each step writes to site/demos/<app>/

log "Running demos (best-effort). Outputs -> $DEMOS_DIR"

# 1 Shift Handoff Board: brief -> proof -> offer -> pack
run_bin BonfyreBrief run "$TEST_WAV" "$DEMOS_DIR/shift_handoff_brief.json"
run_bin BonfyreProof analyze "$DEMOS_DIR/shift_handoff_brief.json" "$DEMOS_DIR/shift_handoff_proof.json"
run_bin BonfyreOffer gen-offer "$DEMOS_DIR/shift_handoff_brief.json" "$DEMOS_DIR/shift_handoff_offer.json"
run_bin BonfyrePack pack "$DEMOS_DIR/shift_handoff_offer.json" "$DEMOS_DIR/shift_handoff_package.zip"

# 2 Memory Atlas: transcribe -> brief -> embed -> render
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/memory_atlas_transcript.json" 2>&1 || true
run_bin BonfyreBrief run "$DEMOS_DIR/memory_atlas_transcript.json" "$DEMOS_DIR/memory_atlas_brief.json" || true
run_bin BonfyreEmbed insert "$DEMOS_DIR/memory_atlas_brief.json" "$DEMOS_DIR/memory_atlas_embed.db" || true
run_bin BonfyreRender render "$DEMOS_DIR/memory_atlas_brief.json" "$DEMOS_DIR/memory_atlas.html" || true

# 3 Freelancer Evidence Vault: transcribe -> brief -> proof -> pack
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/freelancer_transcript.json" 2>&1 || true
run_bin BonfyreBrief run "$DEMOS_DIR/freelancer_transcript.json" "$DEMOS_DIR/freelancer_brief.json" || true
run_bin BonfyreProof analyze "$DEMOS_DIR/freelancer_brief.json" "$DEMOS_DIR/freelancer_proof.json" || true
run_bin BonfyrePack pack "$DEMOS_DIR/freelancer_proof.json" "$DEMOS_DIR/freelancer_package.zip" || true

# 4 Customer Voice Board: transcribe -> tone -> tag -> embed
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/customer_voice_transcript.json" 2>&1 || true
run_bin BonfyreTone extract "$TEST_WAV" "$DEMOS_DIR/customer_voice_tone.json" || true
run_bin BonfyreTag tag "$DEMOS_DIR/customer_voice_transcript.json" "$DEMOS_DIR/customer_tags.json" || true
run_bin BonfyreEmbed insert "$DEMOS_DIR/customer_tags.json" "$DEMOS_DIR/customer_embed.db" || true

# 5 Family History: transcribe -> brief -> render
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/family_transcript.json" 2>&1 || true
run_bin BonfyreBrief run "$DEMOS_DIR/family_transcript.json" "$DEMOS_DIR/family_brief.json" || true
run_bin BonfyreRender render "$DEMOS_DIR/family_brief.json" "$DEMOS_DIR/family.html" || true

# 6 Podcast Plant: media-prep -> transcribe -> brief -> emit
run_bin BonfyreMediaPrep prep "$TEST_WAV" "$DEMOS_DIR/podcast_prep.wav" || true
run_bin BonfyreTranscribe run "$DEMOS_DIR/podcast_prep.wav" > "$DEMOS_DIR/podcast_transcript.json" 2>&1 || true
run_bin BonfyreBrief run "$DEMOS_DIR/podcast_transcript.json" "$DEMOS_DIR/podcast_brief.json" || true
run_bin BonfyreEmit emit "$DEMOS_DIR/podcast_brief.json" "$DEMOS_DIR/podcast_public" || true

# 7 Postmortem Atlas: transcribe -> tag -> embed -> render
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/postmortem_transcript.json" 2>&1 || true
run_bin BonfyreTag tag "$DEMOS_DIR/postmortem_transcript.json" "$DEMOS_DIR/postmortem_tags.json" || true
run_bin BonfyreEmbed insert "$DEMOS_DIR/postmortem_tags.json" "$DEMOS_DIR/postmortem_embed.db" || true
run_bin BonfyreRender render "$DEMOS_DIR/postmortem_tags.json" "$DEMOS_DIR/postmortem.html" || true

# 8 Explain This Repo: ingest -> canon -> brief -> render
# Use current repo README as input
REPO_README="$ROOT/README.md"
if [ -f "$REPO_README" ]; then
  run_bin BonfyreIngest ingest "$REPO_README" "$DEMOS_DIR/explain_repo_ingest.json" || true
  run_bin BonfyreCanon canon "$DEMOS_DIR/explain_repo_ingest.json" "$DEMOS_DIR/explain_repo_canon.json" || true
  run_bin BonfyreBrief run "$DEMOS_DIR/explain_repo_canon.json" "$DEMOS_DIR/explain_repo_brief.json" || true
  run_bin BonfyreRender render "$DEMOS_DIR/explain_repo_brief.json" "$DEMOS_DIR/explain_repo.html" || true
fi

# 9 Town Box: transcribe -> brief -> tag -> render
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/townbox_transcript.json" 2>&1 || true
run_bin BonfyreBrief run "$DEMOS_DIR/townbox_transcript.json" "$DEMOS_DIR/townbox_brief.json" || true
run_bin BonfyreTag tag "$DEMOS_DIR/townbox_brief.json" "$DEMOS_DIR/townbox_tags.json" || true
run_bin BonfyreRender render "$DEMOS_DIR/townbox_brief.json" "$DEMOS_DIR/townbox.html" || true

# 10 Grant Evidence Pack: transcribe -> proof -> pack -> emit
run_bin BonfyreTranscribe run "$TEST_WAV" > "$DEMOS_DIR/grant_transcript.json" 2>&1 || true
run_bin BonfyreProof analyze "$DEMOS_DIR/grant_transcript.json" "$DEMOS_DIR/grant_proof.json" || true
run_bin BonfyrePack pack "$DEMOS_DIR/grant_proof.json" "$DEMOS_DIR/grant_pack.zip" || true
run_bin BonfyreEmit emit "$DEMOS_DIR/grant_pack.zip" "$DEMOS_DIR/grant_public" || true

# 11 Micro-Consulting Storefront: offer -> meter -> pack
run_bin BonfyreOffer gen-offer "$DEMOS_DIR/micro_offer.json" "$DEMOS_DIR/micro_offer.json" || true
run_bin BonfyreMeter run "$DEMOS_DIR/micro_offer.json" "$DEMOS_DIR/micro_meter.json" || true
run_bin BonfyrePack pack "$DEMOS_DIR/micro_offer.json" "$DEMOS_DIR/micro_offer_pack.zip" || true

# 12 Personal Legal Prep Binder: proof -> tag -> pack
run_bin BonfyreProof analyze "$DEMOS_DIR/family_transcript.json" "$DEMOS_DIR/legal_proof.json" || true
run_bin BonfyreTag tag "$DEMOS_DIR/legal_proof.json" "$DEMOS_DIR/legal_tags.json" || true
run_bin BonfyrePack pack "$DEMOS_DIR/legal_proof.json" "$DEMOS_DIR/legal_pack.zip" || true

# 13 OSS Maintainer Cockpit: ingest -> tag -> embed
run_bin BonfyreIngest ingest "$REPO_README" "$DEMOS_DIR/oss_ingest.json" || true
run_bin BonfyreTag tag "$DEMOS_DIR/oss_ingest.json" "$DEMOS_DIR/oss_tags.json" || true
run_bin BonfyreEmbed insert "$DEMOS_DIR/oss_tags.json" "$DEMOS_DIR/oss_embed.db" || true

# 14 Release-Note Radio: narrate -> render -> emit
run_bin BonfyreNarrate verify "$TEST_WAV" "$TEST_WAV" > "$DEMOS_DIR/release_radio_narrate.json" 2>&1 || true
run_bin BonfyreRender render "$DEMOS_DIR/release_radio_narrate.json" "$DEMOS_DIR/release_radio.html" || true
run_bin BonfyreEmit emit "$DEMOS_DIR/release_radio.html" "$DEMOS_DIR/release_radio_pub" || true

# 15 Async Standup Newspaper: tone -> render
run_bin BonfyreTone extract "$TEST_WAV" "$DEMOS_DIR/async_tone.json" || true
run_bin BonfyreRender render "$DEMOS_DIR/async_tone.json" "$DEMOS_DIR/async_standup.html" || true

# 16 Competitive Intelligence Scrapbook: embed -> vec -> index
run_bin BonfyreEmbed insert "$DEMOS_DIR/explain_repo_brief.json" "$DEMOS_DIR/ci_embed.db" || true
run_bin BonfyreVec query "$DEMOS_DIR/ci_embed.db" "test" > "$DEMOS_DIR/ci_vec_query.json" 2>&1 || true
run_bin BonfyreIndex index "$DEMOS_DIR/ci_embed.db" "$DEMOS_DIR/ci_index.json" || true

log "Demos finished. Review $DEMOS_DIR for outputs and logs."
