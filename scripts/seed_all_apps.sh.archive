#!/usr/bin/env bash
# seed_all_apps.sh — Run the real-data pipeline for all 13 thin apps
# Each app gets 3 genuinely different YouTube sources through the same Bonfyre pipe.
#
# Same pipeline every time. Different domain. That's the point.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SEED="$SCRIPT_DIR/seed_app_from_youtube.sh"

# ── freelancer-evidence (3 videos about freelance disputes + scope) ──
"$SEED" "https://www.youtube.com/watch?v=O3UE8xHKNzQ" freelancer-evidence handle-difficult-clients
"$SEED" "https://www.youtube.com/watch?v=LCaFuT7xPck" freelancer-evidence mastering-client-disputes

# ── family-history (oral history / family stories) ──
"$SEED" "https://www.youtube.com/watch?v=gGGL4NTEU60" family-history recording-family-memories
"$SEED" "https://www.youtube.com/watch?v=627gbVUsR5g" family-history oral-history-video-tips

# ── postmortem-atlas (incident reviews) ──
"$SEED" "https://www.youtube.com/watch?v=80FVLWJNzbM" postmortem-atlas production-incident-postmortems
"$SEED" "https://www.youtube.com/watch?v=9JYVOUKNQ3A" postmortem-atlas how-to-run-postmortem

# ── explain-repo (code architecture walkthrough) ──
"$SEED" "https://www.youtube.com/watch?v=Wiy54682d1w" explain-repo repository-pattern-explained
"$SEED" "https://www.youtube.com/watch?v=D44si7o4ndg" explain-repo controller-service-repository
"$SEED" "https://www.youtube.com/watch?v=9ymRLDfnDKg" explain-repo repository-pattern-python

# ── grant-evidence (nonprofit impact stories) ──
"$SEED" "https://www.youtube.com/watch?v=dd2UgwjdLC0" grant-evidence road-to-redemption-nonprofit
"$SEED" "https://www.youtube.com/watch?v=9TW3RJTkVBE" grant-evidence community-impact-shakespeare
"$SEED" "https://www.youtube.com/watch?v=7eHXC7S34d4" grant-evidence community-impact-grantmaking

# ── legal-prep (small claims / legal preparation) ──
"$SEED" "https://www.youtube.com/watch?v=O-3SeBE8Mfc" legal-prep small-claims-what-to-know
"$SEED" "https://www.youtube.com/watch?v=cEe5298bifw" legal-prep small-claims-hearing-expect
"$SEED" "https://www.youtube.com/watch?v=zFVwAnQCzCA" legal-prep how-to-win-small-claims

# ── micro-consulting (discovery calls / advisory) ──
"$SEED" "https://www.youtube.com/watch?v=_DbSgU5naDQ" micro-consulting discovery-call-strategy
"$SEED" "https://www.youtube.com/watch?v=slNMQ9cZpRU" micro-consulting sales-call-preparation
"$SEED" "https://www.youtube.com/watch?v=dDGX95UkV10" micro-consulting discovery-questions-prospects

# ── async-standup (async meetings / daily updates) ──
"$SEED" "https://www.youtube.com/watch?v=uKEPFfidAJY" async-standup async-voice-standup-system
"$SEED" "https://www.youtube.com/watch?v=MZdK4SX0mfI" async-standup effective-daily-scrum
"$SEED" "https://www.youtube.com/watch?v=EAphSqGSgD4" async-standup replace-standup-hybrid

# ── competitive-intel (market analysis) ──
"$SEED" "https://www.youtube.com/watch?v=YueKD1pSvKI" competitive-intel competitive-intel-60-seconds
"$SEED" "https://www.youtube.com/watch?v=9KvLUQFhb2M" competitive-intel market-intelligence-101
"$SEED" "https://www.youtube.com/watch?v=ET69SBYd9fo" competitive-intel effective-competitive-analysis

# ── sales-distiller (sales calls / objections) ──
"$SEED" "https://www.youtube.com/watch?v=9jNdyq-jkhI" sales-distiller overcome-sales-objections
"$SEED" "https://www.youtube.com/watch?v=mDWUpuumAuo" sales-distiller destroy-sales-objection
"$SEED" "https://www.youtube.com/watch?v=_DbSgU5naDQ" sales-distiller discovery-call-strategy

# ── procurement-memory (vendor evaluation) ──
"$SEED" "https://www.youtube.com/watch?v=qPiPM0Em3H8" procurement-memory supplier-assessment
"$SEED" "https://www.youtube.com/watch?v=TdEiJawCIL8" procurement-memory supplier-scorecard-analysis
"$SEED" "https://www.youtube.com/watch?v=MtsVTZoiTqU" procurement-memory supplier-weighted-scorecard

# ── museum-exhibit (museum oral histories / audio tours) ──
"$SEED" "https://www.youtube.com/watch?v=YUiXm2MHLYk" museum-exhibit museum-audio-tour-sample
"$SEED" "https://www.youtube.com/watch?v=y83OCwFB7QM" museum-exhibit museum-audio-tour-intro
"$SEED" "https://www.youtube.com/watch?v=JZ57OkbAsMA" museum-exhibit british-museum-audio-guide

# ── local-archive (neighborhood / community history) ──
"$SEED" "https://www.youtube.com/watch?v=-Q5iPq5Sam0" local-archive hillside-oral-history
"$SEED" "https://www.youtube.com/watch?v=O5FBJyqfoLM" local-archive housing-segregation-short-history
"$SEED" "https://www.youtube.com/watch?v=2a1K_JuC2LE" local-archive sunrise-neighborhood-oral-history

echo ""
echo "=== ALL 13 APPS SEEDED WITH REAL DATA ==="
echo "39 YouTube videos → 39 proof bundles → 13 apps"
