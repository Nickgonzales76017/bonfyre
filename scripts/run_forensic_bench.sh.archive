#!/usr/bin/env bash
# run_forensic_bench.sh — master orchestration benchmark
#
# Acquires corpus, runs all pipeline stages, measures throughput, and emits
# a structured benchmark.md + evidence packet directory.
#
# Usage:
#   ./run_forensic_bench.sh [--bench-dir DIR] [--corpus-dir DIR] \
#       [--max-docs N] [--audio FILE] [--dry-run] [--skip-acquire] \
#       [--skip-transcribe] [--skip-redaction] [--skip-names] \
#       [--skip-entities] [--skip-depo] [--names-file FILE]
#
# Sections benchmarked:
#   1. Corpus acquisition  — pages/sec ingest rate from disk
#   2. Name watchlist      — recall rate on known names in corpus
#   3. Redaction diff      — matched/unmatched across sampled pairs
#   4. Entity clustering   — edges/nodes found, cluster count
#   5. Deposition pipeline — full pipeline on a sample audio file
#   6. Evidence packet     — final completeness score
set -euo pipefail

# ── defaults ─────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="${EPSTEIN_BENCH_DIR:-/tmp/epstein-bench}"
CORPUS_DIR="${BENCH_DIR}/corpus"
NAMES_FILE="${SCRIPT_DIR}/epstein_names.txt"
AUDIO_FILE=""                   # optional: provide a deposition audio file
MAX_DOCS=0                      # 0 = process all docs found
DRY_RUN=0
SKIP_ACQUIRE=0
SKIP_TRANSCRIBE=0
SKIP_REDACTION=0
SKIP_NAMES=0
SKIP_ENTITIES=0
SKIP_DEPO=0

# well-known names used for recall evaluation (ground truth in this corpus)
KNOWN_NAMES=(
  "Jeffrey Epstein"
  "Ghislaine Maxwell"
  "Virginia Giuffre"
  "Alan Dershowitz"
  "Prince Andrew"
  "Leslie Wexner"
  "Bill Richardson"
  "Jean-Luc Brunel"
  "Sarah Kellen"
  "Nadia Marcinkova"
)

# ── args ──────────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $0 [options]

  --bench-dir DIR       Root output directory          (default: /tmp/epstein-bench)
  --corpus-dir DIR      PDF corpus directory            (default: BENCH_DIR/corpus)
  --max-docs N          Limit PDFs processed per stage  (default: 0=all)
  --audio FILE          Audio deposition for stage 5    (required for --skip-depo=0)
  --names-file FILE     Name watchlist                   (default: scripts/epstein_names.txt)
  --dry-run             Print commands, don't run
  --skip-acquire        Skip corpus download
  --skip-transcribe     Skip audio transcription
  --skip-redaction      Skip redaction diff stage
  --skip-names          Skip name watchlist stage
  --skip-entities       Skip entity clustering stage
  --skip-depo           Skip full deposition pipeline
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bench-dir)        BENCH_DIR="$2";        shift 2 ;;
    --corpus-dir)       CORPUS_DIR="$2";       shift 2 ;;
    --max-docs)         MAX_DOCS="$2";         shift 2 ;;
    --audio)            AUDIO_FILE="$2";       shift 2 ;;
    --names-file)       NAMES_FILE="$2";       shift 2 ;;
    --dry-run)          DRY_RUN=1;             shift ;;
    --skip-acquire)     SKIP_ACQUIRE=1;        shift ;;
    --skip-transcribe)  SKIP_TRANSCRIBE=1;     shift ;;
    --skip-redaction)   SKIP_REDACTION=1;      shift ;;
    --skip-names)       SKIP_NAMES=1;          shift ;;
    --skip-entities)    SKIP_ENTITIES=1;       shift ;;
    --skip-depo)        SKIP_DEPO=1;           shift ;;
    -h|--help) usage ;;
    *) echo "Unknown: $1"; usage ;;
  esac
done

# ── helpers ──────────────────────────────────────────────────────────────────
log()      { echo ""; echo "━━━  $*  ━━━"; }
info()     { echo "    $*"; }
run()      { echo "[RUN] $*"; [[ $DRY_RUN -eq 1 ]] || "$@"; }
run_sh()   { echo "[SHELL] $*"; [[ $DRY_RUN -eq 1 ]] || eval "$*"; }

elapsed() {
  # Usage: elapsed START_EPOCH  → "3m 42s"
  local secs=$(( $(date +%s) - $1 ))
  printf "%dm %ds" $(( secs / 60 )) $(( secs % 60 ))
}

sha256_file() {
  if command -v sha256sum &>/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

count_pdfs() {
  local dir="$1"
  [[ -d "$dir" ]] || { echo 0; return; }
  find "$dir" -name '*.pdf' | wc -l | tr -d ' '
}

count_pages_fast() {
  # Quick page count via pdfinfo or PyMuPDF
  local dir="$1"
  if command -v pdfinfo &>/dev/null; then
    find "$dir" -name '*.pdf' | \
      xargs -P 4 -I{} sh -c 'pdfinfo "{}" 2>/dev/null | grep "^Pages:" | awk "{print \$2}"' | \
      awk '{s+=$1} END {print s+0}'
  else
    python3 -c "
import fitz, pathlib, sys
total = 0
for p in pathlib.Path(sys.argv[1]).rglob('*.pdf'):
    try:
        total += len(fitz.open(str(p)))
    except:
        pass
print(total)
" "$dir" 2>/dev/null
  fi
}

has_docs_fts_table() {
  local db="$1"
  [[ -f "$db" ]] || return 1
  command -v sqlite3 &>/dev/null || return 1
  sqlite3 "$db" "SELECT name FROM sqlite_master WHERE type='table' AND name='docs_fts';" 2>/dev/null | grep -q '^docs_fts$'
}

build_watchlist_db() {
  local db="$1" ingest_manifest="$2" corpus_dir="$3"
  command -v sqlite3 &>/dev/null || { info "WARN: sqlite3 not found; cannot auto-build watchlist DB"; return 1; }

  python3 - "$db" "$ingest_manifest" "$corpus_dir" <<'PY'
import json
import sqlite3
import sys
from pathlib import Path

db = Path(sys.argv[1])
manifest = Path(sys.argv[2])
corpus = Path(sys.argv[3])
db.parent.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(str(db))
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS docs_fts")
cur.execute("CREATE VIRTUAL TABLE docs_fts USING fts5(id UNINDEXED, source_file UNINDEXED, page_num UNINDEXED, text)")

rows = 0
doc_id = 1

def ingest_text(path: Path):
  global rows, doc_id
  try:
    text = path.read_text(encoding='utf-8', errors='replace')
  except Exception:
    return
  if not text.strip():
    return
  cur.execute(
    "INSERT INTO docs_fts(id, source_file, page_num, text) VALUES (?, ?, ?, ?)",
    (str(doc_id), path.name, 0, text),
  )
  rows += 1
  doc_id += 1

if manifest.exists():
  for line in manifest.read_text(encoding='utf-8', errors='replace').splitlines():
    line = line.strip()
    if not line:
      continue
    try:
      rec = json.loads(line)
    except Exception:
      continue
    out_path = rec.get('output')
    src_path = rec.get('source')
    candidate = Path(out_path) if out_path else None
    if candidate and candidate.exists():
      ingest_text(candidate)
      continue
    if src_path:
      p = Path(src_path)
      if p.exists():
        ingest_text(p)
else:
  for p in sorted(corpus.rglob('*_djvu.txt')):
    ingest_text(p)

conn.commit()
conn.close()
print(rows)
PY
}

# ── benchmark state (bash 3.2 compatible) ───────────────────────────────────
set_result() {
  local key="$1" value="$2"
  eval "RESULTS_${key}=\"\$value\""
}

get_result() {
  local key="$1" default="${2:-}"
  eval "printf '%s' \"\${RESULTS_${key}:-$default}\""
}

set_timing() {
  local key="$1" value="$2"
  eval "TIMINGS_${key}=\"\$value\""
}

get_timing() {
  local key="$1" default="${2:-}"
  eval "printf '%s' \"\${TIMINGS_${key}:-$default}\""
}

set_status() {
  local key="$1" value="$2"
  eval "STAGE_STATUS_${key}=\"\$value\""
}

get_status() {
  local key="$1" default="${2:-SKIP}"
  eval "printf '%s' \"\${STAGE_STATUS_${key}:-$default}\""
}

record_timing() {
  local stage="$1" start="$2"
  set_timing "$stage" "$(( $(date +%s) - start ))"
}

# ── create output tree ────────────────────────────────────────────────────────
mkdir -p "$BENCH_DIR"/{corpus,redaction,entities,depo,evidence}
BENCH_START=$(date +%s)
BENCH_TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

info "Benchmark root: $BENCH_DIR"
info "Timestamp:      $BENCH_TIMESTAMP"
[[ $DRY_RUN -eq 1 ]] && info "(DRY-RUN mode — no commands will run)"

# ── stage 1: corpus acquisition ───────────────────────────────────────────────
log "Stage 1: Corpus Acquisition"
set_status acquire SKIP

if [[ $SKIP_ACQUIRE -eq 0 ]]; then
  t0=$(date +%s)
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "[DRY-RUN] $SCRIPT_DIR/acquire_epstein_corpus.sh --out $CORPUS_DIR --dry-run"
    set_status acquire PASS
  else
    if bash "$SCRIPT_DIR/acquire_epstein_corpus.sh" --out "$CORPUS_DIR"; then
      set_status acquire PASS
    else
      set_status acquire FAIL
      info "WARN: acquisition script returned non-zero — partial corpus may be available"
    fi
  fi
  record_timing acquire "$t0"
  set_result acquire_docs "$(count_pdfs "$CORPUS_DIR")"
  info "PDFs on disk after acquire: $(get_result acquire_docs)"
else
  info "Skipped — using existing corpus at $CORPUS_DIR"
  set_result acquire_docs "$(count_pdfs "$CORPUS_DIR")"
  info "PDFs found: $(get_result acquire_docs)"
fi

# ── stage 2: ingest page-count benchmark ──────────────────────────────────────
log "Stage 2: Ingest Throughput (pages/sec)"
set_status ingest SKIP

if [[ $DRY_RUN -eq 0 && -d "$CORPUS_DIR" ]]; then
  t0=$(date +%s)
  TOTAL_PAGES=$(count_pages_fast "$CORPUS_DIR")
  t_elapsed=$(( $(date +%s) - t0 ))
  set_result total_pages "$TOTAL_PAGES"
  if [[ $t_elapsed -gt 0 && $TOTAL_PAGES -gt 0 ]]; then
    set_result pages_per_sec "$(python3 -c "print(round($TOTAL_PAGES/$t_elapsed,1))" 2>/dev/null || echo "n/a")"
  else
    set_result pages_per_sec "n/a"
  fi
  record_timing ingest "$t0"
  set_status ingest PASS
  info "Pages: $(get_result total_pages)  Pages/sec: $(get_result pages_per_sec)"
else
  [[ $DRY_RUN -eq 1 ]] && info "[DRY-RUN] would count pages with PyMuPDF/pdfinfo"
  set_result total_pages "n/a"
  set_result pages_per_sec "n/a"
fi

# ── stage 3: redaction diff ────────────────────────────────────────────────────
log "Stage 3: Redaction Diff"
set_status redaction SKIP

if [[ $SKIP_REDACTION -eq 0 && $DRY_RUN -eq 0 ]]; then
  # Heuristic: pick FBI Vault parts (multi-version PDFs released at different times)
  FBI_DIR="$CORPUS_DIR/fbi_vault"
  REDACTION_OUT="$BENCH_DIR/redaction"
  REDACTION_PAIRS=0
  REDACTION_DIFFS=0

  if [[ -d "$FBI_DIR" ]]; then
    # Compare consecutive parts as a proxy (same reports, different redaction states)
    PARTS=( $(find "$FBI_DIR" -name '*.pdf' | sort) )
    t0=$(date +%s)
    for (( i=0; i < ${#PARTS[@]}-1; i++ )); do
      A="${PARTS[$i]}"
      B="${PARTS[$((i+1))]}"
      OUT_FILE="$REDACTION_OUT/diff_$(basename "$A" .pdf)_vs_$(basename "$B" .pdf).md"
      if python3 "$SCRIPT_DIR/redaction_diff.py" \
           --file-a "$A" --file-b "$B" \
           --out "$OUT_FILE" 2>/dev/null; then
        (( REDACTION_PAIRS++ ))
        # Check if diff found anything
        if grep -q "^|" "$OUT_FILE" 2>/dev/null; then
          (( REDACTION_DIFFS++ ))
        fi
      fi
      [[ $MAX_DOCS -gt 0 && $REDACTION_PAIRS -ge $MAX_DOCS ]] && break
    done
    record_timing redaction "$t0"
    set_result redaction_pairs "$REDACTION_PAIRS"
    set_result redaction_diffs "$REDACTION_DIFFS"
    set_status redaction PASS
    info "Pairs compared: $REDACTION_PAIRS  Pairs with diffs: $REDACTION_DIFFS"
  else
    info "WARN: $FBI_DIR not found — skipping redaction diff"
    set_status redaction SKIP
  fi
elif [[ $DRY_RUN -eq 1 && $SKIP_REDACTION -eq 0 ]]; then
  info "[DRY-RUN] python3 redaction_diff.py --file-a A.pdf --file-b B.pdf --out diff.md"
  set_result redaction_pairs "n/a"
  set_result redaction_diffs "n/a"
fi

# ── stage 4: name watchlist ────────────────────────────────────────────────────
log "Stage 4: Name Watchlist Recall"
set_status names SKIP

if [[ $SKIP_NAMES -eq 0 ]]; then
  # Write ground-truth names to file if custom file not provided
  if [[ ! -f "$NAMES_FILE" ]]; then
    info "Writing default known-names watchlist to $NAMES_FILE"
    [[ $DRY_RUN -eq 0 ]] && printf "%s\n" "${KNOWN_NAMES[@]}" > "$NAMES_FILE"
  fi

  NAMES_OUT="$BENCH_DIR/names_report.md"
  BONFYRE_DB="${BONFYRE_DB:-$BENCH_DIR/bonfyre_index.db}"

  if [[ $DRY_RUN -eq 0 && ! -f "$BONFYRE_DB" ]]; then
    info "Name watchlist DB missing — auto-building docs_fts index at $BONFYRE_DB"
    INGEST_MANIFEST="$BENCH_DIR/ingested/ingest_manifest.jsonl"
    BUILT_ROWS=$(build_watchlist_db "$BONFYRE_DB" "$INGEST_MANIFEST" "$CORPUS_DIR" 2>/dev/null || echo 0)
    info "Auto-indexed rows: ${BUILT_ROWS}"
  fi

  if [[ $DRY_RUN -eq 0 && -f "$BONFYRE_DB" ]] && has_docs_fts_table "$BONFYRE_DB"; then
    t0=$(date +%s)
    if bash "$SCRIPT_DIR/name_watchlist_search.sh" \
         --db "$BONFYRE_DB" \
         --names "$NAMES_FILE" \
         --out "$NAMES_OUT"; then
      record_timing names "$t0"
      # Count hits: lines starting with | in summary table
      HITS=$(grep -c "^|" "$NAMES_OUT" 2>/dev/null || echo 0)
      set_result names_hits "$HITS"
      set_result names_recall "$(python3 -c "
total=${#KNOWN_NAMES[@]}; hits=min(int('${HITS}'), total)
print(f'{100*hits//total}%' if total else 'n/a')
" 2>/dev/null || echo "n/a")"
      set_status names PASS
      info "Names searched: ${#KNOWN_NAMES[@]}  Hits found: $HITS  Recall: $(get_result names_recall)"
    else
      set_status names FAIL
    fi
  else
    if [[ $DRY_RUN -eq 1 ]]; then
      info "[DRY-RUN] name_watchlist_search.sh --db $BONFYRE_DB --names $NAMES_FILE --out $NAMES_OUT"
      set_status names PASS
    else
      info "WARN: watchlist DB unavailable at $BONFYRE_DB (auto-bootstrap failed)"
      set_status names SKIP
    fi
    set_result names_hits "n/a"
    set_result names_recall "n/a"
  fi
fi

# ── stage 5: entity clustering ────────────────────────────────────────────────
log "Stage 5: Entity Co-occurrence Clustering"
set_status entities SKIP

if [[ $SKIP_ENTITIES -eq 0 ]]; then
  ENTITY_OUT="$BENCH_DIR/entities"
  MAX_DOCS_ARG=""
  [[ $MAX_DOCS -gt 0 ]] && MAX_DOCS_ARG="--max-docs $MAX_DOCS"

  if [[ $DRY_RUN -eq 0 && -d "$CORPUS_DIR" ]]; then
    t0=$(date +%s)
    if python3 "$SCRIPT_DIR/entity_cluster.py" \
         --corpus "$CORPUS_DIR" \
         --out "$ENTITY_OUT" \
         $MAX_DOCS_ARG \
         --min-edge-weight 2; then
      record_timing entities "$t0"
      # Parse report for stats
      set_result entity_nodes "$(grep -m1 "Unique entities" "$ENTITY_OUT/entity_report.md" \
        | grep -oE '[0-9,]+' | tr -d ',' | head -1 || echo "n/a")"
      set_result entity_edges "$(grep -m1 "Edges" "$ENTITY_OUT/entity_report.md" \
        | grep -oE '[0-9,]+' | tr -d ',' | head -1 || echo "n/a")"
      set_result entity_clusters "$(grep -m1 "Clusters" "$ENTITY_OUT/entity_report.md" \
        | grep -oE '[0-9,]+' | tr -d ',' | head -1 || echo "n/a")"
      set_status entities PASS
      info "Nodes: $(get_result entity_nodes)  Edges: $(get_result entity_edges)  Clusters: $(get_result entity_clusters)"
    else
      set_status entities FAIL
    fi
  elif [[ $DRY_RUN -eq 1 ]]; then
    info "[DRY-RUN] python3 entity_cluster.py --corpus $CORPUS_DIR --out $ENTITY_OUT $MAX_DOCS_ARG"
    set_result entity_nodes "n/a"
    set_result entity_edges "n/a"
    set_result entity_clusters "n/a"
    set_status entities PASS
  else
    info "WARN: $CORPUS_DIR not found — skipping entity clustering"
    set_result entity_nodes "n/a"
    set_result entity_edges "n/a"
    set_result entity_clusters "n/a"
  fi
fi

# ── stage 6: deposition pipeline ─────────────────────────────────────────────
log "Stage 6: Full Deposition Pipeline"
set_status depo SKIP

if [[ $SKIP_DEPO -eq 0 ]]; then
  if [[ -z "$AUDIO_FILE" ]]; then
    info "No --audio provided — skipping deposition pipeline"
    info "  Provide --audio /path/to/depo.m4a to run this stage"
    set_status depo SKIP
  elif [[ ! -f "$AUDIO_FILE" ]]; then
    info "Audio file not found: $AUDIO_FILE"
    set_status depo FAIL
  else
    DEPO_OUT="$BENCH_DIR/depo/$(basename "${AUDIO_FILE%.*}")"
    t0=$(date +%s)
    dry_flag=""
    [[ $DRY_RUN -eq 1 ]] && dry_flag="--dry-run"
    if bash "$SCRIPT_DIR/deposition_pipeline.sh" \
         --audio "$AUDIO_FILE" \
         --out "$DEPO_OUT" \
         $dry_flag; then
      record_timing depo "$t0"
      set_result depo_artifacts "$(find "$DEPO_OUT" -type f 2>/dev/null | wc -l | tr -d ' ')"
      set_status depo PASS
      info "Artifacts produced: $(get_result depo_artifacts)"
    else
      set_status depo FAIL
    fi
  fi
fi

# ── stage 7: evidence packet ──────────────────────────────────────────────────
log "Stage 7: Evidence Packet"

EVIDENCE_DIR="$BENCH_DIR/evidence"
mkdir -p "$EVIDENCE_DIR"

# Score completeness: each completed stage = 1/6 points
SCORE=0
MAX_SCORE=6
for stage in acquire ingest redaction names entities depo; do
  [[ "$(get_status "$stage" SKIP)" == "PASS" ]] && (( SCORE++ )) || true
done
COMPLETENESS=$(python3 -c "print(f'{100*$SCORE//$MAX_SCORE}%')" 2>/dev/null || echo "n/a")

# Copy key outputs to evidence dir
[[ $DRY_RUN -eq 0 ]] && {
  [[ -f "$BENCH_DIR/names_report.md" ]]            && cp "$BENCH_DIR/names_report.md"            "$EVIDENCE_DIR/" 2>/dev/null || true
  [[ -f "$BENCH_DIR/entities/entity_report.md" ]]  && cp "$BENCH_DIR/entities/entity_report.md"  "$EVIDENCE_DIR/" 2>/dev/null || true
  [[ -f "$BENCH_DIR/entities/entity_graph.json" ]] && cp "$BENCH_DIR/entities/entity_graph.json" "$EVIDENCE_DIR/" 2>/dev/null || true
  find "$BENCH_DIR/redaction" -name '*.md' -exec cp {} "$EVIDENCE_DIR/" \; 2>/dev/null || true
  find "$BENCH_DIR/depo" -name '*.proof.json' -exec cp {} "$EVIDENCE_DIR/" \; 2>/dev/null || true
}

# ── benchmark.md ─────────────────────────────────────────────────────────────
log "Writing benchmark.md"
TOTAL_ELAPSED=$(elapsed "$BENCH_START")

BENCH_REPORT="$BENCH_DIR/benchmark.md"
cat > "$BENCH_REPORT" <<MD
# Bonfyre Forensic Benchmark Report

| Field | Value |
|---|---|
| Timestamp | ${BENCH_TIMESTAMP} |
| Total Wall Time | ${TOTAL_ELAPSED} |
| Corpus Directory | ${CORPUS_DIR} |
| Bench Directory | ${BENCH_DIR} |
| Mode | $([ $DRY_RUN -eq 1 ] && echo DRY-RUN || echo LIVE) |
| Completeness Score | ${COMPLETENESS} (${SCORE}/${MAX_SCORE} stages passed) |

---

## Stage Results

| Stage | Status | Key Metric | Wall Time |
|---|---|---|---|
| 1 — Corpus Acquire | $(get_status acquire SKIP) | $(get_result acquire_docs n/a) PDFs | $(get_timing acquire "—")s |
| 2 — Ingest Throughput | $(get_status ingest SKIP) | $(get_result pages_per_sec n/a) pages/sec ($(get_result total_pages n/a) pages) | $(get_timing ingest "—")s |
| 3 — Redaction Diff | $(get_status redaction SKIP) | $(get_result redaction_diffs n/a)/$(get_result redaction_pairs n/a) pairs with diffs | $(get_timing redaction "—")s |
| 4 — Name Watchlist | $(get_status names SKIP) | $(get_result names_recall n/a) recall ($(get_result names_hits n/a) hits) | $(get_timing names "—")s |
| 5 — Entity Clusters | $(get_status entities SKIP) | $(get_result entity_nodes n/a) nodes / $(get_result entity_edges n/a) edges / $(get_result entity_clusters n/a) clusters | $(get_timing entities "—")s |
| 6 — Deposition Pipeline | $(get_status depo SKIP) | $(get_result depo_artifacts n/a) artifacts | $(get_timing depo "—")s |

---

## Evidence Packet
$(find "$EVIDENCE_DIR" -type f 2>/dev/null | sort | while read -r f; do
  echo "- \`$(basename "$f")\`"
done || echo "_No evidence artifacts_")

---

## Known Names Ground Truth

$(printf -- "- %s\n" "${KNOWN_NAMES[@]}")

---

_Generated by \`run_forensic_bench.sh\` — Bonfyre local pipeline benchmark_
MD

info "Benchmark report: $BENCH_REPORT"
cat "$BENCH_REPORT"

# ── summary ───────────────────────────────────────────────────────────────────
log "Benchmark complete"
echo ""
echo "  Report:   $BENCH_REPORT"
echo "  Evidence: $EVIDENCE_DIR"
echo "  Time:     $TOTAL_ELAPSED"
echo "  Score:    $COMPLETENESS"
