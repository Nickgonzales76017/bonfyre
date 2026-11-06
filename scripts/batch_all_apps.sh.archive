#!/usr/bin/env bash
set -euo pipefail

# batch_all_apps.sh — Run full-chain pipeline across ALL apps' seeds
#
# Usage: ./scripts/batch_all_apps.sh [seeds-per-app] [out-base]
#   Default: 3 seeds per app, output to /tmp/bonfyre-bench-all
#
# Processes every app in site/demos/*/seeds.json through the full-chain pipeline.
# Produces per-app and aggregate benchmarks.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEEDS_PER_APP="${1:-3}"
OUT_BASE="${2:-/tmp/bonfyre-bench-all}"
DEMOS_DIR="$ROOT/site/demos"

mkdir -p "$OUT_BASE"

export PATH="$HOME/.local/bin:/opt/homebrew/bin:$PATH"

echo "================================================================"
echo " Bonfyre Full-Chain Benchmark — ALL APPS"
echo " Seeds per app: $SEEDS_PER_APP"
echo " Output: $OUT_BASE"
echo "================================================================"
echo ""

BATCH_START=$(python3 -c "import time; print(int(time.time()*1000))")
APP_COUNT=0
SEED_COUNT=0
FAIL_COUNT=0

for seeds_file in "$DEMOS_DIR"/*/seeds.json; do
  app=$(basename "$(dirname "$seeds_file")")
  APP_DIR="$OUT_BASE/$app"
  mkdir -p "$APP_DIR"
  APP_COUNT=$((APP_COUNT + 1))

  total_seeds=$(python3 -c "import json; print(len(json.load(open('$seeds_file'))))")
  echo "================================================================"
  echo " [$APP_COUNT] $app — $total_seeds seeds available, processing $SEEDS_PER_APP"
  echo "================================================================"

  # Pick N random seeds
  SEEDS=$(python3 -c "
import json, random
seeds = json.load(open('$seeds_file'))
n = min(int('$SEEDS_PER_APP'), len(seeds))
for s in random.sample(seeds, n): print(s)
")

  IDX=0
  for seed in $SEEDS; do
    IDX=$((IDX + 1))
    SEED_COUNT=$((SEED_COUNT + 1))
    URL="https://www.youtube.com/watch?v=${seed}"
    SEED_DIR="$APP_DIR/$seed"

    echo ""
    echo " [$app $IDX/$SEEDS_PER_APP] Seed: $seed"
    if bash "$ROOT/scripts/run_full_chain_proof.sh" "$URL" "$app — $seed" "$app" "$SEED_DIR"; then
      echo "  => OK"
    else
      echo "  => FAILED"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
  done
  echo ""
done

BATCH_END=$(python3 -c "import time; print(int(time.time()*1000))")
BATCH_WALL=$(python3 -c "print(round(($BATCH_END - $BATCH_START) / 1000.0, 3))")

# ── Aggregate all recipes into a master benchmark ────────────
python3 - "$OUT_BASE" "$BATCH_WALL" "$SEED_COUNT" "$FAIL_COUNT" "$APP_COUNT" << 'PYEOF'
import json, sys, os, glob
from collections import defaultdict

out_base = sys.argv[1]
batch_wall = float(sys.argv[2])
total_seeds = int(sys.argv[3])
total_fails = int(sys.argv[4])
app_count = int(sys.argv[5])

recipes = []
for rpath in sorted(glob.glob(os.path.join(out_base, "*/*/recipe.json"))):
    try:
        r = json.load(open(rpath))
        r["_path"] = rpath
        recipes.append(r)
    except Exception:
        pass

if not recipes:
    print("No recipe files found.", file=sys.stderr)
    sys.exit(1)

def stats(vals):
    if not vals: return {}
    vals = sorted(vals)
    n = len(vals)
    return {"min": vals[0], "max": vals[-1], "mean": round(sum(vals)/n, 3),
            "median": vals[n//2], "p95": vals[int(n*0.95)] if n >= 2 else vals[-1],
            "sum": round(sum(vals), 3), "n": n}

# Global aggregation
all_durations = [r["source"]["duration_s"] for r in recipes if r.get("source", {}).get("duration_s")]
all_walls = [r["pipeline_wall_s"] for r in recipes if r.get("pipeline_wall_s")]
all_confs = [r["transcribe"]["avg_confidence"] for r in recipes if r.get("transcribe",{}).get("avg_confidence")]
all_rtfs = [r["transcribe"]["rtf"] for r in recipes if r.get("transcribe",{}).get("rtf")]
all_segs = sum(r["transcribe"]["segments_total"] for r in recipes if r.get("transcribe",{}).get("segments_total"))
all_halluc = sum(r["transcribe"]["segments_hallucinated"] for r in recipes if r.get("transcribe",{}).get("segments_hallucinated") is not None)
all_art = [r.get("total_artifact_bytes", 0) for r in recipes]
all_steps_ok = sum(r.get("steps_ok", 0) for r in recipes)
all_steps_total = sum(r.get("steps_total", 0) for r in recipes)
all_binaries = max((r.get("binaries_used", 0) for r in recipes), default=0)

# Per-app aggregation
per_app = defaultdict(list)
for r in recipes:
    per_app[r.get("app", "unknown")].append(r)

# Step timing across ALL recipes
step_times = defaultdict(list)
for r in recipes:
    for s in r.get("steps", []):
        step_times[s["name"]].append(s["wall_s"])

total_audio = sum(all_durations)
total_wall = sum(all_walls)

benchmark = {
    "title": "Bonfyre Full-Chain Benchmark — All Apps",
    "batch_wall_s": batch_wall,
    "apps_processed": app_count,
    "seeds_processed": total_seeds - total_fails,
    "seeds_failed": total_fails,
    "total_audio_s": round(total_audio, 1),
    "total_audio_min": round(total_audio / 60, 1),
    "total_audio_hours": round(total_audio / 3600, 2),
    "total_pipeline_wall_s": round(total_wall, 1),
    "throughput_audio_per_wall": round(total_audio / max(total_wall, 0.001), 2),
    "total_steps_run": all_steps_total,
    "total_steps_ok": all_steps_ok,
    "step_success_rate_pct": round(all_steps_ok / max(all_steps_total, 1) * 100, 1),
    "max_binaries_per_seed": all_binaries,
    "transcribe_global": {
        "total_segments": all_segs,
        "total_hallucinated": all_halluc,
        "hallucination_pct": round(all_halluc / max(all_segs, 1) * 100, 4),
        "confidence": stats(all_confs),
        "rtf": stats(all_rtfs),
    },
    "artifact_bytes": stats(all_art),
    "per_app": {},
    "step_timing": {n: stats(t) for n, t in step_times.items()},
}

for app_name, app_recipes in per_app.items():
    ad = [r["source"]["duration_s"] for r in app_recipes if r.get("source",{}).get("duration_s")]
    aw = [r["pipeline_wall_s"] for r in app_recipes if r.get("pipeline_wall_s")]
    benchmark["per_app"][app_name] = {
        "seeds": len(app_recipes),
        "total_audio_s": round(sum(ad), 1),
        "total_wall_s": round(sum(aw), 1),
        "avg_wall_s": stats(aw),
    }

with open(os.path.join(out_base, "benchmark.json"), "w") as f:
    json.dump(benchmark, f, indent=2); f.write("\n")

# ── Master benchmark.md ──────────────────────────────────────
L = [
    "# Bonfyre Full-Chain Benchmark — All Apps", "",
    "## Headline Numbers", "",
    f"| Metric | Value |", "|--------|-------|",
    f"| Apps processed | **{app_count}** |",
    f"| Seeds processed | **{total_seeds - total_fails}** |",
    f"| Total audio | **{round(total_audio/3600, 2)} hours** ({round(total_audio/60, 1)} min) |",
    f"| Total pipeline wall | **{round(total_wall/60, 1)} min** |",
    f"| Throughput | **{round(total_audio / max(total_wall, 0.001), 1)}x** (audio/wall) |",
    f"| Pipeline steps run | **{all_steps_ok}/{all_steps_total}** ({round(all_steps_ok/max(all_steps_total,1)*100, 1)}% pass) |",
    f"| Max binaries per seed | **{all_binaries}** |",
    f"| Total derived artifacts | **{sum(all_art):,} bytes** |",
    f"| Total segments transcribed | **{all_segs:,}** |",
    f"| Global hallucination rate | **{round(all_halluc/max(all_segs,1)*100, 2)}%** |",
    f"| Batch wall clock | **{round(batch_wall/60, 1)} min** |",
    "",
]

if all_rtfs:
    L.append(f"- Best RTF: **{min(all_rtfs)}x** | Mean: **{round(sum(all_rtfs)/len(all_rtfs), 3)}x**")
if all_confs:
    L.append(f"- Mean confidence: **{round(sum(all_confs)/len(all_confs), 4)}**")
L.append("")

L += ["## Per-App Summary", "",
    "| App | Seeds | Audio (min) | Wall (min) | Throughput |",
    "|-----|-------|-------------|------------|------------|"]
for app_name in sorted(per_app.keys()):
    ad = per_app[app_name]
    ta = sum(r["source"]["duration_s"] for r in ad if r.get("source",{}).get("duration_s"))
    tw = sum(r["pipeline_wall_s"] for r in ad if r.get("pipeline_wall_s"))
    tp = round(ta / max(tw, 0.001), 1)
    L.append(f"| {app_name} | {len(ad)} | {round(ta/60,1)} | {round(tw/60,1)} | {tp}x |")
L.append("")

L += ["## Step Timing (aggregate across all seeds)", "",
    "| Step | Min (s) | Mean (s) | Max (s) | P95 (s) | Runs |",
    "|------|---------|----------|---------|---------|------|"]
for name in ["fetch-metadata","download-audio","media-prep","transcribe","transcript-clean",
             "paragraph","brief","tag","tone","embed","render","hash","offer","pack",
             "repurpose-tweet","repurpose-linkedin","repurpose-youtube","emit-html",
             "index","compress","stitch","ledger","meter"]:
    if name in step_times:
        st = stats(step_times[name])
        L.append(f"| {name} | {st['min']} | {st['mean']} | {st['max']} | {st['p95']} | {st['n']} |")
L.append("")

L += ["## Competitive Positioning", "",
    f"- **{app_count} live apps** powered by a **single C binary toolchain** (48 binaries)",
    f"- Processes **{round(total_audio/3600, 2)} hours of real-world messy audio** end-to-end",
    f"- **{all_binaries} binary stages** per source — tag, tone, embed, render, hash, offer, pack, repurpose, emit, index, compress, stitch, ledger, meter",
    "- Every artifact is **content-addressed** (SHA-256 Merkle DAG) with full lineage",
    "- **Zero source media retained** — only structured, searchable, provenance-backed artifacts",
    "- Auto-generates **social-ready outputs** (tweet threads, LinkedIn posts, YouTube descriptions)",
    "- Built-in **value accounting** (ledger) and **usage metering** on every run",
    f"- Hallucination rate: **{round(all_halluc/max(all_segs,1)*100, 2)}%** across {all_segs:,} segments of messy real-world audio",
    "- **Static C11 binaries** — no Python runtime, no Docker, no cloud dependencies",
    "",
]

with open(os.path.join(out_base, "benchmark.md"), "w") as f:
    f.write("\n".join(L) + "\n")

print(f"\n  Master benchmark: {os.path.join(out_base, 'benchmark.json')}")
print(f"  Master report:    {os.path.join(out_base, 'benchmark.md')}")
print(f"  Per-seed recipes: {out_base}/<app>/<seed>/recipe.json")
PYEOF

echo ""
echo "================================================================"
echo " ALL APPS BATCH COMPLETE"
echo " Apps: $APP_COUNT | Seeds: $SEED_COUNT | Failed: $FAIL_COUNT"
echo " Wall: ${BATCH_WALL}s"
echo " Output: $OUT_BASE"
echo "================================================================"
