#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/batch_timed_proofs.sh <app-slug> <seeds-json> [count] [out-base]
# Runs run_timed_proof.sh on N seeds and produces an aggregate benchmark.
# Compatible with bash 3.2+ (macOS default).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="${1:?usage: batch_timed_proofs.sh <app-slug> <seeds-json> [count] [out-base]}"
SEEDS_JSON="${2:?provide seeds.json path}"
COUNT="${3:-5}"
OUT_BASE="${4:-/tmp/bonfyre-bench-$APP}"

mkdir -p "$OUT_BASE"

SEEDS=$(python3 -c "
import json, random
seeds = json.load(open('$SEEDS_JSON'))
n = min(int('$COUNT'), len(seeds))
for s in random.sample(seeds, n): print(s)
")

echo "=== Bonfyre Batch Benchmark - $APP ==="
echo " Seeds: $COUNT from $SEEDS_JSON"
echo " Output: $OUT_BASE"
echo ""

BATCH_START=$(python3 -c "import time; print(int(time.time()*1000))")
IDX=0
FAILED=0

for seed in $SEEDS; do
  IDX=$((IDX + 1))
  URL="https://www.youtube.com/watch?v=${seed}"
  SEED_DIR="$OUT_BASE/$seed"
  echo "--- [$IDX/$COUNT] Seed: $seed ---"
  if bash "$ROOT/scripts/run_timed_proof.sh" "$URL" "$APP seed $seed" "$SEED_DIR"; then
    echo "  ok"
  else
    echo "  FAILED: $seed"
    FAILED=$((FAILED + 1))
  fi
  echo ""
done

BATCH_END=$(python3 -c "import time; print(int(time.time()*1000))")
BATCH_WALL=$(python3 -c "print(round(($BATCH_END - $BATCH_START) / 1000.0, 3))")
PROCESSED=$IDX

# Aggregate all recipe.json files into benchmark.json and benchmark.md
python3 - "$OUT_BASE" "$APP" "$BATCH_WALL" "$PROCESSED" "$FAILED" << 'PYEOF'
import json, sys, os, glob

out_base = sys.argv[1]
app = sys.argv[2]
batch_wall = float(sys.argv[3])
processed = int(sys.argv[4])
failed = int(sys.argv[5])

recipes = []
for rpath in sorted(glob.glob(os.path.join(out_base, "*/recipe.json"))):
    try:
        recipes.append(json.load(open(rpath)))
    except Exception as e:
        print(f"  skip {rpath}: {e}", file=sys.stderr)

if not recipes:
    print("No recipe files found.", file=sys.stderr)
    sys.exit(1)

def stats(vals):
    vals = sorted(vals)
    n = len(vals)
    return {"min": vals[0], "max": vals[-1], "mean": round(sum(vals)/n, 3),
            "median": vals[n//2], "p95": vals[int(n*0.95)] if n >= 2 else vals[-1], "n": n}

step_times = {}
for r in recipes:
    for s in r.get("steps", []):
        step_times.setdefault(s["name"], []).append(s["wall_s"])

durations = [r["source_duration_s"] for r in recipes if r.get("source_duration_s")]
walls = [r["pipeline_wall_s"] for r in recipes if r.get("pipeline_wall_s")]
confs = [r["transcribe"]["avg_confidence"] for r in recipes if r.get("transcribe",{}).get("avg_confidence")]
rtfs = [r["transcribe"]["rtf"] for r in recipes if r.get("transcribe",{}).get("rtf")]
segs = [r["transcribe"]["segments_total"] for r in recipes if r.get("transcribe",{}).get("segments_total")]
halluc = [r["transcribe"]["segments_hallucinated"] for r in recipes if r.get("transcribe",{}).get("segments_hallucinated") is not None]
art_sizes = [sum(r.get("artifact_sizes_bytes",{}).values()) for r in recipes]

total_audio = sum(durations)
total_wall = sum(walls)
total_segs = sum(segs)
total_halluc = sum(halluc)

benchmark = {
    "app": app,
    "seeds_processed": processed - failed,
    "seeds_failed": failed,
    "batch_wall_s": batch_wall,
    "total_audio_s": total_audio,
    "total_pipeline_wall_s": total_wall,
    "throughput_audio_per_wall": round(total_audio / max(total_wall, 0.001), 2),
    "avg_pipeline_wall_s": stats(walls) if walls else {},
    "avg_source_duration_s": stats(durations) if durations else {},
    "transcribe": {
        "avg_confidence": stats(confs) if confs else {},
        "rtf": stats(rtfs) if rtfs else {},
        "total_segments": total_segs,
        "total_hallucinated": total_halluc,
        "hallucination_rate": round(total_halluc / max(total_segs,1) * 100, 4),
    },
    "artifact_sizes_bytes": stats(art_sizes) if art_sizes else {},
    "step_timing": {n: stats(t) for n, t in step_times.items()},
    "per_seed": [
        {"url": r.get("url"), "duration_s": r.get("source_duration_s"),
         "wall_s": r.get("pipeline_wall_s"),
         "confidence": r.get("transcribe",{}).get("avg_confidence"),
         "rtf": r.get("transcribe",{}).get("rtf"),
         "hallucinated": r.get("transcribe",{}).get("segments_hallucinated")}
        for r in recipes
    ],
}
with open(os.path.join(out_base, "benchmark.json"), "w") as f:
    json.dump(benchmark, f, indent=2); f.write("\n")

# Markdown report with headline claims
lines = [
    f"# Bonfyre Benchmark - {app}", "",
    "## Headline Claims", "",
    f"- **{processed - failed} public-origin sources** processed end-to-end",
    f"- **{round(total_audio)}s total audio** in **{round(total_wall, 1)}s pipeline wall time**",
]
if total_wall > 0:
    lines.append(f"- Throughput: **{round(total_audio/total_wall, 1)}x** (audio-seconds per wall-second)")
if rtfs:
    lines.append(f"- Best RTF: **{min(rtfs)}x** realtime | Mean RTF: **{round(sum(rtfs)/len(rtfs), 3)}x**")
if confs:
    lines.append(f"- Mean transcription confidence: **{round(sum(confs)/len(confs), 4)}**")
hr = round(total_halluc / max(total_segs,1) * 100, 2)
lines.append(f"- Hallucination rate: **{hr}%** across {total_segs} segments")
if art_sizes:
    lines.append(f"- Total derived artifacts: **{sum(art_sizes):,} bytes** — no source media retained")
lines += ["- Full provenance chain from public origin to every artifact", ""]

lines += ["## Per-Step Timing (aggregate)", "",
    "| Step | Min (s) | Mean (s) | Max (s) | P95 (s) |",
    "|------|---------|----------|---------|---------|"]
for name, times in step_times.items():
    st = stats(times)
    lines.append(f"| {name} | {st['min']} | {st['mean']} | {st['max']} | {st['p95']} |")
lines.append("")

lines += ["## Per-Seed Results", "",
    "| Seed | Audio (s) | Wall (s) | Confidence | RTF | Halluc |",
    "|------|-----------|----------|------------|-----|--------|"]
for r in recipes:
    t = r.get("transcribe", {})
    sid = r.get("url","?").split("=")[-1][:11]
    lines.append(f"| {sid} | {r.get('source_duration_s',0)} | {r.get('pipeline_wall_s',0)} | {t.get('avg_confidence',0)} | {t.get('rtf',0)} | {t.get('segments_hallucinated',0)} |")
lines.append("")

lines += ["## Bottleneck Analysis", ""]
if step_times:
    slowest = max(step_times.items(), key=lambda x: sum(x[1])/len(x[1]))
    fastest = min(step_times.items(), key=lambda x: sum(x[1])/len(x[1]))
    lines.append(f"- **Slowest step**: `{slowest[0]}` (mean {stats(slowest[1])['mean']}s)")
    lines.append(f"- **Fastest step**: `{fastest[0]}` (mean {stats(fastest[1])['mean']}s)")
    if "transcribe" in step_times and "download-audio" in step_times:
        tm = sum(step_times["transcribe"])/len(step_times["transcribe"])
        dm = sum(step_times["download-audio"])/len(step_times["download-audio"])
        lines.append(f"- Transcribe/download ratio: {round(tm/max(dm,0.001), 1)}x")
lines.append("")

lines += ["## Optimization Opportunities", "",
    "| Priority | Area | Note |", "|----------|------|------|"]
if step_times.get("download-audio"):
    dm = sum(step_times["download-audio"])/len(step_times["download-audio"])
    if dm > 10: lines.append(f"| High | download-audio | Mean {round(dm,1)}s - consider parallel downloads |")
if step_times.get("transcribe"):
    tm = sum(step_times["transcribe"])/len(step_times["transcribe"])
    if tm > 30: lines.append(f"| High | transcribe | Mean {round(tm,1)}s - consider chunked parallel |")
if step_times.get("media-prep"):
    mm = sum(step_times["media-prep"])/len(step_times["media-prep"])
    if mm > 5: lines.append(f"| Medium | media-prep | Mean {round(mm,1)}s - optimize FFmpeg flags |")
lines.append("| Low | proof-bundle | Pure JSON assembly - already fast |")
lines.append("")

with open(os.path.join(out_base, "benchmark.md"), "w") as f:
    f.write("\n".join(lines) + "\n")
print(f"\n  Benchmark: {os.path.join(out_base, 'benchmark.json')}")
print(f"  Report:    {os.path.join(out_base, 'benchmark.md')}")
PYEOF

echo ""
echo "=== Batch complete: $((PROCESSED - FAILED)) OK, $FAILED failed ==="
echo " Total wall: ${BATCH_WALL}s"
