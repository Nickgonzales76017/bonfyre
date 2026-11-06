"""Quality Benchmark Pack — evaluate heuristic scores against human ratings."""

import argparse
import json
import os
import sys


def load_benchmarks(bench_dir):
    """Load all benchmark entries with ratings."""
    entries = []
    for name in sorted(os.listdir(bench_dir)):
        entry_dir = os.path.join(bench_dir, name)
        if not os.path.isdir(entry_dir):
            continue
        rating_path = os.path.join(entry_dir, "rating.json")
        if not os.path.exists(rating_path):
            continue
        with open(rating_path) as f:
            rating = json.load(f)
        entry = {"id": name, "dir": entry_dir, "rating": rating}

        transcript_path = os.path.join(entry_dir, "transcript.txt")
        if os.path.exists(transcript_path):
            with open(transcript_path) as f:
                entry["transcript"] = f.read()

        entries.append(entry)
    return entries


def heuristic_score(entry):
    """Compute simple heuristic quality scores from transcript text."""
    text = entry.get("transcript", "")
    if not text:
        return {"readability": 0, "filler_ratio": 0, "paragraph_count": 0}

    words = text.split()
    word_count = len(words)
    filler_words = {"um", "uh", "like", "you know", "basically", "literally"}
    filler_count = sum(1 for w in words if w.lower().strip(".,!?") in filler_words)
    paragraphs = [p for p in text.split("\n\n") if p.strip()]

    return {
        "word_count": word_count,
        "filler_ratio": round(filler_count / max(word_count, 1), 3),
        "paragraph_count": len(paragraphs),
        "avg_paragraph_length": round(word_count / max(len(paragraphs), 1)),
    }


def evaluate(bench_dir):
    """Compare heuristic scores against human ratings."""
    entries = load_benchmarks(bench_dir)
    if not entries:
        print(f"No benchmark entries found in {bench_dir}")
        return

    print(f"Evaluating {len(entries)} benchmark entries:\n")
    for entry in entries:
        r = entry["rating"]
        h = heuristic_score(entry)
        print(f"  [{entry['id']}]")
        print(f"    Human:     accuracy={r['transcript_accuracy']}  summary={r['summary_quality']}  actions={r['action_items_relevant']}")
        print(f"    Heuristic: words={h['word_count']}  filler={h['filler_ratio']}  paragraphs={h['paragraph_count']}")
        if r.get("notes"):
            print(f"    Notes: {r['notes']}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Evaluate benchmark quality scores")
    parser.add_argument("benchmarks", help="Path to benchmarks/ directory")
    args = parser.parse_args()
    evaluate(args.benchmarks)


if __name__ == "__main__":
    main()
