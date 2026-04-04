"""Quality Scoring Loop — heuristic quality scoring for transcription pipeline output."""

import argparse
import json
import os
import re


FILLER_WORDS = {"um", "uh", "like", "you know", "basically", "literally", "actually", "right"}


def score_transcript(text):
    """Score a transcript on readability and cleanliness."""
    if not text.strip():
        return {"score": 0, "issues": ["empty transcript"]}

    words = text.split()
    word_count = len(words)
    sentences = re.split(r'[.!?]+', text)
    sentence_count = len([s for s in sentences if s.strip()])
    paragraphs = [p for p in text.split("\n\n") if p.strip()]

    filler_count = sum(1 for w in words if w.lower().strip(".,!?") in FILLER_WORDS)
    filler_ratio = filler_count / max(word_count, 1)

    avg_sentence_len = word_count / max(sentence_count, 1)

    issues = []
    score = 5.0

    if filler_ratio > 0.05:
        score -= 1.0
        issues.append(f"high filler ratio ({filler_ratio:.1%})")
    if avg_sentence_len > 40:
        score -= 0.5
        issues.append(f"long avg sentence ({avg_sentence_len:.0f} words)")
    if len(paragraphs) < 3 and word_count > 200:
        score -= 0.5
        issues.append("few paragraph breaks")
    if word_count < 50:
        score -= 1.0
        issues.append("very short transcript")

    return {
        "score": max(round(score, 1), 1.0),
        "word_count": word_count,
        "sentence_count": sentence_count,
        "paragraph_count": len(paragraphs),
        "filler_ratio": round(filler_ratio, 3),
        "avg_sentence_length": round(avg_sentence_len, 1),
        "issues": issues,
    }


def score_summary(text):
    """Score a summary on structure and usefulness."""
    if not text.strip():
        return {"score": 0, "issues": ["empty summary"]}

    words = text.split()
    has_action_items = bool(re.search(r'- \[[ x]\]', text))
    has_headers = bool(re.search(r'^#{1,3} ', text, re.MULTILINE))

    score = 5.0
    issues = []

    if len(words) < 20:
        score -= 1.5
        issues.append("very short summary")
    if not has_headers:
        score -= 0.5
        issues.append("no section headers")
    if not has_action_items:
        score -= 0.5
        issues.append("no action items found")

    return {
        "score": max(round(score, 1), 1.0),
        "word_count": len(words),
        "has_headers": has_headers,
        "has_action_items": has_action_items,
        "issues": issues,
    }


def score_job(job_dir):
    """Score all available outputs in a job directory."""
    results = {"job_dir": job_dir}

    transcript_path = os.path.join(job_dir, "transcript.txt")
    if os.path.exists(transcript_path):
        with open(transcript_path) as f:
            results["transcript"] = score_transcript(f.read())

    summary_path = os.path.join(job_dir, "summary.md")
    if os.path.exists(summary_path):
        with open(summary_path) as f:
            results["summary"] = score_summary(f.read())

    # Write quality.json alongside deliverable
    quality_path = os.path.join(job_dir, "quality.json")
    with open(quality_path, "w") as f:
        json.dump(results, f, indent=2)

    return results


def main():
    parser = argparse.ArgumentParser(description="Quality Scoring Loop")
    parser.add_argument("input", help="Job output directory")
    parser.add_argument("--min-score", type=float, default=0, help="Flag if score below threshold")
    args = parser.parse_args()

    results = score_job(args.input)

    for key in ["transcript", "summary"]:
        if key in results:
            r = results[key]
            status = "✓" if r["score"] >= args.min_score else "⚠"
            print(f"  {status} {key}: {r['score']}/5.0")
            if r["issues"]:
                for issue in r["issues"]:
                    print(f"      - {issue}")

    print(f"\nQuality report: {os.path.join(args.input, 'quality.json')}")


if __name__ == "__main__":
    main()
