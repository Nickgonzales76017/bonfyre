import json
from pathlib import Path
from typing import Dict, List

from .cleanup import clean_transcript_text
from .models import Deliverable
from .paragraphs import build_transcript_paragraphs
from .quality import score_quality
from .summary import extract_action_items, summarize_text


def load_benchmark_cases(benchmark_dir: Path) -> List[dict]:
    if not benchmark_dir.exists():
        raise ValueError(f"Benchmark directory does not exist: {benchmark_dir}")
    if not benchmark_dir.is_dir():
        raise ValueError(f"Benchmark path is not a directory: {benchmark_dir}")

    manifest_path = benchmark_dir / "cases.json"
    if not manifest_path.exists():
        raise ValueError(f"Benchmark manifest not found: {manifest_path}")

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    cases = payload.get("cases", [])
    if not cases:
        raise ValueError("Benchmark manifest has no cases.")
    return cases


def overlap_ratio(expected: List[str], actual: List[str]) -> float:
    if not expected:
        return 1.0
    expected_normalized = {item.strip().lower() for item in expected if item.strip()}
    actual_normalized = {item.strip().lower() for item in actual if item.strip()}
    if not expected_normalized:
        return 1.0
    return len(expected_normalized & actual_normalized) / len(expected_normalized)


def build_benchmark_deliverable(transcript_text: str, summary_bullets: int) -> Deliverable:
    cleanup_result = clean_transcript_text(transcript_text)
    cleaned_text = str(cleanup_result["cleaned_text"])
    paragraph_result = build_transcript_paragraphs(cleaned_text)
    provisional = Deliverable(
        title="benchmark-case",
        transcript=cleaned_text,
        transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
        summary_bullets=summarize_text(cleaned_text, bullets=summary_bullets),
        deep_summary_sections=[],
        action_items=extract_action_items(cleaned_text),
        source_kind="benchmark",
        client_name=None,
        output_goal=None,
        context_notes=None,
        quality={},
        processing_notes=[],
    )
    provisional.quality = score_quality(
        raw_transcript_text=transcript_text,
        cleaned_transcript_text=cleaned_text,
        cleanup_result=cleanup_result,
        deliverable=provisional,
    )
    return provisional


def evaluate_benchmark_case(case: dict, summary_bullets: int) -> Dict[str, object]:
    transcript_text = str(case["transcript"])
    deliverable = build_benchmark_deliverable(transcript_text, summary_bullets)

    expected_summary = case.get("expected_summary", [])
    expected_action_items = case.get("expected_action_items", [])

    summary_overlap = overlap_ratio(expected_summary, deliverable.summary_bullets)
    action_overlap = overlap_ratio(expected_action_items, deliverable.action_items)

    return {
        "name": case.get("name", "unnamed-case"),
        "quality": deliverable.quality,
        "summary_overlap": round(summary_overlap, 3),
        "action_item_overlap": round(action_overlap, 3),
        "expected_summary_count": len(expected_summary),
        "actual_summary_count": len(deliverable.summary_bullets),
        "expected_action_item_count": len(expected_action_items),
        "actual_action_item_count": len(deliverable.action_items),
    }


def run_benchmark_pack(benchmark_dir: Path, *, summary_bullets: int = 5) -> Dict[str, object]:
    cases = load_benchmark_cases(benchmark_dir)
    results = [evaluate_benchmark_case(case, summary_bullets) for case in cases]

    average_quality_score = round(
        sum(result["quality"]["score"] for result in results) / len(results), 2
    )
    average_summary_overlap = round(
        sum(result["summary_overlap"] for result in results) / len(results), 3
    )
    average_action_overlap = round(
        sum(result["action_item_overlap"] for result in results) / len(results), 3
    )

    output = {
        "benchmark_dir": str(benchmark_dir),
        "total_cases": len(results),
        "average_quality_score": average_quality_score,
        "average_summary_overlap": average_summary_overlap,
        "average_action_item_overlap": average_action_overlap,
        "results": results,
    }

    output_path = benchmark_dir / "benchmark-results.json"
    output_path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    output["results_path"] = str(output_path)
    return output
