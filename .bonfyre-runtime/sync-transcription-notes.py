#!/usr/bin/env python3
"""Sync transcription job outputs into vault markdown notes.

Reads meta.json + deliverable.md from each output folder and creates/updates
a markdown note in 08-Transcriptions/ with proper frontmatter for Obsidian Bases.

Run after each transcription job completes, or on demand.
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

VAULT = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = VAULT / "10-Code" / "LocalAITranscriptionService" / "outputs"
NOTES_DIR = VAULT / "08-Transcriptions"


def load_meta(job_dir: Path) -> dict:
    meta_path = job_dir / "meta.json"
    if not meta_path.exists():
        return {}
    with open(meta_path, "r") as f:
        return json.load(f)


def load_deliverable(job_dir: Path) -> str:
    deliv_path = job_dir / "deliverable.md"
    if not deliv_path.exists():
        return ""
    with open(deliv_path, "r") as f:
        return f.read()


def build_note(meta: dict, deliverable: str, job_slug: str) -> str:
    quality = meta.get("quality", {})
    created = meta.get("created_at", "")
    if created:
        created = created[:10]  # just the date

    # Build frontmatter
    lines = ["---"]
    lines.append(f"type: transcription")
    lines.append(f"cssclasses:")
    lines.append(f"  - transcription")
    lines.append(f"title: \"{meta.get('job_name', job_slug)}\"")
    lines.append(f"job_slug: \"{job_slug}\"")
    lines.append(f"created: {created}")
    lines.append(f"updated: {datetime.now().strftime('%Y-%m-%d')}")
    lines.append(f"source_kind: {meta.get('source_kind', 'unknown')}")
    lines.append(f"quality_score: {quality.get('score', '')}")
    lines.append(f"quality_status: {quality.get('status', '')}")
    lines.append(f"sentences: {quality.get('cleaned_sentence_count', '')}")
    lines.append(f"summary_count: {quality.get('summary_count', '')}")
    lines.append(f"action_items: {quality.get('action_item_count', '')}")
    lines.append(f"whisper_model: {meta.get('whisper_model', '')}")
    lines.append(f"status: completed")
    lines.append(f"tags:")
    lines.append(f"  - transcription")
    lines.append(f"  - completed")
    lines.append("---")
    lines.append("")

    # Body
    lines.append(f"# {meta.get('job_name', job_slug)}")
    lines.append("")
    lines.append(f"| Field | Value |")
    lines.append(f"|---|---|")
    lines.append(f"| Source | {meta.get('source_kind', '?')} |")
    lines.append(f"| Quality Score | {quality.get('score', 'n/a')} |")
    lines.append(f"| Quality Status | {quality.get('status', 'n/a')} |")
    lines.append(f"| Sentences | {quality.get('cleaned_sentence_count', 'n/a')} |")
    lines.append(f"| Summaries | {quality.get('summary_count', 'n/a')} |")
    lines.append(f"| Action Items | {quality.get('action_item_count', 'n/a')} |")
    lines.append(f"| Whisper Model | {meta.get('whisper_model', 'n/a')} |")
    lines.append(f"| Created | {created} |")
    lines.append("")

    if deliverable:
        lines.append("## Deliverable")
        lines.append("")
        lines.append(deliverable)

    return "\n".join(lines) + "\n"


def sync_all():
    if not OUTPUTS_DIR.exists():
        print(f"No outputs directory at {OUTPUTS_DIR}")
        return

    NOTES_DIR.mkdir(exist_ok=True)
    synced = 0

    for job_dir in sorted(OUTPUTS_DIR.iterdir()):
        if not job_dir.is_dir():
            continue
        meta = load_meta(job_dir)
        if not meta:
            continue

        job_slug = job_dir.name
        deliverable = load_deliverable(job_dir)
        note_content = build_note(meta, deliverable, job_slug)

        note_path = NOTES_DIR / f"Job - {meta.get('job_name', job_slug)}.md"
        note_path.write_text(note_content)
        synced += 1
        print(f"  synced: {note_path.name}")

    print(f"\n{synced} transcription notes synced to 08-Transcriptions/")


if __name__ == "__main__":
    sync_all()
