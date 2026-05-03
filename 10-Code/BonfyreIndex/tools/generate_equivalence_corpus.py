#!/usr/bin/env python3
import json
import shutil
from pathlib import Path


BASE_CREATED = "2026-04-05T00:00:00Z"


def write_family(root: Path, slug: str, payload: dict) -> None:
    family_dir = root / slug
    family_dir.mkdir(parents=True, exist_ok=True)
    with (family_dir / "artifact.json").open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def make_payload(group: str, idx: int, artifact_type: str, source_system: str,
                 atoms: int, operators: int, realizations: int) -> dict:
    return {
        "artifact_id": f"{group}-{idx:03d}",
        "artifact_type": artifact_type,
        "source_system": source_system,
        "created_at": BASE_CREATED,
        "root_hash": f"{idx + 1:064x}",
        "atoms": [{"content_hash": f"{(idx * 17) + i + 1:064x}"} for i in range(atoms)],
        "operators": [{"op": "transform", "node_hash": f"{(idx * 31) + i + 1:064x}"} for i in range(operators)],
        "realizations": [{"format": "txt"} for _ in range(realizations)],
    }


def build_corpus(out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Three duplicate-shape groups with noisy artifact ids and root hashes.
    duplicate_specs = [
        ("creator-text", 12, "text", "Creator Notes", 3, 1, 1),
        ("creator-audio", 8, "audio", "creator_notes", 5, 2, 1),
        ("support-export", 6, "json", "Support Export", 2, 0, 2),
    ]
    for group, count, artifact_type, source_system, atoms, operators, realizations in duplicate_specs:
        for idx in range(count):
            payload = make_payload(group, idx, artifact_type, source_system, atoms, operators, realizations)
            # Add harmless naming noise that normalization should collapse.
            if idx % 2 == 0:
                payload["artifact_type"] = artifact_type.upper()
            if idx % 3 == 0:
                payload["source_system"] = source_system.replace(" ", "_")
            if idx % 4 == 0:
                payload["source_system"] = source_system.replace(" ", "-")
            write_family(out_dir, f"{group}_{idx:03d}", payload)

    # Probable groups: same family_key, multiple canonical variants, but close component totals.
    probable_specs = [
        ("probable-course", 5, "text", "Course Notes", [(4, 1, 1), (4, 2, 1), (5, 1, 1)]),
        ("probable-support", 4, "json", "Support Queue", [(2, 1, 2), (3, 1, 2)]),
    ]
    for group, count, artifact_type, source_system, variants in probable_specs:
        for idx in range(count):
            atoms, operators, realizations = variants[idx % len(variants)]
            payload = make_payload(group, 200 + idx, artifact_type, source_system, atoms, operators, realizations)
            if idx % 2 == 0:
                payload["source_system"] = source_system.replace(" ", "_")
            write_family(out_dir, f"{group}_{idx:03d}", payload)

    # Related groups: same family_key, but structurally farther apart.
    related_specs = [
        ("related-creator", 4, "audio", "Creator Notes", [(2, 0, 1), (7, 4, 3), (9, 5, 4)]),
        ("related-ops", 3, "text", "Ops Reports", [(1, 0, 0), (6, 3, 2), (10, 5, 4)]),
    ]
    for group, count, artifact_type, source_system, variants in related_specs:
        for idx in range(count):
            atoms, operators, realizations = variants[idx % len(variants)]
            payload = make_payload(group, 400 + idx, artifact_type, source_system, atoms, operators, realizations)
            if idx % 2 == 1:
                payload["artifact_type"] = artifact_type.upper()
            write_family(out_dir, f"{group}_{idx:03d}", payload)

    # Unique outliers that should remain distinct.
    outliers = [
        ("video-brief", "video", "Video Briefs", 4, 3, 1),
        ("proof-pack", "markdown", "Proof Pack", 7, 4, 2),
        ("offer-pack", "text", "Offer Pack", 1, 1, 3),
        ("training-audio", "audio", "Training Audio", 9, 2, 4),
    ]
    for idx, (group, artifact_type, source_system, atoms, operators, realizations) in enumerate(outliers):
        payload = make_payload(group, 1000 + idx, artifact_type, source_system, atoms, operators, realizations)
        write_family(out_dir, f"{group}_{idx:03d}", payload)


def main() -> int:
    out_dir = Path("/tmp/bonfyre-equivalence-corpus")
    build_corpus(out_dir)
    print(out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
