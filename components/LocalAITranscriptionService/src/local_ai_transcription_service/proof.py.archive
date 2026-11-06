import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .pipeline import slugify


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_proof_index(proof_root: Path) -> Dict[str, object]:
    index_path = proof_root / "index.json"
    if not index_path.exists():
        return {"created_at": _utc_now(), "updated_at": _utc_now(), "proofs": []}
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"created_at": _utc_now(), "updated_at": _utc_now(), "proofs": []}
    if not isinstance(payload, dict):
        return {"created_at": _utc_now(), "updated_at": _utc_now(), "proofs": []}
    proofs = payload.get("proofs")
    if not isinstance(proofs, list):
        payload["proofs"] = []
    return payload


def save_proof_index(proof_root: Path, payload: Dict[str, object]) -> Path:
    proof_root.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = _utc_now()
    index_path = proof_root / "index.json"
    index_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return index_path


def promote_job_to_proof(
    *,
    job_output: Path,
    proof_root: Path,
    proof_label: Optional[str] = None,
) -> Dict[str, object]:
    if job_output.is_file():
        if job_output.name != "meta.json":
            raise ValueError("Proof promotion path must be a job output directory or meta.json.")
        job_output = job_output.parent

    meta_path = job_output / "meta.json"
    deliverable_path = job_output / "deliverable.md"
    transcript_path = job_output / "transcript.txt"
    raw_transcript_path = job_output / "raw_transcript.txt"

    if not meta_path.exists() or not deliverable_path.exists() or not transcript_path.exists():
        raise ValueError(f"Job output is missing required proof artifacts: {job_output}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    proof_slug = slugify(proof_label or str(meta.get("job_slug") or job_output.name))
    proof_dir = proof_root / proof_slug
    proof_dir.mkdir(parents=True, exist_ok=True)

    copied_files: List[str] = []
    for source_path in (deliverable_path, transcript_path, raw_transcript_path, meta_path):
        if source_path.exists():
            target_path = proof_dir / source_path.name
            shutil.copy2(source_path, target_path)
            copied_files.append(source_path.name)

    summary_payload = {
        "proof_slug": proof_slug,
        "proof_label": proof_label or meta.get("job_name") or job_output.name,
        "promoted_at": _utc_now(),
        "source_job_output": str(job_output.resolve()),
        "job_name": meta.get("job_name"),
        "job_slug": meta.get("job_slug"),
        "source_kind": meta.get("source_kind"),
        "summary_bullets": meta.get("summary_bullets"),
        "quality": meta.get("quality"),
        "intake_manifest": meta.get("intake_manifest"),
        "copied_files": copied_files,
    }
    summary_path = proof_dir / "proof-summary.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2) + "\n", encoding="utf-8")

    index = load_proof_index(proof_root)
    proofs = [entry for entry in index.get("proofs", []) if isinstance(entry, dict)]
    proofs = [entry for entry in proofs if entry.get("proof_slug") != proof_slug]
    proofs.append(
        {
            "proof_slug": proof_slug,
            "proof_label": summary_payload["proof_label"],
            "promoted_at": summary_payload["promoted_at"],
            "source_job_output": summary_payload["source_job_output"],
            "deliverable_path": str((proof_dir / "deliverable.md").resolve()),
            "proof_summary_path": str(summary_path.resolve()),
            "quality": meta.get("quality"),
        }
    )
    index["proofs"] = proofs
    index_path = save_proof_index(proof_root, index)

    return {
        "proof_slug": proof_slug,
        "proof_dir": str(proof_dir.resolve()),
        "proof_summary_path": str(summary_path.resolve()),
        "index_path": str(index_path.resolve()),
        "copied_files": copied_files,
    }
