import hashlib
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict, Optional

from .intake import TRANSCRIPT_SUFFIXES, load_intake_package


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def default_cms_binary() -> Path:
    repo = repo_root()
    candidate = repo / "10-Code" / "BonfyreCMS" / "bonfyre-cms"
    if candidate.exists():
        return candidate
    found = shutil.which("bonfyre-cms")
    if found:
        return Path(found)
    return candidate


def default_cms_schemas() -> Path:
    return repo_root() / "10-Code" / "BonfyreCMS" / "content-types"


def default_cms_db() -> Path:
    return repo_root() / "bonfyre_cms.db"


def project_intake_package_to_cms_entry(package_path: Path, package_payload: Dict[str, object]) -> Dict[str, object]:
    manifest = package_payload.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("Intake package is missing a valid manifest payload.")

    source_file = package_payload.get("sourceFile")
    if not isinstance(source_file, dict):
        raise ValueError("Intake package is missing a valid sourceFile payload.")

    payload = source_file.get("dataBase64")
    payload_text = payload if isinstance(payload, str) else ""
    source_name = str(source_file.get("name") or manifest.get("fileName") or package_path.name)
    source_kind = "transcript_file" if Path(source_name).suffix.lower() in TRANSCRIPT_SUFFIXES else "audio_file"
    source_sidecar = {
        key: source_file.get(key)
        for key in ("name", "type", "size")
        if source_file.get(key) is not None
    }

    return {
        "schema_version": package_payload.get("schemaVersion"),
        "exported_at": package_payload.get("exportedAt"),
        "manifest_job_id": manifest.get("jobId"),
        "manifest_job_slug": manifest.get("jobSlug"),
        "manifest_client_name": manifest.get("clientName"),
        "manifest_client_contact": manifest.get("clientContact"),
        "manifest_job_title": manifest.get("jobTitle"),
        "manifest_output_goal": manifest.get("outputGoal"),
        "manifest_context_notes": manifest.get("contextNotes"),
        "manifest_status": manifest.get("status"),
        "manifest_created_at": manifest.get("createdAt"),
        "manifest_file_name": manifest.get("fileName"),
        "manifest_file_type": manifest.get("fileType"),
        "manifest_file_size": manifest.get("fileSize"),
        "source_file_name": source_name,
        "source_file_type": source_file.get("type"),
        "source_file_kind": source_kind,
        "source_file_size": source_file.get("size"),
        "source_file_payload_chars": len(payload_text) if payload_text else None,
        "source_file_payload_sha1": hashlib.sha1(payload_text.encode("utf-8")).hexdigest() if payload_text else None,
        "manifest_json": manifest,
        "source_file_json": source_sidecar,
    }


def _run_cms(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def _find_existing_intake_package_row(db_path: Path, manifest_job_id: Optional[str], manifest_job_slug: Optional[str]) -> Optional[int]:
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as conn:
        if manifest_job_id:
            row = conn.execute(
                "SELECT id FROM intake_package WHERE manifest_job_id=? ORDER BY id DESC LIMIT 1",
                (manifest_job_id,),
            ).fetchone()
            if row:
                return int(row[0])
        if manifest_job_slug:
            row = conn.execute(
                "SELECT id FROM intake_package WHERE manifest_job_slug=? ORDER BY id DESC LIMIT 1",
                (manifest_job_slug,),
            ).fetchone()
            if row:
                return int(row[0])
    return None


def sync_intake_package_to_cms(
    package_path: Path,
    *,
    db_path: Optional[Path] = None,
    schemas_dir: Optional[Path] = None,
    namespace: str = "root",
    cms_binary: Optional[Path] = None,
) -> Dict[str, object]:
    cms_bin = (cms_binary or default_cms_binary()).resolve()
    schemas = (schemas_dir or default_cms_schemas()).resolve()
    db = (db_path or default_cms_db()).resolve()

    if not cms_bin.exists():
        raise ValueError(f"BonfyreCMS binary not found: {cms_bin}")
    if not schemas.exists():
        raise ValueError(f"BonfyreCMS schemas directory not found: {schemas}")

    package_payload = load_intake_package(package_path)
    entry_payload = project_intake_package_to_cms_entry(package_path, package_payload)
    body = json.dumps(entry_payload, separators=(",", ":"), ensure_ascii=False)

    _run_cms([str(cms_bin), "schema", "migrate", "--db", str(db), "--schemas", str(schemas)])

    entry_id = _find_existing_intake_package_row(
        db,
        str(entry_payload.get("manifest_job_id") or "") or None,
        str(entry_payload.get("manifest_job_slug") or "") or None,
    )
    if entry_id is None:
        proc = _run_cms(
            [
                str(cms_bin),
                "entry",
                "create",
                "intake_package",
                "--db",
                str(db),
                "--schemas",
                str(schemas),
                "--ns",
                namespace,
                body,
            ]
        )
        result = json.loads(proc.stdout)
        return {
            "action": "created",
            "id": int(result["id"]),
            "content_type": "intake_package",
            "db_path": str(db),
            "schemas_dir": str(schemas),
            "manifest_job_id": entry_payload.get("manifest_job_id"),
            "manifest_job_slug": entry_payload.get("manifest_job_slug"),
        }

    _run_cms(
        [
            str(cms_bin),
            "entry",
            "update",
            "intake_package",
            str(entry_id),
            body,
            "--db",
            str(db),
            "--schemas",
            str(schemas),
        ]
    )
    return {
        "action": "updated",
        "id": int(entry_id),
        "content_type": "intake_package",
        "db_path": str(db),
        "schemas_dir": str(schemas),
        "manifest_job_id": entry_payload.get("manifest_job_id"),
        "manifest_job_slug": entry_payload.get("manifest_job_slug"),
    }
