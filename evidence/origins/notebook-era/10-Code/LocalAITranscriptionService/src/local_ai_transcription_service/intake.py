import base64
import binascii
import json
from pathlib import Path
from typing import Dict, List, Tuple


REQUIRED_INTAKE_FIELDS = ("jobId", "jobSlug", "clientName", "jobTitle", "outputGoal", "createdAt")
TRANSCRIPT_SUFFIXES = {".txt", ".md"}
INTAKE_MANIFEST_SUFFIX = ".intake.json"
INTAKE_PACKAGE_SUFFIX = ".intake-package.json"


def load_intake_manifest(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise ValueError(f"Intake manifest does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return load_intake_manifest_payload(payload)


def discover_intake_manifests(intake_dir: Path) -> List[Path]:
    if not intake_dir.exists():
        raise ValueError(f"Intake directory does not exist: {intake_dir}")
    if not intake_dir.is_dir():
        raise ValueError(f"Intake path is not a directory: {intake_dir}")
    return sorted(path for path in intake_dir.iterdir() if path.is_file() and path.name.endswith(INTAKE_MANIFEST_SUFFIX))


def load_intake_package(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise ValueError(f"Intake package does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    manifest = payload.get("manifest")
    source_file = payload.get("sourceFile")
    if not isinstance(manifest, dict):
        raise ValueError("Intake package is missing a valid manifest payload.")
    if not isinstance(source_file, dict):
        raise ValueError("Intake package is missing a valid sourceFile payload.")

    load_intake_manifest_payload(manifest)

    missing_source_fields = [field for field in ("name", "dataBase64") if field not in source_file]
    if missing_source_fields:
        raise ValueError(f"Intake package sourceFile is missing required fields: {', '.join(missing_source_fields)}")

    return payload


def load_intake_manifest_payload(payload: Dict[str, object]) -> Dict[str, object]:
    missing = [field for field in REQUIRED_INTAKE_FIELDS if field not in payload]
    if missing:
        raise ValueError(f"Intake manifest is missing required fields: {', '.join(missing)}")
    return payload


def resolve_intake_source(manifest_path: Path, manifest: Dict[str, object]) -> Tuple[Path, str]:
    file_name = manifest.get("fileName")
    if not file_name:
        raise ValueError("Intake manifest is missing fileName for source resolution.")

    source_path = manifest_path.parent / str(file_name)
    if not source_path.exists():
        raise ValueError(f"Source file referenced by intake manifest was not found: {source_path}")

    source_kind = "transcript_file" if source_path.suffix.lower() in TRANSCRIPT_SUFFIXES else "audio_file"
    return source_path, source_kind


def extract_intake_package_source(
    package_path: Path,
    package_payload: Dict[str, object],
    destination_dir: Path,
) -> Tuple[Dict[str, object], Path, str]:
    manifest = dict(package_payload["manifest"])  # type: ignore[index]
    source_file = dict(package_payload["sourceFile"])  # type: ignore[index]
    file_name = str(source_file["name"])
    encoded_data = str(source_file["dataBase64"])

    try:
        file_bytes = base64.b64decode(encoded_data.encode("utf-8"), validate=True)
    except (binascii.Error, ValueError) as error:
        raise ValueError(f"Intake package contains invalid base64 file data: {package_path}") from error

    destination_dir.mkdir(parents=True, exist_ok=True)
    source_path = destination_dir / file_name
    source_path.write_bytes(file_bytes)

    source_kind = "transcript_file" if source_path.suffix.lower() in TRANSCRIPT_SUFFIXES else "audio_file"
    return manifest, source_path, source_kind
