#!/usr/bin/env python3
"""Resolve machine authority for Generation-10 profile/projection registries.

Canonical counts are invariants, not permission to synthesize identities.  A
GitHub Actions fan-out is authorized only when a tracked machine-readable file
contains exactly the canonical number of uniquely identifiable records.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = Path(__file__).with_name("generation10_contract.json")
SCHEMA = "bonfyre.generation10.action-receipt.v1"

DOMAINS = {
    "format_profiles": {
        "count_key": "format_profiles",
        "path_tokens": ("format", "profile", "artifact"),
        "content_tokens": ("format", "profile"),
    },
    "partner_commons_profiles": {
        "count_key": "partner_commons_profiles",
        "path_tokens": ("partner", "commons", "profile"),
        "content_tokens": ("partner", "authority", "effect"),
    },
    "deep_promoted_specialties": {
        "count_key": "deep_promoted_specialties",
        "path_tokens": ("special", "promot", "capability"),
        "content_tokens": ("special", "promot"),
    },
    "active_closure_projections": {
        "count_key": "active_closure_projections",
        "path_tokens": ("closure", "projection", "active"),
        "content_tokens": ("closure", "projection"),
    },
}

ID_KEYS = (
    "id", "name", "key", "slug", "profile_id", "format_id", "partner_id",
    "specialty_id", "projection_id", "capability_id", "operator_id",
)
MACHINE_SUFFIXES = {".json", ".csv", ".tsv", ".yaml", ".yml", ".toml", ".yaff"}
MAX_FILE = 5 * 1024 * 1024


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_files() -> list[str]:
    proc = subprocess.run(["git", "ls-files"], cwd=ROOT, text=True, capture_output=True)
    return sorted(line.strip() for line in proc.stdout.splitlines() if line.strip()) if proc.returncode == 0 else []


def identity_from_record(record: Any) -> str | None:
    if isinstance(record, str) and record.strip():
        return record.strip()
    if not isinstance(record, dict):
        return None
    for key in ID_KEYS:
        value = record.get(key)
        if isinstance(value, (str, int)) and str(value).strip():
            return str(value).strip()
    return None


def list_identity(records: list[Any]) -> dict[str, Any]:
    identities = [identity_from_record(item) for item in records]
    usable = all(value is not None for value in identities)
    clean = [value for value in identities if value is not None]
    return {
        "record_count": len(records),
        "identity_count": len(clean),
        "identities_unique": usable and len(set(clean)) == len(clean),
        "identities": clean if usable else [],
    }


def parse_json(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    candidates: list[dict[str, Any]] = []
    if isinstance(value, list):
        observation = list_identity(value)
        observation.update({"container": "$", "parser": "json"})
        candidates.append(observation)
    elif isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, list):
                observation = list_identity(item)
                observation.update({"container": key, "parser": "json"})
                candidates.append(observation)
            elif isinstance(item, dict):
                identities = [str(k) for k in item]
                candidates.append({
                    "record_count": len(item),
                    "identity_count": len(identities),
                    "identities_unique": len(set(identities)) == len(identities),
                    "identities": identities,
                    "container": key,
                    "parser": "json-dict-keys",
                })
    return candidates


def parse_table(path: Path) -> list[dict[str, Any]]:
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    with path.open(encoding="utf-8", errors="replace", newline="") as stream:
        reader = csv.DictReader(stream, delimiter=delimiter)
        rows = list(reader)
    fieldnames = reader.fieldnames or []
    id_field = next((field for field in fieldnames if field.lower() in ID_KEYS), None)
    identities = [] if id_field is None else [str(row.get(id_field, "")).strip() for row in rows]
    usable = bool(id_field) and all(identities)
    return [{
        "record_count": len(rows),
        "identity_count": len(identities) if usable else 0,
        "identities_unique": usable and len(set(identities)) == len(identities),
        "identities": identities if usable else [],
        "container": "$rows",
        "parser": "tsv" if delimiter == "\t" else "csv",
        "id_field": id_field,
    }]


def parse_candidate(path: Path) -> list[dict[str, Any]]:
    try:
        if path.suffix.lower() == ".json":
            return parse_json(path)
        if path.suffix.lower() in {".csv", ".tsv"}:
            return parse_table(path)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, csv.Error):
        return []
    return []


def candidate_score(path_text: str, preview: str, config: dict[str, Any]) -> int:
    low_path = path_text.lower()
    low_preview = preview.lower()
    score = sum(5 for token in config["path_tokens"] if token in low_path)
    score += sum(1 for token in config["content_tokens"] if token in low_preview)
    return score


def find_candidates(domain: str, canonical_count: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    config = DOMAINS[domain]
    candidates: list[dict[str, Any]] = []
    authoritative: list[dict[str, Any]] = []
    for raw in git_files():
        path = ROOT / raw
        if path.suffix.lower() not in MACHINE_SUFFIXES or not path.is_file():
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size > MAX_FILE:
            continue
        try:
            preview = path.read_text(encoding="utf-8", errors="replace")[:256_000]
        except OSError:
            continue
        score = candidate_score(raw, preview, config)
        if score <= 0:
            continue
        parsed = parse_candidate(path)
        observation = {
            "path": raw,
            "sha256": sha(path),
            "bytes": size,
            "score": score,
            "containers": [
                {
                    k: v
                    for k, v in item.items()
                    if k != "identities"
                }
                for item in parsed
            ],
        }
        candidates.append(observation)
        for item in parsed:
            if (
                item["record_count"] == canonical_count
                and item["identity_count"] == canonical_count
                and item["identities_unique"]
            ):
                authoritative.append({
                    "path": raw,
                    "sha256": observation["sha256"],
                    "container": item["container"],
                    "parser": item["parser"],
                    "identities": item["identities"],
                })
    candidates.sort(key=lambda item: (-item["score"], item["path"]))
    authoritative.sort(key=lambda item: item["path"])
    return candidates[:50], authoritative


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", choices=sorted(DOMAINS), required=True)
    ap.add_argument("--output-dir", default=".generation10/receipts/registry-authority")
    args = ap.parse_args()

    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    canonical_count = contract["canonical_counts"][DOMAINS[args.domain]["count_key"]]
    candidates, authorities = find_candidates(args.domain, canonical_count)

    # More than one exact registry is itself an authority conflict. Never pick
    # one silently just because its pathname sorts first.
    authority = authorities[0] if len(authorities) == 1 else None
    conflict = len(authorities) > 1
    matrix_authorized = authority is not None and not conflict
    identities = authority["identities"] if matrix_authorized else []

    gaps: list[str] = []
    if not authorities:
        gaps.append("no_exact_unique_machine_registry")
    if conflict:
        gaps.append("multiple_exact_machine_registries_without_precedence_rule")
    if not matrix_authorized:
        gaps.append("dynamic_matrix_not_authorized")

    contract_sha = sha(CONTRACT)
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": "registry_authority",
        "subject": args.domain,
        "git_sha": os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "contract_sha256": contract_sha,
        "canonical_count": canonical_count,
        "candidate_registries": candidates,
        "exact_registry_count": len(authorities),
        "authority": None if authority is None else {k: v for k, v in authority.items() if k != "identities"},
        "matrix_authorized": matrix_authorized,
        "authorized_identity_count": len(identities),
        "authorized_identities": identities,
        "gaps": gaps,
        "outcome": "passed" if matrix_authorized else "coverage_debt",
    }
    receipt["factor_validity"] = {
        "git_sha": receipt["git_sha"],
        "contract_sha256": contract_sha,
        "workflow": receipt["workflow"],
        "run_attempt": receipt["run_attempt"],
    }
    receipt["content_sha256"] = hashlib.sha256(canonical(receipt)).hexdigest()
    out = ROOT / args.output_dir / f"{args.domain}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(canonical(receipt))
    print(out)
    print(json.dumps({
        "domain": args.domain,
        "canonical_count": canonical_count,
        "candidate_count": len(candidates),
        "exact_registry_count": len(authorities),
        "matrix_authorized": matrix_authorized,
        "gaps": gaps,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
