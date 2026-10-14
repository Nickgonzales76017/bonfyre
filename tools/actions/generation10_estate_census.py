#!/usr/bin/env python3
"""Materialization census for the frozen Generation-10 system map.

This does not rewrite canonical counts to whatever the repository happens to
contain.  It reconciles canonical identities against tracked materialization,
gitlinks, manifests, and registry candidates and emits the divergence as a
ReceiptEnvelope.
"""
from __future__ import annotations

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


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True)


def tracked_entries() -> list[dict[str, str]]:
    proc = git("ls-files", "-s")
    entries = []
    if proc.returncode != 0:
        return entries
    for line in proc.stdout.splitlines():
        match = re.match(r"^(\d{6}) ([0-9a-f]{40}) (\d)\t(.+)$", line)
        if match:
            entries.append({"mode": match.group(1), "sha": match.group(2), "stage": match.group(3), "path": match.group(4)})
    return entries


def declared_submodules() -> list[str]:
    path = ROOT / ".gitmodules"
    if not path.is_file():
        return []
    return sorted(set(re.findall(r"^\s*path\s*=\s*(.+?)\s*$", path.read_text(encoding="utf-8", errors="replace"), re.M)))


def power_census(contract: dict[str, Any]) -> dict[str, Any]:
    live = sorted(
        p.name for p in (ROOT / "cmd").iterdir()
        if p.is_dir() and p.name.startswith("Bonfyre") and (p / "Makefile").is_file()
    ) if (ROOT / "cmd").is_dir() else []
    manifest_path = ROOT / "output" / "inventory" / "command_manifest.json"
    manifest_dirs: list[str] = []
    manifest_ids: list[str] = []
    declared_count = None
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            declared_count = manifest.get("command_count")
            manifest_dirs = sorted({str(x.get("command_dir")) for x in manifest.get("commands", []) if x.get("command_dir")})
            manifest_ids = sorted({str(x.get("capability_id")) for x in manifest.get("commands", []) if x.get("capability_id")})
        except (OSError, json.JSONDecodeError):
            pass
    return {
        "canonical_public_powers": contract["canonical_counts"]["public_powers"],
        "live_buildable_command_count": len(live),
        "manifest_declared_command_count": declared_count,
        "manifest_identity_count": len(manifest_ids),
        "live_commands": live,
        "manifest_command_dirs": manifest_dirs,
        "live_not_manifest": sorted(set(live) - set(manifest_dirs)),
        "manifest_not_live": sorted(set(manifest_dirs) - set(live)),
    }


def frappe_census(contract: dict[str, Any], entries: list[dict[str, str]]) -> dict[str, Any]:
    gitlinks = sorted(e["path"] for e in entries if e["mode"] == "160000")
    declared = declared_submodules()
    lineages = []
    for name in contract["frappe_lineages"]:
        slug = name.lower()
        expected = f"integrations/frappe-bench/apps/{slug}"
        tracked_under = [e["path"] for e in entries if e["path"] == expected or e["path"].startswith(expected + "/")]
        blocking_gitlinks = sorted(g for g in gitlinks if expected == g or expected.startswith(g + "/"))
        lineages.append({
            "lineage": name,
            "expected_path": expected,
            "working_tree_exists": (ROOT / expected).exists(),
            "tracked_entries": len(tracked_under),
            "blocking_gitlinks": blocking_gitlinks,
            "materialized": bool(tracked_under) and not blocking_gitlinks and (ROOT / expected).exists(),
        })
    return {
        "canonical_lineages": len(contract["frappe_lineages"]),
        "lineages": lineages,
        "gitlinks": gitlinks,
        "declared_submodule_paths": declared,
        "undeclared_gitlinks": sorted(set(gitlinks) - set(declared)),
        "declared_without_gitlink": sorted(set(declared) - set(gitlinks)),
    }


def measure_registry(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"path": path.relative_to(ROOT).as_posix(), "sha256": sha(path), "bytes": path.stat().st_size, "records": None, "method": None}
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, list):
                result.update(records=len(value), method="json_root_list")
            elif isinstance(value, dict):
                candidates = []
                for key, item in value.items():
                    if isinstance(item, (list, dict)):
                        candidates.append((len(item), key))
                if candidates:
                    count, key = max(candidates)
                    result.update(records=count, method=f"largest_json_container:{key}")
        elif suffix in {".csv", ".tsv"}:
            delimiter = "\t" if suffix == ".tsv" else ","
            with path.open(encoding="utf-8", errors="replace", newline="") as stream:
                rows = list(csv.reader(stream, delimiter=delimiter))
            result.update(records=max(0, len(rows) - 1), method="tabular_rows_minus_header")
        elif suffix in {".yaff", ".yaml", ".yml", ".toml"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            bullet_records = sum(1 for line in text.splitlines() if re.match(r"^\s*-\s+\S", line))
            if bullet_records:
                result.update(records=bullet_records, method="heuristic_bullet_records")
    except (OSError, ValueError, json.JSONDecodeError):
        result["method"] = "unreadable"
    return result


def registry_census(entries: list[dict[str, str]], contract: dict[str, Any]) -> dict[str, Any]:
    tracked = [e["path"] for e in entries if e["mode"] != "160000"]
    specs = {
        "format_profiles": (contract["canonical_counts"]["format_profiles"], ("format",)),
        "partner_commons_profiles": (contract["canonical_counts"]["partner_commons_profiles"], ("partner",)),
        "deep_promoted_specialties": (contract["canonical_counts"]["deep_promoted_specialties"], ("special", "promot")),
        "active_closure_projections": (contract["canonical_counts"]["active_closure_projections"], ("closure", "projection")),
    }
    allowed = {".json", ".yaff", ".yaml", ".yml", ".toml", ".csv", ".tsv"}
    result: dict[str, Any] = {}
    for name, (canonical_count, tokens) in specs.items():
        candidates = []
        for raw in tracked:
            lowered = raw.lower()
            if Path(raw).suffix.lower() not in allowed:
                continue
            if any(token in lowered for token in tokens):
                path = ROOT / raw
                if path.is_file() and path.stat().st_size <= 5 * 1024 * 1024:
                    candidates.append(measure_registry(path))
        candidates = sorted(candidates, key=lambda item: item["path"])[:100]
        result[name] = {
            "canonical_count": canonical_count,
            "registry_candidates": candidates,
            "count_matches": [item["path"] for item in candidates if item.get("records") == canonical_count],
        }
    return result


def main() -> int:
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    entries = tracked_entries()
    powers = power_census(contract)
    frappe = frappe_census(contract, entries)
    registries = registry_census(entries, contract)
    gaps: list[str] = []
    if powers["live_buildable_command_count"] != powers["canonical_public_powers"] or powers["manifest_declared_command_count"] != powers["canonical_public_powers"]:
        gaps.append("public_power_identity_drift")
    if frappe["undeclared_gitlinks"]:
        gaps.append("tracked_gitlinks_missing_gitmodules_declaration")
    if any(not item["materialized"] for item in frappe["lineages"]):
        gaps.append("one_or_more_frappe_lineages_not_materialized_in_hosted_checkout")
    for name, observation in registries.items():
        if not observation["count_matches"]:
            gaps.append(f"no_machine_registry_candidate_matches_{name}_canonical_count")

    contract_sha = sha(CONTRACT)
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": "estate_census",
        "subject": "generation10-materialization",
        "git_sha": os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "contract_sha256": contract_sha,
        "canonical_counts": contract["canonical_counts"],
        "tracked_entry_count": len(entries),
        "powers": powers,
        "frappe": frappe,
        "registries": registries,
        "gaps": sorted(set(gaps)),
        "outcome": "coverage_debt" if gaps else "passed",
    }
    receipt["factor_validity"] = {
        "git_sha": receipt["git_sha"],
        "contract_sha256": contract_sha,
        "workflow": receipt["workflow"],
        "run_attempt": receipt["run_attempt"],
    }
    receipt["content_sha256"] = hashlib.sha256(canonical(receipt)).hexdigest()
    out = ROOT / ".generation10" / "receipts" / "estate-census" / "generation10-materialization.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(canonical(receipt))
    print(out)
    print(json.dumps({
        "outcome": receipt["outcome"],
        "gaps": receipt["gaps"],
        "power_counts": {
            "canonical": powers["canonical_public_powers"],
            "live": powers["live_buildable_command_count"],
            "manifest": powers["manifest_declared_command_count"],
        },
        "frappe_materialized": sum(1 for item in frappe["lineages"] if item["materialized"]),
        "frappe_total": len(frappe["lineages"]),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
