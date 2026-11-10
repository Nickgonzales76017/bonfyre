#!/usr/bin/env python3
"""
hf_verify_recipes.py

Upgrade Bonfyre recipe artifacts from static ontology to verified extraction.

For each recipe:
- resolve the exact Hugging Face repo
- inspect config + tokenizer + safetensors index/header metadata
- enumerate actual tensor names when possible
- compare against recipe pull patterns
- update recipe YAML with verification metadata

Outputs:
- verified_recipes.json
- missing_tensors.json
- variant_patterns.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import fnmatch
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCANNER = ROOT / "tools" / "hf_tensor_scan.py"
REPORT_VERIFIED = ROOT / "verified_recipes.json"
REPORT_MISSING = ROOT / "missing_tensors.json"
REPORT_VARIANTS = ROOT / "variant_patterns.json"
RECIPE_TIMEOUT_SECONDS = 120

ALIAS_RULES = (
    ("vision_tower.", "vision_model."),
    ("vision_model.", "vision_tower."),
    ("language_model.model.", "model."),
    ("model.", "language_model.model."),
    ("multi_modal_projector.", "mm_projector."),
    ("mm_projector.", "multi_modal_projector."),
    ("text_model.", "text_encoder."),
    ("text_encoder.", "text_model."),
    ("visual.", "vision_tower."),
    ("vision_tower.", "visual."),
)


def unique(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def recipe_files() -> List[Path]:
    paths = []
    for rel in ("recipes/google", "recipes/topology", "recipes/cross_fusion"):
        paths.extend(sorted((ROOT / rel).glob("*.yaml")))
    return paths


def load_recipe(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def write_recipe(path: Path, doc: dict) -> None:
    path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=False))


def report_name_for_recipe(path: Path) -> str:
    rel = path.relative_to(ROOT)
    return "__".join(rel.parts).replace(".yaml", ".json")


def repo_inventory_name(repo: str) -> str:
    return repo.replace("/", "__") + ".json"


def flatten_recipe_patterns(recipe: dict) -> List[str]:
    patterns = []
    for item in recipe.get("pull", []) or []:
        if isinstance(item, str):
            patterns.append(item)
    for item in (recipe.get("bonfyre_families", {}) or {}).keys():
        patterns.append(str(item))
    validation = recipe.get("validation", {}) or {}
    for key in ("required_tensors", "optional_tensors"):
        for item in validation.get(key, []) or []:
            if isinstance(item, str):
                patterns.append(item)
    return unique(patterns)


def alias_variants(pattern: str) -> List[str]:
    variants = {pattern}
    changed = True
    while changed:
        changed = False
        for current in list(variants):
            for old, new in ALIAS_RULES:
                if old in current:
                    candidate = current.replace(old, new)
                    if candidate not in variants:
                        variants.add(candidate)
                        changed = True
    return sorted(variants)


def heuristic_candidates(pattern: str, surfaces: Sequence[str]) -> List[str]:
    parts = [part for part in pattern.replace("*", " ").split(".") if len(part) > 2]
    if not parts:
        return []
    hits = [surface for surface in surfaces if all(part in surface for part in parts[-2:])]
    return hits[:20]


def classify_patterns(patterns: Sequence[str], surfaces: Sequence[str]) -> Dict[str, List[dict]]:
    verified = []
    missing = []
    renamed = []
    variant = []
    for pattern in patterns:
        hits = sorted(surface for surface in surfaces if fnmatch.fnmatch(surface, pattern))
        if hits:
            verified.append({"pattern": pattern, "matches": hits[:100]})
            continue
        aliases = [candidate for candidate in alias_variants(pattern) if candidate != pattern]
        alias_hits = []
        for candidate in aliases:
            alias_hits.extend(surface for surface in surfaces if fnmatch.fnmatch(surface, candidate))
        alias_hits = unique(sorted(alias_hits))
        if alias_hits:
            renamed.append(
                {
                    "pattern": pattern,
                    "aliases": aliases[:12],
                    "candidates": alias_hits[:100],
                }
            )
            continue
        heuristic = heuristic_candidates(pattern, surfaces)
        if heuristic:
            variant.append({"pattern": pattern, "candidates": heuristic})
            continue
        missing.append({"pattern": pattern})
    return {
        "verified": verified,
        "missing": missing,
        "renamed": renamed,
        "variant_dependent": variant,
    }


def build_verification_block(recipe: dict, inventory: dict) -> dict:
    patterns = flatten_recipe_patterns(recipe)
    classified = classify_patterns(patterns, inventory.get("surfaces", []))
    refined_exact = unique(
        match
        for item in classified["verified"]
        for match in item.get("matches", [])
        if "*" not in item["pattern"]
    )[:200]
    refined_patterns = unique(
        item["pattern"] for item in classified["verified"] if "*" in item["pattern"]
    )
    notes = []
    if inventory.get("resolution_status") == "resolved":
        notes.append("source_model was resolved to a verified repo id during inspection")
    if not inventory.get("tensor_names"):
        notes.append("no explicit tensor names were discoverable from index files or safetensors headers")
    if inventory.get("header_files"):
        notes.append("single-file safetensors were sampled via header inspection")
    if inventory.get("access_status") == "auth_blocked":
        notes.append("metadata or index fetches were blocked by Hugging Face auth in this environment")
    if inventory.get("resolution_note"):
        notes.append(str(inventory["resolution_note"]))

    pattern_details = []
    for bucket_name in ("verified", "missing", "renamed", "variant_dependent"):
        for item in classified.get(bucket_name, []):
            pattern_details.append({"status": bucket_name, **item})

    return {
        "checked_at": now_iso(),
        "requested_repo": inventory.get("requested_repo"),
        "resolved_repo": inventory.get("resolved_repo"),
        "resolution_status": inventory.get("resolution_status"),
        "resolution_candidates": inventory.get("resolution_candidates", []),
        "access_status": inventory.get("access_status"),
        "metadata_files": inventory.get("metadata_files", []),
        "metadata_summary": inventory.get("metadata_summary", {}),
        "metadata_access": inventory.get("metadata_access", {}),
        "index_files": inventory.get("index_files", []),
        "header_files": inventory.get("header_files", []),
        "tensor_inventory": {
            "tensor_name_count": len(inventory.get("tensor_names", [])),
            "surface_count": len(inventory.get("surfaces", [])),
            "sample": inventory.get("tensor_names", [])[:200],
            "by_file": {
                name: names[:200]
                for name, names in sorted((inventory.get("tensor_files", {}) or {}).items())
            },
        },
        "pull_status": classified,
        "pattern_details": pattern_details,
        "refined_pull": {
            "exact": refined_exact,
            "refined_patterns": refined_patterns,
        },
        "notes": notes,
    }


def refine_pull_section(doc: dict, verification: dict) -> None:
    refined = verification.get("refined_pull", {})
    exact = refined.get("exact", [])
    refined_patterns = refined.get("refined_patterns", [])
    variability = {
        "missing": [item["pattern"] for item in verification.get("pull_status", {}).get("missing", [])],
        "renamed": verification.get("pull_status", {}).get("renamed", []),
        "variant_dependent": verification.get("pull_status", {}).get("variant_dependent", []),
    }
    if exact or refined_patterns or any(variability.values()):
        doc["verified_pull"] = {
            "exact_tensors": exact,
            "refined_patterns": refined_patterns,
            "variability": variability,
        }


def annotate_validation(doc: dict, verification: dict) -> None:
    validation = doc.setdefault("validation", {})
    validation["verification_status"] = verification.get("access_status", "unknown")
    gap_flags = list(validation.get("gap_flags", []) or [])

    if verification.get("access_status") == "auth_blocked":
        gap_flags.append("live tensor verification blocked by Hugging Face auth in current environment")
    if verification.get("access_status") == "offline":
        gap_flags.append("live tensor verification could not reach Hugging Face in current environment")
    if verification.get("pull_status", {}).get("renamed"):
        gap_flags.append("some requested pull patterns matched via alias or rename heuristics")
    if verification.get("pull_status", {}).get("variant_dependent"):
        gap_flags.append("some requested pull patterns appear variant-dependent across exports")
    if verification.get("pull_status", {}).get("missing"):
        gap_flags.append("some requested pull patterns were not observed in the inspected artifact surface")

    validation["gap_flags"] = unique(gap_flags)


def scan_repo_inventory(
    repo: str,
    timeout_seconds: int,
    inventory_cache: Dict[str, dict],
    prebuilt_inventory_dir: Path | None,
) -> dict:
    cached = inventory_cache.get(repo)
    if cached is not None:
        return cached

    if prebuilt_inventory_dir is not None:
        prebuilt = prebuilt_inventory_dir / repo_inventory_name(repo)
        if prebuilt.exists():
            inventory = json.loads(prebuilt.read_text())
            inventory_cache[repo] = inventory
            return inventory

    with tempfile.TemporaryDirectory(prefix="bonfyre_hf_verify_") as tmpdir:
        tmpdir_path = Path(tmpdir)
        emit_yaml = tmpdir_path / "scan.yaml"
        emit_inventory = tmpdir_path / "inventory.json"
        cmd = [
            sys.executable,
            str(SCANNER),
            "--repo",
            repo,
            "--emit",
            str(emit_yaml),
            "--emit-inventory",
            str(emit_inventory),
        ]
        subprocess.run(cmd, check=True, timeout=timeout_seconds, cwd=ROOT)
        inventory = json.loads(emit_inventory.read_text())
        inventory_cache[repo] = inventory
        return inventory


def update_recipe(
    path: Path,
    inventory_cache: Dict[str, dict],
    timeout_seconds: int,
    inventory_dir: Path | None,
    prebuilt_inventory_dir: Path | None,
) -> Dict[str, object]:
    doc = load_recipe(path)
    source_model = doc["source_model"]
    print(f"  scanning {source_model}", file=sys.stderr, flush=True)
    inventory = scan_repo_inventory(source_model, timeout_seconds, inventory_cache, prebuilt_inventory_dir)
    print(
        f"  inventory {inventory.get('access_status')} surfaces={len(inventory.get('surfaces', []))} tensors={len(inventory.get('tensor_names', []))}",
        file=sys.stderr,
        flush=True,
    )
    verification = build_verification_block(doc, inventory)

    original_source = doc["source_model"]
    resolved_source = str(inventory.get("resolved_repo", original_source))
    if resolved_source and resolved_source != original_source:
        doc.setdefault("verification", {})
        doc["verification"]["source_model_original"] = original_source
        doc["source_model"] = resolved_source

    doc["verification"] = {
        **doc.get("verification", {}),
        **verification,
    }
    refine_pull_section(doc, verification)
    annotate_validation(doc, verification)
    print(f"  writing {path.relative_to(ROOT)}", file=sys.stderr, flush=True)
    write_recipe(path, doc)

    if inventory_dir is not None:
        inventory_path = inventory_dir / report_name_for_recipe(path)
        inventory_path.parent.mkdir(parents=True, exist_ok=True)
        inventory_path.write_text(json.dumps(inventory, indent=2, sort_keys=True))

    pull_status = verification.get("pull_status", {})
    return {
        "recipe_file": str(path.relative_to(ROOT)),
        "recipe": doc.get("recipe"),
        "requested_repo": verification.get("requested_repo"),
        "resolved_repo": verification.get("resolved_repo"),
        "resolution_status": verification.get("resolution_status"),
        "access_status": verification.get("access_status"),
        "verified": pull_status.get("verified", []),
        "missing": pull_status.get("missing", []),
        "renamed": pull_status.get("renamed", []),
        "variant_dependent": pull_status.get("variant_dependent", []),
        "metadata_files": verification.get("metadata_files", []),
        "index_files": verification.get("index_files", []),
        "header_files": verification.get("header_files", []),
        "tensor_name_count": verification.get("tensor_inventory", {}).get("tensor_name_count", 0),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Bonfyre HF extraction recipes.")
    parser.add_argument(
        "--recipes",
        nargs="*",
        help="Optional subset of recipe files to verify. Defaults to all generated recipe YAMLs.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(ROOT),
        help="Directory for verified_recipes.json, missing_tensors.json, and variant_patterns.json",
    )
    parser.add_argument(
        "--inventory-dir",
        help="Optional directory for per-recipe inventory JSON snapshots.",
    )
    parser.add_argument(
        "--prebuilt-inventory-dir",
        help="Optional directory of live inventory JSON files keyed by repo name, e.g. google__gemma-3-12b-it.json",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=RECIPE_TIMEOUT_SECONDS,
        help="Per-recipe timeout in seconds.",
    )
    args = parser.parse_args()

    paths = [ROOT / recipe for recipe in args.recipes] if args.recipes else recipe_files()
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    inventory_dir = Path(args.inventory_dir) if args.inventory_dir else None
    if inventory_dir is not None:
        inventory_dir.mkdir(parents=True, exist_ok=True)
    prebuilt_inventory_dir = Path(args.prebuilt_inventory_dir) if args.prebuilt_inventory_dir else None

    verified_report = []
    missing_report = []
    variant_report = []
    inventory_cache: Dict[str, dict] = {}

    for path in paths:
        print(f"Verifying {path.relative_to(ROOT)}", file=sys.stderr, flush=True)
        try:
            result = update_recipe(
                path,
                inventory_cache,
                args.timeout_seconds,
                inventory_dir,
                prebuilt_inventory_dir,
            )
        except subprocess.TimeoutExpired:
            print(
                f"Timed out {path.relative_to(ROOT)} after {args.timeout_seconds}s",
                file=sys.stderr,
                flush=True,
            )
            doc = load_recipe(path)
            verification = {
                "checked_at": now_iso(),
                "requested_repo": doc.get("source_model"),
                "resolved_repo": doc.get("source_model"),
                "resolution_status": "timeout",
                "resolution_candidates": [],
                "access_status": "timeout",
                "metadata_files": [],
                "metadata_summary": {},
                "metadata_access": {},
                "index_files": [],
                "header_files": [],
                "tensor_inventory": {
                    "tensor_name_count": 0,
                    "surface_count": 0,
                    "sample": [],
                    "by_file": {},
                },
                "pull_status": {
                    "verified": [],
                    "missing": [],
                    "renamed": [],
                    "variant_dependent": [],
                },
                "pattern_details": [],
                "refined_pull": {
                    "exact": [],
                    "refined_patterns": [],
                },
                "notes": [f"verification timed out after {args.timeout_seconds} seconds in current environment"],
            }
            doc["verification"] = {
                **doc.get("verification", {}),
                **verification,
            }
            refine_pull_section(doc, verification)
            annotate_validation(doc, verification)
            write_recipe(path, doc)
            result = {
                "recipe_file": str(path.relative_to(ROOT)),
                "recipe": doc.get("recipe"),
                "requested_repo": verification.get("requested_repo"),
                "resolved_repo": verification.get("resolved_repo"),
                "resolution_status": verification.get("resolution_status"),
                "access_status": verification.get("access_status"),
                "verified": [],
                "missing": [],
                "renamed": [],
                "variant_dependent": [],
                "metadata_files": [],
                "index_files": [],
                "header_files": [],
                "tensor_name_count": 0,
            }

        verified_report.append(result)
        if result["missing"]:
            missing_report.append(
                {
                    "recipe_file": result["recipe_file"],
                    "resolved_repo": result["resolved_repo"],
                    "missing": result["missing"],
                }
            )
        if result["renamed"] or result["variant_dependent"]:
            variant_report.append(
                {
                    "recipe_file": result["recipe_file"],
                    "resolved_repo": result["resolved_repo"],
                    "renamed": result["renamed"],
                    "variant_dependent": result["variant_dependent"],
                }
            )

    (report_dir / REPORT_VERIFIED.name).write_text(json.dumps(verified_report, indent=2, sort_keys=True))
    (report_dir / REPORT_MISSING.name).write_text(json.dumps(missing_report, indent=2, sort_keys=True))
    (report_dir / REPORT_VARIANTS.name).write_text(json.dumps(variant_report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
