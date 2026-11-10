#!/usr/bin/env python3
"""
hf_layer_pull.py

Resolve a Bonfyre T_* family to real tensor surfaces in a Hugging Face repo.
This validates family pull patterns against actual inventory before writing a recipe.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FAMILY_INDEX = ROOT / "recipes" / "families" / "family_index.json"


def load_scanner():
    path = ROOT / "tools" / "hf_tensor_scan.py"
    spec = importlib.util.spec_from_file_location("hf_tensor_scan", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load hf_tensor_scan.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def family_entry(name: str) -> dict:
    data = json.loads(FAMILY_INDEX.read_text())
    for item in data.get("families", []):
        if item.get("family") == name:
            return item
    raise SystemExit(f"Unknown family: {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull tensors by Bonfyre family into a recipe artifact.")
    parser.add_argument("--repo", required=True, help="HF repo id")
    parser.add_argument("--family", required=True, help="Bonfyre T_* family")
    parser.add_argument("--out", required=True, help="Output recipe YAML")
    parser.add_argument("--collection", default="family_pull", help="Source collection label")
    parser.add_argument("--allow-unverified", action="store_true", help="Write recipe even when no surfaces verify")
    parser.add_argument("--emit-inventory", help="Optional JSON path for actual inventory output")
    args = parser.parse_args()

    entry = family_entry(args.family)
    patterns = entry.get("tensor_patterns", [])
    scanner = load_scanner()
    inventory = scanner.enumerate_repo_inventory(args.repo)
    surfaces = scanner.collect_surfaces(inventory)
    classified = scanner.classify_patterns(patterns, surfaces)

    verified_surfaces = []
    for item in classified["verified"]:
        verified_surfaces.extend(item.get("matches", []))
    verified_surfaces = scanner.unique(verified_surfaces)

    if not verified_surfaces and not args.allow_unverified:
        raise SystemExit(
            f"No verified surfaces found for family {args.family} in {inventory.get('resolved_repo', args.repo)}. "
            "Use --allow-unverified to preserve speculative recipe generation."
        )

    pull_surfaces = verified_surfaces or list(patterns) or ["config.json"]
    match = scanner.FamilyMatch(
        family=args.family,
        tensor_patterns=list(patterns),
        capabilities=entry.get("capabilities", []),
        workflow_steps=entry.get("workflow_steps", []),
    )
    recipe_name = scanner.safe_slug(Path(args.out).stem)
    yaml_text = scanner.emit_yaml(
        recipe_name,
        str(inventory.get("resolved_repo", args.repo)),
        args.collection,
        pull_surfaces,
        [match],
        inventory=inventory,
    )

    doc = yaml.safe_load(yaml_text)
    doc.setdefault("verification", {})
    doc["verification"]["family_validation"] = {
        "family": args.family,
        "access_status": inventory.get("access_status"),
        "verified_match_count": len(classified["verified"]),
        "verified_surface_count": len(verified_surfaces),
        "verified_surfaces": verified_surfaces[:200],
        "missing_patterns": [item["pattern"] for item in classified["missing"]],
        "renamed_patterns": classified["renamed"],
        "variant_patterns": classified["variant_dependent"],
    }
    doc["verification"]["validated_pull"] = {
        "exact_tensors": [surface for surface in verified_surfaces if "*" not in surface][:200],
        "refined_patterns": [surface for surface in verified_surfaces if "*" in surface][:200],
    }
    doc.setdefault("validation", {})
    doc["validation"]["family_pull_status"] = inventory.get("access_status")
    gap_flags = list(doc["validation"].get("gap_flags", []) or [])
    if inventory.get("access_status") == "auth_blocked":
        gap_flags.append("family pull could not inspect protected metadata in current environment")
    if classified["renamed"]:
        gap_flags.append("family pull matched some patterns via alias or rename heuristics")
    if classified["variant_dependent"]:
        gap_flags.append("family pull includes export-variant-dependent tensor surfaces")
    if classified["missing"]:
        gap_flags.append("family pull includes unresolved tensor patterns")
    doc["validation"]["gap_flags"] = scanner.unique(gap_flags)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=False))
    if args.emit_inventory:
        scanner.emit_inventory_json(Path(args.emit_inventory), inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
