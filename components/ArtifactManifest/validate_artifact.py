#!/usr/bin/env python3
"""Validate a Bonfyre artifact.json manifest against the schema.

Usage:
  validate_artifact.py <manifest.json>
  validate_artifact.py --schema <schema.json> <manifest.json>
  validate_artifact.py --compute-hashes <manifest.json>

Exit codes:
  0  valid
  1  validation errors
  2  file/usage error
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_SCHEMA = SCRIPT_DIR / "artifact.schema.json"


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def canonical_json(obj) -> str:
    """Deterministic JSON: sorted keys, no extra whitespace, ensure_ascii."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------- lightweight schema validation (no jsonschema dependency) ----------

def _check_type(val, expected, path_str):
    errs = []
    if expected == "string" and not isinstance(val, str):
        errs.append(f"{path_str}: expected string, got {type(val).__name__}")
    elif expected == "integer" and not isinstance(val, int):
        errs.append(f"{path_str}: expected integer, got {type(val).__name__}")
    elif expected == "boolean" and not isinstance(val, bool):
        errs.append(f"{path_str}: expected boolean, got {type(val).__name__}")
    elif expected == "array" and not isinstance(val, list):
        errs.append(f"{path_str}: expected array, got {type(val).__name__}")
    elif expected == "object" and not isinstance(val, dict):
        errs.append(f"{path_str}: expected object, got {type(val).__name__}")
    return errs


def validate_manifest(manifest: dict, schema: dict) -> list[str]:
    """Validate manifest against the Bonfyre artifact schema.

    This is a hand-rolled validator that checks required fields, types,
    and structural rules without depending on the jsonschema library.
    """
    errors: list[str] = []

    # --- top-level required fields ---
    top_required = schema.get("required", [])
    for key in top_required:
        if key not in manifest:
            errors.append(f"Missing required top-level field: {key}")

    # --- schema_version ---
    sv = manifest.get("schema_version")
    if sv is not None and sv != "1.0.0":
        errors.append(f"schema_version must be '1.0.0', got '{sv}'")

    # --- artifact_type enum ---
    at = manifest.get("artifact_type")
    allowed = schema["properties"]["artifact_type"].get("enum", [])
    if at is not None and allowed and at not in allowed:
        errors.append(f"artifact_type '{at}' not in allowed: {allowed}")

    # --- atoms ---
    atoms = manifest.get("atoms", [])
    if not isinstance(atoms, list):
        errors.append("atoms must be an array")
    else:
        atom_ids = set()
        for i, atom in enumerate(atoms):
            pfx = f"atoms[{i}]"
            if not isinstance(atom, dict):
                errors.append(f"{pfx}: must be object")
                continue
            for req in ("atom_id", "content_hash", "media_type"):
                if req not in atom:
                    errors.append(f"{pfx}: missing required field '{req}'")
            aid = atom.get("atom_id")
            if aid:
                if aid in atom_ids:
                    errors.append(f"{pfx}: duplicate atom_id '{aid}'")
                atom_ids.add(aid)

    # --- operators ---
    operators = manifest.get("operators", [])
    if not isinstance(operators, list):
        errors.append("operators must be an array")
    else:
        op_ids = set()
        for i, op in enumerate(operators):
            pfx = f"operators[{i}]"
            if not isinstance(op, dict):
                errors.append(f"{pfx}: must be object")
                continue
            for req in ("operator_id", "op", "inputs", "output"):
                if req not in op:
                    errors.append(f"{pfx}: missing required field '{req}'")
            oid = op.get("operator_id")
            if oid:
                if oid in op_ids:
                    errors.append(f"{pfx}: duplicate operator_id '{oid}'")
                op_ids.add(oid)
            inputs = op.get("inputs", [])
            if not isinstance(inputs, list):
                errors.append(f"{pfx}.inputs: must be an array")

    # --- realizations ---
    realizations = manifest.get("realizations", [])
    if not isinstance(realizations, list):
        errors.append("realizations must be an array")
    else:
        real_ids = set()
        for i, r in enumerate(realizations):
            pfx = f"realizations[{i}]"
            if not isinstance(r, dict):
                errors.append(f"{pfx}: must be object")
                continue
            for req in ("realization_id", "media_type"):
                if req not in r:
                    errors.append(f"{pfx}: missing required field '{req}'")
            rid = r.get("realization_id")
            if rid:
                if rid in real_ids:
                    errors.append(f"{pfx}: duplicate realization_id '{rid}'")
                real_ids.add(rid)

    # --- cross-reference checks ---
    all_ids = set()
    all_ids.update(atom_ids if isinstance(atoms, list) else set())
    all_ids.update(op_ids if isinstance(operators, list) else set())
    all_ids.update(real_ids if isinstance(realizations, list) else set())

    if isinstance(operators, list):
        for i, op in enumerate(operators):
            if not isinstance(op, dict):
                continue
            for inp in op.get("inputs", []):
                if inp not in all_ids:
                    errors.append(f"operators[{i}].inputs: reference '{inp}' not found in atoms/operators/realizations")
            out = op.get("output")
            if out and out not in real_ids:
                errors.append(f"operators[{i}].output: reference '{out}' not found in realizations")

    if isinstance(realizations, list):
        for i, r in enumerate(realizations):
            if not isinstance(r, dict):
                continue
            pb = r.get("produced_by")
            if pb and pb not in op_ids:
                errors.append(f"realizations[{i}].produced_by: reference '{pb}' not found in operators")

    return errors


# ---------- Merkle hash computation ----------

def compute_node_hash(op: dict, child_hashes: dict) -> str:
    """Compute SHA-256 hash for an operator node."""
    parts = {
        "op": op["op"],
        "params": op.get("params", {}),
        "inputs_hashes": sorted(child_hashes.get(i, "") for i in op.get("inputs", [])),
        "version": op.get("version", ""),
    }
    return sha256_str(canonical_json(parts))


def compute_merkle_hashes(manifest: dict) -> dict:
    """Compute node_hash for every operator and root_hash for the family.

    Returns dict with 'operators': {operator_id: hash}, 'root_hash': str.
    """
    # Build hash map: atom_id -> content_hash, then walk operators
    hashes: dict[str, str] = {}
    for atom in manifest.get("atoms", []):
        hashes[atom["atom_id"]] = atom.get("content_hash", "")

    # Topological sort (simple: iterate until stable)
    operators = manifest.get("operators", [])
    computed: dict[str, str] = {}
    remaining = list(operators)
    max_iter = len(remaining) + 1
    for _ in range(max_iter):
        next_remaining = []
        for op in remaining:
            inputs = op.get("inputs", [])
            if all(i in hashes for i in inputs):
                h = compute_node_hash(op, hashes)
                computed[op["operator_id"]] = h
                hashes[op["operator_id"]] = h
            else:
                next_remaining.append(op)
        if len(next_remaining) == len(remaining):
            break  # no progress — cycles or missing refs
        remaining = next_remaining

    # root hash = hash of all operator hashes sorted
    all_hashes = sorted(computed.values())
    root = sha256_str(canonical_json(all_hashes)) if all_hashes else ""

    return {"operators": computed, "root_hash": root}


# ---------- CLI ----------

def build_parser():
    p = argparse.ArgumentParser(description="Validate a Bonfyre artifact manifest.")
    p.add_argument("manifest", type=Path, help="Path to artifact.json")
    p.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA, help="Path to schema JSON")
    p.add_argument("--compute-hashes", action="store_true", help="Compute and print Merkle hashes")
    p.add_argument("--update-hashes", action="store_true", help="Compute hashes and write them into the manifest")
    return p


def main():
    args = build_parser().parse_args()

    if not args.manifest.exists():
        print(f"File not found: {args.manifest}", file=sys.stderr)
        return 2

    manifest = load_json(args.manifest)
    schema = load_json(args.schema) if args.schema.exists() else {}

    errors = validate_manifest(manifest, schema)

    if errors:
        print(f"INVALID — {len(errors)} error(s):")
        for e in errors:
            print(f"  - {e}")
    else:
        print("VALID")

    if args.compute_hashes or args.update_hashes:
        result = compute_merkle_hashes(manifest)
        if args.update_hashes:
            # Write hashes back into manifest
            op_map = {op["operator_id"]: op for op in manifest.get("operators", [])}
            for oid, h in result["operators"].items():
                if oid in op_map:
                    op_map[oid]["node_hash"] = h
            manifest["root_hash"] = result["root_hash"]
            with open(args.manifest, "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, indent=2, ensure_ascii=False)
                fh.write("\n")
            print(f"Updated hashes in {args.manifest}")
            print(f"  root_hash: {result['root_hash']}")
        else:
            print(f"\nMerkle root: {result['root_hash']}")
            for oid, h in sorted(result["operators"].items()):
                print(f"  {oid}: {h}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
