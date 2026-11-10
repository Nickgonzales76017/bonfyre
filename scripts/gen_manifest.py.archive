#!/usr/bin/env python3
import argparse
import datetime
import glob
import hashlib
import json
import os
import subprocess


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def git_revision() -> str:
    try:
        rev = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
        return f"git:{rev}"
    except Exception:
        return "git:unknown"


def model_family_from_path(path: str) -> str:
    return os.path.basename(path).split(".")[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate bonfyre.manifest.json")
    parser.add_argument("--target", default="bun-linux-x64", help="Bun compile target")
    parser.add_argument("--out", default="bonfyre.manifest.json", help="Output path")
    parser.add_argument("--release", default="0.7.0", help="Release label")
    parser.add_argument("--schema", default="bonfyre.deploy.v1", help="Manifest schema version")
    parser.add_argument("--runtime-abi", default="bonfyre-abi-v1", help="Runtime ABI label")
    parser.add_argument("--default-model-family", default="qwen3-8b", help="Fallback model family when no .bfmodel files exist")
    args = parser.parse_args()

    operators = []
    for path in sorted(glob.glob("cmd/*/*")):
        if not os.path.isfile(path):
            continue
        base = os.path.basename(path)
        if not (base == "bonfyre" or base.startswith("bonfyre-")):
            continue
        if not os.access(path, os.X_OK):
            continue
        operators.append({
            "name": base.replace("bonfyre-", ""),
            "binary": base,
            "sha256": sha256_file(path),
            "required": True,
        })

    libraries = []
    for path in ["lib/libbonfyre/libbonfyre.so", "lib/libbonfyre/libbonfyre.dylib"]:
        if not os.path.isfile(path):
            continue
        libraries.append({
            "name": os.path.basename(path),
            "sha256": sha256_file(path),
            "abi": args.runtime_abi,
        })

    by_family: dict = {}
    for pattern, field in [
        ("model-memory/*.bfmodel", "bfmodel"),
        ("model-memory/*.akmodel", "akmodel"),
    ]:
        for path in sorted(glob.glob(pattern)):
            fam = model_family_from_path(path)
            entry = by_family.setdefault(fam, {"model_family": fam, "required": False})
            entry[field] = path
    model_memory = [by_family[key] for key in sorted(by_family)]
    if not model_memory:
        model_memory.append({
            "model_family": args.default_model_family,
            "bfmodel": f"model-memory/{args.default_model_family}.bfmodel",
            "akmodel": f"model-memory/{args.default_model_family}.akmodel",
            "required": False,
        })

    # SAE dictionaries (both legacy and Aurekai aliases)
    sae_dicts: list = []
    sae_by_name: dict = {}
    for pattern, field in [
        ("model-memory/*.bfsae", "bfsae"),
        ("model-memory/*.aksae", "aksae"),
    ]:
        for path in sorted(glob.glob(pattern)):
            name = os.path.basename(path).split(".")[0]
            entry = sae_by_name.setdefault(name, {"name": name})
            entry[field] = path
    sae_dicts = [sae_by_name[k] for k in sorted(sae_by_name)]

    # FPQx cross-model alignment manifests
    fpqx_alignments: list = []
    for pattern, field in [
        ("model-memory/*.bffpqx", "bffpqx"),
        ("model-memory/*.akfpqx", "akfpqx"),
    ]:
        for path in sorted(glob.glob(pattern)):
            stem = os.path.basename(path).rsplit(".", 1)[0]
            existing = next((f for f in fpqx_alignments if f.get("stem") == stem), None)
            if existing is None:
                existing = {"stem": stem}
                fpqx_alignments.append(existing)
            existing[field] = path

    doc = {
        "schema_version": args.schema,
        "release": args.release,
        "revision": git_revision(),
        "built_at": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "target": args.target,
        "hyper": {
            "path": "bin/bonfyre-hyper",
            "bun_target": args.target,
            "version": args.release,
        },
        "runtime": {
            "operator_count": len(operators),
            "ffi_registry_zero_drift": True,
            "capabilities_runnable": "unknown",
        },
        "libraries": libraries,
        "operators": operators,
        "model_memory": model_memory,
        "sae_dicts": sae_dicts,
        "fpqx_alignments": fpqx_alignments,
        "signature": {
            "alg": "none",
            "value": "TODO",
        },
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)

    print(f"written: {args.out}")


if __name__ == "__main__":
    main()
