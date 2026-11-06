#!/usr/bin/env python3
import argparse
import json
from datetime import datetime


def main() -> None:
    p = argparse.ArgumentParser(description="Generate release notes from bonfyre.manifest.json")
    p.add_argument("--manifest", required=True, help="Path to bonfyre.manifest.json")
    p.add_argument("--out", default="dist/release-notes.md", help="Output markdown path")
    args = p.parse_args()

    with open(args.manifest, "r", encoding="utf-8") as f:
        m = json.load(f)

    release = m.get("release", "unknown")
    target = m.get("target", "unknown")
    revision = m.get("revision", "unknown")
    built_at = m.get("built_at", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

    operators = m.get("operators", [])
    libraries = m.get("libraries", [])
    models = m.get("model_memory", [])
    sae_dicts = m.get("sae_dicts", [])
    fpqx_alignments = m.get("fpqx_alignments", [])

    lines = []
    lines.append(f"# Bonfyre Release {release}")
    lines.append("")
    lines.append(f"- Target: {target}")
    lines.append(f"- Revision: {revision}")
    lines.append(f"- Built at: {built_at}")
    lines.append(f"- Manifest schema: {m.get('schema_version', 'unknown')}")
    lines.append("")

    lines.append("## Runtime")
    lines.append("")
    lines.append(f"- Operators: {len(operators)}")
    lines.append(f"- Libraries: {len(libraries)}")
    lines.append("")

    if operators:
        lines.append("### Operators")
        lines.append("")
        for op in operators:
            name = op.get("name", "unknown")
            binary = op.get("binary", "unknown")
            req = op.get("required", False)
            lines.append(f"- {name} ({binary}) required={str(req).lower()}")
        lines.append("")

    if libraries:
        lines.append("### Libraries")
        lines.append("")
        for lib in libraries:
            lines.append(f"- {lib.get('name', 'unknown')} abi={lib.get('abi', 'unknown')}")
        lines.append("")

    lines.append("## Model Memory")
    lines.append("")
    if models:
        for model in models:
            public_path = model.get("akmodel") or model.get("bfmodel", "n/a")
            legacy_path = model.get("bfmodel")
            if legacy_path and legacy_path != public_path:
                lines.append(f"- {model.get('model_family', 'unknown')}: {public_path} (legacy: {legacy_path})")
            else:
                lines.append(f"- {model.get('model_family', 'unknown')}: {public_path}")
    else:
        lines.append("- none")
    lines.append("")

    if sae_dicts:
        lines.append("## SAE Dictionaries")
        lines.append("")
        for entry in sae_dicts:
            public_path = entry.get("aksae") or entry.get("bfsae", "n/a")
            legacy_path = entry.get("bfsae")
            name = entry.get("name", "unknown")
            if legacy_path and legacy_path != public_path:
                lines.append(f"- {name}: {public_path} (legacy: {legacy_path})")
            else:
                lines.append(f"- {name}: {public_path}")
        lines.append("")

    if fpqx_alignments:
        lines.append("## FPQx Alignments")
        lines.append("")
        for entry in fpqx_alignments:
            public_path = entry.get("akfpqx") or entry.get("bffpqx", "n/a")
            legacy_path = entry.get("bffpqx")
            stem = entry.get("stem", "unknown")
            if legacy_path and legacy_path != public_path:
                lines.append(f"- {stem}: {public_path} (legacy: {legacy_path})")
            else:
                lines.append(f"- {stem}: {public_path}")
        lines.append("")

    sig = m.get("signature", {})
    lines.append("## Signature")
    lines.append("")
    lines.append(f"- alg: {sig.get('alg', 'none')}")
    lines.append(f"- value: {sig.get('value', 'TODO')}")
    lines.append("")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"written: {args.out}")


if __name__ == "__main__":
    main()
