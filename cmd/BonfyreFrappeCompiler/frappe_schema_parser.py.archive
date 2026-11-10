#!/usr/bin/env python3
"""Extract Frappe DocType JSON into the compiler's portable schema IR.

The compiler consumes this small, deterministic JSON contract instead of a
live bench.  It intentionally reads only checked-in application metadata and
preserves source paths for later parity and migration work.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def source_doctypes(app_root: Path) -> list[dict]:
    doctypes: list[dict] = []
    for candidate in sorted(app_root.rglob("doctype/*/*.json")):
        if candidate.name in {"test_records.json", "test_*.json"}:
            continue
        try:
            raw = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(raw, dict) or not raw.get("doctype"):
            continue
        fields = []
        for position, field in enumerate(raw.get("fields", [])):
            if not isinstance(field, dict) or not field.get("fieldname"):
                continue
            entry = dict(field)
            entry["position"] = position
            fields.append(entry)
        rel_path = candidate.relative_to(app_root)
        doctypes.append({
            "name": raw.get("name") or raw["doctype"],
            "module": raw.get("module") or (rel_path.parts[1] if len(rel_path.parts) > 1 else app_root.name),
            "app": app_root.name,
            "is_submittable": raw.get("is_submittable", 0),
            "istable": raw.get("istable", 0),
            "is_single": raw.get("issingle", raw.get("is_single", 0)),
            "is_tree": raw.get("is_tree", 0),
            "title_field": raw.get("title_field"),
            "sort_field": raw.get("sort_field"),
            "sort_order": raw.get("sort_order"),
            "description": raw.get("description"),
            "documentation": raw.get("documentation"),
            "controller_path": str(rel_path.with_suffix(".py")),
            "source_path": str(rel_path),
            "fields": fields,
        })
    return doctypes


def relations(doctypes: list[dict]) -> list[dict]:
    result: list[dict] = []
    for doctype in doctypes:
        for field in doctype["fields"]:
            field_type = field.get("fieldtype")
            target = field.get("options")
            if field_type not in {"Link", "Dynamic Link", "Table", "Table MultiSelect"} or not target:
                continue
            result.append({
                "type": field_type.lower().replace(" ", "_"),
                "source_doctype": doctype["name"],
                "source_field": field["fieldname"],
                "target_doctype": target,
                "reference_field": field.get("parentfield"),
                "required": field.get("reqd", 0),
            })
    return result


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: frappe_schema_parser.py <app-path> <output-path>", file=sys.stderr)
        return 2
    app_path = Path(sys.argv[1]).resolve()
    output_path = Path(sys.argv[2])
    if not app_path.is_dir():
        print(f"app path is not a directory: {app_path}", file=sys.stderr)
        return 2
    doctypes = source_doctypes(app_path)
    payload = {"version": 1, "app": app_path.name, "doctypes": doctypes, "relations": relations(doctypes)}
    output_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
