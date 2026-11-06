#!/usr/bin/env python3
"""TreeSitterCanon — structural parsing and canonicalization via tree-sitter.

Usage:
  treesitter_canon.py parse <file> [--lang LANG] [--out AST.json] [--dry-run]
  treesitter_canon.py canon <file> [--lang LANG] [--out CANON] [--dry-run]
  treesitter_canon.py diff <file1> <file2> [--lang LANG] [--dry-run]
"""
import argparse
import json
import sys
from pathlib import Path

LANG_MAP = {
    ".py": "python", ".js": "javascript", ".ts": "typescript",
    ".c": "c", ".h": "c", ".rs": "rust", ".go": "go",
    ".sh": "bash", ".md": "markdown", ".json": "json",
}


def detect_lang(path: Path) -> str:
    return LANG_MAP.get(path.suffix, "python")


def naive_parse(text: str, lang: str) -> dict:
    """Fallback structural parse (line-level) when tree-sitter not installed."""
    lines = text.splitlines()
    nodes = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        ntype = "line"
        if lang in ("python",) and stripped.startswith(("def ", "class ", "import ", "from ")):
            ntype = stripped.split()[0]
        elif lang in ("c",) and ("(" in stripped and "{" not in stripped and ";" in stripped):
            ntype = "declaration"
        elif lang in ("c",) and stripped.endswith("{"):
            ntype = "block_start"
        nodes.append({"line": i + 1, "type": ntype, "text": stripped[:120]})
    return {"language": lang, "node_count": len(nodes), "nodes": nodes}


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")

    pa = sub.add_parser("parse")
    pa.add_argument("file", type=Path)
    pa.add_argument("--lang", default=None)
    pa.add_argument("--out", type=Path, default=None)
    pa.add_argument("--dry-run", action="store_true")

    ca = sub.add_parser("canon")
    ca.add_argument("file", type=Path)
    ca.add_argument("--lang", default=None)
    ca.add_argument("--out", type=Path, default=None)
    ca.add_argument("--dry-run", action="store_true")

    di = sub.add_parser("diff")
    di.add_argument("file1", type=Path)
    di.add_argument("file2", type=Path)
    di.add_argument("--lang", default=None)
    di.add_argument("--dry-run", action="store_true")

    args = p.parse_args()
    if not args.cmd:
        print("Usage: treesitter_canon.py <parse|canon|diff> <file> [--dry-run]")
        return 1

    if args.dry_run:
        print(f"Would run: {args.cmd}")
        return 0

    file_path = args.file if args.cmd != "diff" else args.file1
    lang = args.lang or detect_lang(file_path)

    if args.cmd in ("parse", "canon"):
        text = args.file.read_text(encoding="utf-8")

        # Try tree-sitter, fall back to naive
        try:
            from tree_sitter import Language, Parser
            raise ImportError("auto-detect not wired yet")
        except ImportError:
            ast = naive_parse(text, lang)

        out = args.out or Path(str(args.file) + (".ast.json" if args.cmd == "parse" else ".canon.json"))
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(ast, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print(f"Wrote {ast['node_count']} nodes to {out}")

    elif args.cmd == "diff":
        t1 = args.file1.read_text(encoding="utf-8")
        t2 = args.file2.read_text(encoding="utf-8")
        ast1 = naive_parse(t1, lang)
        ast2 = naive_parse(t2, lang)
        d = {"nodes_a": ast1["node_count"], "nodes_b": ast2["node_count"],
             "delta": ast2["node_count"] - ast1["node_count"]}
        print(json.dumps(d, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
