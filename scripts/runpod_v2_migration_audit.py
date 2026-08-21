#!/usr/bin/env python3
"""Audit a repository for Runpod REST v1 / GraphQL migration obligations.

The scanner is credential-free and read-only. It searches only Git-tracked
regular files by default so build products, caches, extracted archives, and
local secrets do not inflate the migration frontier.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

SCHEMA = "bonfyre.runpod-v2-migration-audit.v1"

RULES = (
    ("graphql_endpoint", re.compile(rb"https://api\.runpod\.io/graphql", re.I), "graphql", "REPLACE"),
    ("graphql_query", re.compile(rb"\b(query|mutation)\s+[A-Za-z_][A-Za-z0-9_]*\s*(?:\(|\{)", re.I), "graphql", "REVIEW"),
    ("rest_v1_url", re.compile(rb"https://(?:api|rest)\.runpod\.io/(?:v1|v1/)[^\s\"']*", re.I), "rest_v1", "REPLACE"),
    ("runpod_v1_literal", re.compile(rb"\bRunpod\b.{0,80}\b(?:REST\s+)?v1\b", re.I | re.S), "rest_v1", "REVIEW"),
    ("runpod_graphql_literal", re.compile(rb"\bRunpod\b.{0,80}\bGraphQL\b", re.I | re.S), "graphql", "REVIEW"),
)

TEXT_SUFFIXES = {
    ".py", ".js", ".ts", ".tsx", ".jsx", ".rs", ".go", ".c", ".h", ".hpp",
    ".sh", ".bash", ".zsh", ".fish", ".pl", ".pm", ".rb", ".java", ".kt",
    ".json", ".jsonl", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf",
    ".md", ".mdx", ".txt", ".rst", ".html", ".css", ".xml", ".sql",
}
TEXT_NAMES = {"Dockerfile", "Makefile", "Justfile", "Procfile", "CLAUDE.md", "AGENTS.md"}
MAX_FILE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    rule: str
    legacy_family: str
    disposition: str
    evidence_sha256: str


def tracked_files(repo: Path) -> list[Path]:
    cp = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    out = []
    for raw in cp.stdout.split(b"\0"):
        if not raw:
            continue
        rel = raw.decode("utf-8", errors="surrogateescape")
        path = repo / rel
        if path.is_file() and not path.is_symlink():
            out.append(path)
    return out


def candidate(path: Path) -> bool:
    if path.name in TEXT_NAMES:
        return True
    if path.suffix.lower() in TEXT_SUFFIXES:
        return True
    return path.name.endswith((".env.example", ".example"))


def line_for(data: bytes, offset: int) -> int:
    return data.count(b"\n", 0, offset) + 1


def evidence_digest(rule: str, path: str, line: int, match: bytes) -> str:
    # Bind the observation without persisting the matched bytes. A legacy URL
    # may contain a query string or copied credential and must not be echoed.
    h = hashlib.sha256()
    h.update(rule.encode())
    h.update(b"\0")
    h.update(path.encode("utf-8", errors="surrogateescape"))
    h.update(b"\0")
    h.update(str(line).encode())
    h.update(b"\0")
    h.update(hashlib.sha256(match).digest())
    return h.hexdigest()


def scan(repo: Path, paths: Iterable[Path] | None = None) -> dict:
    findings: list[Finding] = []
    skipped_large = []
    scanned = 0
    source_paths = paths if paths is not None else tracked_files(repo)
    for path in source_paths:
        try:
            rel = path.relative_to(repo).as_posix()
        except ValueError:
            continue
        if not candidate(path):
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size > MAX_FILE_BYTES:
            skipped_large.append({"path": rel, "bytes": size})
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        if b"\0" in data[:4096]:
            continue
        scanned += 1
        for rule, pattern, family, disposition in RULES:
            for match in pattern.finditer(data):
                line = line_for(data, match.start())
                findings.append(
                    Finding(
                        path=rel,
                        line=line,
                        rule=rule,
                        legacy_family=family,
                        disposition=disposition,
                        evidence_sha256=evidence_digest(rule, rel, line, match.group(0)),
                    )
                )
    findings.sort(key=lambda finding: (finding.path, finding.line, finding.rule))
    families = {}
    for finding in findings:
        families[finding.legacy_family] = families.get(finding.legacy_family, 0) + 1
    return {
        "schema": SCHEMA,
        "repo": str(repo),
        "scanned_text_files": scanned,
        "finding_count": len(findings),
        "legacy_families": families,
        "findings": [asdict(finding) for finding in findings],
        "skipped_large": skipped_large,
        "migration_state": "CLEAN" if not findings else "OBLIGATIONS_PRESENT",
        "deadlines": {
            "v1_rate_limit_staging": "2026-09-17",
            "v1_operational_retirement_bound": "2026-11-15",
            "graphql_retirement": "early 2027",
        },
        "rules": {
            "REPLACE": "direct legacy endpoint/reference requiring a v2 replacement",
            "REVIEW": "legacy-family semantic reference requiring mapping before mutation",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("repo", nargs="?", default=".")
    parser.add_argument("--output")
    args = parser.parse_args()
    repo = Path(args.repo).resolve()
    result = scan(repo)
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 1 if result["findings"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
