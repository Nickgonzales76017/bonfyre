#!/usr/bin/env python3
"""Audit tracked Gitlinks against the repository's .gitmodules contract.

A Git mode-160000 entry without a matching submodule declaration is a distinct
repository state: Git still treats the path as a submodule even if the working
copy looks like ordinary archaeology. Tools such as actions/checkout may invoke
submodule-aware cleanup paths and fail before the requested operation begins.

This command is observation-only. It never initializes, deinitializes, deletes,
or rewrites a submodule. Findings are intended for WorkGraph / Run8 repo-shape
classification before any semantic deletion decision.
"""
from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

SCHEMA = "bonfyre.gitlink-contract-audit.v1"
GITLINK_MODE = "160000"


class AuditError(RuntimeError):
    pass


@dataclass(frozen=True)
class ModuleDeclaration:
    name: str
    path: str
    url_present: bool


@dataclass(frozen=True)
class Finding:
    kind: str
    path: str
    path_sha256: str
    target_sha: str | None = None
    declaration: str | None = None


def _path_digest(path: str) -> str:
    return hashlib.sha256(path.encode("utf-8", errors="surrogateescape")).hexdigest()


def _canonical_repo_path(value: str) -> str | None:
    candidate = value.replace("\\", "/")
    if not candidate or candidate.startswith("/"):
        return None
    parts = candidate.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return None
    path = PurePosixPath(candidate)
    if path.is_absolute():
        return None
    return path.as_posix()


def parse_stage_records(raw: bytes) -> dict[str, str]:
    """Return repository-relative Gitlink path -> target commit SHA."""
    gitlinks: dict[str, str] = {}
    for record in raw.split(b"\0"):
        if not record:
            continue
        try:
            metadata, raw_path = record.split(b"\t", 1)
            mode, sha, stage = metadata.split(b" ", 2)
        except ValueError as exc:
            raise AuditError("git ls-files --stage returned a malformed record") from exc
        if mode != GITLINK_MODE.encode() or stage != b"0":
            continue
        path = raw_path.decode("utf-8", errors="surrogateescape")
        gitlinks[path] = sha.decode("ascii", errors="strict")
    return gitlinks


def parse_gitmodules(text: str) -> tuple[list[ModuleDeclaration], list[Finding]]:
    parser = configparser.RawConfigParser(interpolation=None, strict=False)
    try:
        parser.read_string(text)
    except configparser.Error as exc:
        raise AuditError(".gitmodules is malformed") from exc

    declarations: list[ModuleDeclaration] = []
    findings: list[Finding] = []
    for section in parser.sections():
        if not section.startswith("submodule "):
            continue
        raw_name = section[len("submodule ") :].strip()
        if len(raw_name) >= 2 and raw_name[0] == raw_name[-1] == '"':
            name = raw_name[1:-1]
        else:
            name = raw_name
        if not parser.has_option(section, "path"):
            synthetic = f"<section:{name}>"
            findings.append(
                Finding(
                    kind="declaration_missing_path",
                    path=synthetic,
                    path_sha256=_path_digest(synthetic),
                    declaration=name,
                )
            )
            continue
        raw_path = parser.get(section, "path", raw=True)
        canonical = _canonical_repo_path(raw_path)
        if canonical is None:
            findings.append(
                Finding(
                    kind="invalid_declaration_path",
                    path=raw_path,
                    path_sha256=_path_digest(raw_path),
                    declaration=name,
                )
            )
            continue
        declarations.append(
            ModuleDeclaration(
                name=name,
                path=canonical,
                url_present=parser.has_option(section, "url")
                and bool(parser.get(section, "url", raw=True).strip()),
            )
        )
    return declarations, findings


def audit_from_records(
    gitlinks: dict[str, str],
    declarations: Iterable[ModuleDeclaration],
    initial_findings: Iterable[Finding] = (),
) -> dict:
    findings = list(initial_findings)
    by_path: dict[str, list[ModuleDeclaration]] = {}
    for declaration in declarations:
        by_path.setdefault(declaration.path, []).append(declaration)

    for path, matches in sorted(by_path.items()):
        if len(matches) > 1:
            findings.append(
                Finding(
                    kind="duplicate_declaration_path",
                    path=path,
                    path_sha256=_path_digest(path),
                    declaration=",".join(sorted(item.name for item in matches)),
                )
            )
        if path not in gitlinks:
            findings.append(
                Finding(
                    kind="declaration_without_gitlink",
                    path=path,
                    path_sha256=_path_digest(path),
                    declaration=matches[0].name,
                )
            )
        if any(not item.url_present for item in matches):
            findings.append(
                Finding(
                    kind="declaration_missing_url",
                    path=path,
                    path_sha256=_path_digest(path),
                    declaration=matches[0].name,
                )
            )

    for raw_path, target_sha in sorted(gitlinks.items()):
        canonical = _canonical_repo_path(raw_path)
        display_path = canonical if canonical is not None else raw_path
        if canonical is None:
            findings.append(
                Finding(
                    kind="invalid_gitlink_path",
                    path=display_path,
                    path_sha256=_path_digest(display_path),
                    target_sha=target_sha,
                )
            )
            continue
        if canonical not in by_path:
            findings.append(
                Finding(
                    kind="orphan_gitlink",
                    path=canonical,
                    path_sha256=_path_digest(canonical),
                    target_sha=target_sha,
                )
            )

    findings.sort(key=lambda item: (item.kind, item.path, item.declaration or ""))
    kinds: dict[str, int] = {}
    for finding in findings:
        kinds[finding.kind] = kinds.get(finding.kind, 0) + 1
    return {
        "schema": SCHEMA,
        "ok": not findings,
        "gitlink_count": len(gitlinks),
        "declaration_count": sum(len(items) for items in by_path.values()),
        "finding_count": len(findings),
        "finding_kinds": kinds,
        "findings": [asdict(item) for item in findings],
        "mutation_performed": False,
    }


def audit(repo: Path) -> dict:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), "ls-files", "--stage", "-z"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AuditError("unable to read tracked Git index") from exc
    gitlinks = parse_stage_records(completed.stdout)

    modules_path = repo / ".gitmodules"
    if modules_path.exists():
        try:
            text = modules_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise AuditError("unable to read .gitmodules") from exc
        declarations, initial = parse_gitmodules(text)
    else:
        declarations, initial = [], []
    result = audit_from_records(gitlinks, declarations, initial)
    result["repo"] = str(repo.resolve())
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("repo", nargs="?", default=".")
    parser.add_argument("--output")
    parser.add_argument(
        "--fail-on-findings",
        action="store_true",
        help="return 1 when contract drift is observed; default is observation-only",
    )
    args = parser.parse_args()
    try:
        result = audit(Path(args.repo))
    except AuditError as exc:
        result = {"schema": SCHEMA, "ok": False, "audit_error": str(exc), "mutation_performed": False}
        rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            Path(args.output).write_text(rendered, encoding="utf-8")
        else:
            print(rendered, end="")
        return 2
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    if args.fail_on_findings and not result["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
