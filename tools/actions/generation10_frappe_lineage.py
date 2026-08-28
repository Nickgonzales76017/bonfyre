#!/usr/bin/env python3
"""Generation-10 per-lineage Frappe projection probe."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
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


def gitlinks() -> list[str]:
    proc = git("ls-files", "-s")
    if proc.returncode != 0:
        return []
    out = []
    for line in proc.stdout.splitlines():
        match = re.match(r"^160000 [0-9a-f]{40} \d\t(.+)$", line)
        if match:
            out.append(match.group(1))
    return sorted(out)


def declared_submodules() -> list[str]:
    path = ROOT / ".gitmodules"
    if not path.is_file():
        return []
    return sorted(set(re.findall(r"^\s*path\s*=\s*(.+?)\s*$", path.read_text(encoding="utf-8", errors="replace"), re.M)))


def run(argv: list[str], timeout: int = 300) -> dict[str, Any]:
    started = time.monotonic()
    env = os.environ.copy()
    env.update({
        "CFLAGS": "-O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE",
        "OPTFLAGS": "-O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE",
        "BONFYRE_CI": "1",
        "BONFYRE_CI_NO_EXTERNAL_EFFECTS": "1",
    })
    try:
        proc = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, env=env, timeout=timeout)
        return {
            "argv": argv,
            "exit_code": proc.returncode,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
            "stderr_sha256": hashlib.sha256(proc.stderr.encode()).hexdigest(),
            "stdout_tail": proc.stdout[-2500:],
            "stderr_tail": proc.stderr[-2500:],
            "outcome": "passed" if proc.returncode == 0 else "failed",
        }
    except subprocess.TimeoutExpired:
        return {"argv": argv, "exit_code": None, "duration_ms": int((time.monotonic() - started) * 1000), "outcome": "timeout"}


def seal(receipt: dict[str, Any], path: Path) -> None:
    receipt["factor_validity"] = {
        "git_sha": receipt["git_sha"],
        "contract_sha256": receipt["contract_sha256"],
        "workflow": receipt.get("workflow"),
        "run_attempt": receipt.get("run_attempt"),
    }
    receipt["content_sha256"] = hashlib.sha256(canonical(receipt)).hexdigest()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical(receipt))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lineage", required=True)
    ap.add_argument("--output-dir", default=".generation10/receipts/frappe-lineage")
    args = ap.parse_args()

    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    canonical_names = contract["frappe_lineages"]
    lookup = {name.lower(): name for name in canonical_names}
    key = args.lineage.lower()
    if key not in lookup:
        raise SystemExit(f"unknown Frappe lineage: {args.lineage}")
    lineage = lookup[key]
    slug = lineage.lower()
    expected = ROOT / "integrations" / "frappe-bench" / "apps" / slug
    expected_rel = expected.relative_to(ROOT).as_posix()
    links = gitlinks()
    declared = declared_submodules()
    blockers = sorted(link for link in links if expected_rel == link or expected_rel.startswith(link + "/"))
    tracked = git("ls-files", expected_rel).stdout.splitlines()
    materialized = expected.is_dir() and any(expected.iterdir()) and not blockers

    contract_sha = sha(CONTRACT)
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": "frappe_lineage",
        "subject": f"frappe-{slug}",
        "git_sha": os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "contract_sha256": contract_sha,
        "lineage": lineage,
        "expected_path": expected_rel,
        "tracked_entries": len(tracked),
        "working_tree_exists": expected.exists(),
        "blocking_gitlinks": blockers,
        "blocking_gitlinks_declared": {link: link in declared for link in blockers},
        "materialized": materialized,
        "build": [],
        "app_pack": None,
    }

    if not materialized:
        receipt["outcome"] = "coverage_debt"
        receipt["coverage_debt"] = "lineage source is not materialized in the exact hosted checkout"
        out = ROOT / args.output_dir / f"frappe-{slug}.json"
        seal(receipt, out)
        print(out)
        return 0

    builds = [
        run(["make", "-C", str(ROOT / "lib" / "liblambda-tensors"), "-j2"]),
        run(["make", "-C", str(ROOT / "lib" / "libbonfyre"), "-j2"]),
        run(["make", "-C", str(ROOT / "cmd" / "BonfyreFrappeCompiler"), "-j2"]),
    ]
    receipt["build"] = builds
    if any(item["outcome"] != "passed" for item in builds):
        receipt["outcome"] = "failed"
        out = ROOT / args.output_dir / f"frappe-{slug}.json"
        seal(receipt, out)
        print(out)
        return 1

    compiler = ROOT / "cmd" / "BonfyreFrappeCompiler" / "bonfyre-frappe-compiler"
    tmp = Path(tempfile.mkdtemp(prefix=f"g10-frappe-{slug}-"))
    try:
        pack = tmp / f"{slug}.apppack.json"
        invocation = run([
            str(compiler), "--phase", "1", "-r", str(ROOT), "--emit-pack", str(pack), str(expected)
        ], timeout=420)
        pack_result: dict[str, Any] = {"invocation": invocation, "path": str(pack)}
        if invocation["outcome"] == "passed" and pack.is_file():
            try:
                value = json.loads(pack.read_text(encoding="utf-8"))
                pack_result.update({
                    "kind": value.get("kind"),
                    "source_revision": value.get("source_revision"),
                    "sha256": sha(pack),
                    "valid": value.get("kind") == "AppPack" and bool(value.get("source_revision")),
                })
            except json.JSONDecodeError:
                pack_result["valid"] = False
        else:
            pack_result["valid"] = False
        receipt["app_pack"] = pack_result
        receipt["outcome"] = "passed" if pack_result.get("valid") else "failed"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    out = ROOT / args.output_dir / f"frappe-{slug}.json"
    seal(receipt, out)
    print(out)
    return 0 if receipt["outcome"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
