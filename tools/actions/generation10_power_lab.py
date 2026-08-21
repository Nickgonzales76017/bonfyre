#!/usr/bin/env python3
"""Generation-10 native Power swarm runner.

The Power swarm deliberately does not make libquic-transport a global build
barrier. The QUIC/OpenSSL backend is exercised by the separate physical-fabric
lane, so a transport dependency cannot suppress evidence from the remaining
public Power estate.

A Power's Makefile owns its command-specific CFLAGS. Hosted-runner portability
must be additive and must never erase those private include paths, defines, or
compile contracts. The swarm therefore removes workflow CFLAGS/OPTFLAGS for
individual Powers and injects hosted-Linux feature visibility through CC.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = Path(__file__).with_name("generation10_contract.json")
SCHEMA = "bonfyre.generation10.action-receipt.v1"
POWER_PORTABILITY_FLAGS = ("-D_GNU_SOURCE", "-D_DEFAULT_SOURCE")


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=ROOT, text=True, capture_output=True)


def changed_paths(base: str | None, head: str | None) -> list[str]:
    if not base or not head:
        return []
    proc = git("diff", "--name-only", f"{base}...{head}")
    return sorted({line.strip() for line in proc.stdout.splitlines() if line.strip()}) if proc.returncode == 0 else []


def command_dirs() -> list[str]:
    root = ROOT / "cmd"
    if not root.is_dir():
        return []
    return sorted(
        p.name
        for p in root.iterdir()
        if p.is_dir() and p.name.startswith("Bonfyre") and (p / "Makefile").is_file()
    )


def manifest_observation(commands: list[str]) -> dict[str, Any]:
    contract = json.loads(CONTRACT.read_text())
    manifest_path = ROOT / "output" / "inventory" / "command_manifest.json"
    manifest_dirs: list[str] = []
    declared_count = None
    manifest_entry_count = None
    if manifest_path.is_file():
        try:
            value = json.loads(manifest_path.read_text())
            entries = value.get("commands", [])
            declared_count = value.get("command_count")
            manifest_entry_count = len(entries) if isinstance(entries, list) else None
            manifest_dirs = sorted(
                {str(item.get("command_dir")) for item in entries if isinstance(item, dict) and item.get("command_dir")}
            )
        except (OSError, json.JSONDecodeError):
            pass
    live = set(commands)
    manifest = set(manifest_dirs)
    return {
        "canonical_public_powers": contract["canonical_counts"]["public_powers"],
        "live_command_count": len(commands),
        "manifest_declared_command_count": declared_count,
        "manifest_entry_count": manifest_entry_count,
        "manifest_identity_count": len(manifest_dirs),
        "live_not_manifest": sorted(live - manifest),
        "manifest_not_live": sorted(manifest - live),
    }


def additive_cc(base: str) -> str:
    cc = base.strip() or "cc"
    for flag in POWER_PORTABILITY_FLAGS:
        if flag not in cc.split():
            cc = f"{cc} {flag}"
    return cc


def make_env(*, foundation_flags: str | None) -> dict[str, str]:
    env = os.environ.copy()
    env.update({
        "BONFYRE_CI": "1",
        "BONFYRE_CI_NO_EXTERNAL_EFFECTS": "1",
    })
    if foundation_flags is None:
        # The workflow carries hosted-runner CFLAGS. Remove them here because
        # `CFLAGS ?=` in each Power Makefile treats an environment variable as
        # already defined and would lose the Power's own -I/-D contract.
        env.pop("CFLAGS", None)
        env.pop("OPTFLAGS", None)
        # Hosted Linux feature-test visibility is additive through CC instead:
        # command-local CFLAGS, LOCAL_CFLAGS, OPTFLAGS, include paths and
        # linker contracts remain owned by the Power Makefile.
        env["CC"] = additive_cc(env.get("CC", "cc"))
    else:
        env["CFLAGS"] = foundation_flags
        env["OPTFLAGS"] = foundation_flags
    return env


def run_make(directory: Path, timeout: int, *, foundation_flags: str | None) -> dict[str, Any]:
    started = time.monotonic()
    env = make_env(foundation_flags=foundation_flags)
    try:
        proc = subprocess.run(
            ["make", "-C", str(directory), "-j2"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=env,
            timeout=timeout,
        )
        return {
            "path": directory.relative_to(ROOT).as_posix(),
            "exit_code": proc.returncode,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
            "stderr_sha256": hashlib.sha256(proc.stderr.encode()).hexdigest(),
            "stdout_tail": proc.stdout[-2500:],
            "stderr_tail": proc.stderr[-4000:],
            "outcome": "passed" if proc.returncode == 0 else "failed",
        }
    except subprocess.TimeoutExpired:
        return {
            "path": directory.relative_to(ROOT).as_posix(),
            "exit_code": None,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "stderr_tail": f"timeout after {timeout}s",
            "outcome": "timeout",
        }


def build_foundation(timeout: int) -> list[dict[str, Any]]:
    flags = "-O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE"
    return [
        run_make(ROOT / "lib" / "liblambda-tensors", timeout, foundation_flags=flags),
        run_make(ROOT / "lib" / "libbonfyre", timeout, foundation_flags=flags),
    ]


def build_power(name: str, timeout: int) -> dict[str, Any]:
    directory = ROOT / "cmd" / name
    result = run_make(directory, timeout, foundation_flags=None)
    result["command"] = name
    result["build_contract"] = "command_makefile_owned_with_additive_host_feature_visibility"
    result["host_feature_flags"] = list(POWER_PORTABILITY_FLAGS)
    result["built_executables"] = sorted(
        p.name
        for p in directory.iterdir()
        if p.is_file() and p.name != "Makefile" and os.access(p, os.X_OK)
    )
    return result


def classify_failure(item: dict[str, Any]) -> list[str]:
    text = f"{item.get('stdout_tail', '')}\n{item.get('stderr_tail', '')}".lower()
    classes: list[str] = []
    checks = (
        ("host_dependency", ("no such file or directory", "cannot find -l", "fatal error:")),
        ("platform_assumption", ("sys/sysctl.h", ".dylib", "__fp16")),
        ("link_contract", ("undefined reference", "dso missing from command line")),
        ("strict_c_feature_visibility", ("path_max", "clock_monotonic", "clock_realtime", "strtok_r", "strdup", "popen", "open_memstream", "m_pi")),
        ("compiler_warning_contract", ("all warnings being treated as errors", "[-werror=")),
    )
    for label, needles in checks:
        if any(needle in text for needle in needles):
            classes.append(label)
    return classes or ["unclassified_native_build_debt"]


def receipt_base(subject: str) -> dict[str, Any]:
    contract_sha = sha256_file(CONTRACT)
    return {
        "schema": SCHEMA,
        "kind": "power_shard",
        "subject": subject,
        "git_sha": os.environ.get("BONFYRE_SOURCE_SHA")
        or os.environ.get("GITHUB_SHA")
        or git("rev-parse", "HEAD").stdout.strip(),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "contract_sha256": contract_sha,
    }


def seal(receipt: dict[str, Any], out_dir: Path) -> Path:
    receipt["factor_validity"] = {
        "git_sha": receipt["git_sha"],
        "contract_sha256": receipt["contract_sha256"],
        "workflow": receipt.get("workflow"),
        "run_attempt": receipt.get("run_attempt"),
    }
    receipt["content_sha256"] = hashlib.sha256(canonical(receipt)).hexdigest()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{re.sub(r'[^A-Za-z0-9_.-]+', '-', receipt['subject'])}.json"
    path.write_bytes(canonical(receipt))
    return path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, required=True)
    ap.add_argument("--total", type=int, required=True)
    ap.add_argument("--scope", choices=["full", "changed", "inventory"], default="full")
    ap.add_argument("--base")
    ap.add_argument("--head")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--foundation-timeout", type=int, default=420)
    ap.add_argument("--output-dir", default=".generation10/receipts")
    args = ap.parse_args()

    commands = command_dirs()
    if args.scope == "changed":
        changed = set()
        for path in changed_paths(args.base, args.head):
            parts = PurePosixPath(path).parts
            if len(parts) >= 2 and parts[0] == "cmd" and parts[1].startswith("Bonfyre"):
                changed.add(parts[1])
        commands = sorted(set(commands) & changed)
    assigned = [name for index, name in enumerate(commands) if index % args.total == args.shard]

    foundation = build_foundation(args.foundation_timeout)
    foundation_ok = all(item["outcome"] == "passed" for item in foundation)
    results = [build_power(name, args.timeout) for name in assigned] if foundation_ok else []
    failures = [item for item in foundation + results if item["outcome"] != "passed"]
    for item in failures:
        item["failure_classes"] = classify_failure(item)

    receipt = receipt_base(f"powers-{args.shard}-of-{args.total}")
    receipt.update({
        "scope": args.scope,
        "inventory_observation": manifest_observation(command_dirs()),
        "commands_assigned": assigned,
        "core_foundation": foundation,
        "power_build_contract": "preserve each command Makefile's CFLAGS/OPTFLAGS; inject hosted GNU/default feature visibility additively through CC only",
        "host_portability_penetration": {
            "channel": "CC",
            "flags": list(POWER_PORTABILITY_FLAGS),
            "status": "diagnostic_penetration",
            "purpose": "remove strict-C hosted-Linux feature visibility noise without replacing command-owned build contracts",
        },
        "physical_fabric_split": {
            "libquic_transport": "separate Generation-10 physical-fabric lane",
            "reason": "a transport/backend dependency must not globally suppress public Power evidence",
        },
        "results": results,
        "failure_taxonomy": {
            item.get("command") or item.get("path", "unknown"): item.get("failure_classes", [])
            for item in failures
        },
        "outcome": "failed" if failures else "passed",
    })
    path = seal(receipt, Path(args.output_dir))
    print(path)
    if failures:
        print(
            json.dumps(
                {
                    "power_shard": f"{args.shard}/{args.total}",
                    "failed": [
                        {
                            "subject": item.get("command") or item.get("path"),
                            "exit_code": item.get("exit_code"),
                            "failure_classes": item.get("failure_classes", []),
                            "stderr_tail": item.get("stderr_tail", "")[-1400:],
                        }
                        for item in failures
                    ],
                },
                indent=2,
            ),
            file=os.sys.stderr,
        )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
