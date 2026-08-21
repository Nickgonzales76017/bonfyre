#!/usr/bin/env python3
"""Generation-10 WorkGraph lifecycle-chaos runner.

Baseline executions are isolated and verdict-bearing. A second evidence-only
pass deliberately reuses one durable state directory and reverses order to
surface namespace/order coupling without confusing test-assumption debt with a
baseline regression.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = Path(__file__).with_name("generation10_contract.json")
SCHEMA = "bonfyre.generation10.action-receipt.v1"

SCENARIOS = {
    "lease-reap-retry": [
        "tests/requirements/workgraph_lease.sh",
        "tests/requirements/workgraph_reap_multi_mission.sh",
        "tests/requirements/workgraph_retry.sh",
    ],
    "restart-migration": [
        "tests/requirements/workgraph_restart.sh",
        "tests/requirements/workgraph_migration.sh",
        "tests/requirements/daemon_restart.sh",
    ],
    "fanout-race": [
        "tests/requirements/workgraph_fanout.sh",
        "tests/requirements/workgraph_race.sh",
    ],
    "cancel-compensate": [
        "tests/requirements/workgraph_cancel.sh",
        "tests/requirements/workgraph_compensation.sh",
    ],
    "effect-completion": [
        "tests/requirements/workgraph_effect_adapters.sh",
        "tests/requirements/completion_controller.sh",
        "tests/requirements/command_completion.sh",
    ],
}


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value)


def run_test(path: str, state_dir: Path, timeout: int, phase: str) -> dict[str, Any]:
    full = ROOT / path
    state_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update({
        "BONFYRE_STATE_DIR": str(state_dir),
        "BONFYRE_CI": "1",
        "BONFYRE_CI_NO_EXTERNAL_EFFECTS": "1",
        "BONFYRE_AUTHORITY": "observe",
        "NO_COLOR": "1",
        "CFLAGS": "-O2 -Wall -Wextra -Wno-error=format-truncation -std=c11 -D_DEFAULT_SOURCE",
        "CC": "cc -include stdint.h",
    })
    started = time.monotonic()
    try:
        proc = subprocess.run(
            ["bash", str(full)],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=env,
            timeout=timeout,
        )
        return {
            "phase": phase,
            "path": path,
            "state_dir": state_dir.relative_to(ROOT).as_posix(),
            "exit_code": proc.returncode,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
            "stderr_sha256": hashlib.sha256(proc.stderr.encode()).hexdigest(),
            "stdout_tail": proc.stdout[-3000:],
            "stderr_tail": proc.stderr[-3000:],
            "outcome": "passed" if proc.returncode == 0 else "failed",
        }
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        return {
            "phase": phase,
            "path": path,
            "state_dir": state_dir.relative_to(ROOT).as_posix(),
            "exit_code": None,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "stdout_sha256": hashlib.sha256(stdout.encode()).hexdigest(),
            "stderr_sha256": hashlib.sha256(stderr.encode()).hexdigest(),
            "stdout_tail": stdout[-3000:],
            "stderr_tail": stderr[-3000:],
            "outcome": "timeout",
        }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=sorted(SCENARIOS), required=True)
    ap.add_argument("--timeout", type=int, default=300)
    ap.add_argument("--output-dir", default=".generation10/receipts/workgraph-chaos")
    args = ap.parse_args()

    tests = SCENARIOS[args.scenario]
    root = ROOT / ".generation10" / "state" / "workgraph-chaos" / safe_name(args.scenario)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    baseline: list[dict[str, Any]] = []
    for index, test in enumerate(tests):
        isolated = root / "isolated" / f"{index:02d}-{safe_name(Path(test).stem)}"
        baseline.append(run_test(test, isolated, args.timeout, "isolated-baseline"))

    # Evidence-only interference probe. Reuse one durable state and reverse the
    # sequence. Fixed IDs inside legacy requirement tests may make this red; that
    # is recorded as order/state coupling evidence, not a baseline verdict.
    shared = root / "shared-reverse"
    stress: list[dict[str, Any]] = []
    for test in reversed(tests):
        stress.append(run_test(test, shared, args.timeout, "shared-state-reverse"))

    baseline_failures = [item for item in baseline if item["outcome"] != "passed"]
    stress_failures = [item for item in stress if item["outcome"] != "passed"]
    order_sensitive = bool(stress_failures) and not baseline_failures

    contract_sha = sha(CONTRACT)
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": "workgraph_chaos",
        "subject": args.scenario,
        "git_sha": os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "contract_sha256": contract_sha,
        "semantic_owner": "WorkGraph",
        "tests": tests,
        "baseline": baseline,
        "stress": stress,
        "baseline_failure_count": len(baseline_failures),
        "stress_failure_count": len(stress_failures),
        "order_or_shared_state_sensitivity": order_sensitive,
        "stress_verdict_policy": "evidence_only",
        "outcome": "failed" if baseline_failures else "passed",
    }
    receipt["factor_validity"] = {
        "git_sha": receipt["git_sha"],
        "contract_sha256": contract_sha,
        "workflow": receipt["workflow"],
        "run_attempt": receipt["run_attempt"],
    }
    receipt["content_sha256"] = hashlib.sha256(canonical(receipt)).hexdigest()
    out = ROOT / args.output_dir / f"{safe_name(args.scenario)}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(canonical(receipt))
    print(out)
    print(json.dumps({
        "scenario": args.scenario,
        "baseline_failure_count": len(baseline_failures),
        "stress_failure_count": len(stress_failures),
        "order_or_shared_state_sensitivity": order_sensitive,
        "outcome": receipt["outcome"],
        "baseline_failures": [
            {"path": item["path"], "exit_code": item["exit_code"], "stderr_tail": item["stderr_tail"][-1000:]}
            for item in baseline_failures
        ],
    }, indent=2))
    return 1 if baseline_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
