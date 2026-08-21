#!/usr/bin/env python3
"""Generation-10 physical fabric probe for QUIC/OpenDAL/Iroh-adjacent transport debt."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = Path(__file__).with_name("generation10_contract.json")
SCHEMA = "bonfyre.generation10.action-receipt.v1"


def canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pkg(name: str) -> dict[str, Any]:
    exists = subprocess.run(["pkg-config", "--exists", name]).returncode == 0
    version = None
    if exists:
        proc = subprocess.run(["pkg-config", "--modversion", name], text=True, capture_output=True)
        if proc.returncode == 0:
            version = proc.stdout.strip()
    return {"name": name, "exists": exists, "version": version}


def build_quic() -> dict[str, Any]:
    started = time.monotonic()
    env = os.environ.copy()
    env.update({"CFLAGS": "-O2 -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE"})
    proc = subprocess.run(
        ["make", "-C", str(ROOT / "lib" / "libquic-transport"), "-j2"],
        cwd=ROOT, text=True, capture_output=True, env=env
    )
    return {
        "exit_code": proc.returncode,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(proc.stderr.encode()).hexdigest(),
        "stderr_tail": proc.stderr[-4000:],
        "outcome": "passed" if proc.returncode == 0 else "failed",
    }


def main() -> int:
    source = ROOT / "lib" / "libquic-transport" / "bf_quic.c"
    makefile = ROOT / "lib" / "libquic-transport" / "Makefile"
    source_text = source.read_text(encoding="utf-8") if source.is_file() else ""
    make_text = makefile.read_text(encoding="utf-8") if makefile.is_file() else ""
    packages = {
        name: pkg(name)
        for name in ("ngtcp2", "ngtcp2_crypto_ossl", "ngtcp2_crypto_gnutls")
    }
    source_requires_ossl = "ngtcp2_crypto_ossl.h" in source_text
    make_requires_ossl = "ngtcp2_crypto_ossl" in make_text
    gaps: list[str] = []
    if source_requires_ossl and not packages["ngtcp2_crypto_ossl"]["exists"]:
        gaps.append("source_requires_ngtcp2_crypto_ossl_but_runner_backend_absent")
    if make_requires_ossl and not packages["ngtcp2_crypto_ossl"]["exists"]:
        gaps.append("makefile_requires_ngtcp2_crypto_ossl_but_runner_backend_absent")
    if packages["ngtcp2_crypto_gnutls"]["exists"] and not packages["ngtcp2_crypto_ossl"]["exists"]:
        gaps.append("runner_has_gnutls_backend_but_source_is_ossl_specific")

    build = None
    if packages["ngtcp2"]["exists"] and packages["ngtcp2_crypto_ossl"]["exists"]:
        build = build_quic()
    outcome = "coverage_debt" if gaps else (build["outcome"] if build else "coverage_debt")

    contract_sha = digest(CONTRACT)
    receipt: dict[str, Any] = {
        "schema": SCHEMA,
        "kind": "physical_fabric",
        "subject": "quic-backend-portability",
        "git_sha": os.environ.get("BONFYRE_SOURCE_SHA") or os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "contract_sha256": contract_sha,
        "owner": "IrohQUIC",
        "packages": packages,
        "source_requires_ossl": source_requires_ossl,
        "make_requires_ossl": make_requires_ossl,
        "source_sha256": digest(source) if source.is_file() else None,
        "makefile_sha256": digest(makefile) if makefile.is_file() else None,
        "portability_gaps": gaps,
        "build": build,
        "outcome": outcome,
    }
    receipt["factor_validity"] = {
        "git_sha": receipt["git_sha"],
        "contract_sha256": contract_sha,
        "workflow": receipt["workflow"],
        "run_attempt": receipt["run_attempt"],
    }
    receipt["content_sha256"] = hashlib.sha256(canonical(receipt)).hexdigest()
    out = ROOT / ".generation10" / "receipts" / "physical-fabric" / "quic-backend-portability.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(canonical(receipt))
    print(out)
    print(json.dumps({"outcome": outcome, "portability_gaps": gaps, "packages": packages}, indent=2))
    return 1 if outcome == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
