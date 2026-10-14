#!/usr/bin/env python3
"""Generation-10 physical-fabric portability probe.

QUIC is intentionally observed as its own physical owner. The probe distinguishes
package-manager installation, headers, pkg-config metadata, backend choice, and
actual compilation so a naming mismatch cannot masquerade as an absent library.
"""
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


def cmd(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, text=True, capture_output=True)


def pkg(name: str) -> dict[str, Any]:
    exists = cmd(["pkg-config", "--exists", name]).returncode == 0
    version = None
    if exists:
        proc = cmd(["pkg-config", "--modversion", name])
        if proc.returncode == 0:
            version = proc.stdout.strip()
    return {"name": name, "exists": exists, "version": version}


def pkg_modules() -> list[str]:
    proc = cmd(["pkg-config", "--list-all"])
    if proc.returncode != 0:
        return []
    return sorted(
        line.split(None, 1)[0]
        for line in proc.stdout.splitlines()
        if line.strip() and "ngtcp2" in line.lower()
    )


def header(path: str) -> dict[str, Any]:
    candidates = [
        Path("/usr/include") / path,
        Path("/usr/local/include") / path,
        Path("/opt/homebrew/include") / path,
    ]
    found = [p.as_posix() for p in candidates if p.is_file()]
    return {"header": path, "exists": bool(found), "paths": found}


def dpkg_packages() -> list[str]:
    proc = cmd(["dpkg-query", "-W", "-f=${Package}\t${Version}\n", "*ngtcp2*"])
    if proc.returncode != 0:
        return []
    return sorted(line.strip() for line in proc.stdout.splitlines() if line.strip())


def build_quic() -> dict[str, Any]:
    started = time.monotonic()
    env = os.environ.copy()
    env.pop("CFLAGS", None)
    env.pop("OPTFLAGS", None)
    proc = subprocess.run(
        ["make", "-C", str(ROOT / "lib" / "libquic-transport"), "-j2"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        env=env,
    )
    return {
        "exit_code": proc.returncode,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "stdout_sha256": hashlib.sha256(proc.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(proc.stderr.encode()).hexdigest(),
        "stdout_tail": proc.stdout[-2500:],
        "stderr_tail": proc.stderr[-4000:],
        "outcome": "passed" if proc.returncode == 0 else "failed",
    }


def main() -> int:
    source = ROOT / "lib" / "libquic-transport" / "bf_quic.c"
    makefile = ROOT / "lib" / "libquic-transport" / "Makefile"
    source_text = source.read_text(encoding="utf-8") if source.is_file() else ""
    make_text = makefile.read_text(encoding="utf-8") if makefile.is_file() else ""

    package_probes = {
        name: pkg(name)
        for name in (
            "ngtcp2",
            "libngtcp2",
            "ngtcp2_crypto_ossl",
            "libngtcp2_crypto_ossl",
            "ngtcp2_crypto_gnutls",
            "libngtcp2_crypto_gnutls",
        )
    }
    headers = {
        name: header(name)
        for name in (
            "ngtcp2/ngtcp2.h",
            "ngtcp2/ngtcp2_crypto.h",
            "ngtcp2/ngtcp2_crypto_ossl.h",
            "ngtcp2/ngtcp2_crypto_gnutls.h",
        )
    }
    modules = pkg_modules()
    installed = dpkg_packages()

    source_requires_ossl = "ngtcp2_crypto_ossl.h" in source_text
    source_knows_gnutls = "ngtcp2_crypto_gnutls.h" in source_text
    make_requires_ossl = "ngtcp2_crypto_ossl" in make_text

    base_header = headers["ngtcp2/ngtcp2.h"]["exists"]
    ossl_header = headers["ngtcp2/ngtcp2_crypto_ossl.h"]["exists"]
    gnutls_header = headers["ngtcp2/ngtcp2_crypto_gnutls.h"]["exists"]

    gaps: list[str] = []
    if not base_header:
        gaps.append("ngtcp2_base_header_absent")
    if source_requires_ossl and not ossl_header:
        gaps.append("source_requires_ngtcp2_crypto_ossl_but_runner_header_absent")
    if make_requires_ossl and not any("ossl" in name for name in modules):
        gaps.append("makefile_requires_ngtcp2_crypto_ossl_but_pkgconfig_backend_absent")
    if gnutls_header and not ossl_header and source_requires_ossl:
        gaps.append("runner_has_gnutls_backend_but_source_is_ossl_specific")
    if source_knows_gnutls is False and gnutls_header:
        gaps.append("source_has_no_gnutls_backend_path")

    can_attempt = base_header and ossl_header
    build = build_quic() if can_attempt else None
    if build and build["outcome"] != "passed":
        gaps.append("quic_build_failed_with_required_headers_present")

    outcome = "passed" if can_attempt and build and build["outcome"] == "passed" and not gaps else "coverage_debt"

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
        "dpkg_packages": installed,
        "pkg_config_modules": modules,
        "pkg_config_probes": package_probes,
        "headers": headers,
        "source_requires_ossl": source_requires_ossl,
        "source_knows_gnutls": source_knows_gnutls,
        "make_requires_ossl": make_requires_ossl,
        "source_sha256": digest(source) if source.is_file() else None,
        "makefile_sha256": digest(makefile) if makefile.is_file() else None,
        "portability_gaps": sorted(set(gaps)),
        "build_attempted": can_attempt,
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
    print(
        json.dumps(
            {
                "outcome": outcome,
                "portability_gaps": receipt["portability_gaps"],
                "dpkg_packages": installed,
                "pkg_config_modules": modules,
                "headers": headers,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
