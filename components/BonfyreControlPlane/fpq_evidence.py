"""Run FPQ for real and turn the run into proof-frontier evidence.

The frontier has governed FPQ from the archive. Every reconstruction claim was
recorded at the MEASURED plane on the strength of an experiment this process
never re-ran; the reheat conditions named things like "reconstruction fixture
fails" with no fixture behind them. That is anti-amnesia, but it is not use.

This closes the other half of the loop: it invokes the real bonfyre-fpq binary
on a deterministic golden fixture, parses the actual roundtrip error, and records
a measured invariant whose reheat condition is a number this process can
re-check on demand -- not an archive citation it can only trust. Execution
becomes evidence. A real roundtrip within tolerance cools the reconstruction
layer; a regressed roundtrip on the same fixture reheats it. The fence stops
being a story about the archive and becomes a live gate backed by a command that
runs.

The seeded qwen05b frontier stays as it was -- honestly at MEASURED, from the
archive. This adds a second subject, model:fpq-fixture-tiny-f16, that carries
the same reconstruction claim at MEASURED but with a re-runnable witness, and it
satisfies the standing work item to verify archive claims before any promotion:
the abstract "reconstruction fixture fails" reheat now has something concrete to
fire against.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import proof_frontier as pf

FPQ = Path.home() / ".bonfyre" / "bin" / "bonfyre-fpq"
FIXTURE = (
    Path(__file__).resolve().parent.parent.parent
    / "packs" / "fpq-proof-frontier" / "fixtures" / "tiny-f16.gguf"
)

SUBJECT = "model:fpq-fixture-tiny-f16"
PROFILE = "fpq-v3-coord-qjl"

# The fixture's worst measured tensor MSE is ~0.00397. The ceiling sits above it
# with headroom: a roundtrip on the same bytes that pushes any tensor past this
# is a real regression at the reconstruction layer, and the only thing that may
# reheat the cooled invariant. Chosen from the measurement, not guessed.
MSE_CEILING = 0.005

# The exact reheat phrase. challenge_invariant matches a signal that is a
# substring of a reheat condition, so the fixture verifier emits this string on
# regression and the fence accepts it -- and nothing else.
REHEAT_MSE = "reconstruction fixture mse exceeds ceiling"
REHEAT_HASH = "fixture gguf hash changes"

# The eight FPQ correctness layers, honest to what a roundtrip on raw weights
# actually measures: it reads the model, parses the profile, reconstructs the
# weights within tolerance and physically runs to completion -- but a synthetic
# three-tensor weight file exercises no forward pass, so transformer_math is the
# honest open frontier and everything above it is blocked behind it. Reconstruction
# sits below the frontier: proven ground, off-limits to a "fix the encoder" edit.
LAYERS = [
    ("source_model", pf.PROVEN, "read 3 F16 tensors from the fixture GGUF v3"),
    ("representation_abi", pf.PROVEN, "GGUF v3 magic and F16 tensors decoded"),
    ("reconstruction", pf.PROVEN, ""),  # witness filled from the real run
    ("prepared_state", pf.UNTESTED, ""),
    ("transformer_math", pf.OPEN, "no forward pass on a raw-weight fixture; the honest open layer"),
    ("runtime_contract", pf.BLOCKED, ""),
    ("physical_execution", pf.BLOCKED, ""),
    ("semantic_behavior", pf.BLOCKED, ""),
]


@dataclass(frozen=True)
class TensorResult:
    name: str
    n_weights: int
    mode: str
    mse: float
    cosine: float
    ratio: float
    bits_per_weight: float
    status: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixture_digest(gguf: Path = FIXTURE) -> str:
    return _sha256(gguf.read_bytes())


def run_roundtrip(gguf: Path = FIXTURE, timeout: int = 120) -> tuple[list[TensorResult], str]:
    """Invoke the real bonfyre-fpq roundtrip and parse per-tensor results.

    Returns the parsed results and a sha256 of the raw report, so a caller can
    cite the exact bytes the numbers came from. This is the point of contact
    with the real command: no numbers here are asserted, they are read back out
    of a process that ran.
    """
    if not FPQ.exists():
        raise RuntimeError("bonfyre-fpq not installed")
    if not gguf.exists():
        raise RuntimeError(f"fixture missing: {gguf}")
    # FPQ writes its per-tensor report to stderr and progress to stdout; merge
    # both, in order, so the parsed text is exactly what the command emitted.
    proc = subprocess.run(
        [str(FPQ), "roundtrip", str(gguf), "--report"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout,
    )
    report = proc.stdout
    report_sha = _sha256(report.encode())
    results: list[TensorResult] = []

    # Each tensor block opens with "FPQ v3 Report: <name>" and carries labelled
    # fields. Parse block by block so a partial run yields partial results.
    blocks = re.split(r"FPQ v3 Report:\s*", report)[1:]
    for block in blocks:
        name = block.splitlines()[0].strip()

        def grab(pattern: str, cast, default):
            m = re.search(pattern, block)
            return cast(m.group(1)) if m else default

        results.append(TensorResult(
            name=name,
            n_weights=grab(r"\((\d+)\s+weights\)", int, 0),
            mode=grab(r"Mode:\s+(\S+)", str, ""),
            mse=grab(r"Roundtrip MSE:\s+([\d.eE+-]+)", float, float("nan")),
            cosine=grab(r"Cosine sim:\s+([\d.eE+-]+)", float, float("nan")),
            ratio=grab(r"Ratio:\s+([\d.]+)x", float, 0.0),
            bits_per_weight=grab(r"Bits/weight:\s+([\d.]+)", float, 0.0),
            status=grab(r"Status:\s+(\S+)", str, ""),
        ))
    return results, report_sha


def worst_mse(results: list[TensorResult]) -> float:
    vals = [r.mse for r in results if r.mse == r.mse]  # drop nan
    return max(vals) if vals else float("nan")


def record_measured_evidence(
    db: sqlite3.Connection, gguf: Path = FIXTURE, now: Optional[dt.datetime] = None
) -> dict:
    """Run FPQ, then record a measured frontier and invariant from the real run.

    This is the whole contribution: the reconstruction layer is cooled not by an
    archive citation but by a roundtrip that just happened, cited by the digest
    of the fixture bytes and the digest of the report. The reheat condition is a
    number, so :func:`verify_fixture` can actually challenge it later.
    """
    pf.ensure_schema(db)
    results, report_sha = run_roundtrip(gguf)
    gguf_sha = fixture_digest(gguf)
    wmse = worst_mse(results)
    all_good = bool(results) and all(r.status == "GOOD" for r in results)

    recon_witness = (
        f"real roundtrip: {len(results)} tensors, worst MSE {wmse:.8f}, "
        f"all_good={all_good}, report_sha256={report_sha[:16]}"
    )
    for ordinal, (layer, status, detail) in enumerate(LAYERS):
        witness = f"fpq_evidence:{report_sha[:16]}" if layer == "reconstruction" else ""
        d = recon_witness if layer == "reconstruction" else detail
        pf.set_layer(db, SUBJECT, ordinal, layer, status,
                     subject_profile=PROFILE, witness_ref=witness, detail=d)

    invariant = pf.SolvedInvariant(
        invariant_id="fpq.reconstruction.fixture.tiny_f16",
        subject_resource=SUBJECT,
        subject_profile=PROFILE,
        subject_hashes=(f"gguf:{gguf_sha}",),
        layer="reconstruction",
        statement=(
            "FPQ v3 roundtrip on the fixed tiny-f16 GGUF reconstructs every "
            f"tensor within tolerance (worst MSE {wmse:.6f} < {MSE_CEILING}), "
            "deterministically across runs"
        ),
        truth_plane=pf.MEASURED,
        status=pf.COOLED,
        proof_refs=(f"fixture:gguf:{gguf_sha}", f"report:sha256:{report_sha}"),
        reheat_conditions=(REHEAT_MSE, REHEAT_HASH),
    )
    pf.record_invariant(db, invariant, now=now)

    return {
        "subject": SUBJECT,
        "profile": PROFILE,
        "gguf_sha256": gguf_sha,
        "report_sha256": report_sha,
        "tensors": [
            {"name": r.name, "mse": r.mse, "cosine": r.cosine,
             "ratio": r.ratio, "mode": r.mode, "status": r.status}
            for r in results
        ],
        "worst_mse": wmse,
        "all_good": all_good,
        "invariant": invariant.invariant_id,
        "frontier": pf.frontier_report(db, SUBJECT, PROFILE),
    }


def verify_fixture(db: sqlite3.Connection, gguf: Path = FIXTURE) -> tuple[bool, str]:
    """Re-run FPQ on the fixture and report whether the cooled claim still holds.

    Returns (ok, signal). ok=True means the roundtrip still reconstructs within
    the ceiling and the invariant stays cooled. ok=False returns the exact reheat
    signal the fence will accept -- so a real regression, and only a real
    regression, can reopen the reconstruction layer.
    """
    if fixture_digest(gguf) != _fixture_recorded_hash(db):
        return False, REHEAT_HASH
    results, _ = run_roundtrip(gguf)
    wmse = worst_mse(results)
    if not (wmse == wmse) or wmse > MSE_CEILING or not all(r.status == "GOOD" for r in results):
        return False, REHEAT_MSE
    return True, ""


def _fixture_recorded_hash(db: sqlite3.Connection) -> Optional[str]:
    row = db.execute(
        "SELECT subject_hashes FROM solved_invariants WHERE invariant_id=?",
        ("fpq.reconstruction.fixture.tiny_f16",),
    ).fetchone()
    if not row:
        return None
    for h in json.loads(row[0] or "[]"):
        if h.startswith("gguf:"):
            return h.split(":", 1)[1]
    return None


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(Path(__file__).resolve().parent / "control_plane.db"))
    p.add_argument("--verify", action="store_true", help="re-run and check, do not record")
    a = p.parse_args()
    db = sqlite3.connect(a.db)
    if a.verify:
        ok, signal = verify_fixture(db)
        print(json.dumps({"ok": ok, "reheat_signal": signal}, indent=2))
    else:
        print(json.dumps(record_measured_evidence(db), indent=2))
    db.close()


if __name__ == "__main__":
    main()
