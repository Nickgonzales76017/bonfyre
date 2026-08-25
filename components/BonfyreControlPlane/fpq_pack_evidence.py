"""Consume the mature FPQ substrate's recorded quality gates -- honestly.

The FPQ / fpqqwen substrate has run for months and left recorded pack manifests
(``*.fpq-pack.json``) on disk, each carrying a ``quality_gate`` and a
``generation_quality`` verdict. This does not re-run any of it. It reads those
recorded verdicts and folds them into the proof frontier as they actually are --
including the honest failures.

That honesty is the point. A pack whose reconstruction completed but whose
``quality_gate`` is ``failed`` ("native FPQ2 parity failed") is exactly the
atlas's central forbidden inference, witnessed in real data: representation was
built, generation was not good. So this records ``representation_abi`` as proven
(the pack exists and is complete) while leaving ``semantic_behavior`` open, with
the failure as its detail -- never letting a completed pack launder itself into a
generation-quality claim.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import proof_frontier as pf

FPQ_PACKS_DIR = Path.home() / "BonfyreModels" / "fpq"


@dataclass(frozen=True)
class PackVerdict:
    path: str
    model_repo: str
    policy: str
    quality_gate: str            # passed | failed | ...
    generation_quality: str
    complete: bool

    @property
    def subject(self) -> str:
        return f"model:{self.model_repo}"

    @property
    def gate_passed(self) -> bool:
        return self.quality_gate.strip().lower() in ("passed", "pass", "ok")


def read_pack(path: Path) -> Optional[PackVerdict]:
    try:
        d = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None
    if "model_repo" not in d and "schema" not in d:
        return None
    return PackVerdict(
        path=str(path),
        model_repo=d.get("model_repo", ""),
        policy=d.get("policy", ""),
        quality_gate=str(d.get("quality_gate", "")),
        generation_quality=str(d.get("generation_quality", "")),
        complete=str(d.get("status", "")).startswith("complete"),
    )


def record_pack_evidence(db: sqlite3.Connection, verdict: PackVerdict) -> dict:
    """Fold a recorded pack verdict into the frontier, honestly.

    A complete pack proves the representation ABI (it was built and is readable);
    it says nothing good about generation unless the quality gate passed. A failed
    gate leaves semantic_behavior open with the failure recorded -- the completed
    pack cannot launder itself into a semantic claim.
    """
    pf.ensure_schema(db)
    profile = verdict.policy or "fpq"
    if verdict.complete:
        pf.set_layer(db, verdict.subject, 1, "representation_abi", pf.PROVEN,
                     subject_profile=profile, witness_ref=verdict.path,
                     detail="recorded pack complete")
    # semantic_behavior tracks the recorded generation verdict -- never assumed.
    if verdict.gate_passed:
        sem_status, detail = pf.PROVEN, "recorded quality gate passed"
    else:
        sem_status, detail = pf.OPEN, f"recorded quality gate: {verdict.generation_quality[:80]}"
    pf.set_layer(db, verdict.subject, 7, "semantic_behavior", sem_status,
                 subject_profile=profile,
                 witness_ref=verdict.path if verdict.gate_passed else "",
                 detail=detail)
    return {
        "subject": verdict.subject, "profile": profile,
        "quality_gate": verdict.quality_gate,
        "semantic_behavior": sem_status,
        "generation_quality": verdict.generation_quality[:80],
    }


def ingest_dir(db: sqlite3.Connection, packs_dir: Path = FPQ_PACKS_DIR) -> list[dict]:
    """Ingest every recorded pack manifest in a directory. Reads, never runs."""
    out: list[dict] = []
    if not packs_dir.exists():
        return out
    for path in sorted(packs_dir.glob("*.fpq-pack.json")):
        verdict = read_pack(path)
        if verdict and verdict.model_repo:
            out.append(record_pack_evidence(db, verdict))
    return out
