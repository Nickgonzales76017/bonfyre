#!/usr/bin/env python3
"""Seed the FPQ proof frontier from the archive's real evidence.

Everything here is recorded at the MEASURED plane at most, not PROVEN. The
archive reports these experiments; this process did not re-run them, and the
FPQ math claims (category isomorphism, logarithmic convergence) are deliberately
absent because no proof artifact was cited. That restraint is the same four-plane
discipline the FPQ history says was missing.

    python3 seed_fpq_frontier.py            # write to control_plane.db
    python3 seed_fpq_frontier.py --report   # print the frontier and exit
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import proof_frontier as pf

DB = Path(__file__).resolve().parent / "control_plane.db"
SUBJECT = "model:qwen2.5-0.5b"
PROFILE = "fpq-bwa-multiscale-v9"

LAYERS = [
    ("source_model", pf.PROVEN, "architecture/tokenizer/tensor namespace identified"),
    ("representation_abi", pf.PROVEN, "profile magic and shards validated"),
    ("reconstruction", pf.PROVEN, "native-row Q/K/V reconstruction agrees with reference"),
    ("prepared_state", pf.UNTESTED, ""),
    ("transformer_math", pf.OPEN, "divergence localized to the runtime execution path"),
    ("runtime_contract", pf.BLOCKED, ""),
    ("physical_execution", pf.BLOCKED, ""),
    ("semantic_behavior", pf.BLOCKED, "garbage generation -- a symptom of the open layer, not a cause"),
]

NONCAUSES = [
    pf.KnownNonCause(
        noncause_id="fpq.noncause.quantization",
        hypothesis="quantization",
        subject_scope=SUBJECT,
        experiment="fully-lossless FP16 passthrough forced every tensor exact; garbage persisted",
        witness_ref="archive:fp16-passthrough-experiment",
        invalidation_conditions=("a new representation profile is introduced",),
    ),
    pf.KnownNonCause(
        noncause_id="fpq.noncause.qkv_reconstruction",
        hypothesis="reconstruction",
        subject_scope=SUBJECT,
        experiment=(
            "BONFYRE_QWEN_DISABLE_SLI_FAST_SCORE=1 routed Q/K/V through native "
            "reconstruction; deltas dropped to 0 vs reference (q .131->0, k .619->0, v .043->0)"
        ),
        witness_ref="archive:qkv-bypass-experiment",
        invalidation_conditions=("encoded artifact hash changes", "reader/profile version changes"),
    ),
    pf.KnownNonCause(
        noncause_id="fpq.noncause.rope",
        hypothesis="rope",
        subject_scope=SUBJECT,
        experiment="RoPE stayed exact under both the normal and bypass execution paths",
        witness_ref="archive:qkv-bypass-experiment",
        invalidation_conditions=("attention execution path changes",),
    ),
]

INVARIANTS = [
    pf.SolvedInvariant(
        invariant_id="fpq.reconstruction.qkv.qwen05b",
        subject_resource=SUBJECT,
        subject_profile=PROFILE,
        layer="reconstruction",
        statement="trusted native per-row reconstruction of Q/K/V agrees with the reference model",
        truth_plane=pf.MEASURED,
        status=pf.COOLED,
        proof_refs=("archive:qkv-bypass-experiment",),
        reheat_conditions=(
            "encoded artifact hash changes",
            "reader version changes",
            "representation profile changes",
            "reconstruction fixture fails",
        ),
    ),
]


def seed(db: sqlite3.Connection) -> dict:
    pf.ensure_schema(db)
    for ordinal, (layer, status, detail) in enumerate(LAYERS):
        pf.set_layer(db, SUBJECT, ordinal, layer, status, subject_profile=PROFILE, detail=detail)
    for noncause in NONCAUSES:
        pf.record_noncause(db, noncause)
    for invariant in INVARIANTS:
        pf.record_invariant(db, invariant)
    return pf.frontier_report(db, SUBJECT, PROFILE)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--db", default=str(DB))
    args = parser.parse_args()
    db = sqlite3.connect(args.db)
    if args.report:
        pf.ensure_schema(db)
        report = pf.frontier_report(db, SUBJECT, PROFILE)
    else:
        report = seed(db)
    db.close()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
