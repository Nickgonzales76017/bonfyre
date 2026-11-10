"""Consume Bonfyre's own substrate probe manifests -- the real activation layer.

The 14 Reality Foundry substrates are not aspirational: they are vendored,
built, and probed under ``~/.bonfyre/substrates/v6.1``. Each carries two recorded
witnesses this reads (never regenerates):

    activation.json      bonfyre.substrate-activation.v1 -- state, evidence_class,
                         built artifacts, upstream lock digest, the semantic probe
    semantic-proof.json  the probe's verdict: kind, state (passed/failed), assertions,
                         output_digest

The bootstrap contract is strict and worth honoring: "only verified live-upstream
semantic probes create active substrate manifests." So a substrate is treated as
measured only when its activation is ``active``, its evidence is ``live-upstream``,
and its semantic proof ``passed`` -- each backed by the manifest digest. This is
Bonfyre proving its own substrate; this module just surfaces that proof to the
atlas rather than the control plane re-deriving it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

SUBSTRATES_ROOT = Path.home() / ".bonfyre" / "substrates" / "v6.1"

# The atlas architecture id for each substrate directory.
DIR_TO_ATLAS_ID = {
    "automerge": "automerge", "burn": "burn", "cubecl": "cubecl", "daft": "daft",
    "egglog": "egglog", "feldera": "feldera", "gigatoken": "gigatoken",
    "hydro": "hydro", "lance": "lance", "restate": "restate", "tract": "tract",
    "verus": "verus", "yaff": "yaff",
}


@dataclass(frozen=True)
class SubstrateEvidence:
    name: str
    activation_state: str
    evidence_class: str
    proof_state: str
    proof_kind: str
    output_digest: str
    manifest_digest: str

    @property
    def measured(self) -> bool:
        """A real, recorded measurement: activated live-upstream and probe passed."""
        return (self.activation_state == "active"
                and self.evidence_class == "live-upstream"
                and self.proof_state == "passed")

    def witness_ref(self) -> str:
        return f"~/.bonfyre/substrates/v6.1/{self.name}/semantic-proof.json"


def read_substrate(name: str, root: Path = SUBSTRATES_ROOT) -> Optional[SubstrateEvidence]:
    base = root / name
    act_p, proof_p = base / "activation.json", base / "semantic-proof.json"
    if not act_p.exists() or not proof_p.exists():
        return None
    try:
        act = json.loads(act_p.read_text())
        proof = json.loads(proof_p.read_text())
    except (OSError, ValueError):
        return None
    return SubstrateEvidence(
        name=name,
        activation_state=str(act.get("state", "")),
        evidence_class=str(act.get("evidence_class", "")),
        proof_state=str(proof.get("state", "")),
        proof_kind=str(proof.get("kind", "")),
        output_digest=str(proof.get("output_digest", "")),
        manifest_digest=str(act.get("manifest_digest", "")),
    )


def all_substrates(root: Path = SUBSTRATES_ROOT) -> dict[str, SubstrateEvidence]:
    out: dict[str, SubstrateEvidence] = {}
    if not root.exists():
        return out
    for name in sorted(DIR_TO_ATLAS_ID):
        ev = read_substrate(name, root)
        if ev is not None:
            out[name] = ev
    return out


def measured_substrates(root: Path = SUBSTRATES_ROOT) -> dict[str, SubstrateEvidence]:
    return {n: e for n, e in all_substrates(root).items() if e.measured}
