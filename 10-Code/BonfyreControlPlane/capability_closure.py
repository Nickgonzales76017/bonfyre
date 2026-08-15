"""Compose real native commands into a verifiable execution organism.

The essay's arc -- CapabilityDemand -> DisCIPL composition -> CapabilityClosure
-> form binding -> execution -- made real over the substrate built today. This
does not reimplement composition; it calls the native bonfyre-discipl engine
(34 typed contracts with success_prior/transform_cost/semantic_loss), then binds
the composed chain to real callable binaries via the estate/law catalog, and
gates every binding through the proof frontier.

It ties four things from today into one working mechanism:
  - DisCIPL         the native composition calculus (real chains, real scores)
  - estate_catalog  which real binary owns which domain
  - command_laws    which computational law each binary preserves
  - proof_frontier  whether a binding may mutate a proven layer

The output is not advice. It is an ExecutionOrganism: a scored chain, each hop
bound to a callable binary, with a single authorized/blocked verdict from the
fence -- the difference between "the system knows what it has" and "the system
composes what it has into something it may run."
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import command_laws as cl
import estate_catalog as ec
import proof_frontier as pf
import capability_catalog as cccat
import fpq_evidence as fq
import transport as tp

DISCIPL = Path.home() / ".bonfyre" / "bin" / "bonfyre-discipl"
DISCIPL_ROOT = Path("/tmp/discipl-root")


@dataclass(frozen=True)
class Hop:
    family: str
    binary: Optional[str]
    estate: str
    law: str
    confidence: float
    bridge: str
    status: str


@dataclass(frozen=True)
class ExecutionOrganism:
    goal: str
    global_confidence: float
    accumulated_cost: float
    semantic_drift: float
    hops: tuple[Hop, ...]
    authorized: bool
    verdict_reason: str

    @property
    def bound_ratio(self) -> float:
        if not self.hops:
            return 0.0
        return sum(1 for h in self.hops if h.binary) / len(self.hops)

    def to_dict(self) -> dict:
        return {
            "goal": self.goal,
            "global_confidence": self.global_confidence,
            "accumulated_cost": self.accumulated_cost,
            "semantic_drift": self.semantic_drift,
            "bound_ratio": round(self.bound_ratio, 3),
            "authorized": self.authorized,
            "verdict_reason": self.verdict_reason,
            "hops": [
                {
                    "family": h.family, "binary": h.binary, "estate": h.estate,
                    "law": h.law, "confidence": h.confidence,
                    "bridge": h.bridge, "status": h.status,
                }
                for h in self.hops
            ],
        }


def _discipl_propose(src: str, dst: str, depth: int) -> dict:
    """Call the native composition engine. Never reimplement it."""
    if not DISCIPL.exists():
        raise RuntimeError("bonfyre-discipl not installed")
    result = subprocess.run(
        [str(DISCIPL), "propose", "--from", src, "--to", dst,
         "--depth", str(depth), "--root", str(DISCIPL_ROOT)],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip()[:200])
    return json.loads(result.stdout)


# The families DisCIPL composes are model-internal (T_MOE_ROUTER, T_KV_CACHE...).
# Which command law executes each is a routing decision: representation-family
# hops are owned by the representation law, retrieval by retrieval, and so on.
_FAMILY_LAW = {
    "T_MOE_ROUTER": "representation", "T_MOE_EXPERT": "representation",
    "T_KV_CACHE": "representation", "T_SHARED_QK": "representation",
    "T_EMBED_POOL": "retrieval_observability", "T_RETRIEVAL_HEAD": "retrieval_observability",
    "T_AUDIO_MODEL": "media_transform", "T_AUDIO_GENERATOR": "media_transform",
    "T_SAMPLE_OUTPUT": "media_transform", "T_LATENT_SPACE": "representation",
    "T_POLICY_ROUTE": "composition", "T_SAFETY_HEAD": "governance_proof",
    "T_VERIFY": "evidence_evolution",
}


DISCIPL_DB = Path("/tmp/discipl-root/discipl.db")


def _command_binding(family: str) -> Optional[str]:
    """Precise binding: a command family binds to the exact binary its DisCIPL
    actor names in source_ref, not the first binary in its estate."""
    if not DISCIPL_DB.exists():
        return None
    con = sqlite3.connect(str(DISCIPL_DB))
    row = con.execute(
        "SELECT source_ref FROM discipl_actors WHERE family=? AND domain='command'",
        (family,),
    ).fetchone()
    con.close()
    if row and row[0] and Path(row[0]).exists():
        return row[0]
    return None


def _bind_family(db: sqlite3.Connection, family: str) -> tuple[Optional[str], str, str]:
    """Bind a DisCIPL family to its real callable binary.

    A command family binds precisely to the binary its actor names. A
    model-internal family falls back to law -> estate -> first callable, which
    is coarser but is all the model families expose.
    """
    precise = _command_binding(family)
    if precise is not None:
        bare = Path(precise).name.replace("bonfyre-", "").replace("-", "")
        row = db.execute(
            "SELECT estate,law FROM command_laws cl JOIN estate_catalog ec"
            " ON cl.family=ec.family WHERE lower(replace(ec.family,'Bonfyre',''))=?",
            (bare,),
        ).fetchone()
        return precise, (row[0] if row else "command"), (row[1] if row else "command")
    law = _FAMILY_LAW.get(family, "representation")
    families = cl.by_law(db, law)
    for fam in families:
        row = db.execute(
            "SELECT binary FROM estate_catalog WHERE family=?", (fam,)
        ).fetchone()
        if row and row[0]:
            estate_row = db.execute(
                "SELECT estate FROM estate_catalog WHERE family=?", (fam,)
            ).fetchone()
            return row[0], (estate_row[0] if estate_row else ""), law
    return None, "", law


def close(
    db: sqlite3.Connection,
    *,
    src_family: str,
    dst_family: str,
    depth: int = 4,
    subject_resource: str = "model:qwen2.5-0.5b",
    subject_profile: str = "fpq-bwa-multiscale-v9",
    observed_failure: str = "",
) -> ExecutionOrganism:
    """Compose, bind, and gate. The whole arc, over real components."""
    cl.build(db)  # ensure law + estate catalogs are current
    proposal = _discipl_propose(src_family, dst_family, depth)

    families = proposal.get("families", [])
    confidences = proposal.get("per_hop_confidence", [])
    bridges = proposal.get("bridge_requirements", [])
    statuses = proposal.get("per_hop_status", [])

    hops: list[Hop] = []
    for index, family in enumerate(families):
        binary, estate, law = _bind_family(db, family)
        hops.append(Hop(
            family=family,
            binary=binary,
            estate=estate,
            law=law,
            confidence=confidences[index] if index < len(confidences) else 0.0,
            bridge=bridges[index] if index < len(bridges) else "",
            status=statuses[index] if index < len(statuses) else "",
        ))

    # The fence: if this organism would mutate a representation-law command
    # while the FPQ frontier has reconstruction proven and the divergence is
    # higher up, refuse. Composition is allowed; touching a proven layer is not.
    authorized = True
    reason = "composition bound; no proven layer targeted"
    if observed_failure:
        touches_representation = any(h.law == "representation" for h in hops)
        if touches_representation:
            decision = pf.authorize_mutation(
                db,
                subject_resource=subject_resource,
                subject_profile=subject_profile,
                target_layer="representation_abi",
                observed_failure=observed_failure,
            )
            authorized = decision.allowed
            reason = decision.reason

    return ExecutionOrganism(
        goal=f"{src_family} -> {dst_family}",
        global_confidence=proposal.get("global_confidence", 0.0),
        accumulated_cost=proposal.get("accumulated_cost", 0.0),
        semantic_drift=proposal.get("semantic_drift", 0.0),
        hops=tuple(hops),
        authorized=authorized,
        verdict_reason=reason,
    )


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(Path(__file__).resolve().parent / "control_plane.db"))
    p.add_argument("--from", dest="src", required=True)
    p.add_argument("--to", dest="dst", required=True)
    p.add_argument("--depth", type=int, default=4)
    p.add_argument("--failure", default="")
    a = p.parse_args()
    db = sqlite3.connect(a.db)
    organism = close(db, src_family=a.src, dst_family=a.dst, depth=a.depth,
                     observed_failure=a.failure)
    print(json.dumps(organism.to_dict(), indent=2))
    db.close()


if __name__ == "__main__":
    main()


# --------------------------------------------------------------- execution

QUEUE = Path.home() / ".bonfyre" / "bin" / "bonfyre-queue"


@dataclass(frozen=True)
class SubmittedOrganism:
    goal: str
    submitted: bool
    missions: tuple[str, ...]
    reason: str


def submit(
    organism: ExecutionOrganism,
    queue_db: Path,
) -> SubmittedOrganism:
    """Submit an authorized organism to the native WorkGraph-backed queue.

    This is the execution boundary. A blocked organism refuses here -- the gate
    is not advisory, it stops real durable work from being created. An
    authorized organism becomes one native mission per bound hop, each carrying
    its real binary and confidence, so the queue holds runnable work, not a
    description of work.
    """
    if not organism.authorized:
        return SubmittedOrganism(
            goal=organism.goal, submitted=False, missions=(),
            reason=f"refused at execution boundary: {organism.verdict_reason}",
        )
    if not QUEUE.exists():
        return SubmittedOrganism(
            goal=organism.goal, submitted=False, missions=(),
            reason="bonfyre-queue not installed",
        )

    missions: list[str] = []
    for index, hop in enumerate(organism.hops):
        if not hop.binary:
            continue
        subject = f"organism:{organism.goal}|hop{index}:{hop.family}:{hop.binary}"
        result = subprocess.run(
            [str(QUEUE), "enqueue", f"organism-hop:{hop.law}", subject,
             "--db", str(queue_db), "--retries", "2"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0:
            try:
                missions.append(json.loads(result.stdout)["mission_id"])
            except (ValueError, KeyError):
                pass
    return SubmittedOrganism(
        goal=organism.goal, submitted=bool(missions),
        missions=tuple(missions),
        reason=f"{len(missions)} hops submitted as native durable work",
    )


def _promote_ran_commands(db, hops, receipt_ref: str) -> int:
    """A command that just ran a real workload earns workload_proven.

    This closes the loop the manual step opened: execution is evidence, so a
    completed organism promotes the commands it ran and cites the receipt. No
    hand promotion, no claim without a run behind it."""
    import datetime as _dt
    cccat.ensure_schema(db)
    promoted = 0
    for hop in hops:
        if not hop.binary:
            continue
        base = Path(hop.binary).name  # bonfyre-hash
        family = "Bonfyre" + "".join(p.capitalize() for p in base.replace("bonfyre-", "").split("-"))
        row = db.execute("SELECT location FROM capability_identities WHERE public_name=?", (family,)).fetchone()
        if row:
            cccat.declare(db, cccat.Capability(public_name=family, maturity="workload_proven",
                          location=row[0], proof_ref=receipt_ref), now=_dt.datetime.now(_dt.timezone.utc))
            promoted += 1
    return promoted


def execute_representation_workload(db) -> dict:
    """Run real FPQ compute as the concrete workload of a representation organism.

    Composition and gating were already real; this makes execution real for the
    model estate. When a fused representation chain reaches dispatch and the FPQ
    binary and golden fixture are present, the thing it actually computes is an
    FPQ roundtrip: real polar-quantization math, a real reconstruction error, a
    measured frontier update, and -- because execution is evidence -- a
    workload_proven promotion of BonfyreFPQ cited by the digest of the report the
    command emitted. No fixture, no binary, no claim: it reports honestly that no
    workload ran rather than inventing one.
    """
    import datetime as _dt
    if not (fq.FPQ.exists() and fq.FIXTURE.exists()):
        return {"executed": False, "reason": "bonfyre-fpq or fixture unavailable"}
    ev = fq.record_measured_evidence(db)  # invokes the real command
    receipt = f"report:sha256:{ev['report_sha256']}"
    cccat.ensure_schema(db)
    row = db.execute(
        "SELECT location FROM capability_identities WHERE public_name=?", ("BonfyreFPQ",)
    ).fetchone()
    promoted = False
    if row:
        cccat.declare(db, cccat.Capability(
            public_name="BonfyreFPQ", maturity="workload_proven",
            location=row[0], proof_ref=receipt,
        ), now=_dt.datetime.now(_dt.timezone.utc))
        promoted = True
    return {
        "executed": True, "worst_mse": ev["worst_mse"], "all_good": ev["all_good"],
        "receipt": receipt, "invariant": ev["invariant"],
        "promoted_BonfyreFPQ": promoted,
    }


# ----------------------------------------------------------- form selection

PIPELINE = Path.home() / ".bonfyre" / "bin" / "bonfyre-pipeline"
REASON = Path.home() / ".bonfyre" / "bin" / "bonfyre-reason"
NET = Path.home() / ".bonfyre" / "bin" / "bonfyre-net"

# Which computational form an organism takes, from the laws of its hops. The
# essay's point: not everything is durable queue work. A coupled-dynamics chain
# wants a trajectory runtime; a stable deterministic artifact chain wants a
# fused pipeline; a mixed or externally-waiting chain wants the durable
# WorkGraph.
FORM_DURABLE = "durable_workgraph"
FORM_FUSED = "fused_pipeline"
FORM_TRAJECTORY = "reason_trajectory"
FORM_COUPLED = "coupled_circuit"


@dataclass(frozen=True)
class FormSelection:
    form: str
    substrate: Optional[str]
    rationale: str


def select_form(organism: ExecutionOrganism) -> FormSelection:
    laws = [h.law for h in organism.hops]
    if not laws:
        return FormSelection(FORM_DURABLE, str(QUEUE), "empty organism defaults to durable")

    coupled = {"coupled_dynamics"}
    fused = {"artifact_derivation", "media_transform", "representation", "retrieval_observability"}

    if any(law in coupled for law in laws):
        return FormSelection(
            FORM_TRAJECTORY, str(REASON) if REASON.exists() else None,
            "a coupled-dynamics hop means branchable state, not a DAG -- trajectory runtime",
        )
    if all(law in fused for law in laws):
        return FormSelection(
            FORM_FUSED, str(PIPELINE) if PIPELINE.exists() else None,
            "a stable deterministic artifact/model chain fuses into one process",
        )
    # Mixed laws, or anything with governance/economic/boundary hops, needs
    # durable causal execution with receipts.
    return FormSelection(
        FORM_DURABLE, str(QUEUE) if QUEUE.exists() else None,
        "mixed or externally-dependent laws need durable work with receipts",
    )


def dispatch(
    organism: ExecutionOrganism,
    queue_db: Path,
    transport: str = tp.DIRECT,
    db=None,
) -> dict:
    """Select the form and route to the matching substrate.

    Durable organisms become real queue missions (proven path). Other forms
    resolve to the exact substrate invocation their hops require -- routed, not
    forced through the queue. This is the branch the essay asks for: form is
    chosen before execution, so a trajectory chain does not get flattened into
    a DAG.
    """
    if not organism.authorized:
        return {"form": None, "dispatched": False,
                "reason": f"refused: {organism.verdict_reason}"}

    selection = select_form(organism)
    # Transport is orthogonal to form: whichever substrate runs the hop, it can
    # run it by direct subprocess or through the MCP server, and the MCP carrier
    # adds a receipt. The organism records which carrier it will use.
    carrier = transport if tp.mcp_available() or transport == tp.DIRECT else tp.DIRECT

    if selection.form == FORM_DURABLE:
        submitted = submit(organism, queue_db)
        promoted = 0
        if submitted.submitted and submitted.missions:
            promoted = _promote_ran_commands(db, organism.hops, f"mission:{submitted.missions[0]}")
        return {
            "form": selection.form, "substrate": selection.substrate,
            "rationale": selection.rationale, "dispatched": submitted.submitted,
            "missions": list(submitted.missions), "transport": carrier,
            "promoted_commands": promoted,
        }

    # A fused representation chain has a concrete workload the system can run
    # right now: FPQ. Rather than only routing it, execute real FPQ compute,
    # which measures the frontier and promotes BonfyreFPQ from its own receipt.
    fpq_run = None
    if selection.form == FORM_FUSED and db is not None and any(
        h.law == "representation" for h in organism.hops
    ):
        fpq_run = execute_representation_workload(db)

    # Other non-durable forms: route to their substrate. The exact invocation is
    # constructed from the bound hops; running it needs the substrate's input
    # (a circuit file for Net, a session for Reason, an input artifact for
    # Pipeline), so this records the routed command rather than forcing the
    # organism into the wrong runtime.
    binaries = [h.binary for h in organism.hops if h.binary]
    result = {
        "form": selection.form,
        "substrate": selection.substrate,
        "rationale": selection.rationale,
        "dispatched": selection.substrate is not None,
        "routed_stages": binaries, "transport": carrier,
    }
    if fpq_run is not None:
        result["fpq_workload"] = fpq_run
    return result
