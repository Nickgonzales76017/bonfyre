"""Maturity blast-radius: the atlas auditing itself with CollapseFront.

atlas.py validate enforces no-maturity-laundering at commit time -- a
measured/proven architecture must carry a witness. This asks the sharper,
runtime question CollapseFront makes possible:

  * which maturity claims are single-witness fragile (one retraction from
    collapse), and
  * if a given witness file turns out to be false/deleted, which maturity claims
    across the whole registry go dark (its blast radius)?

We build a CollapseFront support lattice over the atlas's own claims:

  ground      w:<path>     -- a witness file, true iff it exists on disk
  conclusion  mat:<arch>   -- "<arch> is measured/proven", an OR over its
                              witnesses (one real witness suffices)

collapse_front(witness) and critical_support(maturity claim) are then the two
transposed projections of the atlas's maturity collapse matrix. A measured/proven
architecture whose maturity conclusion is already false at baseline (no existing
witness) is laundering the label -- and this finds it against the real files, not
against the text of the yaff.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO_ROOT, "components", "BonfyreControlPlane"))

from support_lattice import Lattice, _Node, _evaluate  # noqa: E402  reuse the generic core
CLAIMED_MATURITIES = ("measured", "proven")


@dataclass
class MaturityAudit:
    lattice: Lattice
    claims: dict[str, dict]  # arch name -> {maturity, witnesses, existing_witnesses}


def witness_kind(path: str) -> str:
    """Classify a witness pointer.

    ``local`` means the current checkout can verify the witness directly.
    ``external`` means the pointer is deliberate evidence that lives outside the
    repository checkout (another repo, or Bonfÿre host state). ``missing`` is
    reserved for a repository-local pointer that should exist here but does not.

    This distinction matters in CI: ``~/.bonfyre`` is runtime/host evidence by
    design. A clean GitHub runner not having another machine's substrate proof is
    not evidence that the measured claim was fabricated.
    """
    if path.startswith("external:"):
        return "external"
    expanded = os.path.expanduser(path)
    if path.startswith("~/.bonfyre/"):
        return "local" if os.path.exists(expanded) else "external"
    if os.path.isabs(expanded) or expanded != path:  # other absolute / ~ paths
        return "local" if os.path.exists(expanded) else "missing"
    return "local" if os.path.exists(os.path.join(REPO_ROOT, path)) else "missing"


def _witness_exists(path: str) -> bool:
    # A witness "supports" its claim if it is a real local file. An external
    # witness is honest but unverifiable here, so it does not lend local support.
    return witness_kind(path) == "local"


def build_maturity_lattice(index_path: str | None = None) -> MaturityAudit:
    index_path = index_path or os.path.join(HERE, "atlas.index.json")
    with open(index_path) as fh:
        index = json.load(fh)

    nodes: dict[str, _Node] = {}
    grounds: set[str] = set()
    conclusions: set[str] = set()
    labels: dict[str, str] = {}
    claims: dict[str, dict] = {}
    false_grounds: set[str] = set()

    architectures = index["architectures"]
    if isinstance(architectures, dict):
        arch_items = [(k, v) for k, v in architectures.items()]
    else:
        arch_items = [(a.get("canonical_name"), a) for a in architectures]
    for key, arch in arch_items:
        maturity = arch.get("maturity")
        if maturity not in CLAIMED_MATURITIES:
            continue
        name = arch.get("canonical_name") or key
        witnesses = list(arch.get("witnesses", []))
        existing = []
        witness_grounds = []
        for w in witnesses:
            gid = f"w:{w}"
            if gid not in nodes:
                nodes[gid] = _Node(gid, "ground")
                grounds.add(gid)
                labels[gid] = w
                if not _witness_exists(w):
                    false_grounds.add(gid)
            witness_grounds.append(gid)
            if _witness_exists(w):
                existing.append(w)
        mid = f"mat:{name}"
        # measured/proven stands if ANY witness exists -> OR. No witnesses at all
        # means an OR over the empty set, which evaluates false: laundering.
        nodes[mid] = _Node(mid, "or", witness_grounds)
        conclusions.add(mid)
        labels[mid] = f"{name} is {maturity}"
        claims[name] = {
            "maturity": maturity,
            "witnesses": witnesses,
            "existing_witnesses": existing,
        }

    lat = Lattice(nodes=nodes, grounds=grounds, conclusions=conclusions, labels=labels)
    lat._false_grounds = false_grounds  # type: ignore[attr-defined]
    return MaturityAudit(lattice=lat, claims=claims)


def _baseline(lat: Lattice) -> dict[str, bool]:
    # a witness that does not exist on disk is retracted from the start
    return _evaluate(lat, getattr(lat, "_false_grounds", set()))


def laundered_claims(audit: MaturityAudit) -> list[str]:
    """measured/proven architectures with NO real witness of any kind -- not a
    local file, not even an honest external pointer. This is true laundering."""
    out = []
    for name, c in audit.claims.items():
        has_local = bool(c["existing_witnesses"])
        has_external = any(witness_kind(w) == "external" for w in c["witnesses"])
        if not has_local and not has_external:
            out.append(name)
    return sorted(out)


def externally_witnessed_claims(audit: MaturityAudit) -> list[tuple[str, list[str]]]:
    """measured/proven architectures whose ONLY witnesses are external pointers --
    honest, but not verifiable from this repo. A distinct category from laundered."""
    out = []
    for name, c in audit.claims.items():
        if c["existing_witnesses"]:
            continue
        ext = [w for w in c["witnesses"] if witness_kind(w) == "external"]
        if ext:
            out.append((name, ext))
    return sorted(out)


def fragile_claims(audit: MaturityAudit) -> list[tuple[str, str]]:
    """(arch, sole_witness) for claims resting on exactly one existing witness --
    a single retraction from collapse."""
    out = []
    for name, c in audit.claims.items():
        if len(c["existing_witnesses"]) == 1:
            out.append((name, c["existing_witnesses"][0]))
    return sorted(out)


def blast_radius(audit: MaturityAudit, witness_path: str) -> list[str]:
    """Maturity claims that collapse if this witness alone is retracted."""
    lat = audit.lattice
    gid = f"w:{witness_path}"
    if gid not in lat.grounds:
        raise KeyError(f"no such witness ground {witness_path!r}")
    base = _baseline(lat)
    already_false = getattr(lat, "_false_grounds", set())
    after = _evaluate(lat, already_false | {gid})
    return sorted(
        audit.claims[name]["maturity"] and name
        for name in audit.claims
        if base.get(f"mat:{name}", True) and not after.get(f"mat:{name}", True)
    )


def report(index_path: str | None = None) -> dict:
    audit = build_maturity_lattice(index_path)
    laundered = laundered_claims(audit)
    fragile = fragile_claims(audit)
    # widest blast radius across all witnesses
    widest = []
    for gid in audit.lattice.grounds:
        w = audit.lattice.labels[gid]
        try:
            radius = blast_radius(audit, w)
        except KeyError:
            continue
        if len(radius) >= 2:
            widest.append((w, radius))
    widest.sort(key=lambda wr: len(wr[1]), reverse=True)
    return {
        "claims_audited": len(audit.claims),
        "laundered": laundered,
        "externally_witnessed": externally_witnessed_claims(audit),
        "fragile_single_witness": fragile,
        "widest_blast_radius": widest[:10],
    }


if __name__ == "__main__":
    import pprint
    pprint.pp(report())
