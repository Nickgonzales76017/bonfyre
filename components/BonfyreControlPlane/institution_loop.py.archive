"""One institutional goal, metabolized end to end -- the nervous system.

Every organ built this session exists; this connects them on real state. A goal
becomes an AtomicForm, autofilled from verified actors, its evidence slots
filled from real fabric artifacts (the same ones BonfyreFS mounts and ForeignTwin
materialization produces), compiled to submit-ready, and -- only when ready --
published as a real BonfyreCMS entry, the content estate's durable record.

The pairing the instruction names: BonfyreCMS + BonfyreFS + today's
BonfyreFS-adjacent work. The evidence an AtomicForm cites is a content-addressed
fabric artifact; BonfyreFS mounts it as a file; ForeignTwin fetch materializes
remote reality into the same fabric; and BonfyreCMS publishes the compiled
package as a typed article, ANN-indexable and servable. The fabric is the
shared substrate; CMS is the surface; the form is the human boundary between.

It never crosses the human line. Publishing a draft is a durable internal
record; the external submit to ACM stays a human decision.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import actors
import atomic_forms as af
import evidence_binding as eb
import opportunity as opp

CMS = Path.home() / ".bonfyre" / "bin" / "bonfyre-cms"
CMS_SCHEMAS = Path("/Users/nickgonzales/Documents/Bonfyre/cmd/BonfyreCMS/content-types")


@dataclass(frozen=True)
class LoopResult:
    goal: str
    form_ready: bool
    blockers: tuple[str, ...]
    published_entry: Optional[int]
    detail: str


def _fabric_digests(fabric_db: Path, n: int) -> list[str]:
    if not fabric_db.exists():
        return []
    con = sqlite3.connect(str(fabric_db))
    rows = [r[0] for r in con.execute("SELECT digest FROM artifacts LIMIT ?", (n,))]
    con.close()
    return rows


def _fabric_candidates(fabric_db: Path, limit: int = 500) -> list[eb.EvidenceCandidate]:
    """Read fabric artifacts as binding candidates, tolerant of the schema.

    The fabric's artifact table carries a digest and, in richer builds, a kind
    and a name/path; this reads whatever is present so a slot can be matched to
    the right artifact instead of the first one."""
    if not fabric_db.exists():
        return []
    con = sqlite3.connect(str(fabric_db))
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(artifacts)")}
        if "digest" not in cols:
            return []
        name_col = next((c for c in ("name", "path", "source", "title") if c in cols), None)
        kind_col = next((c for c in ("kind", "type", "content_type") if c in cols), None)
        select = ["digest"]
        select.append(name_col or "''")
        select.append(kind_col or "''")
        rows = con.execute(f"SELECT {', '.join(select)} FROM artifacts LIMIT ?", (limit,)).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()
    return [eb.EvidenceCandidate(digest=r[0], name=r[1] or "", kind=r[2] or "") for r in rows]


def _publish_cms(article: dict, cms_db: Path) -> Optional[int]:
    """Publish the compiled package as a real cms_article entry."""
    if not CMS.exists():
        return None
    subprocess.run([str(CMS), "schema", "migrate", "--db", str(cms_db),
                    "--schemas", str(CMS_SCHEMAS)], capture_output=True, text=True, timeout=30)
    subprocess.run([str(CMS), "entry", "create", "cms_article", json.dumps(article),
                    "--db", str(cms_db), "--schemas", str(CMS_SCHEMAS)],
                   capture_output=True, text=True, timeout=30)
    listing = subprocess.run(
        [str(CMS), "entry", "list", "cms_article", "--db", str(cms_db),
         "--schemas", str(CMS_SCHEMAS)],
        capture_output=True, text=True, timeout=30,
    )
    for line in listing.stdout.splitlines():
        line = line.strip().rstrip(",")
        if line.startswith("{") and article["external_id"] in line:
            try:
                return json.loads(line)["id"]
            except (ValueError, KeyError):
                continue
    return None


def run(
    form: af.AtomicForm,
    *,
    control_db: Path,
    fabric_db: Path,
    cms_db: Path,
    evidence_from_fabric: bool = True,
) -> LoopResult:
    """Metabolize one goal: autofill, fill evidence, compile, publish if ready."""
    actors_con = sqlite3.connect(str(control_db))
    af.autofill(form, actors_con)
    actors_con.close()

    if evidence_from_fabric:
        needed = [s for s in form.evidence if not (s.artifact_digest or s.from_twin)]
        # Slots with matchers are bound to the RIGHT artifact by EvidenceBindingGraph,
        # a semantic test with a witness -- not by position. Matcher-less slots keep
        # the legacy positional fill for backward compatibility.
        matched = [s for s in needed if s.has_matcher]
        legacy = [s for s in needed if not s.has_matcher]
        if matched:
            candidates = _fabric_candidates(fabric_db)
            reqs = [eb.EvidenceRequirement(
                req_id=s.name, kind=s.match_kind,
                name_contains=tuple(s.match_name), tags=tuple(s.match_tags),
            ) for s in matched]
            result = eb.bind_all(reqs, candidates)
            by_name = {s.name: s for s in matched}
            for req_id, binding in result.bound.items():
                by_name[req_id].artifact_digest = binding.digest
        if legacy:
            digests = _fabric_digests(fabric_db, len(legacy))
            for slot, digest in zip(legacy, digests):
                slot.artifact_digest = digest

    readiness = af.submit_ready(form)
    if not readiness.ready:
        return LoopResult(form.form_id, False, readiness.blockers, None,
                         f"{readiness.blocker_count} blockers; not published")

    title = next((f.value for f in form.fields if f.name in ("article_title", "title")), form.title)
    article = {
        "title": title, "status": "draft",
        "external_id": form.form_id, "idempotency_key": form.form_id,
        "source_site": "bonfyre-atomic-form", "source_post_id": form.form_id,
        "source_post_type": "outline", "source_event": "atomic_form.submit_ready",
        "source_schema_version": "1", "source_received_at": "2026-08-15",
    }
    entry_id = _publish_cms(article, cms_db)
    return LoopResult(
        form.form_id, True, (), entry_id,
        f"published as cms_article entry {entry_id}" if entry_id else "ready but publish failed",
    )


@dataclass(frozen=True)
class OpportunityLoopResult:
    opp_id: str
    status: str
    drove: bool
    loop: Optional[LoopResult]
    reason: str


def drive_reachable(
    opportunities: list,
    unlocks: list,
    forms: dict,
    *,
    control_db: Path,
    fabric_db: Path,
    cms_db: Path,
) -> list[OpportunityLoopResult]:
    """Let reachability decide what the pipeline pursues.

    The opportunity engine computes, over real state, which opportunities are
    reachable now. Only those drive the AtomicForm -> submit-ready -> CMS-draft
    loop. A blocked or merely-unlockable opportunity is refused with its hard
    blockers named -- so the celld IP authority, the ACM verification, the
    maintainer merge each hold the line exactly where they should. The loop still
    stops at a draft; reachable capacity is pursued, never auto-committed past the
    human boundary.
    """
    con = sqlite3.connect(str(control_db))
    evals = opp.reachable_capacity(con, opportunities, unlocks)
    con.close()

    out: list[OpportunityLoopResult] = []
    for o in opportunities:
        ev = evals[o.opp_id]
        if ev.status != opp.REACHABLE_NOW:
            hard = ", ".join(b.kind for b in ev.hard_blockers) or ev.status
            out.append(OpportunityLoopResult(o.opp_id, ev.status, False, None,
                                             f"not driven ({ev.status}): {hard}"))
            continue
        form = forms.get(o.opp_id)
        if form is None:
            out.append(OpportunityLoopResult(o.opp_id, ev.status, False, None,
                                             "reachable but no form bound"))
            continue
        lr = run(form, control_db=control_db, fabric_db=fabric_db, cms_db=cms_db)
        out.append(OpportunityLoopResult(o.opp_id, ev.status, True, lr, lr.detail))
    return out
