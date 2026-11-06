#!/usr/bin/env python3
"""Emit language-neutral replay vectors for the control kernel.

The Python modules in components/BonfyreControlPlane are a reference
implementation, not the runtime. Their value after freezing is that they define
what the native kernel must do. These vectors are that definition in a form Zig
can consume: an initial state, an ordered event sequence, and the exact state
the fold must produce.

Every vector carries `derived_from` naming the Run 6 incident it encodes, so a
native implementation that fails one knows which failure it just reintroduced.

    python3 generate_vectors.py           # write vectors/*.json
    python3 generate_vectors.py --check    # replay them against the reference
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

REFERENCE = Path(__file__).resolve().parents[3] / "components/BonfyreControlPlane"
sys.path.insert(0, str(REFERENCE))

import capability_catalog as cc  # noqa: E402
import external_events as ee  # noqa: E402
import provider_state as ps  # noqa: E402
import resource_admission as ra  # noqa: E402
import scheduling as sch  # noqa: E402
import work_graph as wg  # noqa: E402

UTC = dt.timezone.utc
VECTOR_DIR = Path(__file__).resolve().parent / "vectors"

CODEX_LIMIT = (
    "You've hit your usage limit. Upgrade to Pro "
    "(https://chatgpt.com/explore/pro), visit "
    "https://chatgpt.com/codex/settings/usage to purchase more credits or "
    "try again at Aug 19th, 2026 10:53 PM."
)
CLAUDE_LIMIT = "session limit · resets 7:20pm (America/Chicago)"


def ts(day: int, hour: int = 0, minute: int = 0, second: int = 0) -> str:
    return dt.datetime(2026, 8, day, hour, minute, second, tzinfo=UTC).isoformat()


def parse(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text)


# --------------------------------------------------------------- definitions

PROVIDER_VECTORS = [
    {
        "name": "hard_capacity_survives_a_later_transient",
        "derived_from": "run6: codex relaunched at 22:43 into a window resetting Aug 19",
        "provider": "codex",
        "events": [
            {"kind": "hard_capacity", "at": ts(13, 22, 23, 23), "raw": CODEX_LIMIT},
            {"kind": "transient_failure", "at": ts(13, 22, 30), "detail": "local pipe failure"},
        ],
        "query_at": ts(13, 22, 43, 48),
        "expect": {"status": "capacity_exhausted", "circuit_until": ts(19, 22, 53), "available": False},
    },
    {
        "name": "success_does_not_clear_capacity",
        "derived_from": "run6: a served response is not returned capacity",
        "provider": "codex",
        "events": [
            {"kind": "hard_capacity", "at": ts(13, 22), "raw": CODEX_LIMIT},
            {"kind": "success", "at": ts(13, 23)},
        ],
        "query_at": ts(14),
        "expect": {"status": "capacity_exhausted", "circuit_until": ts(19, 22, 53), "available": False},
    },
    {
        "name": "capacity_returns_at_the_stated_reset",
        "derived_from": "run6: circuit length was guessed because the reset was discarded",
        "provider": "codex",
        "events": [{"kind": "hard_capacity", "at": ts(13, 22), "raw": CODEX_LIMIT}],
        "query_at": ts(19, 22, 54),
        "expect": {"status": "ready", "circuit_until": None, "available": True},
    },
    {
        "name": "claude_wall_clock_reset_is_recovered",
        "derived_from": "run6: claude states a local time with no date",
        "provider": "claude",
        "events": [{"kind": "hard_capacity", "at": ts(13, 22, 23), "raw": CLAUDE_LIMIT}],
        "query_at": ts(13, 23),
        "expect": {"status": "capacity_exhausted", "circuit_until": ts(14, 0, 20), "available": False},
    },
    {
        "name": "manual_pause_needs_explicit_resume",
        "derived_from": "run6 freeze: providers manually paused",
        "provider": "codex",
        "events": [{"kind": "manual_pause", "at": ts(13, 22, 46), "detail": "run6 freeze"}],
        "query_at": ts(25),
        "expect": {"status": "manual_pause", "circuit_until": None, "available": False},
    },
]

ADMISSION_VECTORS = [
    {
        "name": "celld_clone_refused_without_headroom",
        "derived_from": "run6: ENOSPC while building the 316-crate celld workspace",
        "policy": {"floor": 10, "quota": 20, "max_grant": 40},
        "free_gib": 18,
        "grants": [],
        "request": {"plane": "run5", "gib": 15},
        "expect": {"verdict": "reject"},
    },
    {
        "name": "concurrent_planes_cannot_double_spend",
        "derived_from": "run6: five planes each observed the same free bytes",
        "policy": {"floor": 10, "quota": 20, "max_grant": 40},
        "free_gib": 40,
        "grants": [{"plane": "run1", "gib": 12}, {"plane": "run2", "gib": 12}],
        "request": {"plane": "run3", "gib": 12},
        "expect": {"verdict": "defer"},
    },
    {
        "name": "defer_when_waiting_could_help",
        "derived_from": "run6: no distinction between not-now and never",
        "policy": {"floor": 10, "quota": 20, "max_grant": 40},
        "free_gib": 40,
        "grants": [{"plane": "run1", "gib": 20}],
        "request": {"plane": "run2", "gib": 15},
        "expect": {"verdict": "defer"},
    },
    {
        "name": "admit_with_real_headroom",
        "derived_from": "baseline",
        "policy": {"floor": 10, "quota": 20, "max_grant": 40},
        "free_gib": 40,
        "grants": [],
        "request": {"plane": "run5", "gib": 15},
        "expect": {"verdict": "admit"},
    },
]

WORK_VECTORS = [
    {
        "name": "work_reaches_satisfied",
        "derived_from": "run6: 242 rows, 242 open, 0 closed",
        "steps": [
            {"op": "enqueue", "target": "run5_recursive_external", "subject": "s1"},
            {"op": "claim", "plane": "run5_recursive_external", "at": ts(13, 1)},
            {"op": "effected", "at": ts(13, 2)},
            {"op": "satisfy", "receipt": "receipt:88", "at": ts(13, 3)},
        ],
        "expect": {"state": "satisfied", "open_count": 0},
    },
    {
        "name": "unroutable_work_is_refused",
        "derived_from": "run6: an item targeting a plane called coordinator",
        "steps": [{"op": "enqueue", "target": "coordinator", "subject": "native_catalog_absorption"}],
        "expect": {"error": "UnknownPlane", "open_count": 0},
    },
    {
        "name": "expired_lease_returns_the_work",
        "derived_from": "run6: planes lost to pipe deadlock, rc137 and ENOSPC",
        "steps": [
            {"op": "enqueue", "target": "run5_recursive_external", "subject": "s1"},
            {"op": "claim", "plane": "run5_recursive_external", "at": ts(13, 1), "lease_minutes": 30},
            {"op": "reap", "at": ts(13, 1, 40)},
        ],
        "expect": {"state": "open", "open_count": 1},
    },
    {
        "name": "illegal_transition_raises",
        "derived_from": "run6: silent stalls instead of errors",
        "steps": [
            {"op": "enqueue", "target": "run5_recursive_external", "subject": "s1"},
            {"op": "satisfy", "receipt": "receipt:1", "at": ts(13, 1)},
        ],
        "expect": {"error": "IllegalTransition"},
    },
]

COOLING_VECTORS = [
    {
        "name": "static_blocker_leaves_the_context_cut",
        "derived_from": "run6: proton re-read on every pass while never changing",
        "reheat_on": "bridge credentials appear",
        "unchanged_checks": 3,
        "expect": {"attention": "cool", "in_frontier": False},
    },
    {
        "name": "cooled_watch_reheats_on_its_signal",
        "derived_from": "run6: no way to express reheat",
        "reheat_on": "bridge credentials appear",
        "unchanged_checks": 3,
        "reheat": True,
        "expect": {"attention": "hot", "in_frontier": True},
    },
    {
        "name": "no_condition_stays_warm_rather_than_vanishing",
        "derived_from": "cooling without a way back is forgetting",
        "reheat_on": "",
        "unchanged_checks": 5,
        "expect": {"attention": "warm", "in_frontier": True},
    },
]


# ------------------------------------------------------------------ replay


def replay_provider(vector: dict) -> dict:
    db = sqlite3.connect(":memory:")
    ps.ensure_schema(db)
    for event in vector["events"]:
        reset = None
        kind = event["kind"]
        if "raw" in event:
            kind, reset = ps.classify_failure(event["raw"], now=parse(event["at"]))
        ps.record(
            db,
            vector["provider"],
            kind,
            observed_at=parse(event["at"]),
            reset_at=reset,
            detail=event.get("detail", ""),
        )
    now = parse(vector["query_at"])
    state = ps.current_state(db, vector["provider"], now=now)
    return {
        "status": state.status,
        "circuit_until": state.circuit_until.isoformat() if state.circuit_until else None,
        "available": state.available_at(now),
    }


def replay_admission(vector: dict) -> dict:
    db = sqlite3.connect(":memory:")
    ra.ensure_schema(db)
    policy = ra.AdmissionPolicy(
        protected_floor_bytes=vector["policy"]["floor"] * ra.GIB,
        per_plane_quota_bytes=vector["policy"]["quota"] * ra.GIB,
        max_grant_bytes=vector["policy"]["max_grant"] * ra.GIB,
    )
    probe = lambda _v: vector["free_gib"] * ra.GIB  # noqa: E731
    for grant in vector["grants"]:
        ra.request_grant(
            db,
            ra.ResourceRequest(grant["plane"], "build", grant["gib"] * ra.GIB),
            policy,
            probe=probe,
        )
    decision, _ = ra.request_grant(
        db,
        ra.ResourceRequest(
            vector["request"]["plane"], "build", vector["request"]["gib"] * ra.GIB
        ),
        policy,
        probe=probe,
    )
    return {"verdict": decision.verdict}


PLANES = (
    "run1_capital_conversion",
    "run2_relationship_adoption",
    "run3_four_economies_campaign",
    "run4_institutional_fabric",
    "run5_recursive_external",
)


def replay_work(vector: dict) -> dict:
    db = sqlite3.connect(":memory:")
    wg.ensure_schema(db)
    for plane in PLANES:
        wg.register_plane(db, plane, now=parse(ts(13)))
    item_id = None
    try:
        for step in vector["steps"]:
            op = step["op"]
            if op == "enqueue":
                item_id = wg.enqueue(
                    db,
                    source_plane="run2_relationship_adoption",
                    target_plane=step["target"],
                    item_kind="vector",
                    subject_ref=step["subject"],
                    now=parse(ts(13)),
                )
            elif op == "claim":
                kwargs = {}
                if "lease_minutes" in step:
                    kwargs["lease"] = dt.timedelta(minutes=step["lease_minutes"])
                wg.claim(db, step["plane"], now=parse(step["at"]), **kwargs)
            elif op == "effected":
                wg.mark_effected(db, item_id, "vector", now=parse(step["at"]))
            elif op == "satisfy":
                wg.satisfy(db, item_id, "vector", step["receipt"], now=parse(step["at"]))
            elif op == "reap":
                wg.reap_expired_leases(db, now=parse(step["at"]))
    except wg.UnknownPlane:
        return {"error": "UnknownPlane", "open_count": wg.open_count(db)}
    except wg.IllegalTransition:
        return {"error": "IllegalTransition"}
    result = {"open_count": wg.open_count(db)}
    if item_id is not None:
        result["state"] = wg.get(db, item_id).state
    return result


def replay_cooling(vector: dict) -> dict:
    db = sqlite3.connect(":memory:")
    sch.ensure_schema(db)
    now = parse(ts(14))
    sch.schedule(
        db, "w", "subject", temperature=sch.WARM, reheat_on=vector["reheat_on"], now=now
    )
    for _ in range(vector["unchanged_checks"]):
        sch.record_check(db, "w", changed=False, now=now)
    if vector.get("reheat"):
        sch.reheat(db, "w", now=now)
    temperature = db.execute("SELECT temperature FROM watches WHERE watch_id='w'").fetchone()[0]
    return {
        "attention": temperature,
        "in_frontier": "subject" in sch.frontier_subjects(db, now=now),
    }


SUITES = {
    "provider": (PROVIDER_VECTORS, replay_provider),
    "admission": (ADMISSION_VECTORS, replay_admission),
    "work": (WORK_VECTORS, replay_work),
    "cooling": (COOLING_VECTORS, replay_cooling),
}


def epoch_ms(iso: str) -> int:
    return int(parse(iso).timestamp() * 1000)


def emit_flat(path: Path) -> int:
    """Emit the same vectors as flat records the C runner reads directly.

    A JSON parser in the kernel's test harness would be a dependency the kernel
    does not otherwise need, and the vectors are simple enough that a flat form
    loses nothing. JSON stays for anything else that wants to consume them.
    """
    lines: list[str] = ["# generated by generate_vectors.py -- do not edit"]
    count = 0

    for vector in PROVIDER_VECTORS:
        lines.append(f"provider {vector['name']}")
        lines.append(f"  derived_from {vector['derived_from']}")
        for event in vector["events"]:
            kind = event["kind"]
            reset = 0
            if "raw" in event:
                classified, parsed_reset = ps.classify_failure(
                    event["raw"], now=parse(event["at"])
                )
                kind = classified
                reset = epoch_ms(parsed_reset.isoformat()) if parsed_reset else 0
            lines.append(f"  event {kind} {epoch_ms(event['at'])} {reset}")
        lines.append(f"  now {epoch_ms(vector['query_at'])}")
        expect = vector["expect"]
        circuit = epoch_ms(expect["circuit_until"]) if expect["circuit_until"] else 0
        lines.append(
            f"  expect {expect['status']} {circuit} {1 if expect['available'] else 0}"
        )
        count += 1

    for vector in ADMISSION_VECTORS:
        lines.append(f"admission {vector['name']}")
        lines.append(f"  derived_from {vector['derived_from']}")
        policy = vector["policy"]
        lines.append(
            f"  policy {policy['floor']} {policy['quota']} {policy['max_grant']}"
        )
        committed = sum(g["gib"] for g in vector["grants"])
        plane_committed = sum(
            g["gib"] for g in vector["grants"] if g["plane"] == vector["request"]["plane"]
        )
        lines.append(
            f"  request {vector['request']['gib']} {vector['free_gib']} "
            f"{committed} {plane_committed}"
        )
        lines.append(f"  expect {vector['expect']['verdict']}")
        count += 1

    for vector in COOLING_VECTORS:
        lines.append(f"cooling {vector['name']}")
        lines.append(f"  derived_from {vector['derived_from']}")
        lines.append(f"  has_condition {1 if vector['reheat_on'] else 0}")
        lines.append(f"  unchanged {vector['unchanged_checks']}")
        lines.append(f"  reheat {1 if vector.get('reheat') else 0}")
        expected = vector["expect"]["attention"]
        lines.append(
            f"  expect {expected} {1 if vector['expect']['in_frontier'] else 0}"
        )
        count += 1

    # Reset parsing is part of the contract and is exercised directly, because a
    # provider that states a reset the kernel cannot read falls back to guessing.
    for name, raw, expected_iso in (
        ("codex_ordinal_date", CODEX_LIMIT, ts(19, 22, 53)),
        ("iso_reset", "session limit; resets 2026-08-19T22:53:00Z", ts(19, 22, 53)),
        ("no_reset_present", "quota exhausted", None),
    ):
        lines.append(f"reset {name}")
        lines.append(f"  text {raw}")
        lines.append(f"  expect {epoch_ms(expected_iso) if expected_iso else 0}")
        count += 1

    path.write_text("\n".join(lines) + "\n")
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="replay instead of writing")
    args = parser.parse_args()

    VECTOR_DIR.mkdir(parents=True, exist_ok=True)
    failures = 0
    total = 0

    for suite, (vectors, replay) in SUITES.items():
        for vector in vectors:
            total += 1
            actual = replay(vector)
            expected = vector["expect"]
            if actual != expected:
                failures += 1
                print(f"MISMATCH {suite}/{vector['name']}")
                print(f"  expected {expected}")
                print(f"  actual   {actual}")
        if not args.check:
            path = VECTOR_DIR / f"{suite}.json"
            path.write_text(json.dumps({"suite": suite, "vectors": vectors}, indent=2) + "\n")

    if not args.check:
        flat = emit_flat(VECTOR_DIR / "control.vec")
        print(f"{flat} flat records written to vectors/control.vec")

    verb = "checked" if args.check else "written"
    print(f"{total} vectors {verb}, {failures} mismatches")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
