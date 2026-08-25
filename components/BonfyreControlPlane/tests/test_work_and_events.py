"""Tests for the queue lifecycle, the occurrence spine and the capability catalog.

Same convention as test_control_plane.py: each test names the Run 6 incident it
makes impossible.
"""

import datetime as dt
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import capability_catalog as cc  # noqa: E402
import external_events as ee  # noqa: E402
import work_graph as wg  # noqa: E402

UTC = dt.timezone.utc

PLANES = (
    "run1_capital_conversion",
    "run2_relationship_adoption",
    "run3_four_economies_campaign",
    "run4_institutional_fabric",
    "run5_recursive_external",
)


def at(day: int, hour: int = 0, minute: int = 0) -> dt.datetime:
    return dt.datetime(2026, 8, day, hour, minute, tzinfo=UTC)


def fresh_work_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    wg.ensure_schema(db)
    for plane in PLANES:
        wg.register_plane(db, plane, now=at(13))
    return db


class PlaneTypingTests(unittest.TestCase):
    def test_the_coordinator_item_is_refused_at_the_boundary(self):
        """Run 6 accumulated one item aimed at a plane called 'coordinator'
        that never existed. The governor could only report invalid_targets=1
        after the fact; it should have been impossible to insert."""
        db = fresh_work_db()
        with self.assertRaises(wg.UnknownPlane):
            wg.enqueue(
                db,
                source_plane="run5_recursive_external",
                target_plane="coordinator",
                item_kind="native_catalog_absorption",
                subject_ref="native_catalog_absorption",
                priority=1200,
            )
        self.assertEqual(wg.open_count(db), 0)

    def test_a_registered_plane_accepts_work(self):
        db = fresh_work_db()
        item_id = wg.enqueue(
            db,
            source_plane="run5_recursive_external",
            target_plane="run2_relationship_adoption",
            item_kind="recursive_REL",
            subject_ref="bernstein",
            now=at(13),
        )
        self.assertIsNotNone(item_id)
        self.assertEqual(wg.open_count(db, "run2_relationship_adoption"), 1)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.db = fresh_work_db()
        self.item = wg.enqueue(
            self.db,
            source_plane="run2_relationship_adoption",
            target_plane="run5_recursive_external",
            item_kind="system_absorption",
            subject_ref="firecrawl-routing",
            source_ref="receipt:41",
            now=at(13),
        )

    def test_work_can_actually_be_closed(self):
        """The defect: 242 rows, 242 open, 0 closed, 0 consumed."""
        claimed = wg.claim(self.db, "run5_recursive_external", now=at(13, 1))
        self.assertEqual([c.id for c in claimed], [self.item])

        wg.mark_effected(self.db, self.item, "run5_recursive_external", now=at(13, 2))
        wg.satisfy(
            self.db, self.item, "run5_recursive_external", "receipt:88", now=at(13, 3)
        )

        self.assertEqual(wg.get(self.db, self.item).state, wg.SATISFIED)
        self.assertEqual(wg.open_count(self.db), 0)

    def test_satisfying_requires_a_receipt(self):
        wg.claim(self.db, "run5_recursive_external", now=at(13, 1))
        wg.mark_effected(self.db, self.item, "run5", now=at(13, 2))
        with self.assertRaises(ValueError):
            wg.satisfy(self.db, self.item, "run5", "", now=at(13, 3))

    def test_illegal_transitions_raise_instead_of_silently_stalling(self):
        with self.assertRaises(wg.IllegalTransition):
            wg.satisfy(self.db, self.item, "run5", "receipt:1", now=at(13, 1))

    def test_a_terminal_item_is_never_handed_out_again(self):
        wg.invalidate(self.db, self.item, "governor", "no longer relevant", now=at(13, 1))
        self.assertEqual(wg.get(self.db, self.item).state, wg.INVALIDATED)
        self.assertEqual(wg.claim(self.db, "run5_recursive_external", now=at(13, 2)), [])
        with self.assertRaises(wg.IllegalTransition):
            wg.unblock(self.db, self.item, "run5", now=at(13, 3))

    def test_a_lease_from_a_killed_plane_returns_the_work(self):
        """Run 6 lost planes to pipe deadlock, rc137 and ENOSPC. Work claimed by
        a dead plane must not vanish."""
        wg.claim(
            self.db,
            "run5_recursive_external",
            lease=dt.timedelta(minutes=30),
            now=at(13, 1),
        )
        self.assertEqual(wg.get(self.db, self.item).state, wg.LEASED)

        reaped = wg.reap_expired_leases(self.db, now=at(13, 1, 40))
        self.assertEqual(reaped, 1)
        self.assertEqual(wg.get(self.db, self.item).state, wg.OPEN)

    def test_blocked_external_work_can_come_back(self):
        wg.claim(self.db, "run5_recursive_external", now=at(13, 1))
        wg.block_external(self.db, self.item, "run5", "waiting on maintainer", now=at(13, 2))
        self.assertEqual(wg.get(self.db, self.item).state, wg.BLOCKED_EXTERNAL)
        wg.unblock(self.db, self.item, "watcher", now=at(14))
        self.assertEqual(wg.get(self.db, self.item).state, wg.OPEN)

    def test_duplicate_fan_out_does_not_reopen_the_same_consequence(self):
        """185 of the 242 rows were generic receipt_recursion duplicates."""
        again = wg.enqueue(
            self.db,
            source_plane="run2_relationship_adoption",
            target_plane="run5_recursive_external",
            item_kind="system_absorption",
            subject_ref="firecrawl-routing",
            source_ref="receipt:41",
            now=at(13, 5),
        )
        self.assertIsNone(again)
        self.assertEqual(wg.open_count(self.db), 1)

    def test_transitions_are_journalled(self):
        wg.claim(self.db, "run5_recursive_external", now=at(13, 1))
        wg.mark_effected(self.db, self.item, "run5", now=at(13, 2))
        wg.satisfy(self.db, self.item, "run5", "receipt:88", now=at(13, 3))
        moves = self.db.execute(
            "SELECT from_state,to_state FROM work_transitions WHERE item_id=? ORDER BY id",
            (self.item,),
        ).fetchall()
        self.assertEqual(
            moves,
            [("", "open"), ("open", "leased"), ("leased", "effected"), ("effected", "satisfied")],
        )


class ExternalEventTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        ee.ensure_schema(self.db)

    def test_the_wandb_decline_survives_a_dead_epoch(self):
        """The Run 6 loss: the agent read the W&B decline, then disk exhaustion
        killed the epoch before campaign tables were updated. The freeze still
        says 'delivered / waiting_on Weights & Biases'."""
        event_id = ee.observe(
            self.db,
            source="gmail",
            actor="Weights & Biases",
            event_kind=ee.DECLINED,
            subject_ref="wandb-credits-ask",
            observed_at=at(13, 22, 10),
            evidence_ref="gmail:thread-xyz",
        )
        self.assertIsNotNone(event_id)

        # Epoch dies here. Nothing else ran. The observation is still durable.
        pending = ee.unprojected(self.db)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].actor, "Weights & Biases")

        applied = {}
        projected = ee.project(
            self.db, lambda actor, status, _event: applied.__setitem__(actor, status)
        )
        self.assertEqual(projected, 1)
        self.assertEqual(applied["Weights & Biases"], "declined")
        self.assertEqual(ee.unprojected(self.db), [])

    def test_re_reading_the_same_inbox_does_not_double_count(self):
        for _ in range(3):
            ee.observe(
                self.db,
                source="gmail",
                actor="Hugging Face",
                event_kind=ee.ACKNOWLEDGED,
                subject_ref="ticket-42221",
                observed_at=at(13, 21),
            )
        self.assertEqual(len(ee.unprojected(self.db)), 1)

    def test_projection_is_replayable_after_a_crash(self):
        ee.observe(
            self.db,
            source="gmail",
            actor="LangChain",
            event_kind=ee.REDIRECTED,
            subject_ref="langchain-ask",
            observed_at=at(13, 21, 30),
        )

        def explode(_actor, _status, _event):
            raise RuntimeError("disk full")

        with self.assertRaises(RuntimeError):
            ee.project(self.db, explode)
        # Not marked projected, so a later run still sees it.
        self.assertEqual(len(ee.unprojected(self.db)), 1)

        seen = {}
        ee.project(self.db, lambda a, s, _e: seen.__setitem__(a, s))
        self.assertEqual(seen["LangChain"], "redirected")

    def test_unknown_event_kinds_are_refused(self):
        with self.assertRaises(ValueError):
            ee.observe(self.db, source="gmail", actor="x", event_kind="vibes")

    def test_every_declared_kind_projects(self):
        """A kind with no projection used to be marked projected while doing
        nothing. That dropped a GitHub `assign` notification for an issue
        actually assigned to us, between the spine and the work queue."""
        seen = []
        for index, kind in enumerate(sorted(ee.EVENT_KINDS)):
            ee.observe(
                self.db,
                source="github",
                actor=f"actor-{index}",
                event_kind=kind,
                subject_ref=f"subject-{index}",
                observed_at=at(13, 1, index),
            )
        projected = ee.project(
            self.db, lambda actor, status, event: seen.append((event.event_kind, status))
        )
        self.assertEqual(projected, len(ee.EVENT_KINDS))
        self.assertEqual(
            len(seen),
            len(ee.EVENT_KINDS),
            "every occurrence must reach the projection callback",
        )


class CommitmentLedgerTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        ee.ensure_schema(self.db)

    def test_pipeline_and_cash_never_merge(self):
        """Run 6 carried $158,540 face value against $0 realized. Those are
        different kinds of fact; one number describing both describes nothing."""
        ee.record_commitment(
            self.db, actor="Runpod", category=ee.QUALIFIED_ASK, amount_usd=7500
        )
        ee.record_commitment(
            self.db, actor="Modal", category=ee.QUALIFIED_ASK, amount_usd=7500
        )
        ee.record_commitment(
            self.db, actor="FUTO", category=ee.SUBMITTED_GRANT, amount_usd=20000
        )

        totals = ee.ledger_totals(self.db)
        self.assertEqual(totals[ee.QUALIFIED_ASK], 15000)
        self.assertEqual(totals[ee.SUBMITTED_GRANT], 20000)
        self.assertEqual(totals[ee.REALIZED_CASH], 0.0)
        self.assertFalse(hasattr(ee, "total"), "there must be no combined total")

    def test_unknown_categories_are_refused(self):
        with self.assertRaises(ValueError):
            ee.record_commitment(self.db, actor="x", category="value", amount_usd=1)


class CapabilityCatalogTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        cc.ensure_schema(self.db)

    def test_declared_identity_is_not_callable_capability(self):
        """91 public command identities, 0 compiled tools."""
        cc.declare(self.db, cc.Capability("BonfyreQueue", maturity="defined"))
        cc.declare(self.db, cc.Capability("BonfyreOffer", maturity="implemented"))
        summary = cc.summary(self.db)
        self.assertEqual(summary["declared"], 2)
        self.assertEqual(summary["callable"], 0)

    def test_probing_promotes_only_what_resolves(self):
        cc.declare(self.db, cc.Capability("BonfyreQueue", maturity="defined"))
        cc.declare(self.db, cc.Capability("BonfyreOffer", maturity="defined"))
        resolver = lambda name: "/usr/local/bin/BonfyreQueue" if name == "BonfyreQueue" else None  # noqa: E731

        counts = cc.probe(self.db, resolver=resolver, now=at(14))
        self.assertEqual(counts, {"resolved": 1, "unresolved": 1})
        self.assertEqual(cc.summary(self.db)["callable"], 1)

    def test_a_vanished_binary_is_demoted_not_left_claiming_callable(self):
        cc.declare(self.db, cc.Capability("BonfyreQueue", maturity="activated"))
        cc.probe(self.db, resolver=lambda _name: None, now=at(14))
        self.assertEqual(cc.summary(self.db)["callable"], 0)

    def test_help_working_does_not_imply_proven(self):
        cc.declare(self.db, cc.Capability("BonfyreQueue", maturity="resolvable"))
        capability = cc.load(self.db)[0]
        self.assertTrue(capability.callable_now)
        self.assertFalse(capability.proven)

    def test_context_packet_names_what_not_to_probe_for(self):
        cc.declare(
            self.db,
            cc.Capability("BonfyreModel", maturity="workload_proven", location="/opt/bf/BonfyreModel"),
        )
        cc.declare(self.db, cc.Capability("BonfyreOffer", maturity="defined"))
        packet = cc.context_packet(self.db)
        self.assertIn("BonfyreModel", packet)
        self.assertIn("proven", packet)
        self.assertIn("DECLARED BUT NOT CALLABLE", packet)
        self.assertIn("BonfyreOffer", packet)

    def test_unknown_maturity_is_refused(self):
        with self.assertRaises(ValueError):
            cc.Capability("BonfyreQueue", maturity="probably_fine")


if __name__ == "__main__":
    unittest.main(verbosity=2)
