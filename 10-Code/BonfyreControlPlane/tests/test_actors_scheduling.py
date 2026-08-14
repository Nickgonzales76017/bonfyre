"""Tests for the actor graph, attention scheduling, and the Tarbell seed."""

import datetime as dt
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import actors  # noqa: E402
import pack_loader  # noqa: E402
import scheduling as sch  # noqa: E402
import work_graph as wg  # noqa: E402

UTC = dt.timezone.utc
AUG14 = dt.datetime(2026, 8, 14, tzinfo=UTC)
PACKS = ROOT.parents[1] / "packs"
TARBELL_ORG = "org:tarbell-center"


class ActorGraphTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        actors.ensure_schema(self.db)

    def test_provenance_is_mandatory(self):
        """A CRM that cannot tell 'read it on their site' from 'a model guessed'
        degrades into confident nonsense within a few autonomous passes."""
        with self.assertRaises(ValueError):
            actors.Actor("person:x", actors.PERSON, "X", provenance="")

    def test_edge_kinds_stay_distinct(self):
        """Second law of the operating model: collapsing edge meaning is a
        named future error."""
        actors.upsert_actor(
            self.db, actors.Actor("org:a", actors.ORGANIZATION, "A", provenance="test")
        )
        actors.upsert_actor(
            self.db,
            actors.Actor("person:b", actors.PERSON, "B", org_id="org:a", provenance="test"),
        )
        actors.add_edge(self.db, "org:a", actors.EMPLOYS, "person:b", provenance="test")
        actors.add_edge(
            self.db, "person:b", actors.AUTHORITY_OVER, "org:a", provenance="test"
        )
        self.assertEqual(len(actors.neighbours(self.db, "person:b")), 2)
        self.assertEqual(
            len(actors.neighbours(self.db, "person:b", actors.EMPLOYS)), 1
        )

    def test_untyped_edges_are_refused(self):
        actors.upsert_actor(
            self.db, actors.Actor("org:a", actors.ORGANIZATION, "A", provenance="test")
        )
        with self.assertRaises(ValueError):
            actors.add_edge(self.db, "org:a", "related_to", "org:a", provenance="test")

    def test_unverified_actors_are_listable_before_outreach(self):
        actors.upsert_actor(
            self.db,
            actors.Actor(
                "person:c", actors.PERSON, "C", provenance="brief", confidence=actors.ASSERTED
            ),
        )
        actors.upsert_actor(
            self.db,
            actors.Actor(
                "person:d", actors.PERSON, "D", provenance="site", confidence=actors.VERIFIED
            ),
        )
        self.assertEqual(actors.unverified(self.db), ["person:c"])


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        sch.ensure_schema(self.db)

    def test_a_static_blocker_stops_riding_in_the_frontier_cut(self):
        """Proton never obtained transport and consumed frontier attention on
        every pass anyway."""
        sch.schedule(
            self.db,
            "proton-inbox",
            "Proton inbox transport",
            temperature=sch.WARM,
            reheat_on="bridge credentials appear or port changes",
        )
        self.assertIn("Proton inbox transport", sch.frontier_subjects(self.db, now=AUG14))

        for _ in range(sch.COOL_AFTER_UNCHANGED_ATTEMPTS):
            temperature = sch.record_check(self.db, "proton-inbox", changed=False, now=AUG14)
        self.assertEqual(temperature, sch.COOL)
        self.assertNotIn(
            "Proton inbox transport", sch.frontier_subjects(self.db, now=AUG14)
        )

    def test_a_cooled_watch_reheats_when_its_signal_arrives(self):
        sch.schedule(
            self.db,
            "proton-inbox",
            "Proton inbox transport",
            temperature=sch.COOL,
            reheat_on="credentials appear",
        )
        self.assertNotIn("Proton inbox transport", sch.frontier_subjects(self.db, now=AUG14))
        sch.reheat(self.db, "proton-inbox", now=AUG14)
        self.assertIn("Proton inbox transport", sch.frontier_subjects(self.db, now=AUG14))

    def test_a_change_resets_attention_to_hot(self):
        sch.schedule(self.db, "w", "subject", reheat_on="anything")
        sch.record_check(self.db, "w", changed=False, now=AUG14)
        self.assertEqual(sch.record_check(self.db, "w", changed=True, now=AUG14), sch.HOT)

    def test_cooling_without_a_reheat_condition_is_refused(self):
        """Cooling with no way back is just forgetting."""
        with self.assertRaises(ValueError):
            sch.schedule(self.db, "w", "subject", temperature=sch.COOL)

    def test_a_watch_with_no_condition_stays_warm_rather_than_vanishing(self):
        sch.schedule(self.db, "w", "subject", temperature=sch.WARM)
        for _ in range(5):
            temperature = sch.record_check(self.db, "w", changed=False, now=AUG14)
        self.assertEqual(temperature, sch.WARM)


class PackLoaderTests(unittest.TestCase):
    """A campaign is data. If instantiating one needs new Python, the kernel is
    missing a primitive -- so this suite loads Tarbell through a loader that
    knows nothing about Tarbell."""

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        wg.ensure_schema(self.db)
        for plane in (
            "run1_capital_conversion",
            "run2_relationship_adoption",
            "run3_four_economies_campaign",
            "run4_institutional_fabric",
            "run5_recursive_external",
        ):
            wg.register_plane(self.db, plane, now=AUG14)
        self.report = pack_loader.load_directory(self.db, PACKS / "tarbell", now=AUG14)

    def test_no_campaign_specific_module_exists(self):
        self.assertFalse(
            (ROOT / "campaigns").exists(),
            "a campaign must not carry its own source tree",
        )

    def test_the_pack_loads_actors_edges_work_and_watches(self):
        self.assertEqual(self.report["actors"], 12)
        self.assertEqual(self.report["edges"], 13)
        self.assertEqual(self.report["queued"], 6)
        self.assertEqual(self.report["watches"], 3)

    def test_every_person_is_recorded_unverified(self):
        """Nothing in the brief was checked against tarbellcenter.org by this
        process, so nothing may claim to have been."""
        people = actors.people_at(self.db, TARBELL_ORG)
        self.assertEqual(len(people), 5)
        for person in people:
            self.assertEqual(person.confidence, actors.ASSERTED)
        self.assertEqual(len(actors.unverified(self.db)), self.report["actors"])

    def test_staff_carry_distinct_authority_not_just_employment(self):
        self.assertTrue(
            actors.neighbours(self.db, "person:vincent-manancourt", actors.AUTHORITY_OVER)
        )
        self.assertFalse(
            actors.neighbours(self.db, "person:shakeel-hashim", actors.AUTHORITY_OVER)
        )

    def test_campaign_work_lands_on_real_planes(self):
        for item_id in range(1, 7):
            item = wg.get(self.db, item_id)
            self.assertIn(item.target_plane, wg.known_planes(self.db))
            self.assertEqual(item.state, wg.OPEN)

    def test_closed_rounds_are_cooled_not_polled(self):
        subjects = sch.frontier_subjects(self.db, now=AUG14)
        self.assertTrue(any("Summit" in s for s in subjects))
        self.assertFalse(any("grant round reopening" in s for s in subjects))

    def test_loading_a_pack_twice_changes_nothing(self):
        again = pack_loader.load_directory(self.db, PACKS / "tarbell", now=AUG14)
        self.assertEqual(again["queued"], 0)
        self.assertEqual(again["already_present"], 6)
        self.assertEqual(len(actors.people_at(self.db, TARBELL_ORG)), 5)

    def test_an_actor_pack_without_provenance_is_refused(self):
        pack = pack_loader.parse_pack("pack p\ngeneration 1\n\nactor org:x\n  display_name X\n")
        with self.assertRaises(ValueError):
            pack_loader.load_actors(self.db, pack, AUG14)


if __name__ == "__main__":
    unittest.main(verbosity=2)
