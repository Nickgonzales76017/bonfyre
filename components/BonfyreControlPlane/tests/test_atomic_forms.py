"""An AtomicForm compiles a submit-ready package, or reports exact blockers."""
import sqlite3, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import atomic_forms as af
import actors


def _form():
    return af.AtomicForm(
        form_id="f1", title="t",
        fields=[af.Field("title", "text", value="x"),
                af.Field("editor", "actor", autofill_actor="person:e"),
                af.Field("thesis", "text")],
        evidence=[af.EvidenceSlot("proof")],
        review_gate="board",
    )


class BlockerTests(unittest.TestCase):
    def test_an_incomplete_form_reports_every_blocker_not_a_false_green(self):
        r = af.submit_ready(_form())
        self.assertFalse(r.ready)
        # empty editor, empty thesis, empty evidence, unpassed review.
        self.assertGreaterEqual(r.blocker_count, 4)

    def test_a_form_is_ready_only_when_everything_is_satisfied(self):
        form = _form()
        form.fields[1].value = "Jane"      # editor
        form.fields[2].value = "the thesis"  # thesis
        form.evidence[0].artifact_digest = "abc123"
        form.review_passed = True
        r = af.submit_ready(form)
        self.assertTrue(r.ready)
        self.assertEqual(r.blocker_count, 0)

    def test_an_evidence_slot_accepts_a_foreign_twin_projection(self):
        form = af.AtomicForm(form_id="f", title="t",
                            evidence=[af.EvidenceSlot("ext", from_twin="twin:x")])
        r = af.submit_ready(form)
        self.assertTrue(r.ready)


class AutofillTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        actors.ensure_schema(self.db)

    def test_only_a_verified_actor_autofills_a_field(self):
        actors.upsert_actor(self.db, actors.Actor(
            "person:v", actors.PERSON, "Verified Vera", provenance="site",
            confidence=actors.VERIFIED))
        actors.upsert_actor(self.db, actors.Actor(
            "person:a", actors.PERSON, "Asserted Al", provenance="brief",
            confidence=actors.ASSERTED))
        form = af.AtomicForm(form_id="f", title="t", fields=[
            af.Field("v_field", "actor", autofill_actor="person:v"),
            af.Field("a_field", "actor", autofill_actor="person:a"),
        ])
        af.autofill(form, self.db)
        self.assertEqual(form.fields[0].value, "Verified Vera")
        self.assertIsNone(form.fields[1].value)  # asserted -> not autofilled


if __name__ == "__main__":
    unittest.main(verbosity=2)
