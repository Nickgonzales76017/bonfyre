"""One goal metabolized: form -> evidence -> submit-ready -> CMS publish."""
import sqlite3, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import institution_loop as il
import atomic_forms as af

FABRIC = Path.home() / ".bonfyre" / "estate-fabric" / "fabric.db"
CONTROL = Path(__file__).resolve().parent.parent / "control_plane.db"


def _form(with_evidence_slots=3):
    return af.AtomicForm(
        form_id="test:loop",
        title="Test article",
        fields=[af.Field("article_title", "text", value="Test article"),
                af.Field("thesis", "text", value="A thesis.")],
        evidence=[af.EvidenceSlot(f"e{i}") for i in range(with_evidence_slots)],
    )


class LoopTests(unittest.TestCase):
    def test_an_unfilled_form_is_not_published(self):
        # No fabric -> evidence slots stay empty -> not ready -> not published.
        r = il.run(_form(), control_db=CONTROL, fabric_db=Path("/nonexistent.db"),
                   cms_db=Path("/tmp/loop_test_a.db"))
        self.assertFalse(r.form_ready)
        self.assertIsNone(r.published_entry)
        self.assertGreater(len(r.blockers), 0)

    @unittest.skipUnless(FABRIC.exists() and il.CMS.exists(),
                         "fabric or CMS not available")
    def test_a_ready_form_publishes_a_real_cms_entry(self):
        r = il.run(_form(), control_db=CONTROL, fabric_db=FABRIC,
                   cms_db=Path("/tmp/loop_test_b.db"))
        self.assertTrue(r.form_ready)
        self.assertEqual(len(r.blockers), 0)
        self.assertIsNotNone(r.published_entry)

    @unittest.skipUnless(FABRIC.exists() and il.CMS.exists(),
                         "fabric or CMS not available")
    def test_evidence_is_filled_from_real_fabric_artifacts(self):
        form = _form()
        il.run(form, control_db=CONTROL, fabric_db=FABRIC,
               cms_db=Path("/tmp/loop_test_c.db"))
        # every slot now carries a real fabric digest
        for slot in form.evidence:
            self.assertIsNotNone(slot.artifact_digest)
            self.assertGreater(len(slot.artifact_digest), 16)


if __name__ == "__main__":
    unittest.main(verbosity=2)
