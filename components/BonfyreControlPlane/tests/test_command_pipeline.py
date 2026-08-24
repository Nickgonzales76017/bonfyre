"""Filling families/kinds/bindings makes the closure compose real pipelines."""
import sqlite3, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import command_pipeline_seed as cps
import capability_closure as cc


class KindGraphTests(unittest.TestCase):
    def test_edges_are_derived_from_data_flow_not_decreed(self):
        # Transcribe produces transcript; Brief consumes transcript -> an edge
        # must exist. Transcribe produces no kind Repurpose consumes -> no direct
        # edge (it must route through Brief).
        c_produces = dict(cps.COMMAND_KINDS)
        t_out = set(c_produces["Transcribe"][1])
        brief_in = set(c_produces["Brief"][0])
        repurpose_in = set(c_produces["Repurpose"][0])
        self.assertTrue(t_out & brief_in)
        self.assertFalse(t_out & repurpose_in)

    def test_every_command_has_a_real_binary(self):
        for command in cps.COMMAND_KINDS:
            self.assertIn(command, cps._BINARY)


@unittest.skipUnless(cps.DISCIPL_DB.exists() and cc.DISCIPL.exists(),
                     "DisCIPL not initialized")
class PreciseBindingTests(unittest.TestCase):
    def test_a_command_family_binds_to_its_exact_binary(self):
        binary = cc._command_binding("T_TRANSCRIBE")
        self.assertIsNotNone(binary)
        self.assertTrue(binary.endswith("bonfyre-transcribe"))

    def test_distinct_hops_bind_distinct_binaries(self):
        import command_laws as cl
        db = sqlite3.connect(":memory:")
        cl.build(db)
        org = cc.close(db, src_family="T_TRANSCRIBE", dst_family="T_REPURPOSE", depth=6)
        binaries = [h.binary for h in org.hops if h.binary]
        # The old coarse binding put the same binary on every hop; precise
        # binding must not.
        self.assertEqual(len(binaries), len(set(binaries)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
