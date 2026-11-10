"""The closure composes real commands and refuses proven-layer mutations."""
import sqlite3, sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import capability_closure as cc
import command_laws as cl
import proof_frontier as pf

DISCIPL_PRESENT = cc.DISCIPL.exists()


class BindingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        cl.build(self.db)

    def test_a_representation_family_binds_to_a_representation_law_binary(self):
        binary, estate, law = cc._bind_family(self.db, "T_MOE_ROUTER")
        self.assertEqual(law, "representation")
        self.assertIsNotNone(binary)

    def test_an_unknown_family_defaults_to_representation(self):
        _, _, law = cc._bind_family(self.db, "T_MADE_UP")
        self.assertEqual(law, "representation")


@unittest.skipUnless(DISCIPL_PRESENT, "bonfyre-discipl not installed")
class ClosureIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        cl.build(self.db)
        pf.ensure_schema(self.db)
        # Seed the FPQ frontier so the fence has something to enforce.
        for ordinal, layer in enumerate(
            ["source_model", "representation_abi", "reconstruction",
             "prepared_state", "transformer_math"]
        ):
            status = pf.OPEN if layer == "transformer_math" else pf.PROVEN
            pf.set_layer(self.db, "model:qwen2.5-0.5b", ordinal, layer, status,
                         subject_profile="fpq-bwa-multiscale-v9")

    def test_composition_binds_every_hop_to_a_real_binary(self):
        organism = cc.close(self.db, src_family="T_AUDIO_MODEL",
                            dst_family="T_SAMPLE_OUTPUT", depth=4)
        self.assertGreater(len(organism.hops), 0)
        self.assertEqual(organism.bound_ratio, 1.0)
        for hop in organism.hops:
            self.assertIsNotNone(hop.binary)

    def test_a_garbage_failure_cannot_authorize_a_representation_mutation(self):
        organism = cc.close(self.db, src_family="T_MOE_ROUTER",
                            dst_family="T_MOE_EXPERT", depth=3,
                            observed_failure="qwen generation is garbage")
        self.assertFalse(organism.authorized)
        self.assertIn("symptom", organism.verdict_reason)

    def test_composition_without_a_failure_is_authorized(self):
        organism = cc.close(self.db, src_family="T_AUDIO_MODEL",
                            dst_family="T_SAMPLE_OUTPUT", depth=4)
        self.assertTrue(organism.authorized)


if __name__ == "__main__":
    unittest.main(verbosity=2)
