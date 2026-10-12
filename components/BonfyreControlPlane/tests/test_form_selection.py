"""An organism's laws pick its execution form -- not everything is queue work."""
import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import capability_closure as cc


def _organism(laws, authorized=True):
    hops = tuple(
        cc.Hop(family=f"T_{i}", binary=f"/bin/b{i}", estate="e", law=law,
               confidence=0.7, bridge="", status="resolved")
        for i, law in enumerate(laws)
    )
    return cc.ExecutionOrganism(
        goal="g", global_confidence=0.5, accumulated_cost=0.1, semantic_drift=0.0,
        hops=hops, authorized=authorized, verdict_reason="ok",
    )


class FormSelectionTests(unittest.TestCase):
    def test_a_coupled_dynamics_hop_selects_a_trajectory_not_a_dag(self):
        sel = cc.select_form(_organism(["representation", "coupled_dynamics"]))
        self.assertEqual(sel.form, cc.FORM_TRAJECTORY)

    def test_a_pure_artifact_chain_fuses(self):
        sel = cc.select_form(_organism(["media_transform", "artifact_derivation"]))
        self.assertEqual(sel.form, cc.FORM_FUSED)

    def test_a_representation_chain_fuses(self):
        sel = cc.select_form(_organism(["representation", "representation"]))
        self.assertEqual(sel.form, cc.FORM_FUSED)

    def test_mixed_laws_go_durable(self):
        sel = cc.select_form(_organism(["artifact_derivation", "economic"]))
        self.assertEqual(sel.form, cc.FORM_DURABLE)

    def test_governance_hop_forces_durable_receipts(self):
        sel = cc.select_form(_organism(["media_transform", "governance_proof"]))
        self.assertEqual(sel.form, cc.FORM_DURABLE)

    def test_a_blocked_organism_dispatches_nowhere(self):
        res = cc.dispatch(_organism(["representation"], authorized=False),
                          Path("/tmp/never.db"))
        self.assertFalse(res["dispatched"])
        self.assertIsNone(res["form"])

    def test_each_form_routes_to_a_distinct_substrate(self):
        forms = {
            cc.select_form(_organism(["coupled_dynamics"])).substrate,
            cc.select_form(_organism(["media_transform"])).substrate,
            cc.select_form(_organism(["economic", "media_transform"])).substrate,
        }
        # reason, pipeline, queue -- three distinct real substrates.
        self.assertEqual(len({s for s in forms if s}), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
