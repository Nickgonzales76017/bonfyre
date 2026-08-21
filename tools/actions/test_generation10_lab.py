import importlib.util
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("generation10_lab", HERE / "generation10_lab.py")
lab = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(lab)


class Generation10ContractTests(unittest.TestCase):
    def test_frozen_contract_validates(self):
        self.assertEqual(lab.validate_contract(lab.load_contract()), [])

    def test_all_eight_laws_are_unique(self):
        contract = lab.load_contract()
        self.assertEqual(len(contract["conservation_laws"]), 8)
        self.assertEqual(len(set(contract["conservation_laws"])), 8)

    def test_completion_requires_witness_not_exit_code(self):
        fixture, expected = lab.fault_fixture("cli_exit_zero_without_witness")
        self.assertIn(expected, lab.reject_effect(fixture))

    def test_authority_and_receipt_are_independent_gates(self):
        fixture, _ = lab.fault_fixture("authority_leak")
        self.assertIn("external_effect_without_authority", lab.reject_effect(fixture))
        fixture, _ = lab.fault_fixture("effect_without_receipt")
        self.assertIn("external_effect_without_receipt", lab.reject_effect(fixture))

    def test_factor_staleness_invalidates_green_evidence(self):
        fixture, expected = lab.fault_fixture("stale_factor_envelope")
        self.assertIn(expected, lab.reject_effect(fixture))

    def test_semantic_gc_requires_deletion_proof(self):
        fixture, expected = lab.fault_fixture("semantic_gc_without_deletion_proof")
        self.assertIn(expected, lab.reject_effect(fixture))

    def test_all_fault_scenarios_fail_closed(self):
        for scenario in lab.load_contract()["fault_scenarios"]:
            fixture, expected = lab.fault_fixture(scenario)
            self.assertIn(expected, lab.reject_effect(fixture), scenario)

    def test_content_hash_is_order_independent_for_objects(self):
        self.assertEqual(lab.sha256_value({"b": 2, "a": 1}), lab.sha256_value({"a": 1, "b": 2}))


if __name__ == "__main__":
    unittest.main()
