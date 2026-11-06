"""Tests for the anti-amnesia machinery, written against the FPQ false loops.

Each test encodes a real incident from the FPQ archive: a settled result that a
later worker ignored, and the mutation the fence should have refused.
"""

import datetime as dt
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import proof_frontier as pf  # noqa: E402

UTC = dt.timezone.utc
AT = dt.datetime(2026, 8, 15, tzinfo=UTC)

# The eight FPQ correctness layers, low to high.
FPQ_LAYERS = [
    "source_model",       # 0
    "representation_abi", # 1
    "reconstruction",     # 2
    "prepared_state",     # 3
    "transformer_math",   # 4
    "runtime_contract",   # 5
    "physical_execution", # 6
    "semantic_behavior",  # 7
]

SUBJECT = "model:qwen2.5-0.5b"
PROFILE = "fpq-bwa-multiscale-v9"


def _frontier(db, open_at="transformer_math"):
    """Seed a frontier proven up to the given open layer."""
    for ordinal, layer in enumerate(FPQ_LAYERS):
        if layer == open_at:
            status = pf.OPEN
        elif ordinal < FPQ_LAYERS.index(open_at):
            status = pf.PROVEN
        else:
            status = pf.BLOCKED
        pf.set_layer(db, SUBJECT, ordinal, layer, status, subject_profile=PROFILE)


class SolvedInvariantTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        pf.ensure_schema(self.db)

    def test_a_cooled_invariant_must_name_what_reheats_it(self):
        """Something settled forever that reality can never reopen is the same
        failure this exists to prevent, run backwards."""
        with self.assertRaises(ValueError):
            pf.SolvedInvariant(
                invariant_id="i1",
                subject_resource=SUBJECT,
                layer="reconstruction",
                statement="Q/K/V reconstruction matches reference",
                status=pf.COOLED,
                reheat_conditions=(),
            )

    def test_a_symptom_does_not_challenge_a_cooled_layer(self):
        pf.record_invariant(
            self.db,
            pf.SolvedInvariant(
                invariant_id="fpq.reconstruction.qkv",
                subject_resource=SUBJECT,
                layer="reconstruction",
                statement="native-row Q/K/V reconstruction agrees with reference",
                status=pf.COOLED,
                reheat_conditions=("encoded artifact hash changes", "reader version changes"),
            ),
        )
        # "generation is garbage" is not a listed reheat condition.
        self.assertFalse(pf.challenge_invariant(self.db, "fpq.reconstruction.qkv", "generation is garbage"))
        # The real condition does reopen it.
        self.assertTrue(pf.challenge_invariant(self.db, "fpq.reconstruction.qkv", "encoded artifact hash changes"))


class KnownNonCauseTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        pf.ensure_schema(self.db)

    def test_records_the_two_real_fpq_falsifications(self):
        pf.record_noncause(
            self.db,
            pf.KnownNonCause(
                noncause_id="fpq.noncause.quantization",
                hypothesis="quantization",
                subject_scope=SUBJECT,
                experiment="fully-lossless FP16 passthrough still produced garbage",
                invalidation_conditions=("a new representation profile is introduced",),
            ),
        )
        found = pf.noncauses_for(self.db, SUBJECT)
        self.assertEqual(len(found), 1)
        self.assertIn("FP16", found[0].experiment)


class RegressionFenceTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        pf.ensure_schema(self.db)
        _frontier(self.db, open_at="transformer_math")

    def test_garbage_generation_cannot_reopen_the_encoder(self):
        """The canonical loop: L7 symptom, agent reaches for the L1 encoder."""
        decision = pf.authorize_mutation(
            self.db,
            subject_resource=SUBJECT,
            subject_profile=PROFILE,
            target_layer="representation_abi",
            observed_failure="model generation is garbage",
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.first_open, "transformer_math")
        self.assertIn("symptom", decision.reason)

    def test_reconstruction_is_off_limits_when_it_is_proven(self):
        decision = pf.authorize_mutation(
            self.db,
            subject_resource=SUBJECT,
            subject_profile=PROFILE,
            target_layer="reconstruction",
            observed_failure="first token wrong",
        )
        self.assertFalse(decision.allowed)

    def test_the_open_layer_is_allowed(self):
        decision = pf.authorize_mutation(
            self.db,
            subject_resource=SUBJECT,
            subject_profile=PROFILE,
            target_layer="transformer_math",
            observed_failure="attention diverges at layer 19",
        )
        self.assertTrue(decision.allowed)

    def test_a_known_noncause_denies_regardless_of_frontier(self):
        pf.record_noncause(
            self.db,
            pf.KnownNonCause(
                noncause_id="fpq.noncause.quantization",
                hypothesis="quantization",
                subject_scope=SUBJECT,
                experiment="fully-lossless FP16 passthrough still produced garbage",
            ),
        )
        # Even naming a plausible target, if it is a known non-cause, deny.
        decision = pf.authorize_mutation(
            self.db,
            subject_resource=SUBJECT,
            target_layer="quantization",
            observed_failure="bad output",
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.matched_noncause, "fpq.noncause.quantization")

    def test_a_real_reheat_signal_reopens_a_cooled_layer(self):
        pf.record_invariant(
            self.db,
            pf.SolvedInvariant(
                invariant_id="fpq.reconstruction.qkv",
                subject_resource=SUBJECT,
                subject_profile=PROFILE,
                layer="reconstruction",
                statement="reconstruction matches reference",
                status=pf.COOLED,
                reheat_conditions=("encoded artifact hash changed",),
            ),
        )
        denied = pf.authorize_mutation(
            self.db,
            subject_resource=SUBJECT,
            subject_profile=PROFILE,
            target_layer="reconstruction",
            observed_failure="garbage",
        )
        self.assertFalse(denied.allowed)

        allowed = pf.authorize_mutation(
            self.db,
            subject_resource=SUBJECT,
            subject_profile=PROFILE,
            target_layer="reconstruction",
            observed_failure="garbage",
            reheat_signal="encoded artifact hash changed",
        )
        self.assertTrue(allowed.allowed)

    def test_frontier_report_names_the_ceiling(self):
        report = pf.frontier_report(self.db, SUBJECT, PROFILE)
        self.assertEqual(report["first_open_layer"], "transformer_math")
        self.assertEqual(report["mutation_ceiling"], "transformer_math")

    def test_generalizes_beyond_fpq(self):
        """The fence is not FPQ-specific. A Bernstein adapter proven below an
        open relationship-projection layer is equally off-limits."""
        subj = "repo:sipyourdrink-ltd/bernstein"
        for ordinal, layer in enumerate(
            ["external_execution", "receipt_import", "actor_projection", "relationship_projection"]
        ):
            status = pf.OPEN if layer == "relationship_projection" else pf.PROVEN
            pf.set_layer(self.db, subj, ordinal, layer, status)
        decision = pf.authorize_mutation(
            self.db,
            subject_resource=subj,
            target_layer="external_execution",
            observed_failure="relationship edge missing",
        )
        self.assertFalse(decision.allowed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
