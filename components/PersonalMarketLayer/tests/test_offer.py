import json
import tempfile
import unittest
from pathlib import Path

from personal_market_layer.offer import generate_offer_package, sync_offer_pipeline


class OfferTests(unittest.TestCase):
    def test_generate_offer_package_from_reviewed_proof(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proof_dir = root / "proofs" / "founder-sample-pickfu"
            proof_dir.mkdir(parents=True)

            (proof_dir / "deliverable.md").write_text(
                "# Founder Sample\n\n## Summary\n- Strong summary\n\n## Action Items\n- Do the thing\n\n## Transcript\nReal transcript",
                encoding="utf-8",
            )
            (proof_dir / "proof-summary.json").write_text(
                json.dumps(
                    {
                        "proof_slug": "founder-sample-pickfu",
                        "proof_label": "Founder Sample - PickFu",
                        "source_kind": "audio_file",
                        "quality": {"score": 95, "summary_count": 5, "action_item_count": 5},
                        "intake_manifest": {"contextNotes": "Public founder interview sample."},
                    }
                ),
                encoding="utf-8",
            )
            (proof_dir / "proof-review.json").write_text(
                json.dumps(
                    {
                        "review_score": 100,
                        "recommendation": "promote",
                    }
                ),
                encoding="utf-8",
            )

            result = generate_offer_package(
                proof_dir=proof_dir,
                output_root=root / "offers",
                vault_monetization_root=root / "05-Monetization",
            )

            self.assertTrue((root / "offers" / "founder-sample-pickfu" / "offer.json").exists())
            self.assertTrue((root / "offers" / "founder-sample-pickfu" / "offer.md").exists())
            self.assertTrue((root / "offers" / "founder-sample-pickfu" / "outreach.md").exists())
            self.assertTrue((root / "offers" / "founder-sample-pickfu" / "listing.md").exists())
            self.assertTrue((root / "offers" / "founder-sample-pickfu" / "variants.md").exists())
            self.assertTrue((root / "offers" / "founder-sample-pickfu" / "variant-outreach.md").exists())
            self.assertTrue((root / "05-Monetization" / "Offer - Founder Sample - PickFu Offer.md").exists())
            self.assertTrue((root / "05-Monetization" / "_generated-offers.json").exists())
            self.assertTrue((root / "05-Monetization" / "_Generated Offer Catalog.md").exists())
            self.assertEqual(result["offer_name"], "Founder Sample - PickFu Offer")

    def test_sync_offer_pipeline_writes_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proof_dir = root / "proofs" / "founder-sample-pickfu"
            proof_dir.mkdir(parents=True)

            (proof_dir / "deliverable.md").write_text(
                "# Founder Sample\n\n## Summary\n- Strong summary\n\n## Action Items\n- Do the thing\n\n## Transcript\nReal transcript",
                encoding="utf-8",
            )
            (proof_dir / "proof-summary.json").write_text(
                json.dumps(
                    {
                        "proof_slug": "founder-sample-pickfu",
                        "proof_label": "Founder Sample - PickFu",
                        "source_kind": "audio_file",
                        "quality": {"score": 95, "summary_count": 5, "action_item_count": 5},
                        "intake_manifest": {"contextNotes": "Public founder interview sample."},
                    }
                ),
                encoding="utf-8",
            )
            (proof_dir / "proof-review.json").write_text(
                json.dumps({"review_score": 100, "recommendation": "promote"}),
                encoding="utf-8",
            )
            (root / "proof-index.json").write_text(
                json.dumps(
                    {
                        "proofs": [
                            {
                                "proof_slug": "founder-sample-pickfu",
                                "proof_label": "Founder Sample - PickFu",
                                "proof_summary_path": str((proof_dir / "proof-summary.json").resolve()),
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            result = sync_offer_pipeline(
                proof_index=root / "proof-index.json",
                output_root=root / "offers",
                vault_monetization_root=root / "05-Monetization",
            )

            self.assertEqual(result["reviewed_proof_count"], 1)
            self.assertEqual(result["generated_offer_count"], 1)
            self.assertTrue((root / "05-Monetization" / "_Offer Pipeline Snapshot.md").exists())
            self.assertTrue((root / "05-Monetization" / "_offer-pipeline-snapshot.json").exists())

    def test_sync_offer_pipeline_excludes_non_promoted_proofs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proof_dir = root / "proofs" / "customer-sample"
            proof_dir.mkdir(parents=True)

            (proof_dir / "deliverable.md").write_text(
                "# Customer Sample\n\n## Summary\n- Thin summary\n\n## Action Items\n- Weak action\n\n## Transcript\nReal transcript",
                encoding="utf-8",
            )
            (proof_dir / "proof-summary.json").write_text(
                json.dumps(
                    {
                        "proof_slug": "customer-sample",
                        "proof_label": "Customer Sample",
                        "source_kind": "audio_file",
                        "quality": {"score": 70, "summary_count": 3, "action_item_count": 2},
                        "intake_manifest": {"contextNotes": "Public customer sample."},
                    }
                ),
                encoding="utf-8",
            )
            (proof_dir / "proof-review.json").write_text(
                json.dumps({"review_score": 70, "recommendation": "usable-with-review"}),
                encoding="utf-8",
            )
            (root / "proof-index.json").write_text(
                json.dumps(
                    {
                        "proofs": [
                            {
                                "proof_slug": "customer-sample",
                                "proof_label": "Customer Sample",
                                "proof_summary_path": str((proof_dir / "proof-summary.json").resolve()),
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            result = sync_offer_pipeline(
                proof_index=root / "proof-index.json",
                output_root=root / "offers",
                vault_monetization_root=root / "05-Monetization",
            )

            self.assertEqual(result["reviewed_proof_count"], 0)
            self.assertEqual(result["generated_offer_count"], 0)
