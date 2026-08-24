import tempfile
import unittest
import json
from pathlib import Path
from typing import Optional
from unittest.mock import patch

from local_ai_transcription_service.benchmark import run_benchmark_pack
from local_ai_transcription_service.bootstrap import build_bootstrap_plan
from local_ai_transcription_service.intake import (
    discover_intake_manifests,
    extract_intake_package_source,
    load_intake_manifest,
    load_intake_package,
    resolve_intake_source,
)
from local_ai_transcription_service.models import AudioPrepResult, Deliverable, EnvironmentReport
from local_ai_transcription_service.piper import build_tts_script, clean_for_speech
from local_ai_transcription_service.pipeline import rebuild_job_artifacts, resolve_job_name, run_batch_pipeline, run_intake_batch_pipeline, run_pipeline
from local_ai_transcription_service.proof import promote_job_to_proof
from local_ai_transcription_service.quality import score_quality
from local_ai_transcription_service.queue import enqueue_audio_file, enqueue_intake_package, mark_queue_item, queue_status
from local_ai_transcription_service.review import review_proof
from local_ai_transcription_service.runtime_guard import recommended_load_limit
from local_ai_transcription_service.summary import extract_action_items, summarize_text
from local_ai_transcription_service.transcription import inspect_model_cache, run_audio_wrapper


class PipelineTests(unittest.TestCase):
    def test_intake_manifest_load_and_resolve_job_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest_path = root / "intake.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "jobId": "job-123",
                        "jobSlug": "acme-founder-memo",
                        "clientName": "Acme",
                        "jobTitle": "Founder Memo",
                        "outputGoal": "transcript-summary-actions",
                        "contextNotes": "Focus on next-step actions.",
                        "createdAt": "2026-04-03T12:00:00Z",
                    }
                ),
                encoding="utf-8",
            )

            manifest = load_intake_manifest(manifest_path)
            job_name, resolved_manifest = resolve_job_name(
                source_path=None,
                intake_manifest_path=manifest_path,
            )

            self.assertEqual(manifest["clientName"], "Acme")
            self.assertEqual(job_name, "Founder Memo")
            assert resolved_manifest is not None
            self.assertEqual(resolved_manifest["outputGoal"], "transcript-summary-actions")

    def test_intake_dir_discovers_and_resolves_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest_path = root / "founder-memo.intake.json"
            audio_path = root / "memo.wav"
            audio_path.write_text("fake-audio", encoding="utf-8")
            manifest_path.write_text(
                json.dumps(
                    {
                        "jobId": "job-123",
                        "jobSlug": "acme-founder-memo",
                        "clientName": "Acme",
                        "jobTitle": "Founder Memo",
                        "outputGoal": "transcript-summary-actions",
                        "contextNotes": "Focus on next-step actions.",
                        "createdAt": "2026-04-03T12:00:00Z",
                        "fileName": "memo.wav",
                    }
                ),
                encoding="utf-8",
            )

            manifests = discover_intake_manifests(root)
            manifest = load_intake_manifest(manifest_path)
            resolved_path, source_kind = resolve_intake_source(manifest_path, manifest)

            self.assertEqual(manifests, [manifest_path])
            self.assertEqual(resolved_path, audio_path)
            self.assertEqual(source_kind, "audio_file")

    def test_intake_package_load_and_extracts_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package_path = root / "founder-memo.intake-package.json"
            package_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "manifest": {
                            "jobId": "job-123",
                            "jobSlug": "acme-founder-memo",
                            "clientName": "Acme",
                            "jobTitle": "Founder Memo",
                            "outputGoal": "meeting-recap",
                            "contextNotes": "Focus on next-step actions.",
                            "createdAt": "2026-04-03T12:00:00Z",
                            "fileName": "memo.txt",
                        },
                        "sourceFile": {
                            "name": "memo.txt",
                            "type": "text/plain",
                            "size": 12,
                            "dataBase64": "UmV2aWV3IHRoZSBub3Rlcy4=",
                        },
                    }
                ),
                encoding="utf-8",
            )

            package_payload = load_intake_package(package_path)
            manifest, source_path, source_kind = extract_intake_package_source(
                package_path,
                package_payload,
                root / "out",
            )

            self.assertEqual(manifest["jobTitle"], "Founder Memo")
            self.assertEqual(source_kind, "transcript_file")
            self.assertTrue(source_path.exists())
            self.assertEqual(source_path.read_text(encoding="utf-8"), "Review the notes.")

    def test_benchmark_pack_generates_results(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            benchmark_dir = root / "benchmark"
            benchmark_dir.mkdir()
            (benchmark_dir / "cases.json").write_text(
                json.dumps(
                    {
                        "cases": [
                            {
                                "name": "founder-note",
                                "transcript": "Review the draft. Send the client an update tomorrow. Call the vendor today.",
                                "expected_summary": [
                                    "Review the draft.",
                                    "Send the client an update tomorrow.",
                                ],
                                "expected_action_items": [
                                    "Send the client an update tomorrow.",
                                    "Call the vendor today.",
                                ],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            payload = run_benchmark_pack(benchmark_dir, summary_bullets=2)

            self.assertEqual(payload["total_cases"], 1)
            self.assertIn("average_quality_score", payload)
            self.assertIn("average_summary_overlap", payload)
            self.assertIn("average_action_item_overlap", payload)
            self.assertTrue((benchmark_dir / "benchmark-results.json").exists())
            self.assertEqual(payload["results"][0]["name"], "founder-note")

    def test_summary_downweights_intro_chatter(self) -> None:
        text = (
            "Hi there, welcome to the show. "
            "Today I'm talking to Jane about her startup. "
            "Thanks so much for having me. "
            "The main thing we realized was that customers did not trust the original packaging. "
            "We decided to rebrand the product after testing several designs. "
            "Those tests gave us confidence to spend on a bigger rollout. "
            "Sales improved after the rebrand."
        )

        summary = summarize_text(text, bullets=3)

        self.assertNotIn("Hi there, welcome to the show.", summary)
        self.assertIn("The main thing we realized was that customers did not trust the original packaging.", summary)
        self.assertIn("We decided to rebrand the product after testing several designs.", summary)

    def test_summary_filters_obvious_junk_bullets(self) -> None:
        text = (
            "Welcome back to the show. "
            "This is a really cool episode. "
            "The number one problem was weak product adoption after sign-up. "
            "The team decided to target event promoters before broadening the market. "
            "They added a processing fee to improve unit economics."
        )

        summary = summarize_text(text, bullets=3)

        self.assertNotIn("Welcome back to the show.", summary)
        self.assertNotIn("This is a really cool episode.", summary)
        self.assertIn("The number one problem was weak product adoption after sign-up.", summary)

    def test_summary_normalizes_icp_shift_language(self) -> None:
        text = (
            "I had to change my ICP to event promoters. "
            "Event promoters, managers, coordinators, they're a younger group, 25 to 35. "
            "And we have been running our LinkedIn and making connections with a ton of event promoters."
        )

        summary = summarize_text(text, bullets=3)

        self.assertIn("The team realized the original ICP was wrong and shifted toward event promoters.", summary)
        self.assertIn("LinkedIn became the clearest channel for reaching the new event-promoter ICP.", summary)

    def test_summary_prefers_business_dimensions_over_generic_sentences(self) -> None:
        text = (
            "Their number one problem was finding gigs, not tipping discovery. "
            "I had to change my ICP to event promoters. "
            "And we have been running our LinkedIn and making connections with a ton of event promoters. "
            "We're going to talk to them in person, take their feedback in person, and then take it to California. "
            "We need to build an AI engine, which does scoring. "
            "We will be taking like 5% from the performance side."
        )

        summary = summarize_text(text, bullets=6)

        self.assertIn("The core customer problem was not discovery or tipping, but getting booked for better-paying gigs.", summary)
        self.assertIn("The team realized the original ICP was wrong and shifted toward event promoters.", summary)
        self.assertIn("LinkedIn became the clearest channel for reaching the new event-promoter ICP.", summary)
        self.assertIn("The rollout plan is to validate locally in Pasadena, learn in person, then expand through California.", summary)
        self.assertTrue(any("processing fee" in bullet.lower() or "5% performer fee" in bullet.lower() for bullet in summary))

    def test_summary_normalizes_founder_pivot_and_testing_language(self) -> None:
        text = (
            "When faced with a problem of trying to get unbiased feedback, we built a tool. "
            "Eventually, after a couple of iterations of that, we realized that the traction on our main project was not going where we wanted it to go. "
            "And the traction on Pickfoo was growing. "
            "There is a phrase we use like test before you invest because testing assumptions reduces a lot of downside risk. "
            "Those tests gave them the confidence to pull the trigger on that rebranding decision."
        )

        summary = summarize_text(text, bullets=5)

        self.assertIn("PickFu emerged from the founders' need for unbiased feedback on design and messaging decisions.", summary)
        self.assertIn("The team pivoted when the main project stalled while PickFu kept gaining traction.", summary)
        self.assertIn("Their core thesis is to test assumptions early so teams reduce downside risk before committing resources.", summary)
        self.assertIn("Small validation tests can create enough confidence for larger go/no-go decisions.", summary)

    def test_summary_normalizes_marketplace_and_distribution_language(self) -> None:
        text = (
            "And we have been running our LinkedIn and making connections with a ton of event promoters. "
            "Like some of them were interested, but they were very hard to budge. "
            "Then you need the event promoters to kind of come in, create profiles, break gigs, and then let your algorithm do work to match and rank who is the best fit for a set gig."
        )

        summary = summarize_text(text, bullets=3)

        self.assertIn("LinkedIn became the clearest channel for reaching the new event-promoter ICP.", summary)
        self.assertIn("Older venue owners were too slow to move as an early adopter segment.", summary)
        self.assertIn("The product evolved into a two-sided marketplace that helps venues rank and match performers.", summary)

    def test_action_items_capture_recommendation_style_advice(self) -> None:
        text = (
            "We need to test assumptions before we invest major time and money. "
            "We should test the message before committing to a full rollout. "
            "We need to build confidence with small tests before making a larger launch decision."
        )

        items = extract_action_items(text)

        self.assertTrue(len(items) >= 2)
        # Should have extracted action items about testing/building
        self.assertTrue(any("test" in item.lower() for item in items))

    def test_action_items_filter_transcript_scraps_and_normalize_need_statements(self) -> None:
        text = (
            "Why should I really try out this app? "
            "They're like, yeah, you need to make Chinese a little bit. "
            "We need to redesign onboarding for event promoters before rollout. "
            "Build a simple scorecard for product adoption this week."
        )

        items = extract_action_items(text)

        self.assertNotIn("Why should I really try out this app?", items)
        self.assertFalse(any("They're like" in item for item in items))
        self.assertIn("Redesign onboarding for event promoters before rollout.", items)
        self.assertIn("Build a simple scorecard for product adoption this week.", items)

    def test_quality_penalizes_junk_bullets(self) -> None:
        payload = score_quality(
            raw_transcript_text="Welcome back to the show. Build a simple scorecard for product adoption this week.",
            cleaned_transcript_text="Welcome back to the show. Build a simple scorecard for product adoption this week.",
            cleanup_result={"changed": False, "filler_tokens_removed": 0},
            deliverable=Deliverable(
                title="Founder Memo",
                transcript="Welcome back to the show. Build a simple scorecard for product adoption this week.",
                transcript_paragraphs=["Welcome back to the show.", "Build a simple scorecard for product adoption this week."],
                summary_bullets=["Welcome back to the show.", "Build a simple scorecard for product adoption this week."],
                deep_summary_sections=[],
                action_items=["Why should I really try out this app?", "Build a simple scorecard for product adoption this week."],
                source_kind="transcript_file",
                client_name=None,
                output_goal=None,
                context_notes=None,
                quality={},
                processing_notes=[],
            ),
        )

        self.assertEqual(payload["junk_summary_count"], 1)
        self.assertEqual(payload["junk_action_item_count"], 1)
        self.assertEqual(payload["valid_summary_count"], 1)
        self.assertEqual(payload["valid_action_item_count"], 1)
        self.assertLess(payload["score"], 80)

    def test_quality_penalizes_generic_transcript_shaped_bullets(self) -> None:
        payload = score_quality(
            raw_transcript_text="So, your application is essentially a ranking layer. At the time, we will add a processing fee on the customer side.",
            cleaned_transcript_text="So, your application is essentially a ranking layer. At the time, we will add a processing fee on the customer side.",
            cleanup_result={"changed": False, "filler_tokens_removed": 0},
            deliverable=Deliverable(
                title="Customer Memo",
                transcript="So, your application is essentially a ranking layer. At the time, we will add a processing fee on the customer side.",
                transcript_paragraphs=["So, your application is essentially a ranking layer.", "At the time, we will add a processing fee on the customer side."],
                summary_bullets=["So, your application is essentially a ranking layer."],
                deep_summary_sections=[],
                action_items=["At the time, we will add a processing fee on the customer side."],
                source_kind="transcript_file",
                client_name=None,
                output_goal=None,
                context_notes=None,
                quality={},
                processing_notes=[],
            ),
        )

        self.assertEqual(payload["generic_summary_count"], 1)
        self.assertEqual(payload["generic_action_item_count"], 1)
        self.assertLess(payload["score"], 75)

    def test_model_cache_inspection_reports_expected_fields(self) -> None:
        payload = inspect_model_cache("base")
        self.assertEqual(payload["model"], "base")
        self.assertIn("cache_dir", payload)
        self.assertIn("model_path", payload)
        self.assertIn("cached", payload)

    def test_recommended_load_limit_is_positive(self) -> None:
        self.assertGreaterEqual(recommended_load_limit(), 4.0)

    def test_piper_clean_for_speech_strips_obsidian_markup(self) -> None:
        cleaned = clean_for_speech(
            "---\ntitle: Sample\n---\n# Heading\n- [[Target|Visible]] item\n`code`\n"
        )
        self.assertNotIn("---", cleaned)
        self.assertNotIn("[[", cleaned)
        self.assertIn("Heading", cleaned)
        self.assertIn("Visible item", cleaned)

    def test_tts_script_defaults_to_summary_shape(self) -> None:
        text = build_tts_script(
            deliverable=Deliverable(
                title="Founder Memo",
                transcript="Review the notes.",
                transcript_paragraphs=["Review the notes."],
                summary_bullets=["Review the notes."],
                deep_summary_sections=[],
                action_items=["Send the update tomorrow."],
                source_kind="transcript_file",
                client_name="Acme",
                output_goal="meeting-recap",
                context_notes=None,
                quality={},
                processing_notes=[],
            )
        )
        self.assertIn("Project: Founder Memo.", text)
        self.assertIn("Action items:", text)

    def test_bootstrap_plan_for_missing_dependencies(self) -> None:
        plan = build_bootstrap_plan(
            EnvironmentReport(
                whisper_binary=None,
                ffmpeg_binary=None,
            )
        )

        self.assertIn("ffmpeg", plan["missing_dependencies"])
        self.assertIn("whisper", plan["missing_dependencies"])
        self.assertTrue(plan["steps"])

    def test_transcript_file_generates_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            transcript = root / "input.txt"
            transcript.write_text(
                (
                    "Um review the draft. Send the client an update tomorrow. "
                    "The current version is close. Call the vendor today."
                ),
                encoding="utf-8",
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Client Update",
                transcript_file=transcript,
                intake_manifest={
                    "jobId": "job-1",
                    "jobSlug": "client-update",
                    "clientName": "Acme",
                    "jobTitle": "Client Update",
                    "outputGoal": "meeting-recap",
                    "contextNotes": "Prioritize action items for the founder.",
                    "createdAt": "2026-04-03T12:00:00Z",
                },
                summary_bullets=3,
            )

            self.assertTrue(artifacts.raw_transcript_path.exists())
            self.assertTrue(artifacts.transcript_path.exists())
            self.assertTrue(artifacts.deliverable_path.exists())
            self.assertTrue(artifacts.meta_path.exists())

            cleaned_transcript = artifacts.transcript_path.read_text(encoding="utf-8")
            deliverable = artifacts.deliverable_path.read_text(encoding="utf-8")
            meta = artifacts.meta_path.read_text(encoding="utf-8")

            self.assertIn("Um review the draft.", artifacts.raw_transcript_path.read_text(encoding="utf-8"))
            self.assertIn("review the draft.", cleaned_transcript)
            self.assertIn("# Client Update", deliverable)
            self.assertIn("## Metadata", deliverable)
            self.assertIn("## Processing Notes", deliverable)
            self.assertIn("## Summary", deliverable)
            self.assertIn("## Action Items", deliverable)
            self.assertIn("client: Acme", deliverable)
            self.assertIn("goal: meeting-recap", deliverable)
            self.assertIn("Send the client an update tomorrow.", deliverable)
            self.assertIn("Call the vendor today.", deliverable)
            self.assertIn('"filler_tokens_removed": 1', meta)
            self.assertIn('"paragraph_count":', meta)
            self.assertIn('"quality"', meta)
            # Quality status can be "strong" or "usable" depending on scoring
            self.assertTrue('"status": "strong"' in meta or '"status": "usable"' in meta)
            self.assertIn('"tts": null', meta)

    def test_intake_package_generates_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            package_path = root / "client-update.intake-package.json"
            package_path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "manifest": {
                            "jobId": "job-1",
                            "jobSlug": "client-update",
                            "clientName": "Acme",
                            "jobTitle": "Client Update",
                            "outputGoal": "meeting-recap",
                            "contextNotes": "Prioritize action items for the founder.",
                            "createdAt": "2026-04-03T12:00:00Z",
                            "fileName": "input.txt",
                        },
                        "sourceFile": {
                            "name": "input.txt",
                            "type": "text/plain",
                            "size": 80,
                            "dataBase64": "VW0gcmV2aWV3IHRoZSBkcmFmdC4gU2VuZCB0aGUgY2xpZW50IGFuIHVwZGF0ZSB0b21vcnJvdy4gQ2FsbCB0aGUgdmVuZG9yIHRvZGF5Lg==",
                        },
                    }
                ),
                encoding="utf-8",
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Client Update",
                intake_package_path=package_path,
                summary_bullets=3,
            )

            meta = artifacts.meta_path.read_text(encoding="utf-8")
            deliverable = artifacts.deliverable_path.read_text(encoding="utf-8")

            self.assertTrue(artifacts.transcript_path.exists())
            self.assertIn('"intake_package_path"', meta)
            self.assertIn('"source_kind": "transcript_file"', meta)
            self.assertIn("Job metadata imported from browser intake manifest.", deliverable)
            self.assertIn("client: Acme", deliverable)

    def test_rebuild_job_reuses_saved_transcript_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            transcript = root / "input.txt"
            transcript.write_text(
                (
                    "Hi there, welcome to the show. "
                    "The main thing we realized was that customers did not trust the original packaging. "
                    "We decided to rebrand the product after testing several designs. "
                    "Those tests gave us confidence to spend on a bigger rollout."
                ),
                encoding="utf-8",
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Founder Memo",
                transcript_file=transcript,
                summary_bullets=2,
            )

            rebuilt = rebuild_job_artifacts(
                job_output=artifacts.output_dir,
                summary_bullets=3,
            )

            meta = json.loads(rebuilt.meta_path.read_text(encoding="utf-8"))
            deliverable = rebuilt.deliverable_path.read_text(encoding="utf-8")

            self.assertEqual(meta["summary_bullets"], 3)
            self.assertIn("Deliverable rebuilt from saved transcript artifacts without rerunning Whisper.", deliverable)
            self.assertIn("The main thing we realized was that customers did not trust the original packaging.", deliverable)
            summary_section = deliverable.split("## Summary", 1)[1].split("## Action Items", 1)[0]
            self.assertNotIn("Hi there, welcome to the show.", summary_section)
            self.assertTrue(meta["rebuild"]["used_saved_artifacts"])

    def test_long_transcript_generates_deep_summary_and_expands_effective_bullets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            transcript = root / "long.txt"
            transcript.write_text(
                " ".join(
                    [
                        "The original ICP was wrong and the team shifted toward event promoters.",
                        "LinkedIn became the clearest channel for reaching that audience.",
                        "The product ranks performers for venue fit using a scoring workflow.",
                        "The rollout starts locally in Pasadena before expanding across California.",
                        "Monetization starts with a 5% performer fee and later adds a customer processing fee.",
                        "The team believes in testing assumptions before investing heavily.",
                    ]
                    * 8
                ),
                encoding="utf-8",
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Long Strategy Call",
                transcript_file=transcript,
                summary_bullets=3,
            )

            deliverable = artifacts.deliverable_path.read_text(encoding="utf-8")
            meta = json.loads(artifacts.meta_path.read_text(encoding="utf-8"))

            self.assertIn("## Deep Summary", deliverable)
            self.assertIn("  - ", deliverable)
            self.assertGreater(meta["effective_summary_bullets"], 3)
            self.assertGreater(meta["extraction_controller"]["deep_summary_chunk_count"], 1)

    def test_queue_tracks_package_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue_path = root / "queue.json"
            package_path = root / "client-update.intake-package.json"
            package_path.write_text("{}", encoding="utf-8")

            with patch("local_ai_transcription_service.queue.QUEUE_PATH", queue_path):
                queued = enqueue_intake_package(package_path)
                self.assertEqual(queued["status"], "queued")

                status = queue_status()
                self.assertEqual(status["counts"]["queued"], 1)
                self.assertEqual(status["next_item"]["source_path"], str(package_path.resolve()))

                processing = mark_queue_item(package_path, status="processing")
                self.assertEqual(processing["status"], "processing")
                self.assertEqual(processing["attempt_count"], 1)

                completed = mark_queue_item(package_path, status="completed", output_path=root / "outputs" / "job")
                self.assertEqual(completed["status"], "completed")

                status = queue_status()
                self.assertEqual(status["counts"]["completed"], 1)

    def test_queue_can_stage_direct_audio_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue_path = root / "queue.json"
            audio_path = root / "sample.mp3"
            audio_path.write_text("fake-audio", encoding="utf-8")

            with patch("local_ai_transcription_service.queue.QUEUE_PATH", queue_path):
                queued = enqueue_audio_file(audio_path)
                self.assertEqual(queued["status"], "queued")
                self.assertEqual(queued["source_kind"], "audio_file")

                status = queue_status()
                self.assertEqual(status["counts"]["queued"], 1)
                self.assertEqual(status["next_item"]["source_kind"], "audio_file")

    def test_promote_job_to_proof_copies_artifacts_and_updates_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            transcript = root / "input.txt"
            transcript.write_text(
                "Review the notes. Send the client an update tomorrow. Call the vendor today.",
                encoding="utf-8",
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Client Update",
                transcript_file=transcript,
                summary_bullets=2,
            )

            promoted = promote_job_to_proof(
                job_output=artifacts.output_dir,
                proof_root=root / "proofs",
                proof_label="Client Update Proof",
            )

            proof_dir = Path(promoted["proof_dir"])
            self.assertTrue((proof_dir / "deliverable.md").exists())
            self.assertTrue((proof_dir / "transcript.txt").exists())
            self.assertTrue((proof_dir / "meta.json").exists())
            self.assertTrue((proof_dir / "proof-summary.json").exists())

            index_payload = json.loads((root / "proofs" / "index.json").read_text(encoding="utf-8"))
            self.assertEqual(len(index_payload["proofs"]), 1)
            self.assertEqual(index_payload["proofs"][0]["proof_slug"], "client-update-proof")

    def test_review_proof_writes_scorecard(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            transcript = root / "input.txt"
            transcript.write_text(
                "Review the notes. Send the client an update tomorrow. Call the vendor today.",
                encoding="utf-8",
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Client Update",
                transcript_file=transcript,
                summary_bullets=2,
            )

            promoted = promote_job_to_proof(
                job_output=artifacts.output_dir,
                proof_root=root / "proofs",
                proof_label="Client Update Proof",
            )

            review = review_proof(Path(promoted["proof_dir"]))

            self.assertIn("review_score", review)
            self.assertIn("recommendation", review)
            self.assertTrue((Path(promoted["proof_dir"]) / "proof-review.json").exists())

    def test_review_proof_holds_junk_buyer_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            proof_dir = root / "proof"
            proof_dir.mkdir()
            (proof_dir / "proof-summary.json").write_text(
                json.dumps(
                    {
                        "proof_slug": "junk-proof",
                        "proof_label": "Junk Proof",
                        "quality": {
                            "score": 95,
                            "summary_count": 5,
                            "valid_summary_count": 3,
                            "junk_summary_count": 2,
                            "action_item_count": 4,
                            "valid_action_item_count": 2,
                            "junk_action_item_count": 2,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (proof_dir / "deliverable.md").write_text(
                (
                    "# Junk Proof\n\n"
                    "## Summary\n"
                    "- Welcome back to the show.\n"
                    "- The number one problem was weak product adoption after sign-up.\n"
                    "- The team decided to target event promoters before broadening the market.\n\n"
                    "## Action Items\n"
                    "- Why should I really try out this app?\n"
                    "- They're like, yeah, you need to make Chinese a little bit.\n"
                    "- Build a simple scorecard for product adoption this week.\n\n"
                    "## Transcript\n"
                    "Raw transcript.\n"
                ),
                encoding="utf-8",
            )

            review = review_proof(proof_dir)

            self.assertEqual(review["recommendation"], "hold")
            self.assertTrue(review["review_snapshot"]["junk_summary_bullets"])
            self.assertTrue(review["review_snapshot"]["junk_action_bullets"])

    def test_transcript_batch_generates_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "inputs"
            input_dir.mkdir()
            (input_dir / "first.txt").write_text(
                "Um review the draft. Send the client an update tomorrow.",
                encoding="utf-8",
            )
            (input_dir / "second.txt").write_text(
                "Call the vendor today. The schedule is almost final.",
                encoding="utf-8",
            )

            summary_path = run_batch_pipeline(
                output_root=root / "outputs",
                transcript_dir=input_dir,
                summary_bullets=2,
            )

            self.assertTrue(summary_path.exists())
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["source_kind"], "transcript_file")
            self.assertEqual(payload["total_jobs"], 2)
            self.assertEqual(payload["successful_jobs"], 2)
            self.assertEqual(payload["failed_jobs"], 0)
            self.assertEqual(len(payload["jobs"]), 2)
            self.assertEqual(payload["jobs"][0]["job_slug"], "first")
            self.assertEqual(payload["jobs"][1]["job_slug"], "second")
            self.assertIn("quality", payload["jobs"][0])
            self.assertEqual(payload["failures"], [])

    @patch("local_ai_transcription_service.pipeline.run_pipeline")
    def test_intake_batch_pipeline_processes_manifest_directory(self, mock_run_pipeline) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            intake_dir = root / "intake"
            intake_dir.mkdir()
            transcript_file = intake_dir / "memo.txt"
            transcript_file.write_text("Review the draft. Send the client an update tomorrow.", encoding="utf-8")
            manifest_path = intake_dir / "founder-memo.intake.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "jobId": "job-123",
                        "jobSlug": "acme-founder-memo",
                        "clientName": "Acme",
                        "jobTitle": "Founder Memo",
                        "outputGoal": "meeting-recap",
                        "contextNotes": "Focus on next-step actions.",
                        "createdAt": "2026-04-03T12:00:00Z",
                        "fileName": "memo.txt",
                    }
                ),
                encoding="utf-8",
            )

            successful_artifact = run_pipeline(
                output_root=root / "seed-output",
                job_name="Founder Memo",
                transcript_file=transcript_file,
                summary_bullets=2,
            )
            mock_run_pipeline.return_value = successful_artifact

            summary_path = run_intake_batch_pipeline(
                output_root=root / "outputs",
                intake_dir=intake_dir,
                summary_bullets=2,
            )

            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["source_kind"], "intake_manifest")
            self.assertEqual(payload["successful_jobs"], 1)
            self.assertEqual(payload["failed_jobs"], 0)
            self.assertEqual(payload["jobs"][0]["manifest_path"], str(manifest_path))

    @patch("local_ai_transcription_service.pipeline.run_pipeline")
    def test_batch_continues_when_one_file_fails(self, mock_run_pipeline) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "inputs"
            input_dir.mkdir()
            first = input_dir / "first.txt"
            second = input_dir / "second.txt"
            first.write_text("Review the notes.", encoding="utf-8")
            second.write_text("Call the vendor today.", encoding="utf-8")

            successful_artifact = run_pipeline(
                output_root=root / "seed-output",
                job_name="first",
                transcript_file=first,
                summary_bullets=2,
            )

            def fake_run_pipeline(**kwargs):
                source = kwargs["transcript_file"]
                if source == first:
                    return successful_artifact
                raise RuntimeError("mock batch failure")

            mock_run_pipeline.side_effect = fake_run_pipeline

            summary_path = run_batch_pipeline(
                output_root=root / "outputs",
                transcript_dir=input_dir,
                summary_bullets=2,
            )

            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            failures_path = root / "outputs" / "batch-failures.json"
            failures_payload = json.loads(failures_path.read_text(encoding="utf-8"))

            self.assertEqual(payload["total_jobs"], 2)
            self.assertEqual(payload["successful_jobs"], 1)
            self.assertEqual(payload["failed_jobs"], 1)
            self.assertEqual(len(payload["jobs"]), 1)
            self.assertEqual(len(payload["failures"]), 1)
            self.assertEqual(payload["failures"][0]["job_slug"], "second")
            self.assertEqual(payload["retry_candidates"], [str(second)])
            self.assertTrue(failures_path.exists())
            self.assertEqual(failures_payload["total_failures"], 1)
            self.assertEqual(failures_payload["failures"][0]["error_type"], "RuntimeError")

    @patch("local_ai_transcription_service.transcription.transcribe_with_whisper")
    @patch("local_ai_transcription_service.transcription.normalize_audio_with_ffmpeg")
    @patch(
        "local_ai_transcription_service.transcription.inspect_environment",
        return_value=EnvironmentReport(
            whisper_binary="/usr/local/bin/whisper",
            ffmpeg_binary="/usr/local/bin/ffmpeg",
        ),
    )
    def test_audio_wrapper_uses_ffmpeg_when_available(
        self,
        _mock_env,
        mock_normalize,
        mock_transcribe,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_audio = root / "meeting.m4a"
            source_audio.write_text("fake-audio", encoding="utf-8")
            output_dir = root / "outputs"
            normalized_audio = output_dir / "meeting.normalized.wav"

            def fake_normalize(source_path: Path, prepared_path: Path, *, ffmpeg_binary: str) -> Path:
                self.assertEqual(source_path, source_audio)
                self.assertEqual(prepared_path, normalized_audio)
                self.assertEqual(ffmpeg_binary, "/usr/local/bin/ffmpeg")
                prepared_path.parent.mkdir(parents=True, exist_ok=True)
                prepared_path.write_text("normalized", encoding="utf-8")
                return prepared_path

            def fake_transcribe(
                audio_path: Path,
                *,
                output_dir: Optional[Path] = None,
                whisper_binary: Optional[str] = None,
                model: str = "base",
                language: Optional[str] = None,
            ) -> str:
                self.assertEqual(audio_path, normalized_audio)
                self.assertEqual(output_dir, root / "outputs")
                self.assertEqual(whisper_binary, "/usr/local/bin/whisper")
                self.assertEqual(model, "base")
                self.assertIsNone(language)
                assert output_dir is not None
                transcript_path = output_dir / "meeting.normalized.txt"
                transcript_path.write_text("Normalized transcript", encoding="utf-8")
                return "Normalized transcript"

            mock_normalize.side_effect = fake_normalize
            mock_transcribe.side_effect = fake_transcribe

            result = run_audio_wrapper(source_audio, output_dir=output_dir)

            self.assertTrue(result.normalized)
            self.assertEqual(result.prepared_audio_path, normalized_audio)
            self.assertTrue(result.transcript_path.exists())
            self.assertEqual(result.transcript_path.read_text(encoding="utf-8").strip(), "Normalized transcript")
            self.assertIsInstance(result.model_cached, bool)

    @patch("local_ai_transcription_service.pipeline.run_audio_wrapper")
    def test_audio_pipeline_generates_outputs(self, mock_run_audio_wrapper) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            audio_file = root / "call.wav"
            audio_file.write_text("fake-audio", encoding="utf-8")
            transcript_path = root / "outputs" / "client-call" / "call.normalized.txt"
            transcript_path.parent.mkdir(parents=True, exist_ok=True)
            transcript_path.write_text(
                "Review the notes. Send the follow-up email tomorrow. The next version is almost ready. Call the team today.",
                encoding="utf-8",
            )

            mock_run_audio_wrapper.return_value = AudioPrepResult(
                source_audio_path=audio_file,
                prepared_audio_path=root / "outputs" / "client-call" / "call.normalized.wav",
                normalized=True,
                transcript_path=transcript_path,
                whisper_model="tiny.en",
                language="en",
                model_cached=True,
                environment=EnvironmentReport(
                    whisper_binary="/usr/local/bin/whisper",
                    ffmpeg_binary="/usr/local/bin/ffmpeg",
                ),
            )

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Client Call",
                audio_file=audio_file,
                intake_manifest={
                    "jobId": "job-2",
                    "jobSlug": "client-call",
                    "clientName": "Northstar",
                    "jobTitle": "Client Call",
                    "outputGoal": "transcript-summary-actions",
                    "contextNotes": "Customer interview. Capture blockers.",
                    "createdAt": "2026-04-03T12:00:00Z",
                },
                summary_bullets=2,
                whisper_model="tiny.en",
                language="en",
            )

            self.assertTrue(artifacts.transcript_path.exists())
            self.assertTrue(artifacts.deliverable_path.exists())
            self.assertTrue(artifacts.meta_path.exists())
            self.assertTrue(artifacts.raw_transcript_path.exists())

            meta = artifacts.meta_path.read_text(encoding="utf-8")
            deliverable = artifacts.deliverable_path.read_text(encoding="utf-8")

            self.assertIn('"source_kind": "audio_file"', meta)
            self.assertIn('"audio_wrapper_used": true', meta)
            self.assertIn('"whisper_model": "tiny.en"', meta)
            self.assertIn('"language": "en"', meta)
            self.assertIn('"quality"', meta)
            self.assertIn('"intake_manifest"', meta)
            self.assertIn("# Client Call", deliverable)
            self.assertIn("Generated from raw audio with local Whisper.", deliverable)
            self.assertIn("Job metadata imported from browser intake manifest.", deliverable)
            self.assertIn("Send the follow-up email tomorrow.", deliverable)
            self.assertIn("Call the team today.", deliverable)
            mock_run_audio_wrapper.assert_called_once()

    @patch("local_ai_transcription_service.pipeline.synthesize_with_piper")
    @patch(
        "local_ai_transcription_service.pipeline.inspect_environment",
        return_value=EnvironmentReport(
            whisper_binary="/usr/local/bin/whisper",
            ffmpeg_binary="/usr/local/bin/ffmpeg",
            piper_binary="/usr/local/bin/piper",
            piper_model="/tmp/piper.onnx",
        ),
    )
    def test_transcript_pipeline_can_generate_tts_audio(self, _mock_env, mock_synthesize) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            transcript = root / "input.txt"
            transcript.write_text(
                "Review the draft. Send the client an update tomorrow. Call the vendor today.",
                encoding="utf-8",
            )

            def fake_synthesize(text: str, output_path: Path, **_kwargs) -> Path:
                self.assertIn("Summary:", text)
                output_path.write_text("fake-wav", encoding="utf-8")
                return output_path

            mock_synthesize.side_effect = fake_synthesize

            artifacts = run_pipeline(
                output_root=root / "outputs",
                job_name="Client Update",
                transcript_file=transcript,
                summary_bullets=2,
                tts_enabled=True,
                tts_input="summary",
            )

            meta = artifacts.meta_path.read_text(encoding="utf-8")
            deliverable = artifacts.deliverable_path.read_text(encoding="utf-8")

            self.assertIsNotNone(artifacts.speech_path)
            assert artifacts.speech_path is not None
            self.assertTrue(artifacts.speech_path.exists())
            self.assertIn('"tts"', meta)
            self.assertIn('"input": "summary"', meta)
            self.assertIn("Local Piper speech generated from the summary layer.", deliverable)


if __name__ == "__main__":
    unittest.main()
