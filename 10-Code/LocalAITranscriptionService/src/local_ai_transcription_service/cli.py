import argparse
from .bootstrap import build_bootstrap_plan
from .benchmark import run_benchmark_pack
import json
from pathlib import Path

from .pipeline import rebuild_job_artifacts, resolve_job_name, run_batch_pipeline, run_intake_batch_pipeline, run_pipeline
from .proof import promote_job_to_proof
from .queue import enqueue_audio_file, enqueue_intake_package, enqueue_transcript_file, mark_queue_item, next_queued_item, queue_status
from .review import review_proof
from .runtime_guard import guarded_runtime, runtime_status
from .transcription import inspect_environment, inspect_model_cache, warm_model_cache


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local transcription delivery scaffold.")
    parser.add_argument("--job-name", help="Human-readable job name. Defaults to source file stem.")
    parser.add_argument("--transcript-file", type=Path, help="Path to an existing transcript file.")
    parser.add_argument("--audio-file", type=Path, help="Path to an audio file to transcribe with whisper.")
    parser.add_argument("--intake-manifest", type=Path, help="Path to an exported browser intake manifest.")
    parser.add_argument("--intake-package", type=Path, help="Path to a one-file browser intake package with embedded source audio.")
    parser.add_argument("--intake-dir", type=Path, help="Path to a directory of exported browser intake manifests and files.")
    parser.add_argument("--rebuild-job", type=Path, help="Path to an existing job output directory or meta.json to rebuild without rerunning Whisper.")
    parser.add_argument("--promote-proof", type=Path, help="Promote an existing job output directory or meta.json into samples/proof-deliverables.")
    parser.add_argument("--review-proof", type=Path, help="Review a promoted proof directory or proof-summary.json.")
    parser.add_argument("--proof-root", type=Path, default=Path("samples/proof-deliverables"), help="Root folder for promoted proof assets.")
    parser.add_argument("--proof-label", help="Optional label used to name the promoted proof asset.")
    parser.add_argument("--enqueue-intake-package", type=Path, help="Add an intake package to the lightweight local job queue.")
    parser.add_argument("--enqueue-audio-file", type=Path, help="Add a direct audio file to the lightweight local job queue.")
    parser.add_argument("--enqueue-transcript-file", type=Path, help="Add a direct transcript file to the lightweight local job queue.")
    parser.add_argument("--queue-status", action="store_true", help="Print the lightweight local queue status and exit.")
    parser.add_argument("--process-queued", action="store_true", help="Process queued intake packages when the machine is ready.")
    parser.add_argument("--max-queued-jobs", type=int, default=1, help="How many queued jobs to process when using --process-queued.")
    parser.add_argument("--transcript-dir", type=Path, help="Path to a directory of transcript files.")
    parser.add_argument("--audio-dir", type=Path, help="Path to a directory of audio files.")
    parser.add_argument("--benchmark-dir", type=Path, help="Path to a benchmark pack directory.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("outputs"),
        help="Root folder for generated jobs.",
    )
    parser.add_argument(
        "--summary-bullets",
        type=int,
        default=5,
        help="How many summary bullets to generate.",
    )
    parser.add_argument(
        "--whisper-model",
        default="base",
        help="Whisper model name for audio transcription.",
    )
    parser.add_argument(
        "--language",
        help="Optional language hint for Whisper, for example en.",
    )
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="Print dependency availability and exit.",
    )
    parser.add_argument(
        "--bootstrap-plan",
        action="store_true",
        help="Print setup steps for missing local dependencies and exit.",
    )
    parser.add_argument(
        "--check-model-cache",
        action="store_true",
        help="Print Whisper model cache status and exit.",
    )
    parser.add_argument(
        "--warm-model-cache",
        action="store_true",
        help="Download the selected Whisper model into the local cache and exit.",
    )
    parser.add_argument(
        "--tts",
        action="store_true",
        help="Generate local speech output with Piper after the deliverable is built.",
    )
    parser.add_argument(
        "--tts-input",
        choices=("summary", "deliverable", "transcript"),
        default="summary",
        help="Which text layer Piper should speak.",
    )
    parser.add_argument(
        "--unsafe-skip-guardrails",
        action="store_true",
        help="Bypass load and lock guardrails. Use only when you are sure the machine can handle it.",
    )
    parser.add_argument(
        "--check-runtime",
        action="store_true",
        help="Print current runtime guardrail status and exit.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.check_env:
        print(json.dumps(inspect_environment().to_dict(), indent=2))
        return 0

    if args.check_runtime:
        print(json.dumps(runtime_status("local_ai_transcription_service"), indent=2))
        return 0

    if args.queue_status:
        print(json.dumps(queue_status(), indent=2))
        return 0

    if args.enqueue_intake_package:
        print(json.dumps(enqueue_intake_package(args.enqueue_intake_package), indent=2))
        return 0

    if args.enqueue_audio_file:
        print(json.dumps(enqueue_audio_file(args.enqueue_audio_file), indent=2))
        return 0

    if args.enqueue_transcript_file:
        print(json.dumps(enqueue_transcript_file(args.enqueue_transcript_file), indent=2))
        return 0

    if args.bootstrap_plan:
        environment = inspect_environment()
        print(json.dumps(build_bootstrap_plan(environment), indent=2))
        return 0

    if args.check_model_cache:
        print(json.dumps(inspect_model_cache(args.whisper_model), indent=2))
        return 0

    if args.warm_model_cache:
        with guarded_runtime(
            process_name="local_ai_transcription_service:model_warmup",
            unsafe_skip=args.unsafe_skip_guardrails,
        ):
            print(json.dumps(warm_model_cache(args.whisper_model), indent=2))
        return 0

    if args.benchmark_dir:
        print(json.dumps(run_benchmark_pack(args.benchmark_dir, summary_bullets=args.summary_bullets), indent=2))
        return 0

    source_count = sum(
        bool(value)
        for value in (
            args.transcript_file,
            args.audio_file,
            args.intake_package,
            args.intake_dir,
            args.rebuild_job,
            args.promote_proof,
            args.review_proof,
            args.enqueue_intake_package,
            args.enqueue_audio_file,
            args.enqueue_transcript_file,
            args.process_queued,
            args.transcript_dir,
            args.audio_dir,
            args.benchmark_dir,
        )
    )
    if source_count == 0:
        parser.error("one input source is required")
    if source_count > 1:
        parser.error("provide only one of --transcript-file, --audio-file, --intake-package, --intake-dir, --rebuild-job, --transcript-dir, --audio-dir, or --benchmark-dir")

    if args.rebuild_job:
        artifacts = rebuild_job_artifacts(
            job_output=args.rebuild_job,
            summary_bullets=args.summary_bullets,
            tts_enabled=args.tts,
            tts_input=args.tts_input,
        )
        print(f"Job rebuilt: {artifacts.job_slug}")
        print(f"Transcript: {artifacts.transcript_path}")
        print(f"Deliverable: {artifacts.deliverable_path}")
        print(f"Meta: {artifacts.meta_path}")
        if artifacts.speech_path and artifacts.speech_path.exists():
            print(f"Speech: {artifacts.speech_path}")
        return 0

    if args.promote_proof:
        promoted = promote_job_to_proof(
            job_output=args.promote_proof,
            proof_root=args.proof_root,
            proof_label=args.proof_label,
        )
        print(json.dumps(promoted, indent=2))
        return 0

    if args.review_proof:
        print(json.dumps(review_proof(args.review_proof), indent=2))
        return 0

    if args.process_queued:
        processed = []
        with guarded_runtime(
            process_name="local_ai_transcription_service",
            unsafe_skip=args.unsafe_skip_guardrails,
        ):
            for _ in range(max(1, args.max_queued_jobs)):
                item = next_queued_item()
                if not item:
                    break
                source_path = Path(str(item["source_path"]))
                source_kind = str(item.get("source_kind") or "intake_package")
                mark_queue_item(source_path, status="processing")
                try:
                    if source_kind == "intake_package":
                        job_name, intake_manifest = resolve_job_name(
                            source_path=None,
                            intake_package_path=source_path,
                        )
                        artifacts = run_pipeline(
                            output_root=args.output_root,
                            job_name=job_name,
                            intake_package_path=source_path,
                            intake_manifest=intake_manifest,
                            summary_bullets=args.summary_bullets,
                            whisper_model=args.whisper_model,
                            language=args.language,
                            tts_enabled=args.tts,
                            tts_input=args.tts_input,
                        )
                    elif source_kind == "audio_file":
                        job_name, intake_manifest = resolve_job_name(
                            source_path=source_path,
                            explicit_job_name=None,
                        )
                        artifacts = run_pipeline(
                            output_root=args.output_root,
                            job_name=job_name,
                            audio_file=source_path,
                            intake_manifest=intake_manifest,
                            summary_bullets=args.summary_bullets,
                            whisper_model=args.whisper_model,
                            language=args.language,
                            tts_enabled=args.tts,
                            tts_input=args.tts_input,
                        )
                    elif source_kind == "transcript_file":
                        job_name, intake_manifest = resolve_job_name(
                            source_path=source_path,
                            explicit_job_name=None,
                        )
                        artifacts = run_pipeline(
                            output_root=args.output_root,
                            job_name=job_name,
                            transcript_file=source_path,
                            intake_manifest=intake_manifest,
                            summary_bullets=args.summary_bullets,
                            whisper_model=args.whisper_model,
                            language=args.language,
                            tts_enabled=args.tts,
                            tts_input=args.tts_input,
                        )
                    else:
                        raise ValueError(f"Unsupported queued source kind: {source_kind}")
                    mark_queue_item(source_path, status="completed", output_path=artifacts.output_dir)
                    processed.append(
                        {
                            "source_path": str(source_path),
                            "source_kind": source_kind,
                            "job_slug": artifacts.job_slug,
                            "output_dir": str(artifacts.output_dir),
                            "status": "completed",
                        }
                    )
                except Exception as error:
                    mark_queue_item(source_path, status="failed", error=str(error))
                    processed.append(
                        {
                            "source_path": str(source_path),
                            "source_kind": source_kind,
                            "status": "failed",
                            "error": str(error),
                        }
                    )
        print(json.dumps({"processed": processed, "queue": queue_status()}, indent=2))
        return 0

    with guarded_runtime(
        process_name="local_ai_transcription_service",
        unsafe_skip=args.unsafe_skip_guardrails,
    ):
        if args.intake_dir:
            if args.intake_manifest or args.intake_package:
                parser.error("--intake-manifest and --intake-package are only supported for single-file runs")
            summary_path = run_intake_batch_pipeline(
                output_root=args.output_root,
                intake_dir=args.intake_dir,
                summary_bullets=args.summary_bullets,
                whisper_model=args.whisper_model,
                language=args.language,
                tts_enabled=args.tts,
                tts_input=args.tts_input,
            )
            print(f"Batch summary: {summary_path}")
            return 0

        if args.transcript_dir or args.audio_dir:
            if args.intake_manifest or args.intake_package:
                parser.error("--intake-manifest and --intake-package are only supported for single-file runs")
            summary_path = run_batch_pipeline(
                output_root=args.output_root,
                transcript_dir=args.transcript_dir,
                audio_dir=args.audio_dir,
                summary_bullets=args.summary_bullets,
                whisper_model=args.whisper_model,
                language=args.language,
                tts_enabled=args.tts,
                tts_input=args.tts_input,
            )
            print(f"Batch summary: {summary_path}")
            return 0

        source_path = args.transcript_file or args.audio_file
        job_name, intake_manifest = resolve_job_name(
            source_path=source_path,
            explicit_job_name=args.job_name,
            intake_manifest_path=args.intake_manifest,
            intake_package_path=args.intake_package,
        )

        artifacts = run_pipeline(
            output_root=args.output_root,
            job_name=job_name,
            transcript_file=args.transcript_file,
            audio_file=args.audio_file,
            intake_package_path=args.intake_package,
            intake_manifest=intake_manifest,
            summary_bullets=args.summary_bullets,
            whisper_model=args.whisper_model,
            language=args.language,
            tts_enabled=args.tts,
            tts_input=args.tts_input,
        )

        print(f"Job created: {artifacts.job_slug}")
        print(f"Transcript: {artifacts.transcript_path}")
        print(f"Deliverable: {artifacts.deliverable_path}")
        print(f"Meta: {artifacts.meta_path}")
        if artifacts.speech_path and artifacts.speech_path.exists():
            print(f"Speech: {artifacts.speech_path}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
