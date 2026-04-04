import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .cleanup import clean_transcript_text
from .intake import (
    INTAKE_MANIFEST_SUFFIX,
    discover_intake_manifests,
    extract_intake_package_source,
    load_intake_manifest,
    load_intake_package,
    resolve_intake_source,
)
from .models import Deliverable, JobArtifacts
from .paragraphs import build_transcript_paragraphs
from .piper import build_tts_script, load_piper_defaults, synthesize_with_piper
from .quality import score_quality
from .summary import generate_best_extraction
from .templates import render_deliverable
from .transcription import inspect_environment, load_transcript_file, run_audio_wrapper


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "job"


def create_job_artifacts(base_output_dir: Path, job_name: str) -> JobArtifacts:
    job_slug = slugify(job_name)
    output_dir = base_output_dir / job_slug
    output_dir.mkdir(parents=True, exist_ok=True)
    return JobArtifacts(
        job_name=job_name,
        job_slug=job_slug,
        output_dir=output_dir,
        raw_transcript_path=output_dir / "raw_transcript.txt",
        transcript_path=output_dir / "transcript.txt",
        deliverable_path=output_dir / "deliverable.md",
        meta_path=output_dir / "meta.json",
        speech_path=output_dir / "speech.wav",
    )


def build_deliverable(
    *,
    transcript_text: str,
    transcript_paragraphs: List[str],
    job_name: str,
    source_kind: str,
    summary_bullets: int,
    intake_manifest: Optional[Dict[str, object]],
    quality: dict,
    processing_notes: List[str],
    summary_output: Optional[Dict[str, object]] = None,
) -> Deliverable:
    extracted = summary_output or generate_best_extraction(transcript_text, bullets=summary_bullets)
    return Deliverable(
        title=job_name,
        transcript=transcript_text.strip(),
        transcript_paragraphs=transcript_paragraphs,
        summary_bullets=extracted["summary_bullets"],  # type: ignore[index]
        deep_summary_sections=extracted.get("deep_summary_sections", []),  # type: ignore[arg-type]
        action_items=extracted["action_items"],  # type: ignore[index]
        source_kind=source_kind,
        client_name=str(intake_manifest.get("clientName")) if intake_manifest else None,
        output_goal=str(intake_manifest.get("outputGoal")) if intake_manifest else None,
        context_notes=str(intake_manifest.get("contextNotes")) if intake_manifest and intake_manifest.get("contextNotes") else None,
        quality=quality,
        processing_notes=processing_notes,
    )


def rebuild_job_artifacts(
    *,
    job_output: Path,
    summary_bullets: Optional[int] = None,
    tts_enabled: bool = False,
    tts_input: str = "summary",
) -> JobArtifacts:
    if job_output.is_file():
        if job_output.name != "meta.json":
            raise ValueError("Rebuild path must be a job output directory or meta.json.")
        job_output = job_output.parent

    meta_path = job_output / "meta.json"
    raw_transcript_path = job_output / "raw_transcript.txt"
    transcript_path = job_output / "transcript.txt"

    if not meta_path.exists():
        raise ValueError(f"Missing meta.json in job output: {job_output}")
    if not raw_transcript_path.exists() and not transcript_path.exists():
        raise ValueError(f"Missing transcript artifacts in job output: {job_output}")

    previous_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    job_name = str(previous_meta.get("job_name") or job_output.name)
    artifacts = create_job_artifacts(job_output.parent, job_name)

    selected_summary_bullets = summary_bullets or int(previous_meta.get("summary_bullets") or 5)
    raw_transcript_text = (
        raw_transcript_path.read_text(encoding="utf-8").strip()
        if raw_transcript_path.exists()
        else transcript_path.read_text(encoding="utf-8").strip()
    )
    source_kind = str(previous_meta.get("source_kind") or "transcript_file")
    intake_manifest = previous_meta.get("intake_manifest")
    if not isinstance(intake_manifest, dict):
        intake_manifest = None

    cleanup_result = clean_transcript_text(raw_transcript_text)
    transcript_text = str(cleanup_result["cleaned_text"])
    if not transcript_text.strip():
        raise ValueError("Transcript is empty after cleanup.")
    paragraph_result = build_transcript_paragraphs(transcript_text)
    summary_output = generate_best_extraction(transcript_text, bullets=selected_summary_bullets)
    draft_deliverable = Deliverable(
        title=job_name,
        transcript=transcript_text.strip(),
        transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
        summary_bullets=summary_output["summary_bullets"],  # type: ignore[index]
        deep_summary_sections=summary_output.get("deep_summary_sections", []),  # type: ignore[arg-type]
        action_items=summary_output["action_items"],  # type: ignore[index]
        source_kind=source_kind,
        client_name=str(intake_manifest.get("clientName")) if intake_manifest else None,
        output_goal=str(intake_manifest.get("outputGoal")) if intake_manifest else None,
        context_notes=str(intake_manifest.get("contextNotes")) if intake_manifest and intake_manifest.get("contextNotes") else None,
        quality={},
        processing_notes=[],
    )
    quality = score_quality(
        raw_transcript_text=raw_transcript_text,
        cleaned_transcript_text=transcript_text,
        cleanup_result=cleanup_result,
        deliverable=draft_deliverable,
    )

    processing_notes = [
        "Deliverable rebuilt from saved transcript artifacts without rerunning Whisper.",
    ]
    if intake_manifest:
        processing_notes.append("Job metadata imported from browser intake manifest.")
    if cleanup_result["changed"]:
        processing_notes.append("Cleanup pass adjusted transcript text before formatting.")
    if paragraph_result["paragraph_count"]:
        processing_notes.append(
            f"Transcript formatted into {paragraph_result['paragraph_count']} paragraph blocks."
        )

    tts_payload = previous_meta.get("tts")
    if tts_enabled:
        environment = inspect_environment()
        if not environment.piper_available:
            raise RuntimeError("Piper is not configured. Check --check-env for piper_binary and piper_model.")
        piper_defaults = load_piper_defaults()
        tts_text = build_tts_script(
            Deliverable(
                title=job_name,
                transcript=transcript_text.strip(),
                transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
                summary_bullets=summary_output["summary_bullets"],  # type: ignore[index]
                deep_summary_sections=summary_output.get("deep_summary_sections", []),  # type: ignore[arg-type]
                action_items=summary_output["action_items"],  # type: ignore[index]
                source_kind=source_kind,
                client_name=str(intake_manifest.get("clientName")) if intake_manifest else None,
                output_goal=str(intake_manifest.get("outputGoal")) if intake_manifest else None,
                context_notes=str(intake_manifest.get("contextNotes")) if intake_manifest and intake_manifest.get("contextNotes") else None,
                quality=quality,
                processing_notes=processing_notes,
            ),
            mode=tts_input,
        )
        synthesize_with_piper(
            tts_text,
            artifacts.speech_path,  # type: ignore[arg-type]
            piper_binary=str(environment.piper_binary),
            model_path=str(environment.piper_model),
            sentence_silence=str(piper_defaults.get("sentence_silence")) if piper_defaults.get("sentence_silence") else None,
            volume=str(piper_defaults.get("volume")) if piper_defaults.get("volume") else None,
        )
        processing_notes.append(f"Local Piper speech generated from the {tts_input} layer.")
        tts_payload = {
            "enabled": True,
            "input": tts_input,
            "speech_path": str(artifacts.speech_path),
            "piper_binary": environment.piper_binary,
            "piper_model": environment.piper_model,
        }

    deliverable = build_deliverable(
        transcript_text=transcript_text,
        transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
        job_name=job_name,
        source_kind=source_kind,
        summary_bullets=selected_summary_bullets,
        intake_manifest=intake_manifest,
        quality=quality,
        processing_notes=processing_notes,
        summary_output=summary_output,
    )

    artifacts.raw_transcript_path.write_text(raw_transcript_text + "\n", encoding="utf-8")
    artifacts.transcript_path.write_text(deliverable.transcript + "\n", encoding="utf-8")
    artifacts.deliverable_path.write_text(render_deliverable(deliverable), encoding="utf-8")
    artifacts.meta_path.write_text(
        json.dumps(
            {
                **previous_meta,
                "job_name": artifacts.job_name,
                "job_slug": artifacts.job_slug,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "summary_bullets": selected_summary_bullets,
                "effective_summary_bullets": summary_output.get("effective_summary_bullets"),
                "cleanup": cleanup_result,
                "paragraphs": paragraph_result,
                "extraction_controller": summary_output,
                "quality": quality,
                "tts": tts_payload,
                "rebuild": {
                    "rebuilt_at": datetime.now(timezone.utc).isoformat(),
                    "used_saved_artifacts": True,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return artifacts


def run_pipeline(
    *,
    output_root: Path,
    job_name: str,
    transcript_file: Optional[Path] = None,
    audio_file: Optional[Path] = None,
    intake_package_path: Optional[Path] = None,
    intake_manifest: Optional[Dict[str, object]] = None,
    summary_bullets: int = 5,
    whisper_model: str = "base",
    language: Optional[str] = None,
    tts_enabled: bool = False,
    tts_input: str = "summary",
) -> JobArtifacts:
    if not transcript_file and not audio_file and not intake_package_path:
        raise ValueError("Provide transcript_file, audio_file, or intake_package_path.")

    artifacts = create_job_artifacts(output_root, job_name)

    if intake_package_path:
        package_payload = load_intake_package(intake_package_path)
        package_manifest, packaged_source_path, packaged_source_kind = extract_intake_package_source(
            intake_package_path,
            package_payload,
            artifacts.output_dir,
        )
        intake_manifest = package_manifest
        if packaged_source_kind == "transcript_file":
            transcript_file = packaged_source_path
        else:
            audio_file = packaged_source_path

    if transcript_file:
        raw_transcript_text = load_transcript_file(transcript_file)
        source_kind = "transcript_file"
        wrapper_payload = None
    else:
        wrapper_result = run_audio_wrapper(
            audio_file,  # type: ignore[arg-type]
            output_dir=artifacts.output_dir,
            whisper_model=whisper_model,
            language=language,
        )
        raw_transcript_text = wrapper_result.transcript_path.read_text(encoding="utf-8").strip()
        source_kind = "audio_file"
        wrapper_payload = wrapper_result.to_dict()

    cleanup_result = clean_transcript_text(raw_transcript_text)
    transcript_text = str(cleanup_result["cleaned_text"])
    if not transcript_text.strip():
        raise ValueError("Transcript is empty after cleanup.")
    paragraph_result = build_transcript_paragraphs(transcript_text)
    summary_output = generate_best_extraction(transcript_text, bullets=summary_bullets)
    quality = score_quality(
        raw_transcript_text=raw_transcript_text,
        cleaned_transcript_text=transcript_text,
        cleanup_result=cleanup_result,
        deliverable=Deliverable(
            title=job_name,
            transcript=transcript_text.strip(),
            transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
            summary_bullets=summary_output["summary_bullets"],  # type: ignore[index]
            deep_summary_sections=summary_output.get("deep_summary_sections", []),  # type: ignore[arg-type]
            action_items=summary_output["action_items"],  # type: ignore[index]
            source_kind=source_kind,
            client_name=str(intake_manifest.get("clientName")) if intake_manifest else None,
            output_goal=str(intake_manifest.get("outputGoal")) if intake_manifest else None,
            context_notes=str(intake_manifest.get("contextNotes")) if intake_manifest and intake_manifest.get("contextNotes") else None,
            quality={},
            processing_notes=[],
        ),
    )

    processing_notes = []
    if source_kind == "audio_file":
        processing_notes.append("Generated from raw audio with local Whisper.")
    else:
        processing_notes.append("Generated from an existing transcript file.")
    if intake_manifest:
        processing_notes.append("Job metadata imported from browser intake manifest.")
    if cleanup_result["changed"]:
        processing_notes.append("Cleanup pass adjusted transcript text before formatting.")
    if paragraph_result["paragraph_count"]:
        processing_notes.append(
            f"Transcript formatted into {paragraph_result['paragraph_count']} paragraph blocks."
        )
    tts_payload = None
    if tts_enabled:
        environment = inspect_environment()
        if not environment.piper_available:
            raise RuntimeError("Piper is not configured. Check --check-env for piper_binary and piper_model.")
        piper_defaults = load_piper_defaults()
        tts_text = build_tts_script(
            Deliverable(
                title=job_name,
                transcript=transcript_text.strip(),
                transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
                summary_bullets=summary_output["summary_bullets"],  # type: ignore[index]
                deep_summary_sections=summary_output.get("deep_summary_sections", []),  # type: ignore[arg-type]
                action_items=summary_output["action_items"],  # type: ignore[index]
                source_kind=source_kind,
                client_name=str(intake_manifest.get("clientName")) if intake_manifest else None,
                output_goal=str(intake_manifest.get("outputGoal")) if intake_manifest else None,
                context_notes=str(intake_manifest.get("contextNotes")) if intake_manifest and intake_manifest.get("contextNotes") else None,
                quality=quality,
                processing_notes=processing_notes,
            ),
            mode=tts_input,
        )
        synthesize_with_piper(
            tts_text,
            artifacts.speech_path,  # type: ignore[arg-type]
            piper_binary=str(environment.piper_binary),
            model_path=str(environment.piper_model),
            sentence_silence=str(piper_defaults.get("sentence_silence")) if piper_defaults.get("sentence_silence") else None,
            volume=str(piper_defaults.get("volume")) if piper_defaults.get("volume") else None,
        )
        processing_notes.append(f"Local Piper speech generated from the {tts_input} layer.")
        tts_payload = {
            "enabled": True,
            "input": tts_input,
            "speech_path": str(artifacts.speech_path),
            "piper_binary": environment.piper_binary,
            "piper_model": environment.piper_model,
        }
    else:
        artifacts.speech_path = None

    deliverable = build_deliverable(
        transcript_text=transcript_text,
        transcript_paragraphs=paragraph_result["paragraphs"],  # type: ignore[arg-type]
        job_name=job_name,
        source_kind=source_kind,
        summary_bullets=summary_bullets,
        intake_manifest=intake_manifest,
        quality=quality,
        processing_notes=processing_notes,
        summary_output=summary_output,
    )

    artifacts.raw_transcript_path.write_text(raw_transcript_text + "\n", encoding="utf-8")
    artifacts.transcript_path.write_text(deliverable.transcript + "\n", encoding="utf-8")
    artifacts.deliverable_path.write_text(render_deliverable(deliverable), encoding="utf-8")
    artifacts.meta_path.write_text(
        json.dumps(
            {
                "job_name": artifacts.job_name,
                "job_slug": artifacts.job_slug,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source_kind": source_kind,
                "summary_bullets": summary_bullets,
                "effective_summary_bullets": summary_output.get("effective_summary_bullets"),
                "audio_wrapper_used": bool(audio_file),
                "whisper_model": whisper_model if audio_file else None,
                "language": language if audio_file else None,
                "cleanup": cleanup_result,
                "paragraphs": paragraph_result,
                "extraction_controller": summary_output,
                "quality": quality,
                "intake_manifest": intake_manifest,
                "audio_wrapper": wrapper_payload,
                "tts": tts_payload,
                "intake_package_path": str(intake_package_path) if intake_package_path else None,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return artifacts


def discover_input_files(
    *,
    transcript_dir: Optional[Path] = None,
    audio_dir: Optional[Path] = None,
) -> List[Path]:
    if transcript_dir and audio_dir:
        raise ValueError("Provide transcript_dir or audio_dir, not both.")
    if not transcript_dir and not audio_dir:
        raise ValueError("Provide transcript_dir or audio_dir.")

    source_dir = transcript_dir or audio_dir
    assert source_dir is not None
    if not source_dir.exists():
        raise ValueError(f"Input directory does not exist: {source_dir}")
    if not source_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {source_dir}")

    return sorted(path for path in source_dir.iterdir() if path.is_file())


def write_batch_summary(
    *,
    output_root: Path,
    successes: Sequence[dict],
    failures: Sequence[dict],
    source_kind: str,
) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "batch-summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "source_kind": source_kind,
                "total_jobs": len(successes) + len(failures),
                "successful_jobs": len(successes),
                "failed_jobs": len(failures),
                "retry_candidates": [item["source_path"] for item in failures],
                "jobs": list(successes),
                "failures": list(failures),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return summary_path


def write_batch_failures(
    *,
    output_root: Path,
    failures: Sequence[dict],
) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    failure_path = output_root / "batch-failures.json"
    failure_path.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "total_failures": len(failures),
                "retry_candidates": [item["source_path"] for item in failures],
                "failures": list(failures),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return failure_path


def run_batch_pipeline(
    *,
    output_root: Path,
    transcript_dir: Optional[Path] = None,
    audio_dir: Optional[Path] = None,
    summary_bullets: int = 5,
    whisper_model: str = "base",
    language: Optional[str] = None,
    tts_enabled: bool = False,
    tts_input: str = "summary",
) -> Path:
    input_files = discover_input_files(transcript_dir=transcript_dir, audio_dir=audio_dir)
    if not input_files:
        raise ValueError("No input files found in the provided directory.")

    source_kind = "transcript_file" if transcript_dir else "audio_file"
    batch_successes = []
    batch_failures = []

    for path in input_files:
        try:
            artifacts = run_pipeline(
                output_root=output_root,
                job_name=path.stem,
                transcript_file=path if transcript_dir else None,
                audio_file=path if audio_dir else None,
                summary_bullets=summary_bullets,
                whisper_model=whisper_model,
                language=language,
                tts_enabled=tts_enabled,
                tts_input=tts_input,
            )
            batch_successes.append(
                {
                    "job_name": artifacts.job_name,
                    "job_slug": artifacts.job_slug,
                    "source_path": str(path),
                    "transcript_path": str(artifacts.transcript_path),
                    "deliverable_path": str(artifacts.deliverable_path),
                    "meta_path": str(artifacts.meta_path),
                    "speech_path": str(artifacts.speech_path) if artifacts.speech_path and artifacts.speech_path.exists() else None,
                    "quality": json.loads(artifacts.meta_path.read_text(encoding="utf-8"))["quality"],
                }
            )
        except Exception as error:
            batch_failures.append(
                {
                    "job_name": path.stem,
                    "job_slug": slugify(path.stem),
                    "source_path": str(path),
                    "error_type": error.__class__.__name__,
                    "error_message": str(error),
                }
            )

    write_batch_failures(output_root=output_root, failures=batch_failures)

    return write_batch_summary(
        output_root=output_root,
        successes=batch_successes,
        failures=batch_failures,
        source_kind=source_kind,
    )


def run_intake_batch_pipeline(
    *,
    output_root: Path,
    intake_dir: Path,
    summary_bullets: int = 5,
    whisper_model: str = "base",
    language: Optional[str] = None,
    tts_enabled: bool = False,
    tts_input: str = "summary",
) -> Path:
    manifest_paths = discover_intake_manifests(intake_dir)
    if not manifest_paths:
        raise ValueError("No intake manifests found in the provided directory.")

    batch_successes = []
    batch_failures = []

    for manifest_path in manifest_paths:
        try:
            intake_manifest = load_intake_manifest(manifest_path)
            source_path, source_kind = resolve_intake_source(manifest_path, intake_manifest)
            job_name = str(intake_manifest.get("jobTitle"))
            artifacts = run_pipeline(
                output_root=output_root,
                job_name=job_name,
                transcript_file=source_path if source_kind == "transcript_file" else None,
                audio_file=source_path if source_kind == "audio_file" else None,
                intake_manifest=intake_manifest,
                summary_bullets=summary_bullets,
                whisper_model=whisper_model,
                language=language,
                tts_enabled=tts_enabled,
                tts_input=tts_input,
            )
            batch_successes.append(
                {
                    "job_name": artifacts.job_name,
                    "job_slug": artifacts.job_slug,
                    "source_path": str(source_path),
                    "manifest_path": str(manifest_path),
                    "transcript_path": str(artifacts.transcript_path),
                    "deliverable_path": str(artifacts.deliverable_path),
                    "meta_path": str(artifacts.meta_path),
                    "speech_path": str(artifacts.speech_path) if artifacts.speech_path and artifacts.speech_path.exists() else None,
                    "quality": json.loads(artifacts.meta_path.read_text(encoding="utf-8"))["quality"],
                }
            )
        except Exception as error:
            batch_failures.append(
                {
                    "job_name": manifest_path.name.replace(INTAKE_MANIFEST_SUFFIX, ""),
                    "job_slug": slugify(manifest_path.stem),
                    "source_path": str(manifest_path),
                    "error_type": error.__class__.__name__,
                    "error_message": str(error),
                }
            )

    write_batch_failures(output_root=output_root, failures=batch_failures)
    return write_batch_summary(
        output_root=output_root,
        successes=batch_successes,
        failures=batch_failures,
        source_kind="intake_manifest",
    )


def resolve_job_name(
    *,
    source_path: Optional[Path],
    explicit_job_name: Optional[str] = None,
    intake_manifest_path: Optional[Path] = None,
    intake_package_path: Optional[Path] = None,
) -> tuple[str, Optional[Dict[str, object]]]:
    if intake_package_path:
        intake_package = load_intake_package(intake_package_path)
        intake_manifest = dict(intake_package["manifest"])  # type: ignore[index]
        if explicit_job_name:
            return explicit_job_name, intake_manifest
        return str(intake_manifest.get("jobTitle")), intake_manifest

    if intake_manifest_path:
        intake_manifest = load_intake_manifest(intake_manifest_path)
        if explicit_job_name:
            return explicit_job_name, intake_manifest
        return str(intake_manifest.get("jobTitle")), intake_manifest

    if explicit_job_name:
        return explicit_job_name, None
    if source_path is None:
        raise ValueError("A source path is required when no intake manifest or explicit job name is provided.")
    return source_path.stem, None
