from pathlib import Path
import site
import shutil
import subprocess
from typing import Optional

from .models import AudioPrepResult, EnvironmentReport
from .piper import resolve_piper_model


def load_transcript_file(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def get_whisper_cache_dir() -> Path:
    return Path.home() / ".cache" / "whisper"


def inspect_model_cache(model: str) -> dict:
    cache_dir = get_whisper_cache_dir()
    model_path = cache_dir / f"{model}.pt"
    return {
        "model": model,
        "cache_dir": str(cache_dir),
        "model_path": str(model_path),
        "cached": model_path.exists(),
    }


def warm_model_cache(model: str) -> dict:
    try:
        import whisper  # type: ignore
    except ImportError as error:
        raise RuntimeError("openai-whisper is not installed for the active python environment.") from error

    cache_state = inspect_model_cache(model)
    if cache_state["cached"]:
        cache_state["warmed"] = False
        return cache_state

    cache_dir = Path(cache_state["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    whisper.load_model(model, download_root=str(cache_dir))
    warmed_state = inspect_model_cache(model)
    warmed_state["warmed"] = True
    return warmed_state


def find_binary(name: str) -> Optional[str]:
    direct_match = shutil.which(name)
    if direct_match:
        return direct_match

    user_scripts_dir = Path(site.getuserbase()) / "bin"
    candidate = user_scripts_dir / name
    if candidate.exists():
        return str(candidate)
    return None


def inspect_environment() -> EnvironmentReport:
    piper_binary = find_binary("piper")
    piper_model = resolve_piper_model()
    return EnvironmentReport(
        whisper_binary=find_binary("whisper"),
        ffmpeg_binary=find_binary("ffmpeg"),
        piper_binary=piper_binary,
        piper_model=piper_model,
    )


def normalize_audio_with_ffmpeg(
    source_audio_path: Path,
    prepared_audio_path: Path,
    *,
    ffmpeg_binary: str,
) -> Path:
    command = [
        ffmpeg_binary,
        "-y",
        "-i",
        str(source_audio_path),
        "-ac",
        "1",
        "-ar",
        "16000",
        str(prepared_audio_path),
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)
    if not prepared_audio_path.exists():
        raise RuntimeError("ffmpeg finished but no normalized audio file was created.")
    return prepared_audio_path


def transcribe_with_whisper(
    audio_path: Path,
    *,
    output_dir: Optional[Path] = None,
    whisper_binary: Optional[str] = None,
    model: str = "base",
    language: Optional[str] = None,
) -> str:
    whisper_binary = whisper_binary or find_binary("whisper")
    if not whisper_binary:
        raise RuntimeError(
            "Whisper CLI is not installed. Use --transcript-file for now or install whisper first."
        )

    target_output_dir = output_dir or audio_path.parent
    target_output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        whisper_binary,
        str(audio_path),
        "--task",
        "transcribe",
        "--model",
        model,
        "--output_format",
        "txt",
        "--output_dir",
        str(target_output_dir),
    ]
    if language:
        command.extend(["--language", language])
    subprocess.run(command, check=True)
    transcript_path = target_output_dir / f"{audio_path.stem}.txt"
    if not transcript_path.exists():
        raise RuntimeError("Whisper finished but no transcript file was created.")
    return transcript_path.read_text(encoding="utf-8").strip()


def run_audio_wrapper(
    source_audio_path: Path,
    *,
    output_dir: Path,
    whisper_model: str = "base",
    language: Optional[str] = None,
) -> AudioPrepResult:
    environment = inspect_environment()
    if not environment.whisper_available:
        raise RuntimeError(
            "Whisper CLI is not installed. Install whisper before running audio transcription."
        )
    model_cache = inspect_model_cache(whisper_model)

    output_dir.mkdir(parents=True, exist_ok=True)
    prepared_audio_path = output_dir / f"{source_audio_path.stem}.normalized.wav"
    transcript_path = output_dir / f"{prepared_audio_path.stem}.txt"

    normalized = False
    audio_for_transcription = source_audio_path

    if environment.ffmpeg_available:
        audio_for_transcription = normalize_audio_with_ffmpeg(
            source_audio_path,
            prepared_audio_path,
            ffmpeg_binary=environment.ffmpeg_binary,
        )
        normalized = True

    transcript_text = transcribe_with_whisper(
        audio_for_transcription,
        output_dir=output_dir,
        whisper_binary=environment.whisper_binary,
        model=whisper_model,
        language=language,
    )

    generated_transcript_path = output_dir / f"{audio_for_transcription.stem}.txt"
    if generated_transcript_path != transcript_path:
        transcript_path.write_text(transcript_text + "\n", encoding="utf-8")
    elif not transcript_path.exists():
        transcript_path.write_text(transcript_text + "\n", encoding="utf-8")

    return AudioPrepResult(
        source_audio_path=source_audio_path,
        prepared_audio_path=audio_for_transcription,
        normalized=normalized,
        transcript_path=transcript_path,
        whisper_model=whisper_model,
        language=language,
        model_cached=bool(model_cache["cached"]),
        environment=environment,
    )
