import json
import re
import subprocess
from pathlib import Path
from typing import Optional

from .models import Deliverable

DEFAULT_NIGHTLY_CONFIG = Path(__file__).resolve().parents[3] / "NightlyBrainstorm" / "nightly.json"
DEFAULT_PIPER_MODEL = Path(__file__).resolve().parents[3] / "NightlyBrainstorm" / "models" / "piper" / "en_US-lessac-medium.onnx"


def load_piper_defaults(config_path: Optional[Path] = None) -> dict:
    candidate = config_path or DEFAULT_NIGHTLY_CONFIG
    if not candidate.exists():
        return {}
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return {
        "piper_bin": payload.get("piper_bin"),
        "piper_model": payload.get("piper_model"),
        "sentence_silence": payload.get("sentence_silence"),
        "volume": payload.get("volume"),
    }


def resolve_piper_model(config_path: Optional[Path] = None) -> Optional[str]:
    defaults = load_piper_defaults(config_path)
    configured_model = defaults.get("piper_model")
    if configured_model and Path(configured_model).exists():
        return str(Path(configured_model))
    if DEFAULT_PIPER_MODEL.exists():
        return str(DEFAULT_PIPER_MODEL)
    return None


def clean_for_speech(text: str) -> str:
    cleaned = text
    cleaned = re.sub(r"\A---\s*\n.*?\n---\s*\n?", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"^#{1,6}\s+", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\[\[([^\]|]+)\|([^\]]+)\]\]", r"\2", cleaned)
    cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", cleaned)
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cleaned)
    cleaned = re.sub(r"\*{1,3}", "", cleaned)
    cleaned = re.sub(r"_{1,3}", "", cleaned)
    cleaned = re.sub(r"```.*?```", "", cleaned, flags=re.DOTALL)
    cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
    cleaned = re.sub(r"^\s*[-*+]\s+", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"^\s*\d+\.\s+", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"^\s*\[[ xX]\]\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"^---+\s*$", "", cleaned, flags=re.MULTILINE)
    cleaned = cleaned.replace("|", "")
    cleaned = re.sub(r"^[-:]+$", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"^>\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = re.sub(r"[^\x00-\x7F]+", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def build_tts_script(deliverable: Deliverable, mode: str = "summary") -> str:
    if mode == "transcript":
        return deliverable.transcript
    if mode == "deliverable":
        sections = [
            deliverable.title,
            "Summary",
            *deliverable.summary_bullets,
            "Action items",
            *deliverable.action_items,
            "Transcript",
            deliverable.transcript,
        ]
        return "\n".join(section for section in sections if section)

    summary_lines = [f"Project: {deliverable.title}."]
    if deliverable.client_name:
        summary_lines.append(f"Client: {deliverable.client_name}.")
    if deliverable.output_goal:
        summary_lines.append(f"Output goal: {deliverable.output_goal}.")
    if deliverable.summary_bullets:
        summary_lines.append("Summary:")
        summary_lines.extend(deliverable.summary_bullets)
    if deliverable.action_items:
        summary_lines.append("Action items:")
        summary_lines.extend(deliverable.action_items)
    return "\n".join(summary_lines)


def synthesize_with_piper(
    text: str,
    output_path: Path,
    *,
    piper_binary: str,
    model_path: str,
    sentence_silence: Optional[str] = None,
    volume: Optional[str] = None,
) -> Path:
    cleaned_text = clean_for_speech(text)
    if not cleaned_text.strip():
        raise RuntimeError("No speakable text remained after speech cleanup.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [piper_binary, "-m", model_path, "-f", str(output_path)]
    if sentence_silence:
        command.extend(["--sentence-silence", sentence_silence])
    if volume:
        command.extend(["--volume", volume])

    subprocess.run(
        command,
        input=cleaned_text,
        text=True,
        check=True,
        capture_output=True,
    )
    if not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError("Piper finished but no audio file was created.")
    return output_path
