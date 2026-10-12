import platform
import shutil
from typing import Dict, List

from .models import EnvironmentReport


def build_bootstrap_plan(environment: EnvironmentReport) -> Dict[str, object]:
    system = platform.system().lower()
    brew_binary = shutil.which("brew")

    missing: List[str] = []
    steps: List[str] = []
    notes: List[str] = []

    if not brew_binary and system == "darwin":
        steps.append(
            'install Homebrew: /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
        )
        notes.append("Homebrew is the simplest install path on macOS for ffmpeg and Python tooling.")

    if not environment.ffmpeg_available:
        missing.append("ffmpeg")
        if system == "darwin":
            steps.append("install ffmpeg: brew install ffmpeg")
        else:
            steps.append("install ffmpeg with your system package manager")

    if not environment.whisper_available:
        missing.append("whisper")
        steps.append("install Whisper CLI: python3 -m pip install openai-whisper")

    if environment.ffmpeg_available and environment.whisper_available:
        notes.append("Environment looks ready for raw audio transcription.")
    else:
        notes.append("Run `--check-env` again after installation to confirm the toolchain is live.")

    return {
        "platform": system,
        "brew_available": brew_binary is not None,
        "missing_dependencies": missing,
        "steps": steps,
        "notes": notes,
    }
