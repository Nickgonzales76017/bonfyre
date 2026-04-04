from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class JobArtifacts:
    job_name: str
    job_slug: str
    output_dir: Path
    raw_transcript_path: Path
    transcript_path: Path
    deliverable_path: Path
    meta_path: Path
    speech_path: Optional[Path] = None


@dataclass
class Deliverable:
    title: str
    transcript: str
    transcript_paragraphs: List[str]
    summary_bullets: List[str]
    deep_summary_sections: List[Dict[str, object]]
    action_items: List[str]
    source_kind: str
    client_name: Optional[str]
    output_goal: Optional[str]
    context_notes: Optional[str]
    quality: Dict[str, object]
    processing_notes: List[str]

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EnvironmentReport:
    whisper_binary: Optional[str]
    ffmpeg_binary: Optional[str]
    piper_binary: Optional[str] = None
    piper_model: Optional[str] = None

    @property
    def whisper_available(self) -> bool:
        return self.whisper_binary is not None

    @property
    def ffmpeg_available(self) -> bool:
        return self.ffmpeg_binary is not None

    @property
    def piper_available(self) -> bool:
        return self.piper_binary is not None and self.piper_model is not None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AudioPrepResult:
    source_audio_path: Path
    prepared_audio_path: Path
    normalized: bool
    transcript_path: Path
    whisper_model: str
    language: Optional[str]
    model_cached: bool
    environment: EnvironmentReport

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["source_audio_path"] = str(self.source_audio_path)
        payload["prepared_audio_path"] = str(self.prepared_audio_path)
        payload["transcript_path"] = str(self.transcript_path)
        return payload
