from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def project_root(entry_file: str) -> Path:
    return Path(entry_file).resolve().parent


def ensure_data_dir(entry_file: str) -> Path:
    data_dir = project_root(entry_file) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def ensure_reports_dir(entry_file: str) -> Path:
    reports_dir = project_root(entry_file) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir


def data_path(entry_file: str, filename: str) -> Path:
    return ensure_data_dir(entry_file) / filename


def report_path(entry_file: str, filename: str) -> Path:
    return ensure_reports_dir(entry_file) / filename


def now_stamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def write_json_report(entry_file: str, filename: str, payload: Dict[str, Any]) -> Path:
    path = report_path(entry_file, filename)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path
