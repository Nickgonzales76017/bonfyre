import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .runtime_guard import RUNTIME_DIR


QUEUE_PATH = RUNTIME_DIR / "local-ai-transcription-queue.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_queue() -> Dict[str, object]:
    if not QUEUE_PATH.exists():
        return {"created_at": _utc_now(), "updated_at": _utc_now(), "items": []}
    try:
        payload = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"created_at": _utc_now(), "updated_at": _utc_now(), "items": []}
    if not isinstance(payload, dict):
        return {"created_at": _utc_now(), "updated_at": _utc_now(), "items": []}
    items = payload.get("items")
    if not isinstance(items, list):
        payload["items"] = []
    return payload


def save_queue(payload: Dict[str, object]) -> Path:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = _utc_now()
    QUEUE_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return QUEUE_PATH


def _enqueue_source(source_path: Path, *, source_kind: str) -> Dict[str, object]:
    source_path = source_path.resolve()
    queue = load_queue()
    items = list(queue.get("items", []))
    for item in items:
        if isinstance(item, dict) and item.get("source_path") == str(source_path) and item.get("source_kind") == source_kind:
            if item.get("status") in {"queued", "processing"}:
                return item

    entry = {
        "job_id": source_path.stem,
        "source_path": str(source_path),
        "source_kind": source_kind,
        "status": "queued",
        "queued_at": _utc_now(),
        "started_at": None,
        "completed_at": None,
        "failed_at": None,
        "attempt_count": 0,
        "last_error": None,
        "last_output": None,
    }
    items.append(entry)
    queue["items"] = items
    save_queue(queue)
    return entry


def enqueue_intake_package(package_path: Path) -> Dict[str, object]:
    return _enqueue_source(package_path, source_kind="intake_package")


def enqueue_audio_file(audio_path: Path) -> Dict[str, object]:
    return _enqueue_source(audio_path, source_kind="audio_file")


def enqueue_transcript_file(transcript_path: Path) -> Dict[str, object]:
    return _enqueue_source(transcript_path, source_kind="transcript_file")


def queue_status() -> Dict[str, object]:
    queue = load_queue()
    items: List[Dict[str, object]] = [item for item in queue.get("items", []) if isinstance(item, dict)]
    counts = {"queued": 0, "processing": 0, "completed": 0, "failed": 0}
    for item in items:
        status = str(item.get("status") or "queued")
        counts[status] = counts.get(status, 0) + 1
    next_item = next((item for item in items if item.get("status") == "queued"), None)
    return {
        "queue_path": str(QUEUE_PATH),
        "total_items": len(items),
        "counts": counts,
        "next_item": next_item,
        "items": items,
    }


def next_queued_item() -> Optional[Dict[str, object]]:
    status = queue_status()
    next_item = status.get("next_item")
    return next_item if isinstance(next_item, dict) else None


def mark_queue_item(source_path: Path, *, status: str, output_path: Optional[Path] = None, error: Optional[str] = None) -> Dict[str, object]:
    queue = load_queue()
    items: List[Dict[str, object]] = [item for item in queue.get("items", []) if isinstance(item, dict)]
    source_path_str = str(source_path.resolve())
    for item in items:
        if item.get("source_path") != source_path_str:
            continue
        item["status"] = status
        if status == "processing":
            item["started_at"] = _utc_now()
            item["attempt_count"] = int(item.get("attempt_count") or 0) + 1
            item["last_error"] = None
        elif status == "completed":
            item["completed_at"] = _utc_now()
            item["last_output"] = str(output_path) if output_path else item.get("last_output")
        elif status == "failed":
            item["failed_at"] = _utc_now()
            item["last_error"] = error
        save_queue(queue)
        return item
    raise ValueError(f"Queue item not found for source path: {source_path_str}")
