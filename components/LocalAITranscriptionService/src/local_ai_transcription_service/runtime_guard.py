import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, Optional

import fcntl


VAULT_ROOT = Path(__file__).resolve().parents[4]
RUNTIME_DIR = VAULT_ROOT / ".bonfyre-runtime"
LOCK_PATH = RUNTIME_DIR / "heavy-process.lock"
CONFIG_PATH = RUNTIME_DIR / "guardrails.json"

DEFAULT_GUARDRAILS = {
    "default_max_load_avg": 12.0,
    "process_limits": {
        "nightly_brainstorm": 8.0,
        "local_ai_transcription_service": 12.0,
        "local_ai_transcription_service:model_warmup": 5.0,
    },
}


def recommended_load_limit() -> float:
    cpu_count = os.cpu_count() or 8
    return max(8.0, round(cpu_count * 1.5, 1))


def current_load_average() -> float:
    return float(os.getloadavg()[0])


def load_guardrail_config() -> Dict[str, object]:
    if not CONFIG_PATH.exists():
        return DEFAULT_GUARDRAILS
    try:
        payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return DEFAULT_GUARDRAILS

    merged = {
        "default_max_load_avg": payload.get("default_max_load_avg", DEFAULT_GUARDRAILS["default_max_load_avg"]),
        "process_limits": dict(DEFAULT_GUARDRAILS["process_limits"]),
    }
    merged["process_limits"].update(payload.get("process_limits", {}))
    return merged


def resolve_load_limit(process_name: str, load_limit: Optional[float] = None) -> float:
    if load_limit is not None:
        return load_limit
    config = load_guardrail_config()
    process_limits = config.get("process_limits", {})
    if isinstance(process_limits, dict) and process_name in process_limits:
        return float(process_limits[process_name])
    return float(config.get("default_max_load_avg", recommended_load_limit()))


def runtime_status(process_name: str) -> dict:
    limit = resolve_load_limit(process_name)
    lock_exists = LOCK_PATH.exists()
    lock_payload = None
    if lock_exists:
        try:
            lock_payload = json.loads(LOCK_PATH.read_text(encoding="utf-8") or "null")
        except json.JSONDecodeError:
            lock_payload = LOCK_PATH.read_text(encoding="utf-8").strip() or None
    return {
        "process_name": process_name,
        "current_load_average": current_load_average(),
        "effective_load_limit": limit,
        "recommended_generic_limit": recommended_load_limit(),
        "lock_path": str(LOCK_PATH),
        "lock_present": lock_exists,
        "lock_payload": lock_payload,
        "config_path": str(CONFIG_PATH),
    }


def assert_safe_load(*, process_name: str, load_limit: Optional[float] = None) -> None:
    limit = resolve_load_limit(process_name, load_limit)
    current = current_load_average()
    if current > limit:
        raise RuntimeError(
            f"Refusing to start {process_name}: current 1m load {current:.2f} exceeds safe limit {limit:.2f}. "
            "Wait for the machine to cool down or rerun with --unsafe-skip-guardrails."
        )


@contextmanager
def guarded_runtime(*, process_name: str, unsafe_skip: bool = False) -> Iterator[None]:
    if unsafe_skip:
        yield
        return

    assert_safe_load(process_name=process_name)
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    with LOCK_PATH.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            lock_file.seek(0)
            existing = lock_file.read().strip()
            detail = f" Existing lock metadata: {existing}" if existing else ""
            raise RuntimeError(
                f"Refusing to start {process_name}: another heavy Bonfyre process is already running.{detail}"
            ) from error

        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(
            json.dumps(
                {
                    "process_name": process_name,
                    "pid": os.getpid(),
                    "started_at": int(time.time()),
                }
            )
        )
        lock_file.flush()

        try:
            yield
        finally:
            lock_file.seek(0)
            lock_file.truncate()
            lock_file.flush()
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
