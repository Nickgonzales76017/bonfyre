#!/usr/bin/env python3
"""Compile read-only Docker/BuildKit disk observations into ResourceWeather.

The script never prunes. It observes daemon disk usage plus BuildKit cache
records and emits bounded aggregate evidence and EffectKernel candidates.
Raw BuildKit descriptions are deliberately not persisted: Dockerfile commands,
paths, registry names, or other build context can appear there.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "bonfyre.docker-resource-weather.v1"
DEFAULT_TARGET_FREE_GIB = 20.0
GIB = 1024**3


class ObservationError(RuntimeError):
    pass


def _json_records(text: str, source: str) -> list[dict[str, Any]]:
    """Parse either NDJSON or one JSON array/object without accepting junk."""
    stripped = text.strip()
    if not stripped:
        return []
    try:
        value = json.loads(stripped)
    except json.JSONDecodeError:
        records: list[dict[str, Any]] = []
        for line_no, line in enumerate(stripped.splitlines(), 1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ObservationError(f"{source} emitted malformed JSON at line {line_no}") from exc
            if not isinstance(item, dict):
                raise ObservationError(f"{source} JSON line {line_no} is not an object")
            records.append(item)
        return records
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
        return list(value)
    raise ObservationError(f"{source} JSON must be an object, object array, or NDJSON objects")


def _run(command: list[str], *, timeout: float = 30.0) -> str:
    try:
        completed = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            timeout=timeout,
            check=False,
            env={"PATH": __import__("os").environ.get("PATH", "")},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ObservationError(f"{command[0]} observation failed ({type(exc).__name__})") from exc
    if completed.returncode != 0:
        # stderr can include environment-specific paths or daemon details. Keep
        # only a digest and the exit code in persisted evidence.
        digest = hashlib.sha256(completed.stderr.encode("utf-8", errors="replace")).hexdigest()
        raise ObservationError(f"command exited {completed.returncode}; stderr_sha256={digest}")
    return completed.stdout


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def aggregate_buildkit(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate BuildKit JSON records without treating record sums as physical bytes."""
    by_type: dict[str, dict[str, int]] = defaultdict(lambda: {"records": 0, "record_sum_bytes": 0})
    total = reclaimable = shared_reclaimable = private_reclaimable = mutable = 0
    reclaimable_count = shared_count = mutable_count = 0
    last_used: list[str] = []
    malformed_sizes = 0

    for record in records:
        size = _nonnegative_int(record.get("Size"))
        if size is None:
            malformed_sizes += 1
            size = 0
        total += size
        kind = str(record.get("Type") or "unknown")[:128]
        by_type[kind]["records"] += 1
        by_type[kind]["record_sum_bytes"] += size

        is_reclaimable = record.get("Reclaimable") is True
        is_shared = record.get("Shared") is True
        is_mutable = record.get("Mutable") is True
        if is_reclaimable:
            reclaimable_count += 1
            reclaimable += size
            if is_shared:
                shared_reclaimable += size
            else:
                private_reclaimable += size
        if is_shared:
            shared_count += 1
        if is_mutable:
            mutable_count += 1
            mutable += size

        raw_last = record.get("LastUsedAt")
        if isinstance(raw_last, str) and raw_last:
            last_used.append(raw_last[:128])

    return {
        "record_count": len(records),
        "record_sum_bytes": total,
        "reclaimable_record_count": reclaimable_count,
        "reclaimable_record_sum_bytes": reclaimable,
        "private_reclaimable_record_sum_bytes": private_reclaimable,
        "shared_reclaimable_record_sum_bytes": shared_reclaimable,
        "shared_record_count": shared_count,
        "mutable_record_count": mutable_count,
        "mutable_record_sum_bytes": mutable,
        "malformed_size_record_count": malformed_sizes,
        "oldest_last_used_at": min(last_used) if last_used else None,
        "newest_last_used_at": max(last_used) if last_used else None,
        "types": dict(sorted(by_type.items())),
        "accounting_note": (
            "BuildKit record sums are scheduler features, not asserted physical disk usage; "
            "shared/parent relationships can make record-level accounting differ from filesystem bytes."
        ),
    }


def sanitize_system_df(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only documented disk-accounting fields, dropping names/IDs if Docker adds them."""
    allowed = ("Type", "TotalCount", "Active", "Size", "Reclaimable")
    output = []
    for record in records[:64]:
        output.append({key: record.get(key) for key in allowed if key in record})
    return output


def policy_candidates(
    *,
    free_bytes: int,
    target_free_bytes: int,
    buildkit: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return typed EffectKernel candidates; never execute them."""
    deficit = max(0, target_free_bytes - free_bytes)
    if deficit == 0:
        return []

    candidates: list[dict[str, Any]] = []
    reclaimable = int(buildkit.get("reclaimable_record_sum_bytes") or 0)
    if reclaimable > 0:
        target_gib = max(1, (target_free_bytes + GIB - 1) // GIB)
        candidates.append(
            {
                "effect_class": "automatic_resource_gc",
                "authority_required": "resource_gc",
                "reason": "free-space target is unmet and BuildKit reports reclaimable cache",
                "command": [
                    "docker",
                    "buildx",
                    "prune",
                    "--force",
                    "--min-free-space",
                    f"{target_gib}gb",
                ],
                "reclaimable_record_sum_bytes_observed": reclaimable,
                "target_deficit_bytes": deficit,
            }
        )

    candidates.append(
        {
            "effect_class": "automatic_resource_gc",
            "authority_required": "resource_gc",
            "reason": "free-space target remains an unmet ResourceGraph obligation",
            "command_family": "docker system prune",
            "volume_deletion_included": False,
            "note": "A later EffectKernel decision may add -a for unused images. Volumes remain a distinct persistent-state effect.",
        }
    )
    return candidates


def observe(path: Path, target_free_gib: float) -> dict[str, Any]:
    disk = shutil.disk_usage(path)
    target_bytes = int(target_free_gib * GIB)
    errors: list[dict[str, str]] = []
    system_records: list[dict[str, Any]] = []
    buildkit_records: list[dict[str, Any]] = []

    docker_path = shutil.which("docker")
    if docker_path:
        try:
            system_records = _json_records(
                _run([docker_path, "system", "df", "--format", "json"]),
                "docker system df",
            )
        except ObservationError as exc:
            errors.append({"surface": "docker_system_df", "error": str(exc)[:512]})
        try:
            buildkit_records = _json_records(
                _run([docker_path, "buildx", "du", "--format=json"]),
                "docker buildx du",
            )
        except ObservationError as exc:
            errors.append({"surface": "docker_buildx_du", "error": str(exc)[:512]})
    else:
        errors.append({"surface": "docker_cli", "error": "docker executable not found"})

    buildkit = aggregate_buildkit(buildkit_records)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return {
        "schema": SCHEMA,
        "observed_at": now,
        "filesystem": {
            "path": str(path.resolve()),
            "total_bytes": disk.total,
            "used_bytes": disk.used,
            "free_bytes": disk.free,
            "target_free_bytes": target_bytes,
            "target_deficit_bytes": max(0, target_bytes - disk.free),
        },
        "docker_cli_available": docker_path is not None,
        "docker_system_df": sanitize_system_df(system_records),
        "buildkit": buildkit,
        "effect_candidates": policy_candidates(
            free_bytes=disk.free,
            target_free_bytes=target_bytes,
            buildkit=buildkit,
        ),
        "observation_errors": errors,
        "persistent_state_boundary": {
            "ordinary_gc_includes_volume_deletion": False,
            "named_volume_deletion_requires_separate_authority": True,
        },
        "factor_validity_warning": (
            "Docker Desktop classic and containerd image stores can retain separate data; "
            "an inactive store may consume disk while being hidden from active-store CLI views."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=Path.cwd())
    parser.add_argument("--target-free-gib", type=float, default=DEFAULT_TARGET_FREE_GIB)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.target_free_gib <= 0:
        parser.error("--target-free-gib must be positive")
    result = observe(args.path, args.target_free_gib)
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
