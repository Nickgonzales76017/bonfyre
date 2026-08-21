#!/usr/bin/env python3
"""Project Agent Trace 0.1.0 attribution into Bonfÿre foreign evidence.

This importer is intentionally claim-conservative. Agent Trace records code
attribution. It does not prove task success, provider fidelity, authority,
effect safety, evidence sufficiency, quality, copyright, or training-data
provenance. The output therefore remains a typed ForeignTwin projection instead
of becoming a ReceiptEnvelope or verified AgentSession.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

SCHEMA = "bonfyre.foreign-agent-trace-attribution.v1"
SUPPORTED_VERSION = "0.1.0"
CONTRIBUTOR_TYPES = {"human", "ai", "mixed", "unknown"}
VCS_TYPES = {"git", "jj", "hg", "svn"}
MAX_FILES = 10000
MAX_CONVERSATIONS_PER_FILE = 10000
MAX_RANGES_PER_CONVERSATION = 100000
MAX_TEXT = 4096
MAX_METADATA_BYTES = 64 * 1024


class ImportError(ValueError):
    pass


def mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ImportError(f"{name} must be an object")
    return value


def array(value: Any, name: str, limit: int) -> list[Any]:
    if not isinstance(value, list):
        raise ImportError(f"{name} must be an array")
    if len(value) > limit:
        raise ImportError(f"{name} exceeds the {limit}-item limit")
    return value


def text(value: Any, name: str, *, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value) or len(value) > MAX_TEXT:
        raise ImportError(f"{name} must be a bounded string")
    return value


def repo_path(value: Any) -> str:
    raw = text(value, "file.path")
    assert raw is not None
    candidate = raw.replace("\\", "/")
    if re.match(r"^[A-Za-z]:/", candidate) or candidate.startswith("/"):
        raise ImportError("file.path must be repository-relative")
    path = PurePosixPath(candidate)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ImportError("file.path must not contain traversal components")
    return path.as_posix()


def uri(value: Any, name: str) -> str | None:
    raw = text(value, name, required=False)
    if raw is None:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        raise ImportError(f"{name} must be an absolute URI")
    return raw


def contributor(value: Any, name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    data = mapping(value, name)
    kind = text(data.get("type"), f"{name}.type")
    if kind not in CONTRIBUTOR_TYPES:
        raise ImportError(f"{name}.type is unsupported")
    model = text(data.get("model_id"), f"{name}.model_id", required=False)
    if model is not None and len(model) > 250:
        raise ImportError(f"{name}.model_id exceeds 250 characters")
    return {
        "actor_class": "agent" if kind == "ai" else kind,
        "foreign_contributor_type": kind,
        "foreign_model_id": model,
        "provider_fidelity_claimed": False,
    }


def range_record(value: Any, inherited: dict[str, Any] | None, name: str) -> dict[str, Any]:
    data = mapping(value, name)
    start = data.get("start_line")
    end = data.get("end_line")
    if not isinstance(start, int) or isinstance(start, bool) or start < 1:
        raise ImportError(f"{name}.start_line must be a positive integer")
    if not isinstance(end, int) or isinstance(end, bool) or end < start:
        raise ImportError(f"{name}.end_line must be >= start_line")
    content_hash = text(data.get("content_hash"), f"{name}.content_hash", required=False)
    qualified_hash = None
    if content_hash is not None:
        if ":" not in content_hash:
            raise ImportError(f"{name}.content_hash must be algorithm-qualified")
        algorithm, digest = content_hash.split(":", 1)
        if not algorithm or not digest:
            raise ImportError(f"{name}.content_hash must be algorithm-qualified")
        qualified_hash = {
            "algorithm": algorithm,
            "foreign_digest": digest,
            "bonfyre_security_identity": False,
        }
    override = contributor(data.get("contributor"), f"{name}.contributor")
    return {
        "start_line": start,
        "end_line": end,
        "contributor": override or inherited or {
            "actor_class": "unknown",
            "foreign_contributor_type": "unknown",
            "foreign_model_id": None,
            "provider_fidelity_claimed": False,
        },
        "foreign_content_hash": qualified_hash,
    }


def parse_timestamp(value: Any) -> str:
    raw = text(value, "timestamp")
    assert raw is not None
    try:
        datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ImportError("timestamp must be RFC3339/ISO8601") from exc
    return raw


def project(record: Any) -> dict[str, Any]:
    data = mapping(record, "trace")
    version = text(data.get("version"), "version")
    if version != SUPPORTED_VERSION:
        raise ImportError(f"unsupported Agent Trace version {version!r}")

    foreign_id = text(data.get("id"), "id")
    assert foreign_id is not None
    try:
        uuid.UUID(foreign_id)
    except ValueError as exc:
        raise ImportError("id must be a UUID") from exc

    vcs_out = None
    if data.get("vcs") is not None:
        vcs = mapping(data["vcs"], "vcs")
        kind = text(vcs.get("type"), "vcs.type")
        revision = text(vcs.get("revision"), "vcs.revision")
        if kind not in VCS_TYPES:
            raise ImportError("vcs.type is unsupported")
        if kind == "git" and not re.fullmatch(r"[0-9a-fA-F]{40}", revision or ""):
            raise ImportError("git vcs.revision must be a 40-character hex SHA")
        vcs_out = {"type": kind, "revision": revision}

    tool_out = None
    if data.get("tool") is not None:
        tool = mapping(data["tool"], "tool")
        tool_out = {
            "name": text(tool.get("name"), "tool.name", required=False),
            "version": text(tool.get("version"), "tool.version", required=False),
        }

    files_out = []
    files = array(data.get("files"), "files", MAX_FILES)
    for file_index, raw_file in enumerate(files):
        file_data = mapping(raw_file, f"files[{file_index}]")
        path = repo_path(file_data.get("path"))
        conversations_out = []
        conversations = array(
            file_data.get("conversations"),
            f"files[{file_index}].conversations",
            MAX_CONVERSATIONS_PER_FILE,
        )
        for conv_index, raw_conv in enumerate(conversations):
            conv_name = f"files[{file_index}].conversations[{conv_index}]"
            conv = mapping(raw_conv, conv_name)
            inherited = contributor(conv.get("contributor"), f"{conv_name}.contributor")
            conversation_url = uri(conv.get("url"), f"{conv_name}.url")
            related_out = []
            for rel_index, raw_related in enumerate(
                array(conv.get("related", []), f"{conv_name}.related", 1000)
            ):
                rel = mapping(raw_related, f"{conv_name}.related[{rel_index}]")
                related_out.append(
                    {
                        "type": text(rel.get("type"), f"{conv_name}.related[{rel_index}].type"),
                        "url": uri(rel.get("url"), f"{conv_name}.related[{rel_index}].url"),
                        "dereferenced": False,
                    }
                )
            ranges_out = [
                range_record(raw_range, inherited, f"{conv_name}.ranges[{range_index}]")
                for range_index, raw_range in enumerate(
                    array(conv.get("ranges"), f"{conv_name}.ranges", MAX_RANGES_PER_CONVERSATION)
                )
            ]
            conversations_out.append(
                {
                    "foreign_conversation_uri": conversation_url,
                    "dereferenced": False,
                    "ranges": ranges_out,
                    "related": related_out,
                }
            )
        files_out.append({"path_at_revision": path, "conversations": conversations_out})

    metadata = data.get("metadata")
    metadata_digest = None
    if metadata is not None:
        metadata = mapping(metadata, "metadata")
        encoded = json.dumps(metadata, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        if len(encoded) > MAX_METADATA_BYTES:
            raise ImportError("metadata exceeds the bounded import size")
        # Foreign metadata can contain vendor/private data. Bind it without
        # promoting arbitrary fields into EvidenceGraph automatically.
        metadata_digest = hashlib.sha256(encoded).hexdigest()

    canonical_foreign = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return {
        "schema": SCHEMA,
        "foreign_system": "cursor-agent-trace",
        "foreign_schema_version": version,
        "foreign_id": foreign_id,
        "foreign_record_sha256": hashlib.sha256(canonical_foreign).hexdigest(),
        "observed_time": parse_timestamp(data.get("timestamp")),
        "source_revision": vcs_out,
        "foreign_carrier": tool_out,
        "files": files_out,
        "foreign_metadata_sha256": metadata_digest,
        "foreign_metadata_promoted": False,
        "claim_boundary": {
            "attribution": True,
            "task_success": False,
            "semantic_correctness": False,
            "authority_compliance": False,
            "effect_safety": False,
            "provider_fidelity": False,
            "evidence_sufficiency": False,
            "quality_assessment": False,
            "legal_ownership": False,
            "training_data_provenance": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("record", help="Agent Trace 0.1.0 JSON record")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        source = json.load(open(args.record, encoding="utf-8"))
        output = project(source)
    except (OSError, json.JSONDecodeError, ImportError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 2
    rendered = json.dumps(output, indent=2, sort_keys=True) + "\n"
    if args.output:
        open(args.output, "w", encoding="utf-8").write(rendered)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
