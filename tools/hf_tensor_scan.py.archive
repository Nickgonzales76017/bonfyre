#!/usr/bin/env python3
"""
hf_tensor_scan.py

Hugging Face metadata and tensor-surface scanner for Bonfyre.

Primary jobs:
- resolve an exact HF repo id when a recipe's source_model is approximate
- inspect configs, tokenizer metadata, safetensors indexes, and safetensors headers
- enumerate actual tensor names present when possible
- compare those names against Bonfyre pull patterns
- emit candidate or verified Bonfyre recipe YAML
- optionally emit full inventory JSON for downstream verification
"""

from __future__ import annotations

import argparse
import datetime as dt
import difflib
import fnmatch
import json
import os
import struct
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import yaml

ROOT = Path(__file__).resolve().parents[1]
FAMILY_INDEX = ROOT / "recipes" / "families" / "family_index.json"
DEFAULT_CACHE = Path(os.getenv("BONFYRE_HF_CACHE", "/tmp/bonfyre_hf_cache"))
HF_BASE_URL = "https://huggingface.co"

METADATA_FILES = (
    "config.json",
    "generation_config.json",
    "tokenizer_config.json",
    "special_tokens_map.json",
    "preprocessor_config.json",
)

LARGE_METADATA_FILES = (
    "tokenizer.json",
)

ALIAS_RULES = (
    ("vision_tower.", "vision_model."),
    ("vision_model.", "vision_tower."),
    ("language_model.model.", "model."),
    ("model.", "language_model.model."),
    ("multi_modal_projector.", "mm_projector."),
    ("mm_projector.", "multi_modal_projector."),
    ("text_model.", "text_encoder."),
    ("text_encoder.", "text_model."),
    ("visual.", "vision_tower."),
    ("vision_tower.", "visual."),
)


@dataclass
class FamilyMatch:
    family: str
    tensor_patterns: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    workflow_steps: List[str] = field(default_factory=list)


def unique(seq: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def safe_slug(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in text).strip("_")


def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_family_index() -> List[dict]:
    data = json.loads(FAMILY_INDEX.read_text())
    return data.get("families", [])


def family_lookup() -> Dict[str, dict]:
    return {entry["family"]: entry for entry in load_family_index()}


def fixture_path(repo: str) -> Optional[Path]:
    fixture_dir = os.getenv("BONFYRE_HF_SCAN_FIXTURE_DIR")
    if not fixture_dir:
        return None
    path = Path(fixture_dir) / f"{repo.replace('/', '__')}.json"
    return path if path.exists() else None


def get_hf_token() -> Optional[str]:
    for key in ("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN"):
        value = os.getenv(key)
        if value:
            return value
    try:
        from huggingface_hub import HfFolder
    except Exception:  # pragma: no cover
        return None
    try:
        return HfFolder.get_token()
    except Exception:
        return None


def hf_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    token = get_hf_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if extra:
        headers.update(extra)
    return headers


def hf_json_get(path: str, timeout: int = 8) -> Tuple[Optional[object], dict]:
    try:
        import requests
    except Exception:  # pragma: no cover
        return None, {"status": "unavailable", "http_status": None}

    url = f"{HF_BASE_URL}{path}"
    try:
        resp = requests.get(url, headers=hf_headers(), timeout=timeout)
    except Exception as exc:
        return None, {"status": "request_error", "http_status": None, "error": type(exc).__name__}
    if resp.status_code == 200:
        try:
            return resp.json(), {"status": "ok", "http_status": 200}
        except Exception as exc:
            return None, {
                "status": "decode_error",
                "http_status": 200,
                "error": type(exc).__name__,
            }
    if resp.status_code in (401, 403):
        return None, {"status": "auth_required", "http_status": resp.status_code}
    if resp.status_code == 404:
        return None, {"status": "not_found", "http_status": 404}
    return None, {"status": "http_error", "http_status": resp.status_code}


def hf_resolve_url(repo: str, filename: str) -> str:
    return f"{HF_BASE_URL}/{repo}/resolve/main/{quote(filename, safe='/')}"


def score_repo_candidate(requested: str, candidate: str) -> float:
    req_ns, req_name = requested.split("/", 1) if "/" in requested else ("", requested)
    cand_ns, cand_name = candidate.split("/", 1) if "/" in candidate else ("", candidate)
    score = difflib.SequenceMatcher(None, req_name, cand_name).ratio()
    if req_ns and cand_ns == req_ns:
        score += 0.25
    if cand_name.startswith(req_name):
        score += 0.2
    if req_name in cand_name:
        score += 0.1
    return score


def resolve_repo(repo: str) -> Dict[str, object]:
    fixture = fixture_path(repo)
    if fixture:
        data = json.loads(fixture.read_text())
        data.setdefault("requested_repo", repo)
        data.setdefault("resolved_repo", data.get("repo_id", repo))
        data.setdefault("resolution_status", "fixture")
        data.setdefault("resolution_candidates", [])
        return data

    info, info_status = hf_json_get(f"/api/models/{repo}")
    if info_status.get("status") == "ok" and isinstance(info, dict):
        tree, tree_status = hf_json_get(f"/api/models/{repo}/tree/main?recursive=false&expand=false")
        siblings = []
        if tree_status.get("status") == "ok" and isinstance(tree, list):
            siblings = [str(item.get("path")) for item in tree if item.get("path")]
        return {
            "requested_repo": repo,
            "resolved_repo": str(info.get("id", repo)),
            "resolution_status": "verified",
            "resolution_candidates": [],
            "siblings": siblings,
            "tags": list(info.get("tags", []) or []),
            "cardData": normalize_card_data(info.get("cardData")),
        }
    exc_name = info_status.get("error", info_status.get("status", "request_error"))
    query = repo.split("/", 1)[-1]
    owner = repo.split("/", 1)[0] if "/" in repo else ""
    candidates = []
    search_data, search_status = hf_json_get(f"/api/models?search={quote(query)}&limit=20")
    if search_status.get("status") == "ok" and isinstance(search_data, list):
        for item in search_data:
            model_id = item.get("id", "")
            if model_id:
                candidates.append(model_id)
    else:
            return {
                "requested_repo": repo,
                "resolved_repo": repo,
                "resolution_status": "offline",
                "resolution_candidates": [],
                "siblings": [],
                "tags": [],
                "cardData": {},
                "resolution_note": (
                    "unable to reach Hugging Face for repo resolution "
                    f"({exc_name}; fallback search also failed with {search_status.get('error', search_status.get('status'))})"
                ),
            }
    ranked = sorted(
        ((score_repo_candidate(repo, candidate), candidate) for candidate in candidates),
        reverse=True,
    )
    if not ranked:
        return {
            "requested_repo": repo,
            "resolved_repo": repo,
            "resolution_status": "unresolved",
            "resolution_candidates": [],
            "siblings": [],
            "tags": [],
            "cardData": {},
        }
    resolved = ranked[0][1]
    resolved_info, resolved_status = hf_json_get(f"/api/models/{resolved}")
    tree, tree_status = hf_json_get(f"/api/models/{resolved}/tree/main?recursive=false&expand=false")
    siblings = []
    if tree_status.get("status") == "ok" and isinstance(tree, list):
        siblings = [str(item.get("path")) for item in tree if item.get("path")]
    if resolved_status.get("status") != "ok" or not isinstance(resolved_info, dict):
        return {
            "requested_repo": repo,
            "resolved_repo": resolved,
            "resolution_status": "resolved",
            "resolution_candidates": [candidate for _, candidate in ranked[:10]],
            "siblings": siblings,
            "tags": [],
            "cardData": {},
            "resolution_note": f"requested namespace '{owner}' could not be verified directly",
        }
    return {
        "requested_repo": repo,
        "resolved_repo": str(resolved_info.get("id", resolved)),
        "resolution_status": "resolved",
        "resolution_candidates": [candidate for _, candidate in ranked[:10]],
        "siblings": siblings,
        "tags": list(resolved_info.get("tags", []) or []),
        "cardData": normalize_card_data(resolved_info.get("cardData")),
        "resolution_note": f"requested namespace '{owner}' could not be verified directly",
    }


def hf_download_text(repo: str, filename: str) -> Tuple[Optional[str], dict]:
    fixture = fixture_path(repo)
    if fixture:
        data = json.loads(fixture.read_text())
        file_bodies = data.get("file_bodies", {})
        if filename in file_bodies:
            return file_bodies[filename], {"status": "fixture", "http_status": 200}
        return None, {"status": "missing_fixture", "http_status": None}

    try:
        import requests
    except Exception:  # pragma: no cover
        return None, {"status": "unavailable", "http_status": None}

    url = hf_resolve_url(repo, filename)
    try:
        resp = requests.get(url, headers=hf_headers(), timeout=8)
    except Exception as exc:
        return None, {"status": "request_error", "http_status": None, "error": type(exc).__name__}

    if resp.status_code == 200:
        return resp.text, {"status": "ok", "http_status": 200}
    if resp.status_code in (401, 403):
        return None, {"status": "auth_required", "http_status": resp.status_code}
    if resp.status_code == 404:
        return None, {"status": "not_found", "http_status": 404}
    return None, {"status": "http_error", "http_status": resp.status_code}


def safetensors_header_tensor_names(repo: str, filename: str) -> Tuple[List[str], dict]:
    fixture = fixture_path(repo)
    if fixture:
        data = json.loads(fixture.read_text())
        return list(data.get("safetensors_headers", {}).get(filename, [])), {
            "status": "fixture",
            "http_status": 200,
        }

    try:
        import requests
    except Exception:  # pragma: no cover
        return [], {"status": "unavailable", "http_status": None}

    url = hf_resolve_url(repo, filename)
    session = requests.Session()
    try:
        first = session.get(
            url,
            headers=hf_headers({"Range": "bytes=0-1048575"}),
            timeout=8,
        )
        first.raise_for_status()
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status in (401, 403):
            return [], {"status": "auth_required", "http_status": status}
        return [], {"status": "request_error", "http_status": status, "error": type(exc).__name__}

    body = first.content
    if len(body) < 8:
        return [], {"status": "short_read", "http_status": first.status_code}
    header_len = struct.unpack("<Q", body[:8])[0]
    need = 8 + header_len
    if len(body) < need:
        try:
            second = session.get(
                url,
                headers=hf_headers({"Range": f"bytes=0-{need - 1}"}),
                timeout=8,
            )
            second.raise_for_status()
            body = second.content
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            return [], {"status": "request_error", "http_status": status, "error": type(exc).__name__}
    try:
        header = json.loads(body[8:need].decode("utf-8"))
    except Exception:
        return [], {"status": "decode_error", "http_status": first.status_code}
    return sorted(key for key in header.keys() if key != "__metadata__"), {
        "status": "ok",
        "http_status": first.status_code,
    }


def collect_metadata_payloads(repo: str, siblings: Sequence[str]) -> Tuple[Dict[str, object], Dict[str, dict]]:
    payloads: Dict[str, object] = {}
    fetch_status: Dict[str, dict] = {}
    token = get_hf_token()
    auth_blocked = False
    for name in siblings:
        if name in METADATA_FILES or name.endswith(".index.json"):
            if auth_blocked and name not in fetch_status:
                fetch_status[name] = {"status": "auth_required", "http_status": 401, "inferred": True}
                continue
            text, status = hf_download_text(repo, name)
            fetch_status[name] = status
            if status.get("status") == "auth_required" and not token:
                auth_blocked = True
            if text is None:
                continue
            try:
                payloads[name] = json.loads(text)
            except Exception:
                payloads[name] = text
    for name in METADATA_FILES:
        if auth_blocked and name not in fetch_status:
            fetch_status[name] = {"status": "auth_required", "http_status": 401, "inferred": True}
            continue
        if name not in payloads:
            text, status = hf_download_text(repo, name)
            fetch_status.setdefault(name, status)
            if status.get("status") == "auth_required" and not token:
                auth_blocked = True
            if text is None:
                continue
            try:
                payloads[name] = json.loads(text)
            except Exception:
                payloads[name] = text
    for name in LARGE_METADATA_FILES:
        if name in siblings and name not in fetch_status:
            fetch_status[name] = {
                "status": "skipped_large_optional",
                "http_status": None,
            }
    return payloads, fetch_status


def summarize_payload(payload: object) -> dict:
    if isinstance(payload, dict):
        keys = sorted(str(key) for key in payload.keys())
        return {
            "type": "dict",
            "top_level_keys": keys[:100],
            "top_level_key_count": len(keys),
        }
    if isinstance(payload, list):
        return {
            "type": "list",
            "length": len(payload),
        }
    if isinstance(payload, str):
        return {
            "type": "text",
            "length": len(payload),
        }
    return {
        "type": type(payload).__name__,
    }


def normalize_card_data(card_data: object) -> object:
    if card_data is None:
        return {}
    if isinstance(card_data, dict):
        return card_data
    dump = getattr(card_data, "to_dict", None)
    if callable(dump):
        try:
            return dump()
        except Exception:
            pass
    if hasattr(card_data, "__dict__"):
        data = {}
        for key, value in vars(card_data).items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                data[key] = value
            elif isinstance(value, list):
                data[key] = [item for item in value if isinstance(item, (str, int, float, bool))]
        return data
    return {"repr": repr(card_data)}


def enumerate_repo_inventory(repo: str) -> Dict[str, object]:
    manifest = resolve_repo(repo)
    resolved_repo = str(manifest.get("resolved_repo", repo))
    siblings = unique(manifest.get("siblings", []))
    payloads, fetch_status = collect_metadata_payloads(resolved_repo, siblings)

    tensors = set()
    index_files = []
    header_files = []
    safetensors_file_inventory: Dict[str, List[str]] = {}
    metadata_access = {
        "ok": [],
        "auth_required": [],
        "not_found": [],
        "other": [],
    }

    for name, status in fetch_status.items():
        state = status.get("status")
        if state in ("ok", "fixture"):
            metadata_access["ok"].append(name)
        elif state == "auth_required":
            metadata_access["auth_required"].append(name)
        elif state == "not_found":
            metadata_access["not_found"].append(name)
        else:
            metadata_access["other"].append({"file": name, **status})

    for name, payload in payloads.items():
        if not name.endswith(".index.json"):
            continue
        if isinstance(payload, dict):
            weight_map = payload.get("weight_map", {})
            if isinstance(weight_map, dict):
                tensors.update(weight_map.keys())
                index_files.append(name)
                bucket = {}
                for tensor_name, shard_name in weight_map.items():
                    bucket.setdefault(str(shard_name), []).append(str(tensor_name))
                for shard_name, names in bucket.items():
                    safetensors_file_inventory.setdefault(shard_name, []).extend(names)

    if not tensors and not (metadata_access["auth_required"] and not get_hf_token()):
        for name in siblings:
            if not name.endswith(".safetensors"):
                continue
            header_names, status = safetensors_header_tensor_names(resolved_repo, name)
            if header_names:
                tensors.update(header_names)
                header_files.append(name)
                safetensors_file_inventory[name] = header_names
            fetch_status.setdefault(name, status)

    metadata_keys = []
    for name, payload in payloads.items():
        if isinstance(payload, dict):
            metadata_keys.append(name)
            if name == "config.json":
                tensors.update(f"config.{key}" for key in payload.keys())
            elif name == "generation_config.json":
                tensors.update(f"generation_config.{key}" for key in payload.keys())
            elif name == "tokenizer_config.json":
                tensors.update(f"tokenizer_config.{key}" for key in payload.keys())
            elif name == "special_tokens_map.json":
                tensors.update(f"special_tokens_map.{key}" for key in payload.keys())
            elif name == "preprocessor_config.json":
                tensors.update(f"preprocessor_config.{key}" for key in payload.keys())

    surfaces = unique(sorted(tensors) + sorted(siblings))
    access_status = "verified" if tensors else "partial"
    if metadata_access["auth_required"] and not tensors:
        access_status = "auth_blocked"
    elif not tensors and not payloads:
        access_status = "metadata_only"
    if manifest.get("resolution_status") == "offline" and not tensors:
        access_status = "offline"

    for file_name, names in list(safetensors_file_inventory.items()):
        safetensors_file_inventory[file_name] = sorted(unique(names))

    metadata_summary = {
        name: summarize_payload(payload)
        for name, payload in sorted(payloads.items())
    }

    return {
        "requested_repo": manifest.get("requested_repo", repo),
        "resolved_repo": resolved_repo,
        "resolution_status": manifest.get("resolution_status", "unresolved"),
        "resolution_candidates": manifest.get("resolution_candidates", []),
        "resolution_note": manifest.get("resolution_note"),
        "siblings": siblings,
        "metadata_summary": metadata_summary,
        "fetch_status": fetch_status,
        "access_status": access_status,
        "metadata_access": metadata_access,
        "metadata_files": sorted(metadata_keys),
        "tensor_names": sorted(tensors),
        "tensor_files": safetensors_file_inventory,
        "surfaces": surfaces,
        "index_files": index_files,
        "header_files": header_files,
        "tags": manifest.get("tags", []),
        "cardData": manifest.get("cardData", {}),
    }


def collect_surfaces(manifest: Dict[str, object]) -> List[str]:
    return list(manifest.get("surfaces", []))


def match_family_patterns(surfaces: Sequence[str], families: Sequence[dict]) -> List[FamilyMatch]:
    matches: List[FamilyMatch] = []
    for family in families:
        pats = family.get("tensor_patterns", [])
        hit_patterns = []
        for pat in pats:
            if any(fnmatch.fnmatch(surface, pat) for surface in surfaces):
                hit_patterns.append(pat)
        if hit_patterns:
            matches.append(
                FamilyMatch(
                    family=family["family"],
                    tensor_patterns=unique(hit_patterns),
                    capabilities=family.get("capabilities", []),
                    workflow_steps=family.get("workflow_steps", []),
                )
            )
    return matches


def infer_capabilities(matches: Sequence[FamilyMatch]) -> List[str]:
    return unique(cap for m in matches for cap in m.capabilities)[:16]


def infer_workflow_steps(matches: Sequence[FamilyMatch]) -> Dict[str, str]:
    labels = {
        "A1.s01": "ingest_or_parse",
        "A2.s02": "transform_or_project",
        "A3.s03": "reason_or_route",
        "A4.s04": "memory_or_cache",
        "A5.s05": "safety_eval_or_emit",
    }
    steps: Dict[str, str] = {}
    for m in matches:
        for step in m.workflow_steps:
            steps.setdefault(step, labels.get(step, "attach_family"))
    return steps


def alias_variants(pattern: str) -> List[str]:
    variants = {pattern}
    changed = True
    while changed:
        changed = False
        for current in list(variants):
            for old, new in ALIAS_RULES:
                if old in current:
                    candidate = current.replace(old, new)
                    if candidate not in variants:
                        variants.add(candidate)
                        changed = True
    return sorted(variants)


def heuristic_candidates(pattern: str, surfaces: Sequence[str]) -> List[str]:
    parts = [part for part in pattern.replace("*", " ").split(".") if len(part) > 2]
    if not parts:
        return []
    hits = [surface for surface in surfaces if all(part in surface for part in parts[-2:])]
    return hits[:20]


def classify_patterns(patterns: Sequence[str], surfaces: Sequence[str]) -> Dict[str, List[dict]]:
    verified = []
    missing = []
    renamed = []
    variant = []
    for pattern in patterns:
        hits = sorted(surface for surface in surfaces if fnmatch.fnmatch(surface, pattern))
        if hits:
            verified.append({"pattern": pattern, "matches": hits[:100]})
            continue
        aliases = [candidate for candidate in alias_variants(pattern) if candidate != pattern]
        alias_hits = []
        for candidate in aliases:
            alias_hits.extend(surface for surface in surfaces if fnmatch.fnmatch(surface, candidate))
        alias_hits = unique(sorted(alias_hits))
        if alias_hits:
            renamed.append(
                {
                    "pattern": pattern,
                    "aliases": aliases[:12],
                    "candidates": alias_hits[:100],
                }
            )
            continue
        heuristic = heuristic_candidates(pattern, surfaces)
        if heuristic:
            variant.append({"pattern": pattern, "candidates": heuristic})
            continue
        missing.append({"pattern": pattern})
    return {
        "verified": verified,
        "missing": missing,
        "renamed": renamed,
        "variant_dependent": variant,
    }


def flatten_recipe_patterns(recipe: dict) -> List[str]:
    patterns = []
    for item in recipe.get("pull", []) or []:
        if isinstance(item, str):
            patterns.append(item)
    for item in (recipe.get("bonfyre_families", {}) or {}).keys():
        patterns.append(str(item))
    validation = recipe.get("validation", {}) or {}
    for key in ("required_tensors", "optional_tensors"):
        for item in validation.get(key, []) or []:
            if isinstance(item, str):
                patterns.append(item)
    return unique(patterns)


def build_verification_block(recipe: dict, inventory: dict) -> dict:
    patterns = flatten_recipe_patterns(recipe)
    classified = classify_patterns(patterns, inventory.get("surfaces", []))
    refined_exact = unique(
        match
        for item in classified["verified"]
        for match in item.get("matches", [])
        if "*" not in item["pattern"]
    )[:200]
    refined_patterns = unique(
        item["pattern"] for item in classified["verified"] if "*" in item["pattern"]
    )
    notes = []
    if inventory.get("resolution_status") == "resolved":
        notes.append("source_model was resolved to a verified repo id during inspection")
    if not inventory.get("tensor_names"):
        notes.append("no explicit tensor names were discoverable from index files or safetensors headers")
    if inventory.get("header_files"):
        notes.append("single-file safetensors were sampled via header inspection")
    if inventory.get("access_status") == "auth_blocked":
        notes.append("metadata or index fetches were blocked by Hugging Face auth in this environment")
    if inventory.get("resolution_note"):
        notes.append(str(inventory["resolution_note"]))

    pattern_details = []
    for bucket_name in ("verified", "missing", "renamed", "variant_dependent"):
        for item in classified.get(bucket_name, []):
            detail = {"status": bucket_name, **item}
            pattern_details.append(detail)

    return {
        "checked_at": now_iso(),
        "requested_repo": inventory.get("requested_repo"),
        "resolved_repo": inventory.get("resolved_repo"),
        "resolution_status": inventory.get("resolution_status"),
        "resolution_candidates": inventory.get("resolution_candidates", []),
        "access_status": inventory.get("access_status"),
        "metadata_files": inventory.get("metadata_files", []),
        "metadata_summary": inventory.get("metadata_summary", {}),
        "metadata_access": inventory.get("metadata_access", {}),
        "index_files": inventory.get("index_files", []),
        "header_files": inventory.get("header_files", []),
        "tensor_inventory": {
            "tensor_name_count": len(inventory.get("tensor_names", [])),
            "surface_count": len(inventory.get("surfaces", [])),
            "sample": inventory.get("tensor_names", [])[:200],
            "by_file": {
                name: names[:200]
                for name, names in sorted((inventory.get("tensor_files", {}) or {}).items())
            },
        },
        "pull_status": classified,
        "pattern_details": pattern_details,
        "refined_pull": {
            "exact": refined_exact,
            "refined_patterns": refined_patterns,
        },
        "notes": notes,
    }


def emit_yaml(
    recipe_name: str,
    repo: str,
    collection: str,
    surfaces: Sequence[str],
    matches: Sequence[FamilyMatch],
    inventory: Optional[dict] = None,
) -> str:
    pull = sorted(surfaces)
    caps = infer_capabilities(matches)
    workflow_steps = infer_workflow_steps(matches)
    bonfyre_families: Dict[str, str] = {}
    for m in matches:
        for pat in m.tensor_patterns:
            bonfyre_families[pat] = m.family

    required = pull[: min(5, len(pull))]
    optional = pull[min(5, len(pull)) : min(15, len(pull))]
    family_name = matches[0].family if matches else "T_UNCLASSIFIED"

    doc = {
        "recipe": recipe_name,
        "source_model": repo,
        "source_collection": collection,
        "pull": pull,
        "bonfyre_families": bonfyre_families,
        "capabilities": caps or ["inspect"],
        "workflow_steps": workflow_steps
        or {
            "A1.s01": "ingest_or_parse",
            "A2.s02": "transform_or_project",
            "A3.s03": "reason_or_route",
            "A4.s04": "memory_or_cache",
            "A5.s05": "safety_eval_or_emit",
        },
        "extract": {
            "command": f"python tools/hf_tensor_scan.py --repo {repo} --emit bonfyre_recipe.yaml",
            "safetensors_glob": [
                "*.safetensors",
                "*.index.json",
                "config.json",
                "tokenizer_config.json",
            ],
        },
        "validation": {
            "required_tensors": required or ["config.json"],
            "optional_tensors": optional or ["generation_config.json"],
            "missing_behavior": "warn",
        },
        "family_index_entry": {
            "family": family_name,
            "source": repo,
            "tensor_patterns": sorted(bonfyre_families) or ["config.json"],
            "attaches_to": list(workflow_steps or ["A3.s03"]),
        },
    }
    if inventory is not None:
        doc["verification"] = build_verification_block(doc, inventory)
    return yaml.safe_dump(doc, sort_keys=False, allow_unicode=False)


def emit_inventory_json(path: Path, inventory: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(inventory, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan HF metadata and emit Bonfyre recipe YAML.")
    parser.add_argument("--repo", required=True, help="HF repo id, e.g. google/gemma-4-31b-it")
    parser.add_argument("--include", action="append", default=[], help="Optional surface glob to keep")
    parser.add_argument("--emit", required=True, help="Output YAML path")
    parser.add_argument("--collection", default="hf_scan", help="Source collection label")
    parser.add_argument("--emit-inventory", help="Optional JSON path for actual inventory output")
    args = parser.parse_args()

    families = load_family_index()
    inventory = enumerate_repo_inventory(args.repo)
    surfaces = collect_surfaces(inventory)
    if args.include:
        filtered = []
        for surface in surfaces:
            if any(fnmatch.fnmatch(surface, pat) for pat in args.include):
                filtered.append(surface)
        surfaces = filtered or surfaces

    matches = match_family_patterns(surfaces, families)
    recipe_name = safe_slug(Path(args.emit).stem or inventory.get("resolved_repo", args.repo).split("/")[-1])
    yaml_text = emit_yaml(
        recipe_name,
        str(inventory.get("resolved_repo", args.repo)),
        args.collection,
        surfaces,
        matches,
        inventory=inventory,
    )
    emit_path = Path(args.emit)
    emit_path.parent.mkdir(parents=True, exist_ok=True)
    emit_path.write_text(yaml_text)
    if args.emit_inventory:
        emit_inventory_json(Path(args.emit_inventory), inventory)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
