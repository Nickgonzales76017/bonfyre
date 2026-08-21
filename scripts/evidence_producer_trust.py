#!/usr/bin/env python3
"""Evaluate whether foreign evidence came from the trusted producer definition.

Artifact identity is intentionally not producer authority. A perfectly valid,
content-addressed artifact can be manufactured by a modified or same-name
producer. Bonfÿre therefore binds the observation to both the subject and the
producer definition before the artifact can be promoted as trusted evidence.

This module is provider-neutral. GitHub workflow blobs, CI jobs, build recipes,
model evaluators, agent verifiers, and other foreign producers can all project
into the same contract without upgrading their native digest into a Bonfÿre
security identity.
"""
from __future__ import annotations

import argparse
import json
import re
from typing import Any

SCHEMA = "bonfyre.evidence-producer-trust.v1"
MAX_TEXT = 4096
ALGORITHM = re.compile(r"^[a-z0-9][a-z0-9._+-]{0,63}$")
HEX = re.compile(r"^[0-9a-fA-F]+$")


class TrustInputError(ValueError):
    pass


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_TEXT:
        raise TrustInputError(f"{name} must be a non-empty bounded string")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise TrustInputError(f"{name} contains control characters")
    return value


def _identity(value: Any, name: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"algorithm", "digest"}:
        raise TrustInputError(f"{name} must contain exactly algorithm and digest")
    algorithm = _text(value["algorithm"], f"{name}.algorithm").lower()
    digest = _text(value["digest"], f"{name}.digest").lower()
    if not ALGORITHM.fullmatch(algorithm):
        raise TrustInputError(f"{name}.algorithm is malformed")
    if not HEX.fullmatch(digest) or len(digest) < 8 or len(digest) > 256:
        raise TrustInputError(f"{name}.digest must be bounded hexadecimal")
    return {"algorithm": algorithm, "digest": digest}


def evaluate(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TrustInputError("observation must be an object")

    expected_locator = _text(value.get("expected_producer_locator"), "expected_producer_locator")
    observed_locator = _text(value.get("observed_producer_locator"), "observed_producer_locator")
    expected_subject = _text(value.get("expected_subject"), "expected_subject")
    observed_subject = _text(value.get("observed_subject"), "observed_subject")
    current_subject = _text(value.get("current_subject"), "current_subject")
    trusted_definition = _identity(value.get("trusted_producer_definition"), "trusted_producer_definition")
    observed_definition = _identity(value.get("observed_producer_definition"), "observed_producer_definition")
    artifact_identity = _identity(value.get("artifact_identity"), "artifact_identity")

    locator_match = observed_locator == expected_locator
    expected_subject_match = observed_subject == expected_subject
    current_subject_match = observed_subject == current_subject
    producer_definition_match = observed_definition == trusted_definition

    if not locator_match:
        reason = "producer_locator_mismatch"
    elif not expected_subject_match or not current_subject_match:
        reason = "stale_or_wrong_subject"
    elif not producer_definition_match:
        reason = "producer_definition_changed"
    else:
        reason = "trusted"

    ok = reason == "trusted"
    return {
        "schema": SCHEMA,
        "ok": ok,
        "reason": reason,
        "producer": {
            "locator_match": locator_match,
            "definition_match": producer_definition_match,
            "trusted_definition": trusted_definition,
            "observed_definition": observed_definition,
        },
        "subject": {
            "expected_match": expected_subject_match,
            "current_match": current_subject_match,
        },
        "artifact": {
            "identity": artifact_identity,
            "identity_bound": True,
            "identity_sufficient_for_producer_trust": False,
        },
        "claim_boundary": {
            "artifact_identity": True,
            "producer_definition_identity": True,
            "subject_binding": True,
            "producer_authorized": ok,
            "artifact_semantically_correct": False,
            "effect_safe": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("observation")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        with open(args.observation, encoding="utf-8") as handle:
            source = json.load(handle)
        result = evaluate(source)
    except (OSError, json.JSONDecodeError, TrustInputError) as exc:
        rendered = json.dumps({"schema": SCHEMA, "ok": False, "reason": "invalid_input", "error": str(exc)}) + "\n"
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                handle.write(rendered)
        else:
            print(rendered, end="")
        return 2

    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(rendered)
    else:
        print(rendered, end="")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
