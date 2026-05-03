#!/usr/bin/env python3
"""Local vector ingest CLI.

Usage:
  index_weaviate.py --emb EMB.npy --meta META.json [--out OUT.json] [--class-name BonfyreTranscript] [--dry-run]

Writes a deterministic local ingest payload even when Weaviate is not available.
If `weaviate` client and `WEAVIATE_URL` are present, it will attempt a real upsert.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emb", type=Path, required=True)
    parser.add_argument("--meta", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--class-name", default="BonfyreTranscript")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def load_embedding(path: Path) -> List[float]:
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() == ".npy":
        import numpy as np  # type: ignore

        return np.load(path).astype("float32").tolist()

    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("vector"), list):
        return [float(value) for value in payload["vector"]]
    raise ValueError(f"Unsupported embedding format: {path}")


def load_metadata(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {"items": payload}


def build_document_id(metadata: Dict[str, Any]) -> str:
    for key in ("job_slug", "jobSlug", "proofLabel", "id", "job_name", "jobName"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().replace(" ", "-").lower()
    return "bonfyre-document"


def build_properties(metadata: Dict[str, Any]) -> Dict[str, Any]:
    quality = metadata.get("quality") if isinstance(metadata.get("quality"), dict) else {}
    intake = metadata.get("intake_manifest") if isinstance(metadata.get("intake_manifest"), dict) else {}
    return {
        "jobSlug": metadata.get("job_slug") or metadata.get("jobSlug"),
        "jobName": metadata.get("job_name") or metadata.get("jobName"),
        "sourceKind": metadata.get("source_kind") or metadata.get("sourceKind"),
        "qualityScore": quality.get("score"),
        "qualityStatus": quality.get("status"),
        "buyerType": intake.get("buyerType"),
        "clientName": intake.get("clientName"),
        "proofLabel": metadata.get("proof_label") or metadata.get("proofLabel"),
        "metaPath": metadata.get("meta_path") or metadata.get("metaPath"),
    }


def maybe_upsert_weaviate(class_name: str, document_id: str, properties: Dict[str, Any], vector: List[float]) -> bool:
    weaviate_url = os.environ.get("WEAVIATE_URL")
    if not weaviate_url:
        return False
    try:
        import weaviate  # type: ignore

        client = weaviate.Client(weaviate_url)
        client.data_object.create(
            data_object=properties,
            class_name=class_name,
            uuid=document_id,
            vector=vector,
        )
        return True
    except Exception:
        return False


def main() -> int:
    args = build_parser().parse_args()
    if args.dry_run:
        print(f"Would ingest embedding {args.emb} with metadata {args.meta} into {args.class_name}")
        return 0

    if not args.emb.exists() or not args.meta.exists():
        print("Missing emb or meta files", file=sys.stderr)
        return 2

    vector = load_embedding(args.emb)
    metadata = load_metadata(args.meta)
    document_id = build_document_id(metadata)
    properties = build_properties(metadata)
    weaviate_upserted = maybe_upsert_weaviate(args.class_name, document_id, properties, vector)

    out_path = args.out or args.meta.with_name(f"{args.meta.stem}.weaviate-batch.json")
    payload = {
        "sourceSystem": "WeaviateIndexer",
        "className": args.class_name,
        "documentId": document_id,
        "embeddingPath": str(args.emb),
        "metadataPath": str(args.meta),
        "vectorLength": len(vector),
        "properties": properties,
        "weaviateUpserted": weaviate_upserted,
    }
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    status_path = out_path.parent / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "sourceSystem": "WeaviateIndexer",
                "status": "completed",
                "ingestPayloadPath": str(out_path),
                "documentId": document_id,
                "weaviateUpserted": weaviate_upserted,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"Wrote ingest payload to {out_path}")
    if weaviate_upserted:
        print("Weaviate upsert completed.")
    else:
        print("Weaviate client/url unavailable; wrote local ingest payload only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
