import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> Dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def load_index(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {"created_at": utc_now(), "updated_at": utc_now(), "offers": []}
    payload = load_json(path)
    offers = payload.get("offers")
    if not isinstance(offers, list):
        payload["offers"] = []
    return payload


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def first_matching_proof(index_path: Path) -> Dict[str, object]:
    payload = load_json(index_path)
    proofs = payload.get("proofs", [])
    if not isinstance(proofs, list):
        raise ValueError("Proof index is missing a valid proofs list.")

    reviewed: List[Dict[str, object]] = []
    for proof in proofs:
        if not isinstance(proof, dict):
            continue
        summary_path = Path(str(proof.get("proof_summary_path")))
        review_path = summary_path.parent / "proof-review.json"
        if not review_path.exists():
            continue
        review = load_json(review_path)
        if review.get("recommendation") == "promote":
            proof = dict(proof)
            proof["proof_review_path"] = str(review_path)
            reviewed.append(proof)

    if not reviewed:
        raise ValueError("No promoted proofs with review scorecards were found.")
    return reviewed[0]


def reviewed_proofs(index_path: Path) -> List[Dict[str, object]]:
    payload = load_json(index_path)
    proofs = payload.get("proofs", [])
    if not isinstance(proofs, list):
        raise ValueError("Proof index is missing a valid proofs list.")

    reviewed: List[Dict[str, object]] = []
    for proof in proofs:
        if not isinstance(proof, dict):
            continue
        summary_path = Path(str(proof.get("proof_summary_path")))
        review_path = summary_path.parent / "proof-review.json"
        if not review_path.exists():
            continue
        review = load_json(review_path)
        if review.get("recommendation") == "promote":
            enriched = dict(proof)
            enriched["proof_review_path"] = str(review_path)
            enriched["review_recommendation"] = review.get("recommendation")
            reviewed.append(enriched)
    return reviewed


def detect_buyer_segment(summary_payload: Dict[str, object]) -> str:
    proof_label = str(summary_payload.get("proof_label", "")).lower()
    context = str(summary_payload.get("intake_manifest", {}).get("contextNotes", "")).lower() if isinstance(summary_payload.get("intake_manifest"), dict) else ""
    joined = f"{proof_label} {context}"

    if "customer" in joined:
        return "customer-research teams"
    if "founder" in joined:
        return "founders and operators"
    if "investor" in joined:
        return "investors and operator-led funds"
    if "business" in joined:
        return "small business owners"
    return "operators with messy audio inputs"


def build_offer_payload(
    *,
    summary_payload: Dict[str, object],
    review_payload: Dict[str, object],
    deliverable_text: str,
) -> Dict[str, object]:
    quality = summary_payload.get("quality", {})
    if not isinstance(quality, dict):
        quality = {}

    buyer_segment = detect_buyer_segment(summary_payload)
    proof_label = str(summary_payload.get("proof_label") or summary_payload.get("job_name") or "Proof Asset")
    source_kind = str(summary_payload.get("source_kind") or "audio_file")
    recommendation = str(review_payload.get("recommendation") or "usable-with-review")
    review_score = int(review_payload.get("review_score") or 0)
    action_count = int(quality.get("action_item_count") or 0)
    summary_count = int(quality.get("summary_count") or 0)

    promise = "Send one messy recording. Get back a clean transcript, a structured summary, and candidate next steps."
    headline = f"Local-first transcript deliverables for {buyer_segment}"
    offer_name = f"{proof_label} Offer"
    price = "$18"
    turnaround = "same day for short files"
    segment_variants = [
        {
            "segment": "founders and operators",
            "headline": "Turn founder voice notes into decision-ready output",
            "hook": "Send one messy memo and get back a transcript, summary, and next steps you can act on today.",
        },
        {
            "segment": "consultants and agencies",
            "headline": "Turn client recordings into clean recap deliverables",
            "hook": "Use local-first processing to return polished recaps without shipping client audio to SaaS tools.",
        },
        {
            "segment": "customer-research teams",
            "headline": "Turn customer calls into structured insight snapshots",
            "hook": "Pull pain points, themes, and next actions from interviews faster than manual note cleanup.",
        },
    ]

    return {
        "created_at": utc_now(),
        "offer_name": offer_name,
        "headline": headline,
        "promise": promise,
        "buyer_segment": buyer_segment,
        "proof_asset": proof_label,
        "price": price,
        "turnaround": turnaround,
        "source_kind": source_kind,
        "review_score": review_score,
        "recommendation": recommendation,
        "proof_quality": quality,
        "offer_angle": (
            f"This proof passed a stricter review gate at score {review_score} with {summary_count} summary bullets and {action_count} action items, "
            "which makes it credible enough to use as a buyer-facing before/after example."
        ),
        "deliverables": [
            "clean transcript",
            "executive summary",
            "action items",
            "processing notes",
        ],
        "segment_variants": segment_variants,
        "outreach_hook": (
            f"I turned a messy public recording into a structured deliverable with {action_count} usable next steps. "
            "I can do the same for one of your calls or voice notes."
        ),
        "deliverable_excerpt": deliverable_text[:1400].strip(),
    }


def render_offer_markdown(payload: Dict[str, object]) -> str:
    deliverables = "\n".join(f"- {item}" for item in payload["deliverables"])
    return f"""# {payload['offer_name']}

## Headline
{payload['headline']}

## Promise
{payload['promise']}

## Buyer
{payload['buyer_segment']}

## Pricing
- price: {payload['price']}
- turnaround: {payload['turnaround']}

## Deliverables
{deliverables}

## Proof Angle
{payload['offer_angle']}

## Offer Hook
{payload['outreach_hook']}
"""


def render_outreach_markdown(payload: Dict[str, object]) -> str:
    return f"""# Outreach Draft

{payload['headline']}

{payload['outreach_hook']}

{payload['promise']}

Price: {payload['price']}. Turnaround: {payload['turnaround']}.
"""


def render_listing_markdown(payload: Dict[str, object]) -> str:
    return f"""# Listing Draft

## {payload['headline']}

{payload['promise']}

### What You Get
- clean transcript
- concise summary
- action items

### Price
{payload['price']}

### Turnaround
{payload['turnaround']}
"""


def render_variants_markdown(payload: Dict[str, object]) -> str:
    lines = ["# Buyer Variants", ""]
    for variant in payload["segment_variants"]:
        lines.extend(
            [
                f"## {variant['segment']}",
                f"- headline: {variant['headline']}",
                f"- hook: {variant['hook']}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def render_variant_outreach_markdown(payload: Dict[str, object]) -> str:
    lines = ["# Variant Outreach Drafts", ""]
    for variant in payload["segment_variants"]:
        lines.extend(
            [
                f"## {variant['segment']}",
                variant["headline"],
                "",
                variant["hook"],
                "",
                payload["promise"],
                "",
                f"Price: {payload['price']}. Turnaround: {payload['turnaround']}.",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def render_vault_offer_note(payload: Dict[str, object]) -> str:
    created = payload["created_at"][:10]
    deliverables = "\n".join(f"- {item}" for item in payload["deliverables"])
    variants = "\n".join(
        f"- {variant['segment']}: {variant['headline']}"
        for variant in payload["segment_variants"]
    )
    return f"""---
type: offer
cssclasses:
  - offer
title: {payload['offer_name']}
created: {created}
updated: {created}
status: draft
tags:
  - offer
  - monetization
  - generated
---

# Offer: {payload['offer_name']}

## Offer
- who it is for: {payload['buyer_segment']}
- painful problem: recordings pile up and never become decision-ready outputs
- promised outcome: {payload['promise']}
- turnaround: {payload['turnaround']}

## Deliverable
{deliverables}

## Pricing
- price: {payload['price']}
- estimated fulfillment cost: local-first compute, near-zero direct model cost
- estimated margin: high if the workflow stays mostly automated

## Sales Angle
- headline: {payload['headline']}
- trust signal: proof asset `{payload['proof_asset']}` reviewed at score `{payload['review_score']}` with recommendation `{payload['recommendation']}`
- call to action: {payload['outreach_hook']}

## Proof
- asset: {payload['proof_asset']}
- angle: {payload['offer_angle']}

## Variants
{variants}

## Delivery Workflow
1. intake
2. local processing
3. review and package
4. deliver and use as proof

## Risks
- this note is generated from a proof asset and still needs human tightening before outbound
- proof strength may not generalize across every buyer segment

## Links
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - Local AI Transcription Service]]
"""


def render_offer_catalog_note(index_payload: Dict[str, object]) -> str:
    lines = [
        "---",
        "type: monetization-catalog",
        "title: Generated Offer Catalog",
        f"created: {str(index_payload.get('created_at', utc_now()))[:10]}",
        f"updated: {str(index_payload.get('updated_at', utc_now()))[:10]}",
        "status: active",
        "tags:",
        "  - monetization",
        "  - generated",
        "---",
        "",
        "# Generated Offer Catalog",
        "",
    ]
    offers = index_payload.get("offers", [])
    if not isinstance(offers, list) or not offers:
        lines.append("- no generated offers yet")
        return "\n".join(lines) + "\n"

    for offer in offers:
        if not isinstance(offer, dict):
            continue
        lines.extend(
            [
                f"## {offer.get('offer_name', 'Generated Offer')}",
                f"- buyer: {offer.get('buyer_segment', 'unknown')}",
                f"- price: {offer.get('price', 'n/a')}",
                f"- proof asset: {offer.get('proof_asset', 'n/a')}",
                f"- review score: {offer.get('review_score', 'n/a')}",
                f"- note: [[05-Monetization/{offer.get('vault_note_name', '')}]]" if offer.get("vault_note_name") else "- note: n/a",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def render_pipeline_snapshot(sync_payload: Dict[str, object]) -> str:
    lines = [
        "---",
        "type: monetization-snapshot",
        "title: Offer Pipeline Snapshot",
        f"created: {str(sync_payload.get('created_at', utc_now()))[:10]}",
        f"updated: {str(sync_payload.get('updated_at', utc_now()))[:10]}",
        "status: active",
        "tags:",
        "  - monetization",
        "  - generated",
        "  - snapshot",
        "---",
        "",
        "# Offer Pipeline Snapshot",
        "",
        f"- reviewed proofs: {sync_payload.get('reviewed_proof_count', 0)}",
        f"- generated offers: {sync_payload.get('generated_offer_count', 0)}",
        "",
    ]
    offers = sync_payload.get("offers", [])
    if not isinstance(offers, list) or not offers:
        lines.append("- no offers generated yet")
        return "\n".join(lines) + "\n"

    for offer in offers:
        if not isinstance(offer, dict):
            continue
        lines.extend(
            [
                f"## {offer.get('offer_name', 'Generated Offer')}",
                f"- proof: {offer.get('proof_asset', 'n/a')}",
                f"- buyer: {offer.get('buyer_segment', 'n/a')}",
                f"- recommendation: {offer.get('recommendation', 'n/a')}",
                f"- review score: {offer.get('review_score', 'n/a')}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def generate_offer_package(
    *,
    proof_index: Optional[Path] = None,
    proof_dir: Optional[Path] = None,
    output_root: Path = Path("outputs"),
    vault_monetization_root: Optional[Path] = None,
) -> Dict[str, object]:
    if proof_dir is None and proof_index is None:
        raise ValueError("Provide proof_dir or proof_index.")

    if proof_dir is None:
        proof = first_matching_proof(proof_index)  # type: ignore[arg-type]
        proof_summary_path = Path(str(proof["proof_summary_path"]))
        proof_dir = proof_summary_path.parent

    summary_payload = load_json(proof_dir / "proof-summary.json")
    review_payload = load_json(proof_dir / "proof-review.json")
    if review_payload.get("recommendation") != "promote":
        raise ValueError(f"Proof is not promotion-ready: {proof_dir}")
    deliverable_text = read_text(proof_dir / "deliverable.md")
    payload = build_offer_payload(
        summary_payload=summary_payload,
        review_payload=review_payload,
        deliverable_text=deliverable_text,
    )

    offer_slug = str(summary_payload.get("proof_slug") or proof_dir.name)
    offer_dir = output_root / offer_slug
    offer_dir.mkdir(parents=True, exist_ok=True)

    offer_json_path = offer_dir / "offer.json"
    offer_md_path = offer_dir / "offer.md"
    outreach_md_path = offer_dir / "outreach.md"
    listing_md_path = offer_dir / "listing.md"
    variants_md_path = offer_dir / "variants.md"
    variant_outreach_md_path = offer_dir / "variant-outreach.md"

    offer_json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    offer_md_path.write_text(render_offer_markdown(payload), encoding="utf-8")
    outreach_md_path.write_text(render_outreach_markdown(payload), encoding="utf-8")
    listing_md_path.write_text(render_listing_markdown(payload), encoding="utf-8")
    variants_md_path.write_text(render_variants_markdown(payload), encoding="utf-8")
    variant_outreach_md_path.write_text(render_variant_outreach_markdown(payload), encoding="utf-8")

    vault_note_path = None
    vault_note_name = None
    if vault_monetization_root is not None:
        vault_monetization_root.mkdir(parents=True, exist_ok=True)
        vault_note_name = f"Offer - {payload['offer_name']}.md"
        vault_note_path = vault_monetization_root / vault_note_name
        vault_note_path.write_text(render_vault_offer_note(payload), encoding="utf-8")

        catalog_index_path = vault_monetization_root / "_generated-offers.json"
        catalog_payload = load_index(catalog_index_path)
        offers = [entry for entry in catalog_payload.get("offers", []) if isinstance(entry, dict)]
        offers = [entry for entry in offers if entry.get("offer_name") != payload["offer_name"]]
        offers.append(
            {
                "offer_name": payload["offer_name"],
                "buyer_segment": payload["buyer_segment"],
                "price": payload["price"],
                "proof_asset": payload["proof_asset"],
                "review_score": payload["review_score"],
                "vault_note_name": vault_note_name,
                "generated_at": payload["created_at"],
            }
        )
        catalog_payload["offers"] = offers
        catalog_payload["updated_at"] = utc_now()
        catalog_index_path.write_text(json.dumps(catalog_payload, indent=2) + "\n", encoding="utf-8")

        catalog_note_path = vault_monetization_root / "_Generated Offer Catalog.md"
        catalog_note_path.write_text(render_offer_catalog_note(catalog_payload), encoding="utf-8")
    else:
        catalog_index_path = None
        catalog_note_path = None

    return {
        "offer_dir": str(offer_dir.resolve()),
        "offer_json": str(offer_json_path.resolve()),
        "offer_md": str(offer_md_path.resolve()),
        "outreach_md": str(outreach_md_path.resolve()),
        "listing_md": str(listing_md_path.resolve()),
        "variants_md": str(variants_md_path.resolve()),
        "variant_outreach_md": str(variant_outreach_md_path.resolve()),
        "vault_note": str(vault_note_path.resolve()) if vault_note_path else None,
        "catalog_index": str(catalog_index_path.resolve()) if catalog_index_path else None,
        "catalog_note": str(catalog_note_path.resolve()) if catalog_note_path else None,
        "proof_dir": str(proof_dir.resolve()),
        "offer_name": payload["offer_name"],
    }


def sync_offer_pipeline(
    *,
    proof_index: Path,
    output_root: Path,
    vault_monetization_root: Path,
) -> Dict[str, object]:
    vault_monetization_root.mkdir(parents=True, exist_ok=True)
    proofs = reviewed_proofs(proof_index)
    generated = []
    for proof in proofs:
        proof_dir = Path(str(Path(str(proof["proof_summary_path"])).parent))
        result = generate_offer_package(
            proof_dir=proof_dir,
            output_root=output_root,
            vault_monetization_root=vault_monetization_root,
        )
        generated_payload = {
            "offer_name": result["offer_name"],
            "proof_asset": proof.get("proof_label"),
            "buyer_segment": load_json(Path(result["offer_json"]))["buyer_segment"],
            "recommendation": proof.get("review_recommendation"),
            "review_score": load_json(proof_dir / "proof-review.json").get("review_score"),
        }
        generated.append(generated_payload)

    snapshot = {
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "reviewed_proof_count": len(proofs),
        "generated_offer_count": len(generated),
        "offers": generated,
    }
    catalog_payload = {
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "offers": [
            {
                "offer_name": offer["offer_name"],
                "buyer_segment": offer["buyer_segment"],
                "price": load_json(output_root / str(Path(str(proof["proof_summary_path"])).parent.name) / "offer.json").get("price"),
                "proof_asset": offer["proof_asset"],
                "review_score": offer["review_score"],
                "vault_note_name": f"Offer - {offer['offer_name']}.md",
                "generated_at": utc_now(),
            }
            for offer, proof in zip(generated, proofs)
        ],
    }
    catalog_index_path = vault_monetization_root / "_generated-offers.json"
    catalog_note_path = vault_monetization_root / "_Generated Offer Catalog.md"
    catalog_index_path.write_text(json.dumps(catalog_payload, indent=2) + "\n", encoding="utf-8")
    catalog_note_path.write_text(render_offer_catalog_note(catalog_payload), encoding="utf-8")

    snapshot_json_path = vault_monetization_root / "_offer-pipeline-snapshot.json"
    snapshot_note_path = vault_monetization_root / "_Offer Pipeline Snapshot.md"
    snapshot_json_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    snapshot_note_path.write_text(render_pipeline_snapshot(snapshot), encoding="utf-8")

    return {
        "proof_index": str(proof_index.resolve()),
        "reviewed_proof_count": len(proofs),
        "generated_offer_count": len(generated),
        "snapshot_json": str(snapshot_json_path.resolve()),
        "snapshot_note": str(snapshot_note_path.resolve()),
    }
