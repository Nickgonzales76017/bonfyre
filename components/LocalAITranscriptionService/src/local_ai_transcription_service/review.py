import json
from pathlib import Path
from typing import Dict, List, Tuple

from .summary import is_junk_bullet


def load_proof_context(proof_path: Path) -> Tuple[Path, Dict[str, object], str]:
    if proof_path.is_file():
        if proof_path.name == "proof-summary.json":
            proof_dir = proof_path.parent
        else:
            raise ValueError("Proof review path must be a proof directory or proof-summary.json.")
    else:
        proof_dir = proof_path

    summary_path = proof_dir / "proof-summary.json"
    deliverable_path = proof_dir / "deliverable.md"
    if not summary_path.exists() or not deliverable_path.exists():
        raise ValueError(f"Proof directory is missing required files: {proof_dir}")

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    deliverable_text = deliverable_path.read_text(encoding="utf-8")
    return proof_dir, summary_payload, deliverable_text


def _extract_section(text: str, heading: str, next_heading: str) -> str:
    if heading not in text:
        return ""
    remainder = text.split(heading, 1)[1]
    if next_heading in remainder:
        return remainder.split(next_heading, 1)[0].strip()
    if "\n## " in remainder:
        return remainder.split("\n## ", 1)[0].strip()
    return remainder.strip()


def _extract_bullets(section: str) -> List[str]:
    bullets: List[str] = []
    for line in section.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            bullets.append(stripped[2:].strip())
    return bullets


def _is_generic_summary_bullet(bullet: str) -> bool:
    lowered = bullet.lower().strip()
    if lowered in {
        "they are on linkedin.",
        "they are on linkedin",
        "older venue owners were slow adopters and hard to move.",
    }:
        return True
    if len(bullet.split()) < 6:
        return True
    return False


def review_proof(proof_path: Path) -> Dict[str, object]:
    proof_dir, summary_payload, deliverable_text = load_proof_context(proof_path)
    quality = summary_payload.get("quality", {})
    if not isinstance(quality, dict):
        quality = {}

    summary_section = _extract_section(deliverable_text, "## Summary", "## Action Items")
    action_section = _extract_section(deliverable_text, "## Action Items", "## Processing Notes")
    summary_bullets = _extract_bullets(summary_section)
    action_bullets = _extract_bullets(action_section)

    score = 0
    findings: List[str] = []

    quality_score = int(quality.get("score", 0) or 0)
    summary_count = int(quality.get("summary_count", len(summary_bullets)) or 0)
    action_count = int(quality.get("action_item_count", len(action_bullets)) or 0)
    valid_summary_count = int(quality.get("valid_summary_count", summary_count) or 0)
    valid_action_count = int(quality.get("valid_action_item_count", action_count) or 0)
    junk_summary_count = int(quality.get("junk_summary_count", 0) or 0)
    junk_action_count = int(quality.get("junk_action_item_count", 0) or 0)

    junk_summary_bullets = [bullet for bullet in summary_bullets if is_junk_bullet(bullet)]
    junk_action_bullets = [bullet for bullet in action_bullets if is_junk_bullet(bullet)]
    generic_summary_bullets = [bullet for bullet in summary_bullets if _is_generic_summary_bullet(bullet)]
    transcript_like_action_bullets = [
        bullet
        for bullet in action_bullets
        if any(token in bullet.lower() for token in ("they're like", "i was like", "thanks for", "welcome back"))
    ]

    if quality_score >= 85:
        score += 25
    elif quality_score >= 70:
        score += 15
    else:
        findings.append("Pipeline quality score is still below strong proof range.")

    if valid_summary_count >= 4:
        score += 25
    elif valid_summary_count >= 3:
        score += 15
    else:
        findings.append("Summary layer is still too thin or too noisy for buyer-facing proof.")

    if valid_action_count >= 3:
        score += 25
    elif valid_action_count >= 2:
        score += 15
    else:
        findings.append("Action layer is still too weak for decision-ready proof.")

    lowered_summary = summary_section.lower()
    if all(token not in lowered_summary for token in ("welcome to", "thanks so much for having me", "coupon code")):
        score += 10
    else:
        findings.append("Summary still carries obvious intro or promo chatter.")

    if "No action items detected" not in action_section and action_section.strip() and action_bullets:
        score += 5
    else:
        findings.append("Deliverable still lacks a usable action section.")

    if not junk_summary_count and not junk_summary_bullets and not generic_summary_bullets:
        score += 10
    else:
        findings.append("Summary still contains transcript-shaped, generic, or junk bullets.")

    if not junk_action_count and not junk_action_bullets and not transcript_like_action_bullets:
        score += 10
    else:
        findings.append("Action items still contain junk, transcript phrasing, or conversational scraps.")

    if summary_count >= valid_summary_count and action_count >= valid_action_count:
        score += 15
    else:
        findings.append("Quality snapshot does not support the buyer-facing bullet counts.")

    if summary_count > valid_summary_count + 1 or action_count > valid_action_count + 1:
        score -= 15
    if junk_summary_bullets or junk_action_bullets:
        score -= 20
    if generic_summary_bullets:
        score -= 10
    if transcript_like_action_bullets:
        score -= 15

    score = max(0, min(score, 100))
    if score >= 85:
        recommendation = "promote"
    elif score >= 65:
        recommendation = "usable-with-review"
    else:
        recommendation = "hold"

    payload = {
        "proof_slug": summary_payload.get("proof_slug"),
        "proof_label": summary_payload.get("proof_label"),
        "review_score": score,
        "recommendation": recommendation,
        "findings": findings,
        "quality_snapshot": quality,
        "review_snapshot": {
            "summary_bullet_count": len(summary_bullets),
            "action_bullet_count": len(action_bullets),
            "junk_summary_bullets": junk_summary_bullets,
            "generic_summary_bullets": generic_summary_bullets,
            "junk_action_bullets": junk_action_bullets,
            "transcript_like_action_bullets": transcript_like_action_bullets,
        },
    }

    review_path = proof_dir / "proof-review.json"
    review_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload
