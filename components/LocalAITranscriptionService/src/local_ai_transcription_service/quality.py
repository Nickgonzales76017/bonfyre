from typing import Dict, Set

from .models import Deliverable
from .summary import bullet_style_labels, is_junk_bullet, split_sentences


def _generic_bullet_count(items: list) -> int:
    count = 0
    for item in items:
        labels = bullet_style_labels(item)
        if labels & {"transcript_shaped", "prompt", "low_signal"}:
            count += 1
    return count


def _unique_words(text: str) -> Set[str]:
    """Extract a set of meaningful lowercase words from text (3+ chars)."""
    return {w.lower() for w in text.split() if len(w) >= 3 and w.isalpha()}


def _summary_coverage(transcript_text: str, bullets: list) -> float:
    """Measure what fraction of transcript topics are represented in summary bullets.
    Returns 0.0–1.0."""
    if not bullets or not transcript_text.strip():
        return 0.0

    transcript_words = _unique_words(transcript_text)
    if not transcript_words:
        return 0.0

    summary_words = set()
    for bullet in bullets:
        summary_words |= _unique_words(bullet)

    overlap = transcript_words & summary_words
    # Coverage = what % of transcript vocab appears in summary
    # We don't expect 100% — a good summary covers ~15-30% of distinct words
    raw_coverage = len(overlap) / len(transcript_words) if transcript_words else 0.0
    # Normalize so that 25% coverage = 1.0, below that scales linearly
    return min(raw_coverage / 0.25, 1.0)


def _action_specificity(items: list) -> float:
    """Score how specific/actionable the action items are. Returns 0.0–1.0."""
    if not items:
        return 0.0

    specific_count = 0
    for item in items:
        words = item.split()
        word_count = len(words)
        # A specific action item is 5+ words (not just "Review the draft")
        # and contains at least one concrete noun/object
        if word_count >= 5:
            specific_count += 1
        elif word_count >= 3:
            specific_count += 0.5

    return min(specific_count / len(items), 1.0)


def score_quality(
    *,
    raw_transcript_text: str,
    cleaned_transcript_text: str,
    cleanup_result: Dict[str, object],
    deliverable: Deliverable,
) -> Dict[str, object]:
    raw_sentences = split_sentences(raw_transcript_text)
    cleaned_sentences = split_sentences(cleaned_transcript_text)
    summary_count = len(deliverable.summary_bullets)
    action_count = len(deliverable.action_items)
    junk_summary_count = sum(1 for bullet in deliverable.summary_bullets if is_junk_bullet(bullet))
    junk_action_count = sum(1 for bullet in deliverable.action_items if is_junk_bullet(bullet))
    generic_summary_count = _generic_bullet_count(deliverable.summary_bullets)
    generic_action_count = _generic_bullet_count(deliverable.action_items)
    valid_summary_count = summary_count - junk_summary_count
    valid_action_count = action_count - junk_action_count
    filler_removed = int(cleanup_result.get("filler_tokens_removed", 0))

    # ── Transcript quality (30 pts max) ──
    transcript_score = 0
    if cleaned_transcript_text.strip():
        transcript_score += 15  # Non-empty transcript
    if len(cleaned_sentences) >= 5:
        transcript_score += 10  # Enough material to work with
    elif cleaned_sentences:
        transcript_score += 5
    if filler_removed > 0 and filler_removed < len(raw_sentences) * 2:
        transcript_score += 5  # Reasonable cleanup happened (not trash audio)

    # ── Summary quality (35 pts max) ──
    summary_score = 0
    if valid_summary_count > 0:
        # Points for having valid bullets (up to 15)
        summary_score += min(15, valid_summary_count * 5)
        # Points for coverage of transcript content (up to 20)
        coverage = _summary_coverage(cleaned_transcript_text, deliverable.summary_bullets)
        summary_score += int(coverage * 20)

    # ── Action item quality (25 pts max) ──
    action_score = 0
    if valid_action_count > 0:
        # Points for having valid items (up to 10)
        action_score += min(10, valid_action_count * 4)
        # Points for specificity (up to 15)
        specificity = _action_specificity(deliverable.action_items)
        action_score += int(specificity * 15)

    # ── Cleanup quality (10 pts max) ──
    cleanup_score = 0
    if cleanup_result.get("changed"):
        cleanup_score += 5
    if raw_sentences and cleaned_sentences and len(cleaned_sentences) <= len(raw_sentences) + 1:
        cleanup_score += 5  # Cleanup didn't destroy content

    # ── Penalties ──
    junk_penalty = (junk_summary_count * 10) + (junk_action_count * 12)
    generic_penalty = (generic_summary_count * 8) + (generic_action_count * 8)

    score = transcript_score + summary_score + action_score + cleanup_score - junk_penalty - generic_penalty
    score = max(0, min(score, 100))

    if score >= 75:
        status = "strong"
    elif score >= 45:
        status = "usable"
    else:
        status = "weak"

    return {
        "score": score,
        "status": status,
        "raw_sentence_count": len(raw_sentences),
        "cleaned_sentence_count": len(cleaned_sentences),
        "summary_count": summary_count,
        "valid_summary_count": valid_summary_count,
        "junk_summary_count": junk_summary_count,
        "generic_summary_count": generic_summary_count,
        "action_item_count": action_count,
        "valid_action_item_count": valid_action_count,
        "junk_action_item_count": junk_action_count,
        "generic_action_item_count": generic_action_count,
        "summary_coverage": round(_summary_coverage(cleaned_transcript_text, deliverable.summary_bullets), 2),
        "action_specificity": round(_action_specificity(deliverable.action_items), 2),
        "cleanup_changed_text": bool(cleanup_result.get("changed")),
        "filler_tokens_removed": filler_removed,
    }
