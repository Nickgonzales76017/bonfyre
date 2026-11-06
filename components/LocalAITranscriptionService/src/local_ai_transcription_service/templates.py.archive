from .models import Deliverable


def render_deliverable(deliverable: Deliverable) -> str:
    summary_lines = "\n".join(f"- {item}" for item in deliverable.summary_bullets) or "- No summary generated"
    action_lines = "\n".join(f"- {item}" for item in deliverable.action_items) or "- No action items detected"
    transcript_body = "\n\n".join(deliverable.transcript_paragraphs) or deliverable.transcript
    processing_lines = "\n".join(f"- {item}" for item in deliverable.processing_notes) or "- No processing notes"
    deep_summary_lines = []
    for section in deliverable.deep_summary_sections:
        title = str(section.get("title", "Section"))
        deep_summary_lines.append(f"- {title}")
        items = section.get("items", [])
        if items:
            for item in items:
                deep_summary_lines.append(f"  - {item.get('lead', '')}")
                for detail in item.get("details", []):
                    deep_summary_lines.append(f"    - {detail}")
                for sibling in item.get("sibling_leads", []):
                    deep_summary_lines.append(f"  - {sibling}")
        else:
            for bullet in section.get("bullets", []):
                deep_summary_lines.append(f"  - {bullet}")
    deep_summary_block = "\n".join(deep_summary_lines) or "- No deep summary generated"

    # Build metadata header
    meta_parts = [f"- source: {deliverable.source_kind}"]
    if deliverable.client_name:
        meta_parts.append(f"- client: {deliverable.client_name}")
    if deliverable.output_goal:
        meta_parts.append(f"- goal: {deliverable.output_goal}")
    if deliverable.context_notes:
        meta_parts.append(f"- context: {deliverable.context_notes}")

    quality = deliverable.quality
    quality_status = quality.get("status", "unknown")
    quality_score = quality.get("score", "n/a")
    meta_parts.append(f"- quality: {quality_status} ({quality_score}/100)")

    meta_block = "\n".join(meta_parts)

    return f"""# {deliverable.title}

## Metadata
{meta_block}

## Summary
{summary_lines}

## Action Items
{action_lines}

## Processing Notes
{processing_lines}

## Deep Summary
{deep_summary_block}

## Transcript
{transcript_body}
"""
