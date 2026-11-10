"""Deliverable Formatter Engine — render pipeline output into polished deliverables."""

import argparse
import json
import os
from datetime import datetime

TEMPLATES = {
    "executive-summary": {
        "sections": ["header", "summary", "action_items", "key_decisions"],
        "description": "Concise executive overview with decisions and next steps",
    },
    "meeting-notes": {
        "sections": ["header", "transcript_excerpt", "summary", "action_items", "attendees"],
        "description": "Full meeting notes with transcript highlights",
    },
    "creator-repurpose": {
        "sections": ["header", "summary", "quotable_moments", "content_angles"],
        "description": "Content repurposing brief for creators",
    },
    "full-deliverable": {
        "sections": ["header", "transcript", "summary", "action_items", "key_decisions", "notes"],
        "description": "Complete deliverable with all available sections",
    },
}


def render_section(section, data):
    """Render a single section to markdown."""
    if section == "header":
        title = data.get("job_title", "Untitled")
        client = data.get("client_name", "")
        date = data.get("created_at", datetime.now().isoformat())[:10]
        lines = [f"# {title}", ""]
        if client:
            lines.append(f"**Client:** {client}")
        lines.append(f"**Date:** {date}")
        lines.append("")
        return "\n".join(lines)

    if section == "summary":
        text = data.get("summary", "_No summary available._")
        return f"## Summary\n\n{text}\n"

    if section == "transcript" or section == "transcript_excerpt":
        text = data.get("transcript", "_No transcript available._")
        if section == "transcript_excerpt" and len(text) > 2000:
            text = text[:2000] + "\n\n_[Transcript truncated]_"
        return f"## Transcript\n\n{text}\n"

    if section == "action_items":
        items = data.get("action_items", [])
        if not items:
            return "## Action Items\n\n_None identified._\n"
        lines = ["## Action Items", ""]
        for item in items:
            lines.append(f"- [ ] {item}")
        lines.append("")
        return "\n".join(lines)

    if section == "key_decisions":
        decisions = data.get("key_decisions", [])
        if not decisions:
            return ""
        lines = ["## Key Decisions", ""]
        for d in decisions:
            lines.append(f"- {d}")
        lines.append("")
        return "\n".join(lines)

    if section == "notes":
        notes = data.get("context_notes", "")
        if not notes:
            return ""
        return f"## Notes\n\n{notes}\n"

    return ""


def format_deliverable(data, template_name="full-deliverable"):
    """Render a complete deliverable from pipeline output data."""
    template = TEMPLATES.get(template_name, TEMPLATES["full-deliverable"])
    parts = []
    for section in template["sections"]:
        rendered = render_section(section, data)
        if rendered:
            parts.append(rendered)
    return "\n---\n\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Deliverable Formatter Engine")
    parser.add_argument("input", help="Job output directory or JSON file")
    parser.add_argument("--template", "-t", default="full-deliverable",
                        choices=list(TEMPLATES.keys()), help="Output template")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--list-templates", action="store_true", help="List available templates")
    args = parser.parse_args()

    if args.list_templates:
        for name, tmpl in TEMPLATES.items():
            print(f"  {name}: {tmpl['description']}")
        return

    # Load data
    if os.path.isfile(args.input) and args.input.endswith(".json"):
        with open(args.input) as f:
            data = json.load(f)
    elif os.path.isdir(args.input):
        manifest = os.path.join(args.input, "manifest.json")
        if os.path.exists(manifest):
            with open(manifest) as f:
                data = json.load(f)
        else:
            print(f"No manifest.json in {args.input}")
            return
    else:
        print(f"Cannot read: {args.input}")
        return

    output = format_deliverable(data, args.template)

    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Written: {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
