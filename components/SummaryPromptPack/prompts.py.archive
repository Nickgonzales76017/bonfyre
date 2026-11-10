"""Summary Prompt Pack — buyer-tuned prompt templates for transcript summarization."""

import argparse
import os

TEMPLATES = {
    "executive-summary": {
        "description": "Concise executive overview with decisions and action items",
        "prompt": """You are summarizing a transcript for a busy founder or executive.

Extract:
1. **Key Decisions** — what was decided
2. **Action Items** — who does what, by when
3. **Open Questions** — unresolved issues that need follow-up

Keep the summary under 300 words. Use bullet points. Be direct.

Transcript:
{transcript}""",
    },
    "meeting-notes": {
        "description": "Full meeting recap with attendees and discussion points",
        "prompt": """You are creating meeting notes from a transcript.

Include:
1. **Attendees** — who was present (infer from the conversation)
2. **Discussion Points** — main topics covered
3. **Decisions Made** — what was agreed
4. **Action Items** — tasks assigned with owners
5. **Next Steps** — what happens after this meeting

Format as clean markdown. Be thorough but concise.

Transcript:
{transcript}""",
    },
    "student-notes": {
        "description": "Learning-focused summary with key concepts and takeaways",
        "prompt": """You are creating study notes from a lecture or educational transcript.

Include:
1. **Key Concepts** — main ideas and definitions
2. **Important Details** — supporting facts and examples
3. **Connections** — how ideas relate to each other
4. **Review Questions** — 3-5 questions to test understanding

Use clear headings and bullet points. Prioritize clarity over brevity.

Transcript:
{transcript}""",
    },
    "creator-repurpose": {
        "description": "Content repurposing brief with quotable moments",
        "prompt": """You are helping a content creator repurpose a conversation into social content.

Extract:
1. **Quotable Moments** — 3-5 standalone quotes that work as social posts
2. **Content Angles** — 2-3 themes that could become separate pieces
3. **Hook Ideas** — attention-grabbing openers based on the conversation
4. **Summary Thread** — a 5-tweet thread summarizing the key points

Be punchy and engaging. Write for social media attention spans.

Transcript:
{transcript}""",
    },
}


def render_prompt(template_name, transcript_text):
    """Render a prompt template with the given transcript."""
    tmpl = TEMPLATES.get(template_name)
    if not tmpl:
        raise ValueError(f"Unknown template: {template_name}")
    return tmpl["prompt"].format(transcript=transcript_text)


def main():
    parser = argparse.ArgumentParser(description="Summary Prompt Pack")
    parser.add_argument("--list", action="store_true", help="List available templates")
    parser.add_argument("--template", "-t", choices=list(TEMPLATES.keys()))
    parser.add_argument("--transcript", help="Path to transcript text file")
    parser.add_argument("--output", "-o", help="Write rendered prompt to file")
    args = parser.parse_args()

    if args.list:
        for name, tmpl in TEMPLATES.items():
            print(f"  {name}: {tmpl['description']}")
        return

    if not args.template or not args.transcript:
        parser.print_help()
        return

    with open(args.transcript) as f:
        text = f.read()

    rendered = render_prompt(args.template, text)

    if args.output:
        with open(args.output, "w") as f:
            f.write(rendered)
        print(f"Written: {args.output}")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
