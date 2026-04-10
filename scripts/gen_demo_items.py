#!/usr/bin/env python3
"""
gen_demo_items.py — Generate demo-items.json from proof directories.

Reads all proof dirs under an app's proofs/ folder and generates
the demo-items.json with real data, provenance links, and output links.

Usage:
  python3 gen_demo_items.py <app-slug>
  python3 gen_demo_items.py freelancer-evidence
"""
import json, os, sys, glob

def main():
    slug = sys.argv[1]
    base = os.path.expanduser(f"~/Projects/pages-{slug}/site/demos/{slug}")
    proofs_dir = os.path.join(base, "proofs")

    if not os.path.isdir(proofs_dir):
        print(f"ERROR: {proofs_dir} not found")
        sys.exit(1)

    items = []
    for proof_path in sorted(glob.glob(os.path.join(proofs_dir, "*"))):
        if not os.path.isdir(proof_path):
            continue
        item_id = os.path.basename(proof_path)

        # Skip old boilerplate items (demo-*-b, demo-*-c)
        if item_id.startswith("demo-") and item_id.endswith(("-b", "-c")):
            continue
        # Also skip the old item-a if we have new real items
        if item_id.startswith("demo-"):
            continue

        # Read source metadata
        source_meta_path = os.path.join(proof_path, "source-meta.json")
        if not os.path.exists(source_meta_path):
            continue  # Not a real-data item

        with open(source_meta_path) as f:
            source = json.load(f)

        # Read brief
        brief_md = os.path.join(proof_path, "brief.md")
        brief_text = ""
        if os.path.exists(brief_md):
            with open(brief_md) as f:
                lines = f.read().strip().split("\n")
                # Extract summary bullets
                in_summary = False
                summary_lines = []
                for line in lines:
                    if line.strip().startswith("## Summary"):
                        in_summary = True
                        continue
                    if line.strip().startswith("## ") and in_summary:
                        break
                    if in_summary and line.strip().startswith("- "):
                        clean = line.strip().lstrip("- ").strip()
                        if clean and len(clean) > 15:
                            summary_lines.append(clean)
                brief_text = " ".join(summary_lines[:4]) if summary_lines else lines[0] if lines else ""

        # Read proof metadata
        proof_json_path = os.path.join(proof_path, "proof.json")
        proof_data = {}
        if os.path.exists(proof_json_path):
            with open(proof_json_path) as f:
                proof_data = json.load(f)

        # Read transcribe metadata
        meta_path = os.path.join(proof_path, "transcribe-meta.json")
        meta = {}
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)

        # Determine tags from content
        title_lower = source.get("title", "").lower()
        tags = extract_tags(title_lower, slug)

        duration = source.get("duration_seconds", 0)
        confidence = meta.get("avg_confidence", 0)

        item = {
            "id": item_id,
            "file": source.get("title", item_id),
            "time": f"Public origin · {duration}s",
            "status": "complete",
            "brief": brief_text,
            "tags": tags,
            "flagged": len(items) == 0,  # Flag first item
            "demo": True,
            "sourceUrl": source.get("url", ""),
            "sourceLabel": "Watch original public video",
            "sourceTitle": source.get("title", ""),
            "publisher": source.get("channel", ""),
            "license": f"Public YouTube video processed transiently. Bonfyre retained only derived artifacts.",
            "sourceCopy": f"Source: {source.get('channel', 'Unknown')} on YouTube. Bonfyre downloaded, transcribed, and deleted the media. Only the transcript, brief, and proof remain.",
            "whyItMatters": get_why_it_matters(slug),
            "searchSummary": ", ".join(tags[:3]),
            "searchIntro": get_search_intro(slug),
            "outputs": get_outputs(slug),
            "outputNotes": get_output_notes(slug),
            "outputLinks": {
                "brief": {
                    "href": f"demos/{slug}/proofs/{item_id}/brief.md",
                    "label": "Open brief"
                },
                "clean transcript": {
                    "href": f"demos/{slug}/proofs/{item_id}/clean.txt",
                    "label": "Open clean transcript"
                },
                "proof bundle": {
                    "href": f"demos/{slug}/proofs/{item_id}/proof.json",
                    "label": "Open proof JSON"
                }
            },
            "searchOutputs": ["search across records"],
            "proofPath": f"demos/{slug}/proofs/{item_id}"
        }
        items.append(item)

    if not items:
        print(f"WARNING: No real-data items found in {proofs_dir}")
        sys.exit(1)

    output_path = os.path.join(base, "demo-items.json")
    with open(output_path, "w") as f:
        json.dump(items, f, indent=2)

    print(f"Wrote {len(items)} items → {output_path}")
    for item in items:
        print(f"  {item['id']}: {item['file'][:60]}...")


def extract_tags(title, slug):
    """Extract relevant tags from title and app slug."""
    tag_map = {
        "freelancer-evidence": ["dispute", "scope", "client", "invoice", "evidence"],
        "family-history": ["oral-history", "family", "memory", "recording", "stories"],
        "postmortem-atlas": ["incident", "postmortem", "production", "review", "sre"],
        "explain-repo": ["architecture", "code", "repository", "walkthrough", "pattern"],
        "grant-evidence": ["nonprofit", "impact", "grant", "community", "evidence"],
        "legal-prep": ["small-claims", "legal", "court", "case-prep", "evidence"],
        "micro-consulting": ["discovery", "consulting", "advisory", "sales", "strategy"],
        "async-standup": ["standup", "async", "remote", "daily", "team"],
        "competitive-intel": ["market", "competitive", "intelligence", "analysis", "strategy"],
        "sales-distiller": ["sales", "objection", "discovery", "call", "closing"],
        "procurement-memory": ["vendor", "procurement", "supplier", "evaluation", "scorecard"],
        "museum-exhibit": ["museum", "exhibit", "audio-tour", "curation", "heritage"],
        "local-archive": ["local-history", "oral-history", "neighborhood", "community", "archive"],
    }
    base_tags = tag_map.get(slug, [slug.replace("-", " ")])
    # Add any matching keywords from title
    keywords = ["guide", "tips", "how-to", "tutorial", "interview", "review", "analysis"]
    for kw in keywords:
        if kw in title:
            base_tags.append(kw)
    return base_tags[:5]


def get_why_it_matters(slug):
    matters = {
        "freelancer-evidence": "Each client interaction becomes part of a searchable evidence trail — scope changes, approvals, and payment records tied to source audio.",
        "family-history": "Family recordings become preserved, searchable oral histories — stories that would otherwise fade with time.",
        "postmortem-atlas": "Incident reviews become a searchable knowledge base — patterns emerge across postmortems that prevent repeated failures.",
        "explain-repo": "Code walkthroughs become structured architecture docs — new team members onboard faster from real explanations.",
        "grant-evidence": "Community stories become grant-ready evidence bundles — impact narratives tied to real interviews and data.",
        "legal-prep": "Legal research becomes organized case prep — relevant precedents and procedures extracted and structured.",
        "micro-consulting": "Advisory sessions become reusable consulting assets — frameworks, objection patterns, and client strategies searchable.",
        "async-standup": "Team updates become a searchable standup archive — blockers, decisions, and progress tracked across time.",
        "competitive-intel": "Market research becomes a structured intelligence database — competitor signals and trends searchable over time.",
        "sales-distiller": "Sales conversations become training material — objection patterns, discovery techniques, and closing strategies extracted.",
        "procurement-memory": "Vendor evaluations become institutional memory — scoring criteria, contract terms, and supplier performance tracked.",
        "museum-exhibit": "Audio tours and oral histories become structured exhibit content — heritage preserved in searchable, publishable form.",
        "local-archive": "Community voices become a permanent local archive — neighborhood stories preserved, searchable, and accessible.",
    }
    return matters.get(slug, "Each source becomes part of a searchable, provenance-traced collection.")


def get_search_intro(slug):
    return f"Search across the {slug.replace('-', ' ')} records to see how Bonfyre connects related content from different sources."


def get_outputs(slug):
    output_map = {
        "freelancer-evidence": ["evidence brief", "clean transcript", "proof bundle", "search across records"],
        "family-history": ["oral history brief", "clean transcript", "proof bundle", "search across stories"],
        "postmortem-atlas": ["postmortem brief", "clean transcript", "proof bundle", "search across incidents"],
        "explain-repo": ["architecture brief", "clean transcript", "proof bundle", "search across repos"],
        "grant-evidence": ["impact brief", "clean transcript", "proof bundle", "search across stories"],
        "legal-prep": ["case prep brief", "clean transcript", "proof bundle", "search across cases"],
        "micro-consulting": ["session brief", "clean transcript", "proof bundle", "search across sessions"],
        "async-standup": ["standup brief", "clean transcript", "proof bundle", "search across standups"],
        "competitive-intel": ["intel brief", "clean transcript", "proof bundle", "search across signals"],
        "sales-distiller": ["call brief", "clean transcript", "proof bundle", "search across calls"],
        "procurement-memory": ["vendor brief", "clean transcript", "proof bundle", "search across vendors"],
        "museum-exhibit": ["exhibit brief", "clean transcript", "proof bundle", "search across exhibits"],
        "local-archive": ["archive brief", "clean transcript", "proof bundle", "search across archives"],
    }
    return output_map.get(slug, ["brief", "clean transcript", "proof bundle"])


def get_output_notes(slug):
    first = get_outputs(slug)[0]
    return {
        first: f"Bonfyre extracted the key points from the source audio into a structured {first}.",
        "clean transcript": "The raw transcript cleaned of filler words, false starts, and artifacts.",
        "proof bundle": "Full provenance chain: source URL, transcription confidence, processing recipe, and quality flags."
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: gen_demo_items.py <app-slug>")
        sys.exit(1)
    main()
