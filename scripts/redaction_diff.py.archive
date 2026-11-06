#!/usr/bin/env python3
"""
redaction_diff.py — byte-level redaction comparison between two PDF releases

Given two PDFs that are believed to be different releases of the same document,
this script surfaces every newly unredacted line: text present in RELEASE_B
but absent (or covered by [REDACTED]/█ markers) in RELEASE_A.

Usage:
    python3 redaction_diff.py release_a.pdf release_b.pdf [--out report.md]

Requirements:
    pip install pdfminer.six PyMuPDF

Outputs to stdout (or --out file) a Markdown report with:
  - Per-page diff summary
  - Every newly unredacted span with surrounding context
  - Hash of both input files + timestamp
"""

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── optional imports with user-friendly errors ───────────────────────────────
try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit("pip install PyMuPDF")

# ── redaction markers ─────────────────────────────────────────────────────────
# Patterns that indicate a passage was redacted in the older release
REDACTED_RE = re.compile(
    r"(\[REDACTED\]"
    r"|\[[\w\s]*EXEMPT[\w\s]*\]"
    r"|\bREDACTED\b"
    r"|\bEXEMPT\b"
    r"|█+"            # filled black boxes (common in FBI/DOJ PDFs)
    r"|\u2588+"       # full-block Unicode
    r")",
    re.IGNORECASE,
)

# Characters to normalise when comparing spans (multiple spaces, newlines)
_WS_RE = re.compile(r"\s+")


def normalise(s: str) -> str:
    return _WS_RE.sub(" ", s).strip()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_pages(path: Path) -> list[list[str]]:
    """Return list-of-pages, each page = list of normalised text lines."""
    doc = fitz.open(str(path))
    pages = []
    for page in doc:
        raw = page.get_text("text")
        # split into non-empty lines
        lines = [normalise(ln) for ln in raw.splitlines() if normalise(ln)]
        pages.append(lines)
    doc.close()
    return pages


def is_redacted_line(line: str) -> bool:
    """True when the line is almost entirely a redaction marker or blank."""
    cleaned = REDACTED_RE.sub("", line).strip()
    # if stripping markers leaves < 5 non-whitespace chars, treat as redacted
    return len(re.sub(r"\s", "", cleaned)) < 5


def diff_pages(
    pages_a: list[list[str]],
    pages_b: list[list[str]],
) -> list[dict]:
    """
    For each page that exists in B, find lines in B that are not in A.
    Returns a list of hit dicts:
      { page: int, line_b: int, text: str, context_before: str, context_after: str }
    """
    hits = []
    n_pages = max(len(pages_a), len(pages_b))

    for pg_idx in range(n_pages):
        lines_a = set(pages_a[pg_idx]) if pg_idx < len(pages_a) else set()
        lines_b = pages_b[pg_idx] if pg_idx < len(pages_b) else []

        for ln_idx, line in enumerate(lines_b):
            if not line:
                continue
            # The line is "new" if it wasn't in A at all, OR
            # if A had a redacted placeholder approximately where this line is
            already_in_a = line in lines_a
            if already_in_a:
                continue

            # Check: does A have a redaction marker at roughly this position?
            a_lines = pages_a[pg_idx] if pg_idx < len(pages_a) else []
            nearby_a = a_lines[max(0, ln_idx - 2): ln_idx + 3]
            was_redacted = any(is_redacted_line(ln) for ln in nearby_a)

            hits.append(
                {
                    "page": pg_idx + 1,
                    "line_b": ln_idx + 1,
                    "text": line,
                    "was_redacted": was_redacted,
                    "context_before": lines_b[ln_idx - 1] if ln_idx > 0 else "",
                    "context_after": lines_b[ln_idx + 1] if ln_idx + 1 < len(lines_b) else "",
                }
            )
    return hits


def render_report(
    path_a: Path,
    path_b: Path,
    hits: list[dict],
    sha_a: str,
    sha_b: str,
) -> str:
    timestamp = datetime.now(timezone.utc).isoformat()
    lines: list[str] = []

    lines += [
        "# Redaction Diff Report",
        "",
        f"**Generated**: {timestamp}",
        "",
        "## Files",
        f"| | Path | SHA-256 |",
        f"|---|---|---|",
        f"| A (older) | `{path_a}` | `{sha_a[:16]}…` |",
        f"| B (newer) | `{path_b}` | `{sha_b[:16]}…` |",
        "",
        f"## Summary",
        f"- Total newly visible spans: **{len(hits)}**",
        f"- Spans replacing known redaction markers: **{sum(1 for h in hits if h['was_redacted'])}**",
        f"- New additions (no prior marker): **{sum(1 for h in hits if not h['was_redacted'])}**",
        "",
        "---",
        "",
        "## Newly Unredacted Content",
        "",
    ]

    current_page = None
    for hit in sorted(hits, key=lambda h: (h["page"], h["line_b"])):
        if hit["page"] != current_page:
            current_page = hit["page"]
            lines += [f"### Page {current_page}", ""]

        tag = "⚠️ was-redacted" if hit["was_redacted"] else "➕ new-addition"
        lines += [
            f"**Line {hit['line_b']}** — {tag}",
            "",
        ]
        if hit["context_before"]:
            lines += [f"> …{hit['context_before']}", ""]
        lines += [
            f"**`{hit['text']}`**",
            "",
        ]
        if hit["context_after"]:
            lines += [f"> {hit['context_after']}…", ""]
        lines.append("")

    if not hits:
        lines.append("_No newly unredacted content detected._")

    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Surface newly unredacted text between two PDF releases")
    ap.add_argument("release_a", help="Older / more-redacted PDF")
    ap.add_argument("release_b", help="Newer / less-redacted PDF")
    ap.add_argument("--out", default="-", help="Output path (- = stdout)")
    args = ap.parse_args()

    path_a = Path(args.release_a)
    path_b = Path(args.release_b)

    for p in (path_a, path_b):
        if not p.exists():
            sys.exit(f"File not found: {p}")

    print(f"Hashing {path_a.name}…", file=sys.stderr)
    sha_a = sha256_file(path_a)
    print(f"Hashing {path_b.name}…", file=sys.stderr)
    sha_b = sha256_file(path_b)

    print(f"Extracting text from {path_a.name}…", file=sys.stderr)
    pages_a = extract_pages(path_a)
    print(f"Extracting text from {path_b.name}…", file=sys.stderr)
    pages_b = extract_pages(path_b)

    print("Diffing…", file=sys.stderr)
    hits = diff_pages(pages_a, pages_b)
    print(f"Found {len(hits)} new spans.", file=sys.stderr)

    report = render_report(path_a, path_b, hits, sha_a, sha_b)

    if args.out == "-":
        print(report)
    else:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"Report written to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
