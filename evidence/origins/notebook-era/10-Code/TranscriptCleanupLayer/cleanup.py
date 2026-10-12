"""Transcript Cleanup Layer — remove filler, fix punctuation, improve readability."""

import argparse
import re
import os

# Filler patterns to remove
FILLER_PATTERNS = [
    r'\b(um|uh|hmm|mhm|uh-huh)\b',
    r'\b(you know)\b',
    r'\b(I mean)\b',
    r'\b(sort of|kind of)\b',
    r'\b(basically|literally|actually|obviously)\b',
]

# Stutter/repetition pattern
STUTTER_PATTERN = re.compile(r'\b(\w+)\s+\1\b', re.IGNORECASE)


def remove_filler(text, aggressive=False):
    """Remove filler words and verbal tics."""
    for pattern in FILLER_PATTERNS:
        flags = re.IGNORECASE
        text = re.sub(pattern, '', text, flags=flags)

    if aggressive:
        text = re.sub(r'\b(like,?\s)', '', text, flags=re.IGNORECASE)

    return text


def fix_stutters(text):
    """Remove repeated consecutive words."""
    return STUTTER_PATTERN.sub(r'\1', text)


def fix_punctuation(text):
    """Normalize punctuation issues."""
    # Multiple spaces → single space
    text = re.sub(r'  +', ' ', text)
    # Space before punctuation
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    # Missing space after punctuation
    text = re.sub(r'([.,!?;:])([A-Za-z])', r'\1 \2', text)
    # Multiple periods → single
    text = re.sub(r'\.{2,}', '.', text)
    # Capitalize after sentence-ending punctuation
    text = re.sub(r'([.!?])\s+([a-z])', lambda m: m.group(1) + ' ' + m.group(2).upper(), text)
    return text


def fix_whitespace(text):
    """Clean up excess whitespace."""
    lines = text.split('\n')
    cleaned = []
    prev_blank = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if not prev_blank:
                cleaned.append('')
            prev_blank = True
        else:
            cleaned.append(stripped)
            prev_blank = False
    return '\n'.join(cleaned).strip()


def cleanup(text, aggressive=False):
    """Run all cleanup passes on transcript text."""
    text = remove_filler(text, aggressive)
    text = fix_stutters(text)
    text = fix_punctuation(text)
    text = fix_whitespace(text)
    return text


def main():
    parser = argparse.ArgumentParser(description="Transcript Cleanup Layer")
    parser.add_argument("input", help="Transcript text file")
    parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    parser.add_argument("--aggressive", action="store_true", help="Remove more filler (including 'like')")
    args = parser.parse_args()

    with open(args.input) as f:
        text = f.read()

    original_words = len(text.split())
    cleaned = cleanup(text, args.aggressive)
    cleaned_words = len(cleaned.split())

    if args.output:
        with open(args.output, "w") as f:
            f.write(cleaned)
        removed = original_words - cleaned_words
        print(f"Cleaned: {original_words} → {cleaned_words} words ({removed} removed)")
        print(f"Written: {args.output}")
    else:
        print(cleaned)


if __name__ == "__main__":
    main()
