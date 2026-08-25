"""Transcript Paragraphizer — split cleaned transcript into readable paragraphs."""

import argparse
import re
import os


# Approximate target paragraph size (in sentences)
TARGET_SENTENCES = 4
# Maximum sentences before forcing a break
MAX_SENTENCES = 7


def split_sentences(text):
    """Split text into sentences."""
    # Split on sentence-ending punctuation followed by space or end
    parts = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in parts if s.strip()]


def detect_topic_shift(prev_sentences, next_sentence):
    """Simple heuristic: detect topic shifts via transitional phrases."""
    transitions = [
        r'^(so|now|anyway|moving on|next|also|another|meanwhile|however|but|on the other hand)',
        r'^(let me|let\'s|I want to|we should|the next|going back)',
        r'^(okay|alright|right)',
    ]
    for pattern in transitions:
        if re.match(pattern, next_sentence, re.IGNORECASE):
            return True
    return False


def paragraphize(text, with_headers=False):
    """Split text into paragraphs with optional section headers."""
    sentences = split_sentences(text)
    if not sentences:
        return text

    paragraphs = []
    current = []

    for i, sentence in enumerate(sentences):
        is_shift = len(current) >= TARGET_SENTENCES and detect_topic_shift(current, sentence)
        is_max = len(current) >= MAX_SENTENCES

        if current and (is_shift or is_max):
            paragraphs.append(' '.join(current))
            current = []

        current.append(sentence)

    if current:
        paragraphs.append(' '.join(current))

    if with_headers:
        output_parts = []
        for i, para in enumerate(paragraphs):
            # Generate a simple header from the first few words
            words = para.split()[:5]
            header = ' '.join(words).rstrip('.,;:')
            if i == 0:
                output_parts.append(f"## Opening\n\n{para}")
            else:
                output_parts.append(f"## {header}...\n\n{para}")
        return '\n\n'.join(output_parts)
    else:
        return '\n\n'.join(paragraphs)


def main():
    parser = argparse.ArgumentParser(description="Transcript Paragraphizer")
    parser.add_argument("input", help="Cleaned transcript text file")
    parser.add_argument("--output", "-o", help="Output file")
    parser.add_argument("--with-headers", action="store_true", help="Add section headers")
    args = parser.parse_args()

    with open(args.input) as f:
        text = f.read()

    result = paragraphize(text, args.with_headers)
    para_count = len([p for p in result.split('\n\n') if p.strip()])

    if args.output:
        with open(args.output, "w") as f:
            f.write(result)
        print(f"Paragraphed: {para_count} paragraphs")
        print(f"Written: {args.output}")
    else:
        print(result)


if __name__ == "__main__":
    main()
