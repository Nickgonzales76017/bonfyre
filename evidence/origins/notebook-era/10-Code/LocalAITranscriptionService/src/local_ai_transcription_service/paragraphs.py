import re
from typing import Dict, List

from .summary import split_sentences


# ── Topic shift signals ──
# These patterns suggest a speaker turn or topic change.
TOPIC_SHIFT_RE = re.compile(
    r"(?:"
    r"^(?:so|now|okay|alright|moving on|next|anyway|let me|the other thing|another thing|on the other hand|speaking of|regarding)"
    r"|"
    r"(?:what about|how about|tell me about|let's talk about|can you explain)"
    r"|"
    r"(?:first|second|third|finally|in addition|also|furthermore|moreover)"
    r")",
    re.IGNORECASE,
)

QUESTION_RE = re.compile(r"\?$")


def build_transcript_paragraphs(
    text: str,
    *,
    min_sentences_per_paragraph: int = 2,
    max_sentences_per_paragraph: int = 5,
) -> Dict[str, object]:
    sentences = split_sentences(text)
    if not sentences:
        return {
            "paragraphs": [],
            "paragraph_count": 0,
            "strategy": "topic_shift",
        }

    paragraphs: List[str] = []
    current_group: List[str] = []

    for i, sentence in enumerate(sentences):
        current_group.append(sentence)

        # Decide whether to break here
        should_break = False

        if len(current_group) >= max_sentences_per_paragraph:
            should_break = True
        elif len(current_group) >= min_sentences_per_paragraph:
            # Break on topic shift signals in the NEXT sentence
            if i + 1 < len(sentences):
                next_sentence = sentences[i + 1]
                if TOPIC_SHIFT_RE.match(next_sentence):
                    should_break = True
                # Break after a question (likely an interviewer turn)
                if QUESTION_RE.search(sentence):
                    should_break = True
            else:
                # Last sentence — close the paragraph
                should_break = True

        if should_break:
            paragraphs.append(" ".join(current_group))
            current_group = []

    # Flush any remaining sentences
    if current_group:
        paragraphs.append(" ".join(current_group))

    return {
        "paragraphs": paragraphs,
        "paragraph_count": len(paragraphs),
        "strategy": "topic_shift",
    }
