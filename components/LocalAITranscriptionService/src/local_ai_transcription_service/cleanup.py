import re
from typing import Dict


# ── Filler words (spoken disfluencies) ──
FILLER_RE = re.compile(r"\b(?:um|uh|erm|ah|hmm|hm|mm|mhm|uh-huh)\b", re.IGNORECASE)

# ── Whisper hallucination artifacts ──
# Whisper base model hallucinates these during silence, music, or low-signal audio.
HALLUCINATION_PATTERNS = [
    re.compile(r"(?:Thank you(?:\s*\.)?(?:\s+|$)){3,}", re.IGNORECASE),
    re.compile(r"(?:Thanks for watching(?:\s*\.)?(?:\s+|$)){2,}", re.IGNORECASE),
    re.compile(r"(?:Please subscribe(?:\s*\.)?(?:\s+|$)){2,}", re.IGNORECASE),
    re.compile(r"(?:\.\.\.)+"),  # Whisper sometimes emits long ellipsis chains
    re.compile(r"(?:you\s*){5,}", re.IGNORECASE),  # "you you you you you..."
    re.compile(r"\b(\w{2,})\s+(?:\1\s+){2,}\1\b", re.IGNORECASE),  # Any word repeated 4+ times
]

# ── Repeated phrases (Whisper loop artifacts) ──
# Catches the same phrase repeated back-to-back 3+ times.
REPEATED_PHRASE_RE = re.compile(r"\b(.{10,80}?)\s*(?:\1\s*){2,}", re.IGNORECASE)

# ── Whitespace / punctuation normalization ──
SPACE_RE = re.compile(r"\s+")
REPEATED_PUNCT_RE = re.compile(r"([,.!?]){2,}")
SPACE_BEFORE_PUNCT_RE = re.compile(r"\s+([,.!?])")

# ── Sentence fragments at the very start/end (Whisper often clips mid-word) ──
LEADING_FRAGMENT_RE = re.compile(r"^[a-z][a-z,]*\s+")  # Starts with lowercase fragment
TRAILING_FRAGMENT_RE = re.compile(r"\s+[A-Za-z]{1,3}$")  # Ends with a tiny orphan word


def clean_transcript_text(text: str) -> Dict[str, object]:
    raw_text = text.strip()
    if not raw_text:
        return {
            "cleaned_text": "",
            "changed": False,
            "filler_tokens_removed": 0,
            "hallucinations_removed": 0,
            "repeated_phrases_removed": 0,
        }

    # 1. Count and remove filler words
    filler_tokens_removed = len(FILLER_RE.findall(raw_text))
    cleaned_text = FILLER_RE.sub("", raw_text)

    # 2. Remove Whisper hallucination artifacts
    hallucinations_removed = 0
    for pattern in HALLUCINATION_PATTERNS:
        matches = pattern.findall(cleaned_text)
        hallucinations_removed += len(matches)
        cleaned_text = pattern.sub("", cleaned_text)

    # 3. Remove repeated phrases (Whisper loop detection)
    repeated_phrases_removed = 0
    repeated_match = REPEATED_PHRASE_RE.search(cleaned_text)
    while repeated_match:
        repeated_phrases_removed += 1
        # Keep one instance of the repeated phrase
        cleaned_text = REPEATED_PHRASE_RE.sub(r"\1", cleaned_text, count=1)
        repeated_match = REPEATED_PHRASE_RE.search(cleaned_text)

    # 4. Normalize whitespace and punctuation
    cleaned_text = SPACE_RE.sub(" ", cleaned_text)
    cleaned_text = SPACE_BEFORE_PUNCT_RE.sub(r"\1", cleaned_text)
    cleaned_text = REPEATED_PUNCT_RE.sub(r"\1", cleaned_text)

    # 5. Fix leading sentence fragments (a lone word or two before the first real sentence)
    cleaned_text = cleaned_text.strip()
    if cleaned_text and not cleaned_text[0].isupper() and len(cleaned_text) > 50:
        # Only strip if the first "sentence" is very short (< 4 words) and looks like a fragment
        first_period = cleaned_text.find(". ")
        if 0 < first_period < 20:
            fragment = cleaned_text[:first_period]
            if len(fragment.split()) <= 2:
                cleaned_text = cleaned_text[first_period + 2:]

    cleaned_text = cleaned_text.strip()

    return {
        "cleaned_text": cleaned_text,
        "changed": cleaned_text != raw_text,
        "filler_tokens_removed": filler_tokens_removed,
        "hallucinations_removed": hallucinations_removed,
        "repeated_phrases_removed": repeated_phrases_removed,
    }
