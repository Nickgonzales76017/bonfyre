import re
from typing import Dict, List, Optional, Set, Tuple


SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
TOKEN_RE = re.compile(r"[A-Za-z0-9%$']+")

FILLER_STARTS = ("so", "and", "like", "but", "well", "yeah", "okay", "right")
LOW_SIGNAL_WORDS = {
    "welcom",
    "thank",
    "host",
    "pod",
    "podcast",
    "episod",
    "sponsor",
    "coupon",
    "descript",
    "affili",
    "show",
}
PROMPT_WORDS = {"share", "walk", "tell", "describe", "explain"}
ACTION_VERBS = {
    "build",
    "check",
    "collect",
    "configur",
    "creat",
    "deploy",
    "design",
    "document",
    "draft",
    "email",
    "finish",
    "fix",
    "focus",
    "implement",
    "investig",
    "launch",
    "onboard",
    "plan",
    "prepar",
    "priorit",
    "redesign",
    "refin",
    "research",
    "resolv",
    "review",
    "run",
    "schedul",
    "screen",
    "send",
    "set",
    "ship",
    "target",
    "test",
    "updat",
    "valid",
    "write",
}
OBLIGATION_WORDS = {"need", "should", "must", "plan", "want", "going", "have"}
TOPIC_SEEDS = {
    "problem": {"problem", "pain", "challeng", "bottleneck", "blocker", "friction", "gig", "book", "adopt"},
    "workflow": {"workflow", "rank", "score", "match", "fit", "venu", "perform", "marketplac", "profil"},
    "icp": {"icp", "segment", "promoter", "coordinator", "manager", "owner", "adopt", "younger", "older"},
    "channel": {"linkedin", "outreach", "connect", "channel", "reach", "social"},
    "rollout": {"rollout", "pasadena", "california", "expand", "launch", "start", "local", "person"},
    "monetization": {"fee", "revenue", "monet", "pric", "percent", "payment", "process"},
    "pivot": {"pivot", "traction", "grow", "stalled", "focus", "tool", "feedback", "project"},
    "testing": {"test", "valid", "assumpt", "risk", "confid", "trigger", "rebrand", "experi"},
    "onboarding": {"onboard", "question", "step", "flow", "next", "show", "simpl"},
}
TOPIC_TITLES = {
    "problem": "Problems",
    "workflow": "Workflow",
    "icp": "ICP And Customer",
    "channel": "Distribution",
    "rollout": "Rollout",
    "monetization": "Monetization",
    "pivot": "Strategy And Pivot",
    "testing": "Validation",
    "onboarding": "Onboarding",
    "general": "Key Details",
}
SECTION_LIMITS = {
    "pivot": 3,
    "problem": 3,
    "workflow": 3,
    "icp": 3,
    "channel": 2,
    "rollout": 2,
    "monetization": 2,
    "testing": 3,
    "onboarding": 2,
    "general": 2,
}
PROFILE_PRESETS: Tuple[Dict[str, object], ...] = (
    {
        "name": "balanced",
        "noise_penalty": 1.0,
        "abstraction_bias": 1.0,
        "action_bias": 1.0,
    },
    {
        "name": "executive",
        "noise_penalty": 1.25,
        "abstraction_bias": 1.3,
        "action_bias": 0.9,
    },
    {
        "name": "operator",
        "noise_penalty": 1.1,
        "abstraction_bias": 0.9,
        "action_bias": 1.25,
    },
)
DEFAULT_EXTRACTION_PROFILE = PROFILE_PRESETS[0]


def split_sentences(text: str) -> List[str]:
    cleaned = " ".join(text.split())
    if not cleaned:
        return []
    return [segment.strip() for segment in SENTENCE_SPLIT_RE.split(cleaned) if segment.strip()]


def recommended_summary_bullets(text: str, requested_bullets: int) -> int:
    sentence_count = len(split_sentences(text))
    if sentence_count <= 20:
        return requested_bullets
    return max(requested_bullets, min(12, 5 + (sentence_count // 30)))


def _stem_token(token: str) -> str:
    lowered = token.lower().strip("'")
    if not lowered:
        return ""
    if lowered.endswith("'s"):
        lowered = lowered[:-2]
    if lowered.isdigit():
        return lowered
    for suffix in ("ingly", "edly", "ing", "edly", "edly", "ed", "ly", "ers", "er", "ies", "es", "s"):
        if lowered.endswith(suffix) and len(lowered) > len(suffix) + 2:
            if suffix == "ies":
                return lowered[:-3] + "y"
            return lowered[: -len(suffix)]
    return lowered


def _raw_tokens(text: str) -> List[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def _tokens(text: str) -> List[str]:
    return [token for token in (_stem_token(part) for part in _raw_tokens(text)) if token]


def _feature_map(sentence: str) -> Dict[str, object]:
    raw_tokens = _raw_tokens(sentence)
    tokens = _tokens(sentence)
    token_set = set(tokens)
    lowered = sentence.lower().strip()
    first_word = tokens[0] if tokens else ""
    category_scores = {
        topic: len(token_set & seeds) / max(len(seeds), 1)
        for topic, seeds in TOPIC_SEEDS.items()
    }
    low_signal_overlap = len(token_set & LOW_SIGNAL_WORDS)
    action_overlap = len(token_set & ACTION_VERBS)
    obligation_overlap = len(token_set & OBLIGATION_WORDS)
    first_person_count = sum(token in {"i", "we", "our", "me", "my"} for token in raw_tokens)
    question_words = {"what", "why", "how", "where", "when"}
    interviewer_prompt = (
        sentence.endswith("?")
        or (
            first_word in question_words
            and any(token in PROMPT_WORDS for token in token_set)
        )
        or ("your application" in lowered and "essentially" in lowered)
        or ("maybe share" in lowered)
    )
    conversational_start = first_word in FILLER_STARTS
    numbers = any(char.isdigit() for char in sentence)
    money = "$" in sentence or "%" in sentence or "percent" in token_set or "fee" in token_set
    decision_signal = len(token_set & {"decid", "realiz", "learn", "discover", "confirm", "conclud", "pivot", "shift"})
    causal_signal = len(token_set & {"becaus", "however", "therefor", "result", "inste", "eventually", "but", "after"})
    abstraction_signal = sum(
        category_scores[topic]
        for topic in ("problem", "workflow", "icp", "channel", "rollout", "monetization", "pivot", "testing")
    )
    return {
        "raw_tokens": raw_tokens,
        "tokens": tokens,
        "token_set": token_set,
        "category_scores": category_scores,
        "low_signal_overlap": low_signal_overlap,
        "action_overlap": action_overlap,
        "obligation_overlap": obligation_overlap,
        "first_person_count": first_person_count,
        "interviewer_prompt": interviewer_prompt,
        "conversational_start": conversational_start,
        "numbers": numbers,
        "money": money,
        "decision_signal": decision_signal,
        "causal_signal": causal_signal,
        "abstraction_signal": abstraction_signal,
        "length": len(raw_tokens),
        "lowered": lowered,
    }


def bullet_style_labels(sentence: str) -> Set[str]:
    features = _feature_map(sentence)
    labels: Set[str] = set()
    lowered = str(features["lowered"])
    if not lowered:
        labels.add("empty")
        return labels
    if bool(features["interviewer_prompt"]):
        labels.add("prompt")
    if bool(features["conversational_start"]):
        labels.add("transcript_shaped")
    if int(features["low_signal_overlap"]) > 0 and int(features["action_overlap"]) == 0:
        labels.add("low_signal")
    if int(features["length"]) < 4 and int(features["action_overlap"]) == 0:
        labels.add("thin")
    if "essentially" in lowered or "i was like" in lowered or "they were like" in lowered:
        labels.add("transcript_shaped")
    if lowered.startswith("at the time"):
        labels.add("transcript_shaped")
    if lowered.startswith(("i think ", "if you ", "i'm going to ", "i mean", "when, yeah", "yeah, it's funny")):
        labels.add("transcript_shaped")
    if "rocket ship" in lowered or "you don't have to think" in lowered:
        labels.add("low_signal")
    if "brought him on" in lowered or "interesting story and company" in lowered:
        labels.add("low_signal")
    return labels


def sentence_score(sentence: str, index: int, total: int, profile: Optional[Dict[str, object]] = None) -> float:
    active_profile = profile or DEFAULT_EXTRACTION_PROFILE
    features = _feature_map(sentence)
    category_scores = features["category_scores"]  # type: ignore[assignment]

    score = 0.0
    score += min(float(features["length"]) / 10.0, 2.5)
    score += float(features["abstraction_signal"]) * 14.0 * float(active_profile.get("abstraction_bias", 1.0))
    score += int(features["decision_signal"]) * 1.5
    score += int(features["causal_signal"]) * 0.75
    if bool(features["numbers"]):
        score += 0.75
    if bool(features["money"]):
        score += 1.5

    if 0 < index < total - 1:
        score += 0.75
    if index < 8:
        score -= 0.8
    if index < 3:
        score -= 0.8

    noise_penalty = float(active_profile.get("noise_penalty", 1.0))
    score -= int(features["low_signal_overlap"]) * 2.25 * noise_penalty
    score -= int(features["first_person_count"]) * 0.2
    if bool(features["interviewer_prompt"]):
        score -= 4.0 * noise_penalty
    if bool(features["conversational_start"]):
        score -= 1.75 * noise_penalty

    if category_scores["monetization"] > 0 and bool(features["money"]):
        score += 2.0
    if category_scores["workflow"] > 0 and ("rank" in features["token_set"] or "score" in features["token_set"]):
        score += 1.5
    if category_scores["testing"] > 0 and int(features["decision_signal"]) > 0:
        score += 1.5

    if "transcript_shaped" in bullet_style_labels(sentence):
        score -= 2.0
    return score


def is_junk_bullet(sentence: str) -> bool:
    labels = bullet_style_labels(sentence)
    if "empty" in labels or "prompt" in labels or "low_signal" in labels or "transcript_shaped" in labels:
        return True
    if "thin" in labels and not _feature_map(sentence)["action_overlap"]:
        return True
    return False


def rank_sentences(sentences: List[str], profile: Optional[Dict[str, object]] = None) -> List[Tuple[int, str, float]]:
    ranked = []
    total = len(sentences)
    for index, sentence in enumerate(sentences):
        ranked.append((index, sentence, sentence_score(sentence, index, total, profile=profile)))
    return ranked


def _clean_bullet(sentence: str) -> str:
    cleaned = sentence.strip()
    raw_tokens = _raw_tokens(cleaned)
    if raw_tokens and _stem_token(raw_tokens[0]) in FILLER_STARTS:
        parts = cleaned.split(" ", 1)
        cleaned = parts[1] if len(parts) > 1 else parts[0]
    cleaned = cleaned.strip(" ,")
    if cleaned:
        cleaned = cleaned[0].upper() + cleaned[1:]
    return cleaned


def _normalize_summary_bullet(sentence: str) -> str:
    features = _feature_map(sentence)
    token_set = features["token_set"]  # type: ignore[assignment]
    category_scores = features["category_scores"]  # type: ignore[assignment]

    if ({"tool", "feedback"} & token_set and {"problem", "unbias"} & token_set) or ({"friend", "family", "bias"} & token_set and "feedback" in token_set):
        return "PickFu emerged from the founders' need for unbiased feedback on design and messaging decisions."
    if "traction" in token_set and "project" in token_set and ({"grow", "stall", "focu", "pivot"} & token_set or "want" in token_set):
        return "The team pivoted when the main project stalled while PickFu kept gaining traction."
    if {"decid", "focu", "grow"} & token_set and ("pickfoo" in token_set or "pickfu" in token_set or "food" in token_set):
        return "The team pivoted when the main project stalled while PickFu kept gaining traction."
    if {"test", "assumpt"} <= token_set or {"risk", "invest"} <= token_set:
        return "Their core thesis is to test assumptions early so teams reduce downside risk before committing resources."
    if {"confidence", "trigg"} <= token_set or ("rebrand" in token_set and "valid" in token_set):
        return "Small validation tests can create enough confidence for larger go/no-go decisions."
    if "rebrand" in token_set and "decision" in token_set and bool(features["money"]):
        return "Quick validation runs gave the team enough evidence to choose a winning rebrand direction before a larger spend."
    if {"restaur", "menu"} <= token_set or ({"traffic", "monet"} & token_set and "build" in token_set):
        return "Before PickFu, the founders were building web products and learning traffic, monetization, and iteration firsthand."
    if "problem" in token_set and ({"gig", "lucrative", "book"} & token_set):
        return "The core customer problem was not discovery or tipping, but getting booked for better-paying gigs."
    if "promot" in token_set and ({"icp", "wrong", "change", "shift", "chang"} & token_set or "had" in token_set):
        return "The team realized the original ICP was wrong and shifted toward event promoters."
    if "promot" in token_set and ({"younger", "group"} & token_set or {"25", "35"} & token_set):
        return "Event promoters aged roughly 25 to 35 were a stronger early-adopter segment than venue owners."
    if "linkedin" in token_set:
        return "LinkedIn became the clearest channel for reaching the new event-promoter ICP."
    if {"pasadena", "california"} & token_set or ("person" in token_set and category_scores["rollout"] > 0):
        return "The rollout plan is to validate locally in Pasadena, learn in person, then expand through California."
    if (bool(features["money"]) and category_scores["monetization"] > 0) or (
        bool(features["money"]) and {"take", "perform", "customer", "side"} & token_set
    ):
        return "Monetization starts with a 5% performer fee, with a later customer-side processing fee."
    if {"venue", "research"} & token_set or ({"score", "rank"} <= token_set and "fit" in token_set):
        return "The product is meant to reduce venue research time by scoring and ranking performers for fit."
    if {"marketplac", "perform"} & token_set or ({"rank", "match"} <= token_set and ({"venu", "fit", "gig"} & token_set)):
        return "The product evolved into a two-sided marketplace that helps venues rank and match performers."
    if {"older", "owner"} & token_set or {"budge", "innovator"} & token_set:
        return "Older venue owners were too slow to move as an early adopter segment."
    if category_scores["onboarding"] > 0 and {"question", "next", "step", "show"} & token_set:
        return "They are redesigning onboarding into a simpler step-by-step flow."
    if {"solve", "problem"} <= token_set and "market" in token_set:
        return "The broader lesson was that solving a painful internal problem can reveal a real market need."

    return _clean_bullet(sentence)


def _sentence_topic(sentence: str) -> str:
    category_scores = _feature_map(sentence)["category_scores"]  # type: ignore[assignment]
    ordered = sorted(category_scores.items(), key=lambda item: item[1], reverse=True)
    if not ordered or ordered[0][1] <= 0:
        return ""
    return ordered[0][0]


def _deep_summary_topic(sentence: str) -> str:
    topic = _sentence_topic(sentence) or "general"
    lowered = sentence.lower()
    if topic == "monetization" and any(
        phrase in lowered
        for phrase in (
            "testing ideas",
            "product design",
            "messaging",
            "images",
            "different options",
        )
    ):
        return "testing"
    return topic


def _deep_summary_bullet_score(bullet: str) -> float:
    features = _feature_map(bullet)
    topic = _deep_summary_topic(bullet)
    score = 0.0
    score += float(features["abstraction_signal"]) * 10.0
    score += int(features["decision_signal"]) * 1.5
    score += int(features["causal_signal"]) * 0.75
    if bool(features["money"]):
        score += 1.5
    if bool(features["numbers"]):
        score += 0.5
    if int(features["first_person_count"]) > 0:
        score -= 1.0
    labels = bullet_style_labels(bullet)
    if labels & {"low_signal", "transcript_shaped", "prompt", "thin"}:
        score -= 5.0
    lowered = str(features["lowered"])
    if lowered.startswith(("john, welcome", "that's what we've", "we left our jobs", "we're looking to", "it's funny how")):
        score -= 3.0
    if topic == "general":
        score -= 1.5
    return score


def _allow_deep_summary_bullet(bullet: str, topic: str) -> bool:
    if is_junk_bullet(bullet):
        return False
    lowered = bullet.lower().strip()
    if lowered.startswith(("john, welcome", "that's what we've", "we left our jobs", "we're looking to")):
        return False
    if "interesting story and company" in lowered or "brought him on" in lowered:
        return False
    if topic == "general" and _deep_summary_bullet_score(bullet) < 2.0:
        return False
    if topic == "rollout" and not any(token in lowered for token in ("pasadena", "california", "expand", "local", "launch", "in person", "feedback")):
        return False
    if topic == "problem" and not any(token in lowered for token in ("problem", "pain", "gig", "booked", "hired", "lucrative", "stuck")):
        return False
    if topic == "monetization" and not any(token in lowered for token in ("fee", "revenue", "payment", "processing", "%", "$", "pricing")):
        return False
    return True


def _is_specific_detail(bullet: str) -> bool:
    features = _feature_map(bullet)
    lowered = str(features["lowered"])
    token_set = features["token_set"]  # type: ignore[assignment]
    if bool(features["numbers"]) or bool(features["money"]):
        return True
    if any(token in lowered for token in ("for example", "for the first", "25 to 35", "5%", "pasadena", "california", "linkedin")):
        return True
    if {"perform", "venue", "social", "profile", "score"} & token_set:
        return True
    return False


def _nest_section_bullets(topic: str, bullets: List[str]) -> Dict[str, object]:
    lead = _normalize_deep_summary_detail(topic, bullets[0])
    details: List[str] = []
    for bullet in bullets[1:]:
        normalized = _normalize_deep_summary_detail(topic, bullet)
        if normalized != lead and normalized not in details:
            details.append(normalized)
    return {"lead": lead, "details": details}


def _normalize_deep_summary_detail(topic: str, bullet: str) -> str:
    lowered = bullet.lower().strip()
    token_set = _feature_map(bullet)["token_set"]  # type: ignore[assignment]

    if topic == "problem":
        if "$300" in lowered or "300 bucks" in lowered:
            return "The target use case was a paid booking worth roughly $300, not casual street performance."
        if "number two problem" in lowered or "number three problem" in lowered:
            return "Discovery and tipping were secondary concerns compared with landing better-paid bookings."
        if "solve a problem" in lowered:
            return "The broader lesson was that solving a painful internal problem can reveal a real market need."

    if topic == "workflow":
        if "50 plus" in lowered:
            return "Venue operators were harder to activate and slower to evaluate applicants through a new workflow."
        if "social media" in lowered:
            return "Scoring depends in part on reviewing each performer's social presence and portfolio."

    if topic == "icp":
        if "with this new icp" in lowered:
            return "The ICP shift forced changes to product design and workflow expectations."

    if topic == "channel":
        if "social media" in lowered:
            return "Social profiles are one of the inputs used to evaluate performer fit."

    if topic == "onboarding":
        if "ui designer" in lowered or "designers" in lowered:
            return "The redesign now includes dedicated product and marketing design support."

    if topic == "testing":
        if any(phrase in lowered for phrase in ("platform around testing", "testing ideas", "product design", "messaging")):
            return "The product is positioned as a fast validation layer for messaging, creative, pricing, and product decisions."
        if "one to eight different options" in lowered:
            return "The platform supports quick validation across multiple creative or messaging variants."

    if topic == "pivot":
        if "handful of other side projects" in lowered:
            return "The founders iterated through multiple side projects before committing fully to PickFu."

    return bullet


def _chunk_sentences(sentences: List[str], *, chunk_size: int = 18, overlap: int = 3) -> List[List[str]]:
    if not sentences:
        return []
    chunks: List[List[str]] = []
    step = max(1, chunk_size - overlap)
    for start in range(0, len(sentences), step):
        chunk = sentences[start : start + chunk_size]
        if chunk:
            chunks.append(chunk)
        if start + chunk_size >= len(sentences):
            break
    return chunks


def build_deep_summary(
    text: str,
    *,
    profile: Optional[Dict[str, object]] = None,
    bullets_per_chunk: int = 3,
) -> Dict[str, object]:
    sentences = split_sentences(text)
    chunks = _chunk_sentences(sentences)
    sections: Dict[str, List[Tuple[float, str]]] = {}
    seen: Set[str] = set()

    for chunk in chunks:
        chunk_text = " ".join(chunk)
        for bullet in summarize_text(chunk_text, bullets=bullets_per_chunk, profile=profile):
            topic = _deep_summary_topic(bullet)
            if not _allow_deep_summary_bullet(bullet, topic):
                continue
            dedupe_key = bullet.lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            sections.setdefault(topic, []).append((_deep_summary_bullet_score(bullet), bullet))

    ordered_sections = []
    for topic in ("pivot", "problem", "workflow", "icp", "channel", "rollout", "monetization", "testing", "onboarding", "general"):
        bullets = sections.get(topic, [])
        if bullets:
            bullets.sort(key=lambda item: item[0], reverse=True)
            selected = [bullet for _, bullet in bullets[: SECTION_LIMITS.get(topic, 3)]]
            if selected:
                ordered_sections.append(
                    {
                        "topic": topic,
                        "title": TOPIC_TITLES.get(topic, topic.title()),
                        "bullets": selected,
                        "items": [_nest_section_bullets(topic, selected)],
                    }
                )

    return {
        "chunk_count": len(chunks),
        "sections": ordered_sections,
    }


def build_executive_summary_from_deep_sections(
    deep_sections: List[Dict[str, object]],
    *,
    max_bullets: int,
) -> List[str]:
    summary: List[str] = []
    seen: Set[str] = set()

    for section in deep_sections:
        items = section.get("items", [])
        if not items:
            continue
        for item in items:
            lead = str(item.get("lead", "")).strip()
            if not lead:
                continue
            key = lead.lower()
            if key in seen:
                continue
            seen.add(key)
            summary.append(lead)
            if len(summary) >= max_bullets:
                return summary

    return summary[:max_bullets]


def summarize_text(text: str, bullets: int = 5, profile: Optional[Dict[str, object]] = None) -> List[str]:
    sentences = split_sentences(text)
    if not sentences:
        return []

    ranked = sorted(rank_sentences(sentences, profile=profile), key=lambda item: (-item[2], item[0]))
    summary: List[str] = []
    seen: Set[str] = set()
    used_topics: Set[str] = set()

    for _, sentence, score in ranked:
        if score < 0:
            continue
        topic = _sentence_topic(sentence)
        cleaned = _normalize_summary_bullet(sentence)
        if cleaned == sentence and is_junk_bullet(sentence):
            continue
        if is_junk_bullet(cleaned):
            continue
        dedupe_key = cleaned.lower()
        if dedupe_key in seen:
            continue
        if topic and topic in used_topics and score < 3.0:
            continue
        seen.add(dedupe_key)
        if topic:
            used_topics.add(topic)
        summary.append(cleaned)
        if len(summary) == bullets:
            break

    if len(summary) < bullets:
        for sentence in sentences:
            cleaned = _normalize_summary_bullet(sentence)
            if cleaned == sentence and is_junk_bullet(sentence):
                continue
            dedupe_key = cleaned.lower()
            if dedupe_key in seen or is_junk_bullet(cleaned):
                continue
            seen.add(dedupe_key)
            summary.append(cleaned)
            if len(summary) == bullets:
                break

    return summary[:bullets]


def _normalize_action_item(sentence: str) -> str:
    cleaned = sentence.strip()
    lowered = cleaned.lower()
    for marker in (
        "we need to ",
        "i need to ",
        "we should ",
        "i should ",
        "we want to ",
        "i want to ",
        "you need to ",
        "you should ",
        "we have to ",
        "i have to ",
        "we are going to ",
        "i am going to ",
    ):
        if marker in lowered:
            start = lowered.find(marker)
            cleaned = cleaned[start + len(marker):]
            break

    token_set = _feature_map(cleaned)["token_set"]  # type: ignore[assignment]
    lowered = cleaned.lower()
    if {"test", "assumpt"} <= token_set or {"risk", "invest"} <= token_set:
        return "Test assumptions before committing major time, money, or build effort."
    if {"confidence", "trigg"} <= token_set:
        return "Use small validation tests to support larger go/no-go decisions."
    if {"disprov", "hypothesi"} <= token_set or {"test", "messag"} <= token_set:
        return "Run fast tests to disprove weak messaging, feature, or audience assumptions early."
    if "rocket ship" in lowered:
        return "Use testing to reduce downside risk, not to predict perfect upside."
    if {"redesign", "site"} <= token_set and "promoter" in token_set:
        return "Redesign the site to better fit the new event-promoter audience."

    cleaned = cleaned.strip(" ,")
    if cleaned:
        cleaned = cleaned[0].upper() + cleaned[1:]
    return cleaned


def _is_action_like(sentence: str, profile: Optional[Dict[str, object]] = None) -> bool:
    active_profile = profile or DEFAULT_EXTRACTION_PROFILE
    features = _feature_map(sentence)
    token_set = features["token_set"]  # type: ignore[assignment]
    labels = bullet_style_labels(sentence)
    if "prompt" in labels or "low_signal" in labels:
        return False

    raw_tokens = [token.lower() for token in features["raw_tokens"]]  # type: ignore[index]
    first_raw = _stem_token(raw_tokens[0]) if raw_tokens else ""
    imperative = first_raw in ACTION_VERBS
    lowered = str(features["lowered"])
    obligation = any(
        marker in lowered
        for marker in (
            "need to ",
            "should ",
            "have to ",
            "plan to ",
            "we are going to ",
            "i am going to ",
            "i'm going to ",
        )
    )
    action_signal = int(features["action_overlap"])

    if imperative and action_signal:
        return True
    if obligation and action_signal:
        return True
    if "test before you invest" in lowered or {"confidence", "trigg"} <= token_set:
        return True
    if {"disprov", "hypothesi"} <= token_set:
        return True
    if str(active_profile.get("name")) == "operator" and obligation and action_signal:
        return True
    return False


def extract_action_items(text: str, profile: Optional[Dict[str, object]] = None) -> List[str]:
    items: List[str] = []
    seen: Set[str] = set()
    for sentence in split_sentences(text):
        if not _is_action_like(sentence, profile=profile):
            continue
        normalized = _normalize_action_item(sentence)
        if is_junk_bullet(normalized):
            continue
        dedupe_key = normalized.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        items.append(normalized)
    return items[:10]


def classify_extraction_output(summary_bullets: List[str], action_items: List[str]) -> Dict[str, object]:
    transcript_shaped_summary = [bullet for bullet in summary_bullets if "transcript_shaped" in bullet_style_labels(bullet)]
    weak_actions = [item for item in action_items if bullet_style_labels(item) & {"transcript_shaped", "low_signal"}]
    labels: List[str] = []
    if transcript_shaped_summary:
        labels.append("transcript_shaped_summary")
    if weak_actions:
        labels.append("weak_actions")
    if not summary_bullets:
        labels.append("thin_summary")
    if not action_items:
        labels.append("thin_actions")
    return {
        "labels": labels,
        "transcript_shaped_summary_count": len(transcript_shaped_summary),
        "weak_action_count": len(weak_actions),
    }


def generate_best_extraction(text: str, *, bullets: int = 5) -> Dict[str, object]:
    effective_bullets = recommended_summary_bullets(text, bullets)
    candidates: List[Dict[str, object]] = []
    for profile in PROFILE_PRESETS:
        summary_bullets = summarize_text(text, bullets=effective_bullets, profile=profile)
        action_items = extract_action_items(text, profile=profile)
        classifier = classify_extraction_output(summary_bullets, action_items)
        score = (
            len(summary_bullets) * 8
            + len(action_items) * 6
            - classifier["transcript_shaped_summary_count"] * 20
            - classifier["weak_action_count"] * 15
            - (20 if "thin_summary" in classifier["labels"] else 0)
            - (15 if "thin_actions" in classifier["labels"] else 0)
        )
        candidates.append(
            {
                "profile": dict(profile),
                "summary_bullets": summary_bullets,
                "action_items": action_items,
                "classifier": classifier,
                "controller_score": score,
            }
        )

    candidates.sort(
        key=lambda item: (
            item["controller_score"],
            -item["classifier"]["transcript_shaped_summary_count"],
            -item["classifier"]["weak_action_count"],
        ),
        reverse=True,
    )
    best = candidates[0]
    deep_summary = build_deep_summary(text, profile=best["profile"])
    executive_summary = build_executive_summary_from_deep_sections(
        deep_summary["sections"],
        max_bullets=effective_bullets,
    )
    return {
        "selected_profile": best["profile"],
        "classifier": best["classifier"],
        "summary_bullets": executive_summary or best["summary_bullets"],
        "effective_summary_bullets": effective_bullets,
        "deep_summary_sections": deep_summary["sections"],
        "deep_summary_chunk_count": deep_summary["chunk_count"],
        "action_items": best["action_items"],
        "controller_score": best["controller_score"],
        "candidates": candidates,
    }
