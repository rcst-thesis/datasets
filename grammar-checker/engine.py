"""
Ilonggo Grammar Checking Engine
Runs all rules against tokenized text and returns structured matches.
"""

from .tokenizer import tokenize, get_sentence_tokens
from .rules import RULES, match_pattern


class GrammarMatch:
    def __init__(self, rule, tokens, span_start, span_end, sentence_offset=0):
        self.rule_id = rule["id"]
        self.rule_name = rule["name"]
        self.severity = rule["severity"]
        self.message = rule["message"]
        self.suggestions = rule.get("suggestions", [])
        self.description = rule.get("description", "")
        self.example_wrong = rule.get("example_wrong", "")
        self.example_right = rule.get("example_right", "")

        # Character offsets in the original text
        matched_tokens = tokens[span_start:span_end]
        self.char_start = matched_tokens[0].start + sentence_offset
        self.char_end = matched_tokens[-1].end + sentence_offset
        self.matched_text = " ".join(t.text for t in matched_tokens)

    def to_dict(self):
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "severity": self.severity,
            "message": self.message,
            "suggestions": self.suggestions,
            "description": self.description,
            "example_wrong": self.example_wrong,
            "example_right": self.example_right,
            "char_start": self.char_start,
            "char_end": self.char_end,
            "matched_text": self.matched_text,
        }


def check_text(text: str, enabled_rules: list[str] | None = None) -> list[dict]:
    """
    Run all (or selected) grammar rules against the given text.
    Returns a list of match dicts sorted by char_start.
    """
    active_rules = RULES
    if enabled_rules is not None:
        active_rules = [r for r in RULES if r["id"] in enabled_rules]

    matches = []
    sentences = get_sentence_tokens(text)

    # Track sentence char offsets
    sentence_offset = 0
    for sent_tokens in sentences:
        if not sent_tokens:
            continue

        for rule in active_rules:
            pattern = rule["pattern"]
            for i in range(len(sent_tokens)):
                matched, end_idx = match_pattern(sent_tokens, pattern, i)
                if matched:
                    m = GrammarMatch(rule, sent_tokens, i, end_idx, sentence_offset)
                    matches.append(m.to_dict())

        # Advance offset by length of sentence text + separator
        if sent_tokens:
            last = sent_tokens[-1]
            sentence_offset += last.end + 1  # approximate

    # Sort by position
    matches.sort(key=lambda m: m["char_start"])
    return matches


def get_stats(text: str) -> dict:
    """Return basic statistics about the text."""
    from .tokenizer import tokenize, ILONGGO_PARTICLES
    tokens = tokenize(text)
    words = [t for t in tokens if t.pos != "PUNCT"]
    particles = [t for t in tokens if t.lower in ILONGGO_PARTICLES]
    verbs = [t for t in tokens if t.pos in ("VERB", "VERB_LOC")]
    return {
        "total_words": len(words),
        "unique_words": len(set(t.lower for t in words)),
        "particles": len(particles),
        "verbs": len(verbs),
    }
