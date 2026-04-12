"""
Ilonggo (Hiligaynon) Tokenizer
Splits text into tokens with metadata for rule matching.
"""
import re


class Token:
    def __init__(self, text, start, end, index):
        self.text = text
        self.lower = text.lower()
        self.start = start   # char offset in original text
        self.end = end
        self.index = index   # position in token list
        self.pos = None      # Part-of-speech tag (set by tagger)

    def __repr__(self):
        return f"Token({self.text!r}, pos={self.pos})"


ILONGGO_PARTICLES = {
    "ang", "sang", "mga", "sa", "ni", "kay", "kag",
    "nga", "kon", "kundi", "pero", "apang", "ukon",
    "tungod", "kay", "agud", "bisan", "pag", "kun"
}

ILONGGO_VERB_PREFIXES = ["mag-", "nag-", "gina-", "gin-", "i-", "ma-", "na-"]
ILONGGO_VERB_SUFFIXES = ["-on", "-an", "-han"]


def tokenize(text: str) -> list[Token]:
    """Tokenize Ilonggo text into a list of Token objects."""
    tokens = []
    # Match words (including hyphenated affixed forms) and punctuation separately
    pattern = re.compile(r"[A-Za-záéíóúÁÉÍÓÚñÑ][A-Za-záéíóúÁÉÍÓÚñÑ'-]*|[.,!?;:\"]")
    for i, m in enumerate(pattern.finditer(text)):
        tok = Token(m.group(), m.start(), m.end(), i)
        tok.pos = _basic_pos(tok.lower)
        tokens.append(tok)
    return tokens


def _basic_pos(word: str) -> str:
    """Very basic POS tagger for Ilonggo words."""
    if word in ILONGGO_PARTICLES:
        return "PARTICLE"

    # Verb detection by common affixes
    if re.match(r"^(mag|nag|gin|gina|ma|na)[a-z]", word):
        return "VERB"
    if word.endswith("on") and len(word) > 3:
        return "VERB"
    if word.endswith("an") and len(word) > 3:
        return "VERB_LOC"

    # Adjective-like words (common Ilonggo descriptors often end in these)
    if re.match(r"^(ma)[a-z]", word):
        return "ADJ"

    return "NOUN"


def get_sentence_tokens(text: str) -> list[list[Token]]:
    """Split text into sentences, return tokens per sentence."""
    # Simple sentence splitter on . ! ?
    sentence_texts = re.split(r"(?<=[.!?])\s+", text.strip())
    result = []
    offset = 0
    for sent in sentence_texts:
        if sent.strip():
            toks = tokenize(sent)
            result.append(toks)
        offset += len(sent) + 1
    return result
