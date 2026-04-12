"""
Ilonggo (Hiligaynon) Grammar Rules
===================================
Rules follow a LanguageTool-inspired pattern:
  - pattern: list of dicts with 'text', 'regex', or 'pos' matchers
  - Each dict can have 'negate': True to invert the match
  - message: explanation shown to the user
  - suggestions: list of fix suggestions
  - severity: 'error' | 'warning' | 'info'

Extend this file to add your own community rules!
"""

import re


# ─── Matcher helpers ──────────────────────────────────────────────────────────

def _tok_matches(tok, spec: dict) -> bool:
    if "text" in spec:
        expected = spec["text"]
        if isinstance(expected, list):
            result = tok.lower in [e.lower() for e in expected]
        else:
            result = tok.lower == expected.lower()
    elif "regex" in spec:
        result = bool(re.search(spec["regex"], tok.text, re.IGNORECASE))
    elif "pos" in spec:
        result = tok.pos == spec["pos"]
    else:
        result = True  # wildcard

    return (not result) if spec.get("negate") else result


def match_pattern(tokens, pattern, start_idx):
    """Try to match a pattern starting at start_idx. Returns (matched, span_end)."""
    if start_idx + len(pattern) > len(tokens):
        return False, start_idx
    for i, spec in enumerate(pattern):
        if not _tok_matches(tokens[start_idx + i], spec):
            return False, start_idx
    return True, start_idx + len(pattern)


# ─── Rule definitions ─────────────────────────────────────────────────────────

RULES = [

    # ── 1. Double ANG in one clause ───────────────────────────────────────────
    {
        "id": "DOUBLE_ANG",
        "name": "Double 'ang' marker",
        "description": (
            "Each clause in Ilonggo should have only ONE 'ang'-marked noun phrase "
            "(the topic/subject). Two 'ang' markers in the same short span usually "
            "signals a grammar error."
        ),
        "severity": "error",
        "pattern": [
            {"text": "ang"},
            {"pos": "PARTICLE", "negate": True},
            {"text": "ang"},
        ],
        "message": "Two 'ang' markers detected close together. Only one topic marker is allowed per clause.",
        "suggestions": ["Replace the second 'ang' with 'sang' if it marks an object, or 'ni' for a personal name."],
        "example_wrong": "Nagkaon ang bata ang mansanas.",
        "example_right": "Nagkaon ang bata sang mansanas.",
    },

    # ── 2. 'at' instead of 'kag' ──────────────────────────────────────────────
    {
        "id": "AT_INSTEAD_OF_KAG",
        "name": "Tagalog 'at' used instead of Ilonggo 'kag'",
        "description": (
            "The Tagalog conjunction 'at' (and) is commonly code-switched into "
            "Ilonggo, but the proper Ilonggo/Hiligaynon word is 'kag'."
        ),
        "severity": "warning",
        "pattern": [
            {"text": "at"},
        ],
        "message": "'at' is Tagalog. The Ilonggo equivalent is 'kag'.",
        "suggestions": ["kag"],
        "example_wrong": "Si Juan at si Maria nagkanta.",
        "example_right": "Si Juan kag si Maria nagkanta.",
    },

    # ── 3. Actor-focus verb (mag-/nag-) + object marked with 'ang' ────────────
    {
        "id": "ACTOR_FOCUS_DOUBLE_ANG",
        "name": "Actor-focus verb with possible object mismarked as 'ang'",
        "description": (
            "When using an actor-focus verb (mag-/nag-), the ACTOR is marked with "
            "'ang'. The object/patient should be marked with 'sang'. "
            "If 'ang' appears twice, one of them is likely wrong."
        ),
        "severity": "error",
        "pattern": [
            {"regex": r"^(mag|nag)[a-z]+"},
            {"text": "ang"},
            {"pos": "NOUN"},
            {"text": "ang"},
        ],
        "message": "Actor-focus verb (mag-/nag-) with two 'ang' markers. The object should be marked 'sang', not 'ang'.",
        "suggestions": ["Change the second 'ang' to 'sang'."],
        "example_wrong": "Nagkaon ang bata ang tinapay.",
        "example_right": "Nagkaon ang bata sang tinapay.",
    },

    # ── 4. Object-focus verb (-on form) + subject marked with 'sang' ──────────
    {
        "id": "OBJ_FOCUS_SANG_TOPIC",
        "name": "Object-focus verb with topic marked as 'sang'",
        "description": (
            "Verbs ending in '-on' are in object focus — the OBJECT is the topic "
            "and must be marked with 'ang'. Using 'sang' for the object with an "
            "-on verb is a common mistake."
        ),
        "severity": "error",
        "pattern": [
            {"regex": r"[a-z]+on$"},
            {"text": "sang"},
        ],
        "message": "An object-focus verb (-on) should have its object marked with 'ang', not 'sang'.",
        "suggestions": ["Change 'sang' to 'ang' for the focused object."],
        "example_wrong": "Kaunon sang bata ang tinapay.",
        "example_right": "Kaunon sang bata ang tinapay.  ✓ (object 'tinapay' marked ang is correct)",
    },

    # ── 5. 'sa' used for a person's name (should be 'kay') ────────────────────
    {
        "id": "SA_FOR_PERSON",
        "name": "'sa' used for a personal name (use 'kay')",
        "description": (
            "In Ilonggo, 'sa' is a locative/dative marker for common nouns and places. "
            "For personal names, the dative marker is 'kay'. "
            "E.g., 'Naghatag siya kay Maria' (not 'sa Maria')."
        ),
        "severity": "warning",
        "pattern": [
            {"text": "sa"},
            {"regex": r"^[A-Z][a-z]+"},  # capitalized = proper name
        ],
        "message": "'sa' before a personal name may be incorrect. Use 'kay' for people.",
        "suggestions": ["kay"],
        "example_wrong": "Naghatag siya sa Maria sang regalo.",
        "example_right": "Naghatag siya kay Maria sang regalo.",
    },

    # ── 6. Missing 'nga' linker between adjective and noun ────────────────────
    {
        "id": "MISSING_NGA_LINKER",
        "name": "Possible missing 'nga' linker between adjective and noun",
        "description": (
            "In Ilonggo, adjectives are linked to the nouns they modify with 'nga'. "
            "E.g., 'matamis nga mangga' (sweet mango). "
            "Omitting 'nga' is a common learner error."
        ),
        "severity": "warning",
        "pattern": [
            {"regex": r"^ma[a-z]+"},  # ma- prefix = adjective
            {"pos": "NOUN"},          # directly followed by noun (no nga)
        ],
        "message": "Possible missing 'nga' linker between adjective and noun.",
        "suggestions": ["Insert 'nga' between the adjective and the noun."],
        "example_wrong": "Matamis mangga.",
        "example_right": "Matamis nga mangga.",
    },

    # ── 7. Tagalog 'ng' used instead of Ilonggo 'sang' / 'nga' ───────────────
    {
        "id": "TAGALOG_NG",
        "name": "Tagalog 'ng' used instead of Ilonggo 'sang' or 'nga'",
        "description": (
            "The Tagalog genitive/linker particle 'ng' does not exist in Ilonggo. "
            "Use 'sang' as the genitive marker for common nouns, or 'nga' as the linker."
        ),
        "severity": "error",
        "pattern": [
            {"text": "ng"},
        ],
        "message": "'ng' is a Tagalog particle and is not used in Ilonggo. Use 'sang' (genitive) or 'nga' (linker).",
        "suggestions": ["sang", "nga"],
        "example_wrong": "Nagkaon siya ng tinapay.",
        "example_right": "Nagkaon siya sang tinapay.",
    },

    # ── 8. Tagalog 'nang' used instead of Ilonggo 'sang' ─────────────────────
    {
        "id": "TAGALOG_NANG",
        "name": "Tagalog 'nang' used instead of Ilonggo equivalent",
        "description": (
            "'nang' is Tagalog. In Ilonggo, use 'sang' for object marking "
            "or 'nga' as a subordinator/linker."
        ),
        "severity": "error",
        "pattern": [
            {"text": "nang"},
        ],
        "message": "'nang' is Tagalog. Use 'sang' or 'nga' in Ilonggo.",
        "suggestions": ["sang", "nga"],
        "example_wrong": "Nagkanta siya nang maayo.",
        "example_right": "Nagkanta siya sing maayo.",
    },

    # ── 9. 'ako' without verb (bare pronoun as predicate — may need 'amo') ────
    {
        "id": "KAMI_KAG_KITA_CONFUSION",
        "name": "'kami' vs 'kita' — exclusive vs inclusive 'we'",
        "description": (
            "Ilonggo distinguishes 'kami' (we, excluding the listener) from "
            "'kita' (we, including the listener). Confusing these is a common error."
        ),
        "severity": "info",
        "pattern": [
            {"text": "kami"},
            {"text": "kag"},
            {"text": ["ikaw", "ka"]},
        ],
        "message": "'Kami kag ikaw' is redundant — if the listener is included, use 'kita' instead.",
        "suggestions": ["kita"],
        "example_wrong": "Kami kag ikaw makaon.",
        "example_right": "Kita makaon.",
    },

    # ── 10. Verb in past tense (nag-) + future time marker ───────────────────
    {
        "id": "TENSE_MISMATCH_NAG_FUTURE",
        "name": "Past-tense verb with future time expression",
        "description": (
            "The 'nag-' prefix signals a completed past action. "
            "Pairing it with future time words like 'ugaling' (later/tomorrow) "
            "or 'sunod' (next) creates a tense mismatch."
        ),
        "severity": "error",
        "pattern": [
            {"regex": r"^nag[a-z]+"},
            {"text": ["ugaling", "sunod", "buwas"]},
        ],
        "message": "Tense mismatch: 'nag-' is past tense but a future time word follows. Use 'mag-' for future actions.",
        "suggestions": ["Change 'nag-' prefix to 'mag-'."],
        "example_wrong": "Nagkanta siya ugaling.",
        "example_right": "Magkanta siya ugaling.",
    },

    # ── 11. Repeated 'nga' (disfluency / error) ───────────────────────────────
    {
        "id": "DOUBLE_NGA",
        "name": "Repeated 'nga' linker",
        "description": "Two consecutive 'nga' tokens is almost always a typo or disfluency.",
        "severity": "error",
        "pattern": [
            {"text": "nga"},
            {"text": "nga"},
        ],
        "message": "Repeated 'nga' detected. Remove the duplicate.",
        "suggestions": ["Remove one 'nga'."],
        "example_wrong": "Matamis nga nga mangga.",
        "example_right": "Matamis nga mangga.",
    },

    # ── 12. 'hindi' (Tagalog negation) instead of 'indi' ─────────────────────
    {
        "id": "TAGALOG_HINDI",
        "name": "Tagalog negation 'hindi' used instead of Ilonggo 'indi'",
        "description": (
            "'hindi' is the Tagalog word for 'no/not'. "
            "In Ilonggo (Hiligaynon), the negation word is 'indi' (for future/general) "
            "or 'wala' (for past/existence)."
        ),
        "severity": "error",
        "pattern": [
            {"text": "hindi"},
        ],
        "message": "'hindi' is Tagalog. Use 'indi' (future negation) or 'wala' (past/existential negation) in Ilonggo.",
        "suggestions": ["indi", "wala"],
        "example_wrong": "Hindi ako makaon.",
        "example_right": "Indi ako makaon.",
    },
]


def get_rule_by_id(rule_id: str) -> dict | None:
    for r in RULES:
        if r["id"] == rule_id:
            return r
    return None
