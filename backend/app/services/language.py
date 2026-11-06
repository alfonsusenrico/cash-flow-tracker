"""Interface languages and regions an account can have, and the Accept-Language fallback (add-language-system D2).

The account's `users.language` is the language of every authenticated request. `Accept-Language` is read only
where there is no account yet, such as registration without an explicit language.
"""

from __future__ import annotations

LANGUAGES = ("en", "id")
REGIONS = ("ID", "US")
DEFAULT_LANGUAGE = "en"
DEFAULT_REGION = "ID"

# Primary subtags that mean a supported language; `in` is the legacy ISO 639 code for Indonesian.
_PRIMARY_SUBTAGS = {"en": "en", "id": "id", "in": "id"}


def _quality(parameters: list[str]) -> float | None:
    """The q-value of one Accept-Language member (RFC 9110 §12.4.2), or None when it is malformed."""
    for parameter in parameters:
        name, _, value = parameter.strip().partition("=")
        if name.strip().lower() != "q":
            continue
        value = value.strip()
        try:
            quality = float(value)
        except ValueError:
            return None
        if not 0 <= quality <= 1 or len(value) > 5:
            return None
        return quality
    return 1.0


def language_from_accept_language(header: str | None) -> str:
    """The supported language the header prefers most, or English (RFC 9110 §12.5.4).

    Members whose primary subtag is not a supported language, members with q=0 and malformed members are
    ignored. Between equal q-values the earlier member wins, as listed order is the tie-breaker clients use.
    """
    best: tuple[float, int, str] | None = None
    for position, member in enumerate((header or "").split(",")):
        tag, *parameters = member.split(";")
        tag = tag.strip()
        if not tag or tag == "*":
            continue
        primary = tag.split("-", 1)[0].lower()
        language = _PRIMARY_SUBTAGS.get(primary)
        if language is None:
            continue
        quality = _quality(parameters)
        if not quality:
            continue
        candidate = (quality, -position, language)
        if best is None or candidate > best:
            best = candidate
    return best[2] if best else DEFAULT_LANGUAGE
