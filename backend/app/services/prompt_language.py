"""Model prompts by output language (localize-server-text D7).

Each prompt has an Indonesian rendering, `prompts/<name>.md`, unchanged and with its existing version, and an
English rendering, `prompts/<name>.en.md`, whose version ends in `-en`. English output is used only for an English
account and only once the owner has turned on `LLM_ENGLISH_OUTPUT_ENABLED` after the English benchmark passed
(owner decision OD2); every other account gets the Indonesian rendering. The version stored with a row names the
rendering that produced it.

This module imports no runtime configuration at import time, so the offline benchmark can use it.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from pathlib import Path

PROMPT_DIR = Path(__file__).with_name("prompts")
OUTPUT_LANGUAGES = ("id", "en")
ENGLISH_VERSION_SUFFIX = "-en"

NOTIFICATION_INTERPRETATION = "notification_interpretation"
CAPTURE_NORMALIZATION = "capture_normalization"
HISTORY_SUGGESTIONS = "history_suggestions"
PURPOSE_NORMALIZATION = "purpose_normalization"

# The Indonesian versions predate output languages and stay as they are.
INDONESIAN_VERSIONS = {
    NOTIFICATION_INTERPRETATION: "notification-interpretation-12",
    CAPTURE_NORMALIZATION: "capture-normalization-2",
    HISTORY_SUGGESTIONS: "history-suggestions-1",
    PURPOSE_NORMALIZATION: "purpose-normalization-1",
}

# Generic descriptions a model must not return when something more specific is known, compared case-insensitively.
# A guard rejects an English placeholder such as "Payment" the way it rejects "Pembayaran".
PLACEHOLDER_DESCRIPTIONS = {
    "id": frozenset({"pembayaran", "belanja", "transaksi", "pengeluaran", "pemasukan", "transfer", "lainnya"}),
    "en": frozenset({
        "payment", "shopping", "spending", "transaction", "income", "transfer", "other", "transfer in",
        "bank transaction",
    }),
}


@dataclass(frozen=True)
class Prompt:
    name: str
    language: str
    version: str
    text: str


def output_language(account_language: str) -> str:
    """The language model output is requested in for an account: English only when enabled (OD2)."""
    from app.core.config import settings

    return "en" if account_language == "en" and settings.llm_english_output_enabled else "id"


@cache
def _prompt_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def load_prompt(name: str, language: str) -> Prompt:
    """The rendering of prompt `name` that asks for output in `language` (an output language, not an account's)."""
    if name not in INDONESIAN_VERSIONS:
        raise ValueError(f"unknown prompt {name!r}")
    if language not in OUTPUT_LANGUAGES:
        raise ValueError(f"unknown output language {language!r}")
    if language == "id":
        return Prompt(name, "id", INDONESIAN_VERSIONS[name], _prompt_text(PROMPT_DIR / f"{name}.md"))
    return Prompt(name, "en", INDONESIAN_VERSIONS[name] + ENGLISH_VERSION_SUFFIX, _prompt_text(PROMPT_DIR / f"{name}.en.md"))


def select_prompt(name: str, account_language: str) -> Prompt:
    """The prompt an account's model call uses."""
    return load_prompt(name, output_language(account_language))


def prompt_for_version(version: str) -> Prompt:
    """The rendering a stored version names, so a retried call uses the prompt its row records."""
    for name, indonesian in INDONESIAN_VERSIONS.items():
        if version == indonesian:
            return load_prompt(name, "id")
        if version == indonesian + ENGLISH_VERSION_SUFFIX:
            return load_prompt(name, "en")
    raise ValueError("unknown prompt version")


def is_placeholder_description(description: str, language: str) -> bool:
    return " ".join(description.split()).casefold() in PLACEHOLDER_DESCRIPTIONS[language]
