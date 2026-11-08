"""Accept-Language matching for accounts not created yet (add-language-system D2, task 2.3)."""
import pytest

from app.services.language import language_from_accept_language


@pytest.mark.parametrize(
    "header,expected",
    [
        ("id-ID,id;q=0.9,en;q=0.8", "id"),
        ("en-US,id;q=0.5", "en"),
        ("in", "id"),
        ("in-ID", "id"),
        ("ms, fr", "en"),
        (None, "en"),
        ("", "en"),
        (";;;,,,", "en"),
        ("id;q=abc", "en"),
        ("id;q=1.5", "en"),
        ("fr-FR, id;q=0.4, en;q=0.3", "id"),
        ("en;q=0.5, id;q=0.5", "en"),
        ("id;q=0, en;q=0.1", "en"),
        ("*", "en"),
        ("ID-id", "id"),
    ],
)
def test_language_from_accept_language(header, expected):
    assert language_from_accept_language(header) == expected
