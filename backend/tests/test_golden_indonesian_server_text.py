"""Indonesian server text stays byte-identical (localize-server-text D10)."""

import json

from tests.golden_server_text import FIXTURE, capture

GOLDEN = json.loads(FIXTURE.read_text())


def test_indonesian_server_text_matches_the_golden_fixture():
    current = capture()
    for section in ("rendered", "inline", "prompts", "errors"):
        assert current[section] == GOLDEN[section], section
