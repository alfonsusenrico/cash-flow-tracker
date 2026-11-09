"""Guard: backend logic never identifies a system category by its display name (add-category-system-keys D6).

Money rules read `categories.system_key` through `app.services.system_categories`. A string constant that is a
system category's seed name, or that quotes one for SQL, is allowed only in the owning module, as a parser
`category_hint` value, in a docstring (comments are not string constants), or in the copy listed below.
"""

from __future__ import annotations

import ast
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "app"
SYSTEM_NAMES = ("Investasi", "Internal Movement", "Penyesuaian Saldo", "Biaya kredit")
OWNING_MODULE = "services/system_categories.py"
PARSER_MODULE = "services/notification_parser.py"
# Visible copy that happens to equal a seed name; none of it identifies a category.
COPY_ALLOWED = {
    ("services/cycle_summary.py", "Investasi"): "label of the net-worth line for investment accounts",
    ("routers/movements.py", "Internal Movement"): "default note of a movement created without notes",
}


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """Ids of bare string statements (docstrings and their like), which explain rather than compare."""
    return {
        id(node.value) for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    }


def _category_hint_values(tree: ast.AST) -> set[int]:
    return {
        id(keyword.value) for node in ast.walk(tree) if isinstance(node, ast.Call)
        for keyword in node.keywords if keyword.arg == "category_hint"
    }


def _named_system_category(value: str) -> str | None:
    for name in SYSTEM_NAMES:
        if value.strip() == name or f"'{name}'" in value or (name == "Penyesuaian Saldo" and name in value):
            return name
    return None


def name_references(root: Path) -> list[str]:
    """`path:line: name` for every string constant under `root` that names a system category outside the allowlist."""
    found = []
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        if relative == OWNING_MODULE:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        skipped = _docstring_nodes(tree)
        if relative == PARSER_MODULE:
            skipped |= _category_hint_values(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str) or id(node) in skipped:
                continue
            name = _named_system_category(node.value)
            if name and (relative, node.value.strip()) not in COPY_ALLOWED:
                found.append((relative, node.lineno, name))
    return [f"{relative}:{line}: {name}" for relative, line, name in sorted(found)]


def test_backend_logic_reads_system_keys_not_names():
    assert name_references(APP_ROOT) == []


def test_guard_names_file_and_line_of_a_planted_name_check(tmp_path):
    routers = tmp_path / "routers"
    routers.mkdir()
    (routers / "planted.py").write_text(
        '"""A docstring may say \'Investasi\'."""\n'
        "# A comment may say 'Internal Movement'.\n"
        "QUERY = \"SELECT 1 FROM categories c WHERE c.name = 'Investasi'\"\n"
        "\n"
        "def movement(row):\n"
        "    return row['name'] == \"Internal Movement\"\n"
        "\n"
        "ADJUSTMENT = 'Penyesuaian Saldo'\n"
    )
    assert name_references(tmp_path) == [
        "routers/planted.py:3: Investasi",
        "routers/planted.py:6: Internal Movement",
        "routers/planted.py:8: Penyesuaian Saldo",
    ]


def test_parser_hints_are_allowed_only_as_category_hint_values(tmp_path):
    services = tmp_path / "services"
    services.mkdir()
    (services / "notification_parser.py").write_text(
        "def parse(ParsedNotification):\n"
        "    ParsedNotification(category_hint=\"Internal Movement\")\n"
        "    return \"Investasi\"\n"
    )
    assert name_references(tmp_path) == ["services/notification_parser.py:3: Investasi"]
