"""Tags the owner puts on transactions to group rows across categories (transaction-tags D1, D2).

A tag keeps the owner's spelling for display and is unique per owner by its normalized name, so a new name that
normalizes to an existing tag reuses that tag. Archived tags stay on their rows and in the ledger filter, but are
not attached anew. Movement legs carry no tags.
"""

from __future__ import annotations

from typing import Any

from app.errors import ErrorCode, api_error

TAG_LIMIT = 10
NAME_MAX_LENGTH = 40

TAG_COLUMNS = """
    tag.id, tag.name, tag.archived_at,
    (SELECT COUNT(*) FROM transaction_tags link
     WHERE link.user_id = tag.user_id AND link.tag_id = tag.id) AS transaction_count
"""


def clean_tag_name(value: str) -> tuple[str, str]:
    """The display name (trimmed, inner whitespace collapsed) and the normalized name it is unique by."""
    display = " ".join(value.split())
    if not 1 <= len(display) <= NAME_MAX_LENGTH:
        raise api_error(422, ErrorCode.TAG_NAME_INVALID, {"max_length": NAME_MAX_LENGTH})
    return display, display.lower()


def tag_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "archived": row["archived_at"] is not None,
        "transaction_count": int(row["transaction_count"]),
    }


def row_tags_sql(alias: str) -> str:
    """A transaction's tags as a JSON array of `{id, name}` ordered by name, for a list or detail select."""
    return f"""COALESCE((
        SELECT json_agg(json_build_object('id', row_tag.id, 'name', row_tag.name)
                        ORDER BY lower(row_tag.name), row_tag.name, row_tag.id)
        FROM transaction_tags row_link JOIN tags row_tag ON row_tag.id = row_link.tag_id
        WHERE row_link.transaction_id = {alias}.id
    ), '[]'::json)"""


def tag_filter_sql(alias: str) -> str:
    """Rows carrying any of the tag IDs bound to the one `%s` placeholder (a list of IDs)."""
    return f"""EXISTS (
        SELECT 1 FROM transaction_tags filter_link
        WHERE filter_link.transaction_id = {alias}.id AND filter_link.user_id = {alias}.user_id
          AND filter_link.tag_id = ANY(%s::uuid[])
    )"""


def list_tags(cur, user_id: str, *, include_archived: bool) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT {TAG_COLUMNS}
        FROM tags tag
        WHERE tag.user_id = %s AND (tag.archived_at IS NULL OR %s)
        ORDER BY tag.archived_at IS NOT NULL, lower(tag.name), tag.name, tag.id
        """,
        (user_id, include_archived),
    )
    return [tag_fields(row) for row in cur.fetchall()]


def load_tag(cur, user_id: str, tag_id: str) -> dict[str, Any]:
    cur.execute(f"SELECT {TAG_COLUMNS} FROM tags tag WHERE tag.user_id = %s AND tag.id = %s", (user_id, tag_id))
    row = cur.fetchone()
    if not row:
        raise api_error(404, ErrorCode.TAG_NOT_FOUND)
    return tag_fields(row)


def lock_tags(cur, user_id: str, tag_ids: list[str]) -> dict[str, dict[str, Any]]:
    """The owner's tags among `tag_ids`, locked against a concurrent merge; any other ID is a 404."""
    ordered_ids = sorted(set(tag_ids))
    cur.execute(
        "SELECT id, archived_at FROM tags WHERE user_id = %s AND id = ANY(%s::uuid[]) ORDER BY id FOR UPDATE",
        (user_id, ordered_ids),
    )
    tags = {str(row["id"]): row for row in cur.fetchall()}
    if len(tags) != len(ordered_ids):
        raise api_error(404, ErrorCode.TAG_NOT_FOUND)
    return tags


def upsert_tag(cur, user_id: str, display: str, normalized: str) -> str:
    """The tag with this normalized name, created when missing and restored when archived."""
    cur.execute(
        """
        INSERT INTO tags (user_id, name, name_normalized) VALUES (%s, %s, %s)
        ON CONFLICT (user_id, name_normalized) DO UPDATE SET archived_at = NULL
        RETURNING id
        """,
        (user_id, display, normalized),
    )
    return str(cur.fetchone()["id"])


def create_tag(cur, user_id: str, name: str) -> dict[str, Any]:
    display, normalized = clean_tag_name(name)
    return load_tag(cur, user_id, upsert_tag(cur, user_id, display, normalized))


def update_tag(cur, user_id: str, tag_id: str, *, name: str | None, archived: bool | None) -> dict[str, Any]:
    if name is None and archived is None:
        raise api_error(422, ErrorCode.NO_FIELDS_TO_UPDATE)
    cleaned = clean_tag_name(name) if name is not None else None
    lock_tags(cur, user_id, [tag_id])
    if cleaned is not None:
        display, normalized = cleaned
        cur.execute(
            "SELECT 1 FROM tags WHERE user_id = %s AND name_normalized = %s AND id <> %s",
            (user_id, normalized, tag_id),
        )
        if cur.fetchone():
            raise api_error(409, ErrorCode.TAG_NAME_TAKEN)
        cur.execute(
            "UPDATE tags SET name = %s, name_normalized = %s WHERE user_id = %s AND id = %s",
            (display, normalized, user_id, tag_id),
        )
    if archived is not None:
        cur.execute(
            """UPDATE tags SET archived_at = CASE WHEN %s THEN COALESCE(archived_at, NOW()) END
               WHERE user_id = %s AND id = %s""",
            (archived, user_id, tag_id),
        )
    return load_tag(cur, user_id, tag_id)


def merge_tags(cur, user_id: str, source_id: str, into_id: str) -> dict[str, Any]:
    """Moves every link of `source_id` to `into_id`, without duplicate links, and deletes `source_id`."""
    if source_id == into_id:
        raise api_error(422, ErrorCode.TAG_MERGE_SAME)
    lock_tags(cur, user_id, [source_id, into_id])
    cur.execute(
        """
        INSERT INTO transaction_tags (transaction_id, tag_id, user_id, created_at)
        SELECT transaction_id, %s, user_id, created_at FROM transaction_tags WHERE user_id = %s AND tag_id = %s
        ON CONFLICT (transaction_id, tag_id) DO NOTHING
        """,
        (into_id, user_id, source_id),
    )
    cur.execute("DELETE FROM tags WHERE user_id = %s AND id = %s", (user_id, source_id))
    return load_tag(cur, user_id, into_id)


def unique_names(names: list[str]) -> dict[str, str]:
    """Each new name's display form by normalized name; repeats of one normalized name count once."""
    cleaned: dict[str, str] = {}
    for name in names:
        display, normalized = clean_tag_name(name)
        cleaned.setdefault(normalized, display)
    return cleaned


def resolve_tag_set(
    cur, user_id: str, tag_ids: list[str], new_names: list[str], *, attached: frozenset[str] = frozenset(),
) -> list[str]:
    """The tag IDs a transaction will carry: `tag_ids` plus the created or reused `new_names`.

    An archived tag may stay on a row that already carries it (`attached`) but is not attached anew. A new name
    that normalizes to an archived tag restores that tag, as creating it in Pengaturan does. The caller writes the
    links in the same database transaction, so a refusal raised after a tag was created leaves nothing behind.
    """
    names = unique_names(new_names)
    requested = set(tag_ids)
    if len(requested) > TAG_LIMIT or len(names) > TAG_LIMIT:
        raise api_error(422, ErrorCode.TAG_LIMIT, {"limit": TAG_LIMIT})
    if requested:
        for tag_id, tag in lock_tags(cur, user_id, list(requested)).items():
            if tag["archived_at"] is not None and tag_id not in attached:
                raise api_error(404, ErrorCode.TAG_NOT_FOUND)
    for normalized, display in names.items():
        requested.add(upsert_tag(cur, user_id, display, normalized))
    if len(requested) > TAG_LIMIT:
        raise api_error(422, ErrorCode.TAG_LIMIT, {"limit": TAG_LIMIT})
    return sorted(requested)


def attached_tag_ids(cur, transaction_id: str) -> frozenset[str]:
    cur.execute("SELECT tag_id FROM transaction_tags WHERE transaction_id = %s", (transaction_id,))
    return frozenset(str(row["tag_id"]) for row in cur.fetchall())


def set_transaction_tags(cur, user_id: str, transaction_id: str, tag_ids: list[str]) -> None:
    """Makes the transaction's tag set exactly `tag_ids`, keeping the links it already has."""
    cur.execute(
        "DELETE FROM transaction_tags WHERE transaction_id = %s AND NOT (tag_id = ANY(%s::uuid[]))",
        (transaction_id, tag_ids),
    )
    if tag_ids:
        cur.execute(
            """
            INSERT INTO transaction_tags (transaction_id, tag_id, user_id)
            SELECT %s, tag_id, %s FROM unnest(%s::uuid[]) AS tag_id
            ON CONFLICT (transaction_id, tag_id) DO NOTHING
            """,
            (transaction_id, user_id, tag_ids),
        )
