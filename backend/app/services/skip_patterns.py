"""Skip patterns for redundant notifications (skip-redundant-notifications D2, D4, D7).

A pattern says that one app's reports in one direction repeat another app's: Bank Jago's "received
from GoPay" repeats GoPay's own credit. With an active pattern the repeat is skipped, booking nothing,
but only when the kept app's report of the same money is already recorded (movement_pairing.same_arrival).
Patterns are learned from "Benar, satu transaksi" in Tinjau or added by hand in Pengaturan, and are
always scoped to one owner.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from app.services.notification_evidence import SUPPORTED_PACKAGES
from app.services.notification_resolution import INSTITUTION_LABELS

INSTITUTIONS = frozenset(SUPPORTED_PACKAGES.values())
DIRECTIONS = frozenset({"income", "expense"})

PATTERN_COLUMNS = "id, redundant_institution, direction, kept_institution, source, created_at, stopped_at"


def institution_label(institution: str) -> str:
    return INSTITUTION_LABELS.get(institution, institution)


def packages_of(institution: str) -> list[str]:
    return sorted(package for package, value in SUPPORTED_PACKAGES.items() if value == institution)


def matching_patterns(cur, user_id: str, institution: str, direction: str) -> list[dict]:
    """The owner's active patterns in `direction` that name `institution` as the redundant or kept app."""
    cur.execute(
        f"""SELECT {PATTERN_COLUMNS} FROM notification_skip_patterns
            WHERE user_id = %s AND direction = %s AND stopped_at IS NULL
              AND (redundant_institution = %s OR kept_institution = %s)
            ORDER BY created_at, id""",
        (user_id, direction, institution, institution),
    )
    return cur.fetchall()


def seen_institutions(cur, user_id: str) -> list[str]:
    """Institutions that have sent the owner at least one notification, in a stable order."""
    cur.execute("SELECT DISTINCT lower(package_name) AS package FROM notification_events WHERE user_id = %s",
                (user_id,))
    found = {SUPPORTED_PACKAGES.get(row["package"]) for row in cur.fetchall()}
    return sorted(value for value in found if value)


def invalid(message: str) -> HTTPException:
    return HTTPException(status_code=422, detail={"code": "invalid_skip_pattern", "message": message})


def validate(cur, user_id: str, redundant: str, direction: str, kept: str) -> None:
    if direction not in DIRECTIONS:
        raise invalid("Pilih jenis uang masuk atau uang keluar.")
    if redundant not in INSTITUTIONS or kept not in INSTITUTIONS:
        raise invalid("Pilih aplikasi yang didukung.")
    if redundant == kept:
        raise invalid("Pilih dua aplikasi yang berbeda.")
    seen = set(seen_institutions(cur, user_id))
    if redundant not in seen or kept not in seen:
        raise invalid("Pilih aplikasi yang sudah pernah mengirim notifikasi.")


def active_pattern(cur, user_id: str, redundant: str, direction: str, kept: str) -> dict | None:
    cur.execute(
        f"""SELECT {PATTERN_COLUMNS} FROM notification_skip_patterns
            WHERE user_id = %s AND redundant_institution = %s AND direction = %s AND kept_institution = %s
              AND stopped_at IS NULL""",
        (user_id, redundant, direction, kept),
    )
    return cur.fetchone()


def insert_pattern(cur, user_id: str, redundant: str, direction: str, kept: str, source: str) -> tuple[dict, bool]:
    """The active pattern, created when none exists; (pattern, created). Concurrent inserts meet the
    unique partial index and resolve to the existing row."""
    cur.execute(
        f"""INSERT INTO notification_skip_patterns (user_id, redundant_institution, direction, kept_institution, source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, redundant_institution, direction, kept_institution) WHERE stopped_at IS NULL
            DO NOTHING RETURNING {PATTERN_COLUMNS}""",
        (user_id, redundant, direction, kept, source),
    )
    created = cur.fetchone()
    if created:
        return created, True
    return active_pattern(cur, user_id, redundant, direction, kept), False


def create_pattern(cur, user_id: str, redundant: str, direction: str, kept: str) -> dict:
    """A pattern the owner adds by hand; 409 when the same pattern is already active."""
    validate(cur, user_id, redundant, direction, kept)
    pattern, created = insert_pattern(cur, user_id, redundant, direction, kept, "manual")
    if not created:
        raise HTTPException(status_code=409, detail={
            "code": "skip_pattern_exists", "message": "Pola ini sudah ada.",
        })
    return pattern


def stop_pattern(cur, user_id: str, pattern_id: str) -> dict:
    """"Berhenti melewati": idempotent; another owner's or an unknown pattern is not found."""
    cur.execute(
        f"""UPDATE notification_skip_patterns SET stopped_at = COALESCE(stopped_at, NOW())
            WHERE user_id = %s AND id = %s RETURNING {PATTERN_COLUMNS}""",
        (user_id, pattern_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail={
            "code": "skip_pattern_not_found", "message": "Pola tidak ditemukan.",
        })
    return row


def learn_from_acknowledgement(cur, user_id: str, event: dict) -> dict | None:
    """The pattern a "Benar, satu transaksi" teaches (D4): the duplicate's app and direction, repeating
    the kept leg's app. Nothing when the kept leg came from no notification (a typed or manual row)."""
    if event.get("provenance_kind") != "duplicate_observation" or not event.get("duplicate_of_transaction_id"):
        return None
    redundant = SUPPORTED_PACKAGES.get(str(event.get("package_name") or "").lower())
    cur.execute(
        """SELECT n.package_name, t.type FROM transactions t
           JOIN notification_events n ON n.user_id = t.user_id AND n.transaction_id = t.id
           WHERE t.user_id = %s AND t.id = %s ORDER BY n.post_time, n.id LIMIT 1""",
        (user_id, str(event["duplicate_of_transaction_id"])),
    )
    kept_row = cur.fetchone()
    kept = SUPPORTED_PACKAGES.get(str(kept_row["package_name"] or "").lower()) if kept_row else None
    if not redundant or not kept or redundant == kept or kept_row["type"] not in DIRECTIONS:
        return None
    pattern, created = insert_pattern(cur, user_id, redundant, kept_row["type"], kept, "confirmation")
    return {**public_pattern(pattern), "created": created}


def public_pattern(row: dict, counts: dict | None = None) -> dict[str, Any]:
    counts = counts or {}
    return {
        "id": str(row["id"]),
        "redundant_institution": row["redundant_institution"],
        "redundant_label": institution_label(row["redundant_institution"]),
        "direction": row["direction"],
        "kept_institution": row["kept_institution"],
        "kept_label": institution_label(row["kept_institution"]),
        "source": row["source"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "skipped_count": int(counts.get("skipped_count") or 0),
        "last_skipped_at": counts["last_skipped_at"].isoformat() if counts.get("last_skipped_at") else None,
    }


def list_patterns(cur, user_id: str) -> dict[str, Any]:
    cur.execute(
        """SELECT p.id, p.redundant_institution, p.direction, p.kept_institution, p.source, p.created_at,
                  p.stopped_at, COUNT(n.id) AS skipped_count, MAX(n.post_time) AS last_skipped_at
            FROM notification_skip_patterns p
            LEFT JOIN notification_events n ON n.user_id = p.user_id AND n.skip_pattern_id = p.id
            WHERE p.user_id = %s AND p.stopped_at IS NULL
            GROUP BY p.id ORDER BY p.created_at, p.id""",
        (user_id,),
    )
    patterns = [public_pattern(row, row) for row in cur.fetchall()]
    institutions = [{"institution": value, "label": institution_label(value)}
                    for value in seen_institutions(cur, user_id)]
    return {"patterns": patterns, "institutions": institutions}
