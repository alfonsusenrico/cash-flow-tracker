"""Accounts reported by more than one app (reconcile-linked-account-reports D1, D5, D6).

A link says that one account is also reported by another institution's app: GoPay's "Tabungan by Jago"
pocket is reported by GoPay and by Bank Jago. A second report of that account's money then books nothing
(movement_pairing.same_arrival). Links come only from the owner's own answer in Tinjau ("Benar, satu
transaksi") or from the V47 conversion of a skip pattern; there are no presets and nothing is learned
across owners.
"""

from __future__ import annotations

from typing import Any


from app.errors import ErrorCode, api_error
from app.services.notification_evidence import SUPPORTED_PACKAGES
from app.services.notification_resolution import INSTITUTION_LABELS

DIRECTIONS = frozenset({"income", "expense"})


def institution_label(institution: str) -> str:
    return INSTITUTION_LABELS.get(institution, institution)


def institution_of(package_name: str | None) -> str | None:
    return SUPPORTED_PACKAGES.get(str(package_name or "").strip().lower())


def active_links(cur, user_id: str) -> list[dict]:
    """The owner's active links, oldest first, so a decision never depends on row order."""
    cur.execute(
        """SELECT id, account_id, institution FROM account_report_links
           WHERE user_id = %s AND stopped_at IS NULL ORDER BY created_at, id""",
        (user_id,),
    )
    return cur.fetchall()


def _account_label(cur, user_id: str, account_id: str) -> str | None:
    cur.execute(
        """SELECT a.name, p.name AS parent FROM accounts a
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
           WHERE a.user_id = %s AND a.id = %s""",
        (user_id, account_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return f"{row['parent']} · {row['name']}" if row["parent"] else row["name"]


def public_link(cur, user_id: str, row: dict, counts: dict | None = None) -> dict[str, Any]:
    counts = counts or {}
    return {
        "id": str(row["id"]),
        "account_id": str(row["account_id"]),
        "account_label": _account_label(cur, user_id, str(row["account_id"])),
        "institution": row["institution"],
        "institution_label": institution_label(row["institution"]),
        "source": row["source"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "skipped_count": int(counts.get("skipped_count") or 0),
        "last_skipped_at": counts["last_skipped_at"].isoformat() if counts.get("last_skipped_at") else None,
    }


def list_links(cur, user_id: str, account_id: str | None = None) -> list[dict[str, Any]]:
    cur.execute(
        """SELECT l.id, l.account_id, l.institution, l.source, l.created_at,
                  COUNT(n.id) AS skipped_count, MAX(n.post_time) AS last_skipped_at
           FROM account_report_links l
           LEFT JOIN notification_events n ON n.user_id = l.user_id AND n.reporting_link_id = l.id
           WHERE l.user_id = %s AND l.stopped_at IS NULL AND (%s::uuid IS NULL OR l.account_id = %s::uuid)
           GROUP BY l.id ORDER BY l.created_at, l.id""",
        (user_id, account_id, account_id),
    )
    return [public_link(cur, user_id, row, row) for row in cur.fetchall()]


def stop_link(cur, user_id: str, link_id: str) -> None:
    """Stops matching from now on; reports the link skipped earlier stay skipped. Idempotent."""
    cur.execute(
        """UPDATE account_report_links SET stopped_at = COALESCE(stopped_at, NOW())
           WHERE user_id = %s AND id = %s RETURNING id""",
        (user_id, link_id),
    )
    if not cur.fetchone():
        raise api_error(404, ErrorCode.ACCOUNT_LINK_NOT_FOUND)


def _first_report(cur, user_id: str, transaction_id: str) -> dict | None:
    cur.execute(
        """SELECT n.package_name, n.interpretation, t.type, t.account_id FROM transactions t
           JOIN notification_events n ON n.user_id = t.user_id AND n.transaction_id = t.id
           WHERE t.user_id = %s AND t.id = %s ORDER BY n.post_time, n.id LIMIT 1""",
        (user_id, transaction_id),
    )
    return cur.fetchone()


def learn_from_acknowledgement(cur, user_id: str, event: dict) -> dict | None:
    """The link a "Benar, satu transaksi" teaches (D5): the named account, also reported by the other app.

    The named account is the kept leg's, unless that leg was a default-pocket fallback and the second
    report named its own account. Nothing is learned when the kept leg came from no notification.
    """
    if event.get("provenance_kind") != "duplicate_observation" or not event.get("duplicate_of_transaction_id"):
        return None
    kept = _first_report(cur, user_id, str(event["duplicate_of_transaction_id"]))
    if not kept or kept["type"] not in DIRECTIONS:
        return None
    second_institution = institution_of(event.get("package_name"))
    kept_institution = institution_of(kept["package_name"])
    if not second_institution or not kept_institution or second_institution == kept_institution:
        return None
    second = event.get("interpretation") or {}
    kept_basis = (kept.get("interpretation") or {}).get("account_basis")
    if kept_basis == "default_pocket" and second.get("account_basis") == "named" and second.get("account_id"):
        account_id, institution = str(second["account_id"]), kept_institution
    else:
        account_id, institution = str(kept["account_id"]), second_institution
    cur.execute(
        """INSERT INTO account_report_links (user_id, account_id, institution, source)
           SELECT %s, a.id, %s, 'confirmation' FROM accounts a WHERE a.user_id = %s AND a.id = %s
           ON CONFLICT (user_id, account_id, institution) WHERE stopped_at IS NULL DO NOTHING
           RETURNING id, account_id, institution, source, created_at""",
        (user_id, institution, user_id, account_id),
    )
    row = cur.fetchone()
    created = row is not None
    if not created:
        cur.execute(
            """SELECT id, account_id, institution, source, created_at FROM account_report_links
               WHERE user_id = %s AND account_id = %s AND institution = %s AND stopped_at IS NULL""",
            (user_id, account_id, institution),
        )
        row = cur.fetchone()
        if not row:
            return None
    return {**public_link(cur, user_id, row), "created": created}
