"""Peta Aliran: where a period's money came from and where it went, as nodes and links that balance.

Every income or expense row of a holder node (a liquid account with its folded pockets, a goal-linked
saving account, or an investment account with its products) is assigned to exactly one link by
`assign_rows`, which both the map and its drawer rows use, so a link's rows always add up to the link.
For every holder node the links in add up to the links out: the opening ledger balance plus what came in
equals what went out plus "Masih ada", the ledger balance at the end of `through`.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.kakeibo import pillar_bucket_sql
from app.services.reimbursements import load_spending_offsets
from app.services.report_figures import cash_flow_totals, reportable_expense_sql, reportable_income_sql
from app.services.waiting_rows import COUNTERPART_SECTIONS, fetch_waiting_rows

KakeiboBreakdown = Callable[..., dict[str, Any]]

PILLAR_LABELS: dict[str, str] = {
    "need": "Kebutuhan",
    "want": "Keinginan",
    "culture": "Budaya",
    "unexpected": "Tak Terduga",
    "saving": "Tabungan",
    "unclassified": "Belum diklasifikasi",
}
PILLAR_ORDER = tuple(PILLAR_LABELS)

OPENING = "opening"
NEGATIVE_OPENING = "neg_opening"
NEGATIVE_CLOSING = "neg_closing"
OTHER_IN = "src:other"
REIMBURSEMENT = "src:reimbursement"
OTHER_OUT = "other_out"
WAITING = "waiting"
REMAINING = "remaining"

UUID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
NODE_ID_PATTERN = re.compile(
    rf"^(?:(?:acc|sav|inv):{UUID_PATTERN}|src:(?:{UUID_PATTERN}|uncategorized|other|reimbursement)"
    rf"|pillar:(?:{'|'.join(PILLAR_ORDER)})|{OPENING}|{NEGATIVE_OPENING}|{NEGATIVE_CLOSING}|{OTHER_OUT}|{WAITING}|{REMAINING})$"
)
ROWS_PAGE_LIMIT = 50
BALANCE_NODES = frozenset({OPENING, REMAINING, NEGATIVE_OPENING, NEGATIVE_CLOSING})


class FlowKeyError(ValueError):
    """A link or node key that is malformed (422) or names nothing of this owner (404)."""

    def __init__(self, code: str, status: int):
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass
class Link:
    key: str
    source: str
    target: str
    kind: str
    amount: int = 0
    count: int = 0
    waiting_count: int = 0
    waiting_amount: int = 0


@dataclass
class Assignment:
    """One row's share of one link; a movement pair is one assignment on its outbound leg."""

    row_id: str
    link_key: str
    amount: int


@dataclass
class FlowModel:
    holders: dict[str, dict[str, Any]]
    sources: dict[str, str]
    links: dict[str, Link] = field(default_factory=dict)
    assignments: list[Assignment] = field(default_factory=list)

    def add(self, source: str, target: str, kind: str, amount: int, row_id: str | None, *, waiting: bool = False) -> None:
        if amount == 0:
            return
        key = f"{source}>{target}"
        link = self.links.get(key)
        if link is None:
            link = self.links[key] = Link(key, source, target, kind)
        link.amount += amount
        if row_id is not None:
            link.count += 1
            self.assignments.append(Assignment(row_id, key, amount))
            if waiting:
                link.waiting_count += 1
                link.waiting_amount += amount


def local_day_end(day: date) -> datetime:
    return datetime.combine(day, time.max, tzinfo=ZoneInfo(settings.tz)).astimezone(timezone.utc)


def _holder_kind(account: dict[str, Any]) -> str:
    return "inv" if account["type"] == "investment" or account["instrument_type"] is not None else "acc"


def holder_of(accounts: dict[str, dict[str, Any]]) -> dict[str, str]:
    """Each account's holder node (OD44): a goal-linked account is its own saving node; a pocket or
    product folds into its parent's node; a top-level account is a liquid or an investment node."""
    result: dict[str, str] = {}

    def resolve(account_id: str, depth: int = 0) -> str:
        if account_id in result:
            return result[account_id]
        account = accounts[account_id]
        if account["goal_linked"]:
            node = f"sav:{account_id}"
        elif account["parent_id"] and account["parent_id"] in accounts and depth < 8:
            node = resolve(account["parent_id"], depth + 1)
        else:
            node = f"{_holder_kind(account)}:{account_id}"
        result[account_id] = node
        return node

    for account_id in accounts:
        resolve(account_id)
    return result


def _load_accounts(cur, user_id: str) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT a.id, a.name, a.parent_id, a.type, a.instrument_type, a.initial_balance, a.is_archived,
               a.display_order, a.created_at,
               EXISTS (SELECT 1 FROM goal_accounts ga WHERE ga.account_id = a.id) AS goal_linked,
               (SELECT g.name FROM goal_accounts ga JOIN goals g ON g.id = ga.goal_id
                 WHERE ga.account_id = a.id ORDER BY g.name LIMIT 1) AS goal_name
        FROM accounts a
        WHERE a.user_id = %s
        ORDER BY a.display_order, a.created_at, a.name
        """,
        (user_id,),
    )
    return {
        str(row["id"]): {**row, "id": str(row["id"]), "parent_id": str(row["parent_id"]) if row["parent_id"] else None}
        for row in cur.fetchall()
    }


def _load_window_rows(cur, user_id: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Income and expense rows of the window with what assignment needs, oldest first."""
    cur.execute(
        f"""
        SELECT t.id, t.account_id, t.type, t.amount, t.date, t.movement_id, t.movement_role,
               t.merchant, t.notes, t.category_id, c.name AS category_name,
               {reportable_expense_sql()} AS reportable_expense,
               {reportable_income_sql()} AS reportable_income,
               COALESCE(c.offsets_spending, FALSE) AS offsets_spending,
               {pillar_bucket_sql()} AS bucket,
               pair.id AS pair_id, pair.account_id AS pair_account_id, pair.amount AS pair_amount,
               pair.date AS pair_date
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        LEFT JOIN LATERAL (
            SELECT p.id, p.account_id, p.amount, p.date FROM transactions p
            WHERE t.movement_id IS NOT NULL AND p.user_id = t.user_id AND p.movement_id = t.movement_id
              AND p.id <> t.id AND p.type IN ('income', 'expense')
            ORDER BY p.id LIMIT 1
        ) pair ON TRUE
        WHERE t.user_id = %s AND t.type IN ('income', 'expense') AND t.date >= %s AND t.date <= %s
        ORDER BY t.date, t.id
        """,
        (user_id, start, end),
    )
    return cur.fetchall()


def _load_opening(cur, user_id: str, start: datetime) -> dict[str, int]:
    """Each account's ledger balance just before the window: initial balance plus rows before it."""
    cur.execute(
        """
        SELECT a.id,
               a.initial_balance
               + COALESCE(SUM(t.amount) FILTER (WHERE t.type = 'income'), 0)
               - COALESCE(SUM(t.amount) FILTER (WHERE t.type = 'expense'), 0) AS balance
        FROM accounts a
        LEFT JOIN transactions t ON t.account_id = a.id AND t.user_id = a.user_id AND t.date < %s
        WHERE a.user_id = %s
        GROUP BY a.id, a.initial_balance
        """,
        (start, user_id),
    )
    return {str(row["id"]): int(row["balance"]) for row in cur.fetchall()}


def _source_for_income(row: dict[str, Any]) -> str:
    if row["reportable_income"]:
        return f"src:{row['category_id']}" if row["category_id"] else "src:uncategorized"
    if row["offsets_spending"]:
        return REIMBURSEMENT
    return OTHER_IN


def assign_rows(
    accounts: dict[str, dict[str, Any]], rows: list[dict[str, Any]], waiting: dict[str, str],
    window_start: datetime, through_end: datetime,
) -> FlowModel:
    """Assigns every row of the window up to `through_end` to one link (design D8 and D9).

    `waiting` maps the row ids the Tinjau badge counts to their section. Rows waiting for a counterpart
    go to "Menunggu pasangan"; rows waiting for a category answer stay in their bucket and are counted
    as waiting there.
    """
    holders = holder_of(accounts)
    model = FlowModel(holders={}, sources={})
    for row in rows:
        if row["date"] > through_end:
            continue
        row_id = str(row["id"])
        amount = int(row["amount"])
        account_id = str(row["account_id"])
        node = holders[account_id]
        model.holders.setdefault(node, {})
        section = waiting.get(row_id)
        pair_in_window = (
            row["pair_id"] is not None and row["pair_account_id"] is not None
            and window_start <= row["pair_date"] <= through_end
        )
        if row["movement_id"] is not None and pair_in_window:
            other = holders.get(str(row["pair_account_id"]))
            if row["type"] == "income":
                # The pair is one link, carried by the outbound leg. A different inbound amount is
                # balanced on its own, so the rows and the links of both nodes still agree.
                difference = amount - int(row["pair_amount"])
                if difference > 0:
                    model.add(OTHER_IN, node, "other", difference, row_id)
                elif difference < 0:
                    model.add(node, OTHER_OUT, "other", -difference, row_id)
                continue
            if other is None or other == node:
                continue  # A move between a parent and its own pocket stays inside the node.
            model.holders.setdefault(other, {})
            kinds = {node.split(":")[0], other.split(":")[0]}
            kind = "investment" if "inv" in kinds else "saving" if "sav" in kinds else "transfer"
            model.add(node, other, kind, amount, row_id, waiting=section is not None)
            continue
        if section in COUNTERPART_SECTIONS:
            if row["type"] == "expense":
                model.add(node, WAITING, "waiting", amount, row_id, waiting=True)
            else:
                model.add(WAITING, node, "waiting", amount, row_id, waiting=True)
            continue
        if row["type"] == "income":
            source = _source_for_income(row)
            model.sources.setdefault(source, row["category_name"] or "Tanpa kategori")
            model.add(source, node, "income" if source != OTHER_IN else "other", amount, row_id, waiting=section is not None)
        elif row["reportable_expense"]:
            model.add(node, f"pillar:{row['bucket']}", "spend", amount, row_id, waiting=section is not None)
        else:
            model.add(node, OTHER_OUT, "other", amount, row_id, waiting=section is not None)
    return model


def _close_holders(model: FlowModel, accounts: dict[str, dict[str, Any]], opening: dict[str, int]) -> dict[str, dict[str, int]]:
    """Adds each holder's opening and closing links; returns their opening and closing balances."""
    holders = holder_of(accounts)
    balances: dict[str, dict[str, int]] = {}
    for account_id, node in holders.items():
        entry = balances.setdefault(node, {"opening": 0, "closing": 0})
        entry["opening"] += opening.get(account_id, 0)
    for node, entry in balances.items():
        moved_in = sum(link.amount for link in model.links.values() if link.target == node)
        moved_out = sum(link.amount for link in model.links.values() if link.source == node)
        entry["closing"] = entry["opening"] + moved_in - moved_out
    for node, entry in balances.items():
        if entry["opening"] > 0:
            model.add(OPENING, node, "opening", entry["opening"], None)
        elif entry["opening"] < 0:
            model.add(node, NEGATIVE_OPENING, "negative_opening", -entry["opening"], None)
        if entry["closing"] > 0:
            model.add(node, REMAINING, "remaining", entry["closing"], None)
        elif entry["closing"] < 0:
            model.add(NEGATIVE_CLOSING, node, "negative_closing", -entry["closing"], None)
    return balances


def _node_label(node_id: str, accounts: dict[str, dict[str, Any]], source_names: dict[str, str]) -> str:
    kind, _, ident = node_id.partition(":")
    if kind in ("acc", "inv"):
        return accounts[ident]["name"]
    if kind == "sav":
        account = accounts[ident]
        return f"{account['goal_name']} · {account['name']}" if account["goal_name"] else account["name"]
    if kind == "pillar":
        return PILLAR_LABELS[ident]
    if node_id == REIMBURSEMENT:
        return "Penggantian uang"
    if node_id == OTHER_IN:
        return "Masuk lainnya"
    if kind == "src":
        return source_names.get(node_id, "Tanpa kategori")
    return {
        OPENING: "Saldo sebelum periode",
        NEGATIVE_OPENING: "Saldo minus sebelum periode",
        NEGATIVE_CLOSING: "Saldo minus di akhir",
        OTHER_OUT: "Tidak dihitung belanja",
        WAITING: "Menunggu pasangan",
        REMAINING: "Masih ada",
    }[node_id]


def _node_kind(node_id: str) -> str:
    kind = node_id.split(":")[0]
    if kind == "acc":
        return "account"
    if kind == "sav":
        return "saving"
    if kind == "inv":
        return "investment"
    if kind == "pillar":
        return "pillar"
    if kind == "src" or node_id in (OPENING, NEGATIVE_CLOSING):
        return "source"
    return {NEGATIVE_OPENING: "negative_opening", OTHER_OUT: "other_out", WAITING: "waiting", REMAINING: "remaining"}[node_id]


def _node_rank(node_id: str, accounts: dict[str, dict[str, Any]], account_order: dict[str, int], income_order: dict[str, int]) -> int:
    kind, _, ident = node_id.partition(":")
    if kind in ("acc", "sav", "inv"):
        return account_order.get(ident, 999)
    if kind == "pillar":
        return PILLAR_ORDER.index(ident)
    if kind == "src":
        return income_order.get(node_id, 900)
    return {OPENING: 950, NEGATIVE_CLOSING: 960, NEGATIVE_OPENING: 970, OTHER_OUT: 10, WAITING: 20, REMAINING: 30}.get(node_id, 999)


@dataclass
class FlowContext:
    accounts: dict[str, dict[str, Any]]
    rows: list[dict[str, Any]]
    waiting: dict[str, str]
    opening: dict[str, int]
    start: datetime
    end: datetime
    start_date: date
    end_date: date
    through: date
    through_end: datetime


def load_flow_context(
    cur, user_id: str, *, start: datetime, end: datetime, start_date: date, end_date: date,
    through: date, include_opening: bool,
) -> FlowContext:
    """Reads what the map or its rows need: accounts, the window's rows, its waiting rows and, for the
    map, every account's balance at the window start. A constant number of statements."""
    accounts = _load_accounts(cur, user_id)
    rows = _load_window_rows(cur, user_id, start, end)
    waiting = {str(row["id"]): row["section"] for row in fetch_waiting_rows(cur, user_id, start, end)}
    opening = _load_opening(cur, user_id, start) if include_opening else {}
    through_end = min(end, local_day_end(through))
    return FlowContext(accounts, rows, waiting, opening, start, end, start_date, end_date, through, through_end)


def build_flow(
    cur, user_id: str, context: FlowContext, *, kakeibo_breakdown: KakeiboBreakdown,
) -> dict[str, Any]:
    """Nodes, links and the summary for the rows up to the end of `through`.

    The node set and its order come from the whole window, so replaying day by day never moves a node.
    """
    full = assign_rows(context.accounts, context.rows, context.waiting, context.start, context.end)
    _close_holders(full, context.accounts, context.opening)
    model = assign_rows(context.accounts, context.rows, context.waiting, context.start, context.through_end)
    balances = _close_holders(model, context.accounts, context.opening)
    source_names = {**full.sources, **model.sources}

    account_order = {account_id: index for index, account_id in enumerate(context.accounts)}
    income_totals: dict[str, int] = {}
    for link in full.links.values():
        if link.kind == "income":
            income_totals[link.source] = income_totals.get(link.source, 0) + link.amount
    income_order = {node: index for index, (node, _) in enumerate(sorted(income_totals.items(), key=lambda item: -item[1]))}

    node_ids = {link.source for link in full.links.values()} | {link.target for link in full.links.values()}
    holders = holder_of(context.accounts)
    members: dict[str, list[str]] = {}
    for account_id, holder in holders.items():
        if holder.partition(":")[2] != account_id:
            members.setdefault(holder, []).append(context.accounts[account_id]["name"])
    nodes = []
    for node_id in sorted(node_ids, key=lambda value: (_node_kind(value), _node_rank(value, context.accounts, account_order, income_order), value)):
        entry: dict[str, Any] = {
            "id": node_id,
            "kind": _node_kind(node_id),
            "label": _node_label(node_id, context.accounts, source_names),
            "rank": _node_rank(node_id, context.accounts, account_order, income_order),
        }
        kind, _, ident = node_id.partition(":")
        if kind in ("acc", "sav", "inv"):
            entry["account_id"] = ident
            entry["opening"] = balances.get(node_id, {}).get("opening", 0)
            entry["closing"] = balances.get(node_id, {}).get("closing", 0)
            entry["archived"] = bool(context.accounts[ident]["is_archived"])
            entry["members"] = members.get(node_id, [])
        if kind == "pillar":
            entry["pillar"] = ident
        if node_id in (OPENING, REMAINING):
            entry["folded_by_default"] = True
        nodes.append(entry)

    links = sorted(model.links.values(), key=lambda link: (-link.amount, link.key))
    # Belanja, Masuk and Keluar follow the report rules on the same rows as analytics, so at the window
    # end they equal its figures even where a waiting leg is drawn apart from spending.
    in_range = [row for row in context.rows if row["date"] <= context.through_end]
    belanja_gross = sum(int(row["amount"]) for row in in_range if row["reportable_expense"])
    masuk = sum(int(row["amount"]) for row in in_range if row["reportable_income"])
    offsets = load_spending_offsets(cur, user_id, context.start, context.through_end)
    belanja = cash_flow_totals(masuk, belanja_gross, offsets.applied)
    keluar = int(kakeibo_breakdown(cur, user_id, context.start, context.through_end, masuk, offsets)["total_allocated"])
    spend_gross = sum(link.amount for link in model.links.values() if link.kind == "spend")

    def net_into(prefix: str) -> int:
        total = 0
        for link in model.links.values():
            if link.kind not in ("saving", "investment"):
                continue
            if link.target.startswith(prefix) and not link.source.startswith(prefix):
                total += link.amount
            elif link.source.startswith(prefix) and not link.target.startswith(prefix):
                total -= link.amount
        return total

    summary = {
        "masuk": masuk,
        "spend_gross": spend_gross,
        "belanja_gross": belanja_gross,
        "reimbursement_offset": belanja["reimbursement_offset"],
        "belanja": belanja["outflow"],
        "keluar": keluar,
        "saving_net": net_into("sav:"),
        "investment_net": net_into("inv:"),
        "waiting_out": sum(link.amount for link in model.links.values() if link.target == WAITING),
        "waiting_in": sum(link.amount for link in model.links.values() if link.source == WAITING),
        "waiting_in_buckets": sum(link.waiting_amount for link in model.links.values() if link.kind == "spend"),
        "remaining": sum(link.amount for link in model.links.values() if link.target == REMAINING),
        "other_out": sum(link.amount for link in model.links.values() if link.target == OTHER_OUT),
    }
    return {
        "window": {
            "start": context.start_date.isoformat(),
            "end": context.end_date.isoformat(),
            "through": context.through.isoformat(),
        },
        "nodes": nodes,
        "links": [
            {
                "key": link.key, "from": link.source, "to": link.target, "kind": link.kind,
                "amount": link.amount, "count": link.count,
                "waiting_count": link.waiting_count, "waiting_amount": link.waiting_amount,
            }
            for link in links
        ],
        "summary": summary,
    }


def _check_key(accounts: dict[str, dict[str, Any]], node_id: str) -> None:
    if not NODE_ID_PATTERN.match(node_id):
        raise FlowKeyError("flow_key_invalid", 422)
    kind, _, ident = node_id.partition(":")
    if kind in ("acc", "sav", "inv") and ident not in accounts:
        raise FlowKeyError("flow_link_not_found", 404)


def flow_rows(context: FlowContext, *, link: str | None, node: str | None, offset: int, limit: int) -> dict[str, Any]:
    """The rows behind one link or one node, with their total count and amount (D9)."""
    if (link is None) == (node is None):
        raise FlowKeyError("flow_key_invalid", 422)
    model = assign_rows(context.accounts, context.rows, context.waiting, context.start, context.through_end)
    if link is not None:
        source, separator, target = link.partition(">")
        if not separator:
            raise FlowKeyError("flow_key_invalid", 422)
        _check_key(context.accounts, source)
        _check_key(context.accounts, target)
        if {source, target} & BALANCE_NODES:
            # Opening and closing balances are explained by the drawer, not listed as rows.
            return {"rows": [], "total_count": 0, "total_amount": 0, "offset": offset, "limit": limit}
        if link not in model.links:
            raise FlowKeyError("flow_link_not_found", 404)
        wanted = {link}
    else:
        _check_key(context.accounts, node)
        wanted = {key for key, item in model.links.items() if node in (item.source, item.target)}
        if not wanted and node not in BALANCE_NODES:
            raise FlowKeyError("flow_link_not_found", 404)
    by_id = {str(row["id"]): row for row in context.rows}
    selected = [assignment for assignment in model.assignments if assignment.link_key in wanted]
    selected.sort(key=lambda item: (by_id[item.row_id]["date"], item.row_id), reverse=True)
    zone = ZoneInfo(settings.tz)
    page = []
    for assignment in selected[offset:offset + limit]:
        row = by_id[assignment.row_id]
        page.append({
            "id": assignment.row_id,
            "link": assignment.link_key,
            "type": row["type"],
            "amount": assignment.amount,
            "row_amount": int(row["amount"]),
            "date": row["date"].isoformat(),
            "local_date": row["date"].astimezone(zone).date().isoformat(),
            "account_id": str(row["account_id"]),
            "account_name": context.accounts[str(row["account_id"])]["name"],
            "category_name": row["category_name"],
            "merchant": row["merchant"],
            "notes": row["notes"],
            "movement_id": str(row["movement_id"]) if row["movement_id"] else None,
            "waiting": assignment.row_id in context.waiting,
        })
    return {
        "rows": page,
        "total_count": len(selected),
        "total_amount": sum(item.amount for item in selected),
        "offset": offset,
        "limit": limit,
    }


def clamp_through(through: date | None, start_date: date, end_date: date, today: date) -> date:
    """`through` defaults to today in the current period and to the window end otherwise."""
    if through is None:
        return today if start_date <= today <= end_date else end_date
    if through < start_date or through > end_date:
        raise FlowKeyError("through_outside_window", 422)
    return through

