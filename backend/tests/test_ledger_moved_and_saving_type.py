"""Transaksi "Tabungan & investasi" with every top-up, and the summary's `moved` (improve-ledger-readability
decisions 3 and 4), on fresh synthetic owners.

Every name and amount is synthetic; the two movements of the "Pindah saldo" hero are the spec's example.
"""
from __future__ import annotations

from datetime import timedelta

from test_cycle_summary import make_owner, now_utc, owner, unlimited_rate_limits  # noqa: F401 - fixtures
from test_ledger_saving_flow import listing, saver, topup  # noqa: F401 - fixtures

MOVED_FIRST = 247_856
MOVED_SECOND = 648_026


def all_pages(owner, query, limit):
    pages, offset = [], 0
    while True:
        page = listing(owner, f"{query}&include=daily_totals&limit={limit}&offset={offset}")
        pages.append(page)
        offset += limit
        if offset >= page["total"]:
            return pages


def day_movements(pages) -> int:
    """The sum of each distinct day's `movements`; a day split across pages carries the same figures on each."""
    days = {}
    for page in pages:
        for day in page["daily_totals"]:
            assert days.setdefault(day["date"], day) == day, day["date"]
    return sum(day["movements"] for day in days.values())


# --- Ledger Type and Waiting Filters: top-ups under Tabungan & investasi -------------------------------------


def test_every_topup_is_listed_under_saving_and_saving_net_is_unchanged(saver):
    saved = saver.move("bank", "pocket", 1_000_000)["movement_id"]
    # Investment cash changes no saving figure and is never listed under Tabungan & investasi.
    saver.move("bank", "rdn", 600_000)
    before = listing(saver, "type=saving&include=type_counts")
    assert {row["movement_id"] for row in before["transactions"]} == {saved}
    assert before["summary"]["cumulative"]["saving_net"] == 1_000_000

    from_rdn = topup(saver, "rdn", "fund", 500_000)
    from_savings = topup(saver, "pocket", "fund", 100_000)
    after = listing(saver, "type=saving&include=type_counts")

    by_movement = {}
    for row in after["transactions"]:
        by_movement.setdefault(row["movement_id"], []).append(row["saving_flow"])
    assert by_movement == {saved: ["saved", "saved"], from_rdn: ["invested", "invested"], from_savings: [None, None]}
    # The top-up funded from savings changes no figure, so Beranda's "Ditabung & investasi" counts only the
    # RDN top-up, exactly as before this change.
    assert after["summary"]["cumulative"]["saving_net"] == 1_000_000 + 500_000
    assert after["type_counts"]["saving"] == after["total"] == 6

    investment = listing(saver, "type=investment&include=type_counts")
    assert {row["movement_id"] for row in investment["transactions"]} == {from_rdn, from_savings}
    assert investment["type_counts"]["investment"] == investment["total"] == 4


def test_topup_is_one_row_under_saving_with_logical_movements(saver):
    saved = saver.move("bank", "pocket", 200_000, when=now_utc() - timedelta(minutes=10))["movement_id"]
    from_savings = topup(saver, "pocket", "fund", 100_000)
    page = listing(saver, "type=saving&logical_movements=true&include=type_counts")
    assert [(row["movement_id"], row["movement_role"]) for row in page["transactions"]] == [
        (from_savings, "outbound"), (saved, "outbound"),
    ]
    assert page["type_counts"]["saving"] == page["total"] == 2
    assert page["type_counts"]["investment"] == 1


# --- Hero for Masuk and Pindah Saldo: summary `moved` ---------------------------------------------------------


def test_moved_counts_each_movement_once_and_equals_the_day_totals(saver):
    saver.move("bank", "wallet", MOVED_FIRST, when=now_utc() - timedelta(minutes=30))
    saver.move("bank", "wallet", MOVED_SECOND, when=now_utc() - timedelta(minutes=20))
    saver.expense(45_000, "makan")
    saver.tx("income", 300_000, category="gaji")

    for logical in ("true", "false"):
        query = f"type=movement&logical_movements={logical}"
        pages = all_pages(saver, query, 1)
        summary = pages[0]["summary"]
        assert summary["cumulative"]["moved"] == MOVED_FIRST + MOVED_SECOND
        assert day_movements(pages) == summary["cumulative"]["moved"]
        assert summary["cycle"]["moved"] == MOVED_FIRST + MOVED_SECOND
        assert summary["period"] is None
        # The Pindah saldo hero's figure only: inflow and outflow keep their meaning.
        assert (summary["cumulative"]["inflow"], summary["cumulative"]["outflow"]) == (0, 0)

    everything = listing(saver, "logical_movements=true")["summary"]
    assert everything["cumulative"]["moved"] == MOVED_FIRST + MOVED_SECOND
    assert everything["cumulative"]["inflow"] == 300_000 and everything["cumulative"]["outflow"] == 45_000
    assert listing(saver, "type=expense")["summary"]["cumulative"]["moved"] == 0
    assert listing(saver, "type=income")["summary"]["cycle"]["moved"] == 0


def test_moved_of_each_block_covers_its_own_window(saver):
    saver.move("bank", "wallet", MOVED_FIRST, when=now_utc() - timedelta(days=70))
    saver.move("bank", "wallet", MOVED_SECOND, when=now_utc() - timedelta(minutes=20))

    whole = listing(saver, "type=movement&logical_movements=true")["summary"]
    assert whole["cumulative"]["moved"] == MOVED_FIRST + MOVED_SECOND
    # Without a confirmed salary the cycle block is the current calendar month.
    assert whole["cycle"]["moved"] == MOVED_SECOND

    pages = all_pages(saver, "type=movement&logical_movements=true&period=month", 1)
    period = pages[0]["summary"]["period"]
    assert period["moved"] == pages[0]["summary"]["cumulative"]["moved"] == MOVED_SECOND
    assert day_movements(pages) == period["moved"]
    assert period["label_end_date"] is None

    start = (now_utc() - timedelta(days=71)).isoformat().replace("+", "%2B")
    end = (now_utc() - timedelta(days=69)).isoformat().replace("+", "%2B")
    dated = listing(saver, f"type=movement&logical_movements=true&from_date={start}&to_date={end}")["summary"]
    assert dated["period"]["moved"] == dated["cumulative"]["moved"] == MOVED_FIRST
    assert dated["cycle"]["moved"] == 0


def test_moved_reads_only_the_owners_movements(saver, make_owner):
    other = make_owner()
    other.move("bank", "wallet", 900_000)
    saver.move("bank", "wallet", MOVED_FIRST)
    summary = listing(saver, "logical_movements=true")["summary"]
    assert summary["cumulative"]["moved"] == summary["cycle"]["moved"] == MOVED_FIRST
