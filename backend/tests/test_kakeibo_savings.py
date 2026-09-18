from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from app.routers.dashboard import get_kakeibo_breakdown
from app.routers.movements import _ensure_internal_movement_categories


def test_get_or_create_internal_categories_has_null_kakeibo():
    cur = MagicMock()
    user_id = str(uuid4())
    # First query returns no existing categories
    cur.fetchall.return_value = []
    # Both inserts return an ID
    cur.fetchone.side_effect = [{"id": str(uuid4())}, {"id": str(uuid4())}]

    exp_id, inc_id = _ensure_internal_movement_categories(cur, user_id)
    assert exp_id is not None
    assert inc_id is not None

    # Inspect executed queries
    insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO categories" in str(c)]
    assert len(insert_calls) == 2

    # Check expense category insertion: kakeibo_type is NULL
    exp_insert_sql = insert_calls[0][0][0]
    assert "'expense', NULL, TRUE" in exp_insert_sql
    assert "kakeibo_type = NULL" in exp_insert_sql

    # Check income category insertion: kakeibo_type is NULL
    inc_insert_sql = insert_calls[1][0][0]
    assert "'income', NULL, TRUE" in inc_insert_sql


def test_kakeibo_breakdown_excludes_liquid_moves_and_preauth():
    cur = MagicMock()
    user_id = str(uuid4())
    start_utc = datetime(2026, 8, 29, 0, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2026, 9, 28, 23, 59, 59, tzinfo=timezone.utc)

    # 1. pillar_map mock (expenses by pillar, excluding internal movements)
    # 2. saving_transfers mock
    cur.fetchall.return_value = [
        {"pillar": "need", "total_amount": 951903},
        {"pillar": "want", "total_amount": 70400},
    ]
    cur.fetchone.return_value = {"saving_transfers": 0}

    res = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow=290050)

    assert res["need_spent"] == 951903
    assert res["want_spent"] == 70400
    assert res["saving_spent"] == 0
    assert res["total_allocated"] == 951903 + 70400

    # Verify query uses COALESCE on notes and excludes pre-auth
    query_call = cur.execute.call_args_list[1]
    query_sql = query_call[0][0]
    assert "COALESCE(t.notes, '') NOT LIKE" in query_sql
    assert "COALESCE(t.notes, '') NOT ILIKE '%%pre-auth%%'" in query_sql


def test_kakeibo_breakdown_includes_true_saving_transfers():
    cur = MagicMock()
    user_id = str(uuid4())
    start_utc = datetime(2026, 8, 29, 0, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2026, 9, 28, 23, 59, 59, tzinfo=timezone.utc)

    # 1. pillar_map mock
    cur.fetchall.return_value = [
        {"pillar": "need", "total_amount": 500000},
        {"pillar": "want", "total_amount": 200000},
    ]
    # 2. Genuine transfer to savings/investment account: Rp 300.000
    cur.fetchone.return_value = {"saving_transfers": 300000}

    res = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow=1000000)

    assert res["need_spent"] == 500000
    assert res["want_spent"] == 200000
    assert res["saving_spent"] == 300000
    assert res["total_allocated"] == 1000000
    assert res["need_pct"] == 50.0
    assert res["want_pct"] == 20.0
    assert res["saving_pct"] == 30.0
