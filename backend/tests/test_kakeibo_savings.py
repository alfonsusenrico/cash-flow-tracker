from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from app.routers.dashboard import get_kakeibo_breakdown
from app.services.reimbursements import SpendingOffsets
from app.routers.movements import _ensure_internal_movement_categories


def test_get_or_create_internal_categories_has_null_kakeibo():
    cur = MagicMock()
    user_id = str(uuid4())
    # Neither keyed category exists, so each lookup is followed by an insert that returns an ID.
    cur.fetchone.side_effect = [None, {"id": str(uuid4())}, None, {"id": str(uuid4())}]

    exp_id, inc_id = _ensure_internal_movement_categories(cur, user_id)
    assert exp_id is not None
    assert inc_id is not None

    insert_calls = [c for c in cur.execute.call_args_list if "INSERT INTO categories" in str(c)]
    assert len(insert_calls) == 2
    for call, kind in zip(insert_calls, ("expense", "income")):
        sql, params = call[0]
        # Created with the movement key, excluded from budget and without a Kakeibo pillar.
        assert "%s, NULL, TRUE, %s" in sql and "kakeibo_type = NULL" in sql
        assert params[4:] == (kind, "internal_movement")


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
    cur.fetchone.side_effect = [{"into_savings": 0, "out_of_savings": 0}, {"paid_from_savings": 0}]

    res = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow=290050, offsets=SpendingOffsets())

    assert res["need_spent"] == 951903
    assert res["want_spent"] == 70400
    assert res["saving_spent"] == 0
    assert res["total_allocated"] == 951903 + 70400

    # Savings attribution follows linked bilateral movement roles and the accounts' current roles,
    # not note text and not the pillar stored on the leg (organize-accounts-by-use D3).
    query_call = cur.execute.call_args_list[1]
    query_sql = query_call[0][0]
    assert "outbound.movement_role = 'outbound'" in query_sql
    assert "inbound.movement_role = 'inbound'" in query_sql
    assert "sa.role IS NOT DISTINCT FROM 'tabungan'" in query_sql and "da.role IS NOT DISTINCT FROM 'tabungan'" in query_sql
    assert "outbound.kakeibo_type = 'saving'" not in query_sql


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
    # 2. Genuine transfer to savings/investment account: Rp 300.000; 3. nothing paid from savings
    cur.fetchone.side_effect = [{"into_savings": 300000, "out_of_savings": 0}, {"paid_from_savings": 0}]

    res = get_kakeibo_breakdown(cur, user_id, start_utc, end_utc, total_inflow=1000000, offsets=SpendingOffsets())

    assert res["need_spent"] == 500000
    assert res["want_spent"] == 200000
    assert res["saving_spent"] == 300000
    assert res["total_allocated"] == 1000000
    assert res["need_pct"] == 50.0
    assert res["want_pct"] == 20.0
    assert res["saving_pct"] == 30.0
