from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import uuid
import pytest
from fastapi.testclient import TestClient

from app.routers.dashboard import (
    calc_delta_pct,
    format_idr_compact,
    generate_financial_narrative,
    get_kakeibo_breakdown,
)


def test_delta_percentage_calculator():
    # Regular positive growth
    assert calc_delta_pct(10_000_000, 8_000_000) == 25.0
    # Negative reduction
    assert calc_delta_pct(6_000_000, 8_000_000) == -25.0
    # Zero previous with current positive
    assert calc_delta_pct(5_000_000, 0) == 100.0
    # Zero current and zero previous
    assert calc_delta_pct(0, 0) == 0.0


def test_compact_idr_formatting():
    assert format_idr_compact(10_000_000) == "10.000.000"
    assert format_idr_compact(450_000) == "450.000"
    assert format_idr_compact(0) == "0"


def test_financial_narrative_generator():
    # Scenario: Spend jumped from 8m to 10m (+25%) with "Makan & Minum" (+75%)
    narrative, trend = generate_financial_narrative(
        current_outflow=10_000_000,
        prev_outflow=8_000_000,
        outflow_delta_pct=25.0,
        current_net=-2_000_000,
        top_driver_name="Makan & Minum",
        top_driver_delta_pct=75.0,
    )
    assert trend == "MENINGKAT"
    assert "naik 25.0%" in narrative
    assert "Rp 10.000.000" in narrative
    assert "Makan & Minum (+75.0%)" in narrative
    assert "defisit Rp 2.000.000" in narrative

    # Scenario: Spend decreased
    narrative_down, trend_down = generate_financial_narrative(
        current_outflow=7_000_000,
        prev_outflow=10_000_000,
        outflow_delta_pct=-30.0,
        current_net=3_000_000,
        top_driver_name="Belanja",
        top_driver_delta_pct=-50.0,
    )
    assert trend_down == "MENURUN"
    assert "turun 30.0%" in narrative_down
    assert "surplus Rp 3.000.000" in narrative_down


def test_kakeibo_breakdown_calculation():
    user_id = str(uuid.uuid4())
    start = datetime(2026, 8, 25, tzinfo=timezone.utc)
    end = datetime(2026, 9, 24, tzinfo=timezone.utc)

    mock_cur = MagicMock()
    # 1. Expenses by pillar: need: 4.5m, want: 3.5m, saving: 0
    mock_cur.fetchall.return_value = [
        {"pillar": "need", "total_amount": 4_500_000},
        {"pillar": "want", "total_amount": 3_500_000},
    ]
    # 2. Saving transfers: 2.0m
    mock_cur.fetchone.return_value = {"saving_transfers": 2_000_000}

    total_inflow = 10_000_000
    res = get_kakeibo_breakdown(mock_cur, user_id, start, end, total_inflow)

    assert res["need_spent"] == 4_500_000
    assert res["need_pct"] == 45.0
    assert res["want_spent"] == 3_500_000
    assert res["want_pct"] == 35.0
    assert res["saving_spent"] == 2_000_000
    assert res["saving_pct"] == 20.0
    assert res["want_status"] == "warning"  # > 30%
    assert res["kakeibo_status"] == "warning"


def test_dashboard_overview_kakeibo_and_comparison_endpoint():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        user_id = str(uuid.uuid4())
        mock_user = {
            "id": user_id,
            "username": "tester",
            "currency": "IDR",
            "payday_day": 25,
            "emergency_fund_multiplier": 6,
            "monthly_spending_budget": 10_000_000,
        }
        app.dependency_overrides[get_current_user] = lambda: mock_user

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("app.routers.dashboard.db_conn", return_value=mock_conn), \
             patch("app.routers.dashboard.get_accounts_with_balances") as mock_accs:

            mock_accs.return_value = [
                {"id": str(uuid.uuid4()), "name": "BCA Main", "type": "bank", "balance": 15_000_000, "parent_id": None}
            ]

            # Sequential DB calls inside get_dashboard_overview:
            # 1. totals_row: inflow 10m, outflow 8m, tx_count 12
            # 2. calculate_ketahanan_dana: goals fetchall
            # 3. primary_spent_30d
            # 4. primary_budget
            # 5. obligations
            # 6. cat_budget
            # 7. upcoming recurring rules: 500k upcoming
            # 8. grouped_days (trendline)
            # 9. cat_rows (current categories)
            # 10. prev_totals_row (inflow 9m, outflow 6m, tx_count 10)
            # 11. prev_cat_map (previous categories)
            # 12. kakeibo expenses fetchall
            # 13. kakeibo saving transfers fetchone
            # 14. goals glance fetchall
            # 15. obligations glance fetchall
            # 16. recent txs fetchall

            mock_cur.fetchone.side_effect = [
                {"inflow": 10_000_000, "outflow": 8_000_000, "tx_count": 12},  # totals_row
                {"primary_spent_30d": 4_000_000},                              # primary_spent_30d
                {"primary_budget": 4_000_000},                                # primary_budget
                {"monthly_commitments": 0},                                   # obligations
                {"total_budget": 10_000_000},                                 # cat_budget
                {"upcoming_amount": 500_000},                                 # upcoming recurring rules
                {"inflow": 9_000_000, "outflow": 6_400_000, "tx_count": 10},  # prev_totals_row
                {"saving_transfers": 1_000_000},                              # kakeibo saving transfers
            ]

            food_cid = str(uuid.uuid4())
            mock_cur.fetchall.side_effect = [
                [],                                                           # goals for ketahanan dana
                [],                                                           # grouped_days
                [                                                             # cat_rows
                    {
                        "id": food_cid,
                        "name": "Makan & Minum",
                        "icon": "utensils",
                        "color": "#f97316",
                        "monthly_budget": 5_000_000,
                        "is_primary": True,
                        "kakeibo_type": "need",
                        "spent": 5_000_000,
                    }
                ],
                [{"id": food_cid, "name": "Makan & Minum", "spent": 3_000_000}], # prev_cat_map
                [                                                             # kakeibo expenses
                    {"pillar": "need", "total_amount": 5_000_000},
                    {"pillar": "want", "total_amount": 3_000_000},
                ],
                [],                                                           # goals glance
                [],                                                           # obligations glance
                [],                                                           # recent txs
            ]

            with TestClient(app) as client:
                res = client.get("/api/dashboard/overview?timeframe=cycle&cycle_offset=0")
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True

                # Check comparison
                comp = data["comparison"]
                assert comp["outflow_delta_pct"] == 25.0  # (8m - 6.4m)/6.4m = +25%
                assert comp["inflow_delta_pct"] == 11.1   # (10m - 9m)/9m = +11.1%

                # Check Kakeibo
                kakeibo = data["kakeibo"]
                assert kakeibo["need_spent"] == 5_000_000
                assert kakeibo["need_pct"] == 50.0
                assert kakeibo["want_spent"] == 3_000_000
                assert kakeibo["want_pct"] == 30.0
                assert kakeibo["saving_spent"] == 1_000_000
                assert kakeibo["saving_pct"] == 10.0
                assert kakeibo["want_status"] == "healthy"

                # Check narrative
                narrative = data["narrative"]
                assert narrative["trend_status"] == "MENINGKAT"
                assert "Makan & Minum" in narrative["summary"]
                assert "naik 25.0%" in narrative["summary"]

                # Check spend velocity
                assert "spend_velocity" in data
                assert data["kpis"]["upcoming_recurring_expense"] == 500_000

        app.dependency_overrides.clear()


def test_category_kakeibo_crud():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        user_id = str(uuid.uuid4())
        app.dependency_overrides[get_current_user] = lambda: {"id": user_id, "username": "tester"}

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("app.routers.categories.db_conn", return_value=mock_conn):
            cat_id = str(uuid.uuid4())
            # 1. create category
            mock_cur.fetchone.side_effect = [
                None,  # name conflict check
                {
                    "id": cat_id,
                    "name": "Kopi & Kafe",
                    "icon": "coffee",
                    "color": "#8b5cf6",
                    "kind": "expense",
                    "monthly_budget": 500_000,
                    "is_primary": False,
                    "kakeibo_type": "want",
                    "is_archived": False,
                    "created_at": datetime.now(timezone.utc),
                },
            ]

            with TestClient(app) as client:
                res = client.post(
                    "/api/categories",
                    json={
                        "name": "Kopi & Kafe",
                        "icon": "coffee",
                        "color": "#8b5cf6",
                        "kind": "expense",
                        "monthly_budget": 500_000,
                        "is_primary": False,
                        "kakeibo_type": "want",
                    },
                )
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True
                assert data["category"]["kakeibo_type"] == "want"

        app.dependency_overrides.clear()


def test_transaction_kakeibo_support():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        user_id = str(uuid.uuid4())
        app.dependency_overrides[get_current_user] = lambda: {"id": user_id, "username": "tester"}

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("app.routers.transactions.db_conn", return_value=mock_conn):
            tx_id = str(uuid.uuid4())
            acc_id = str(uuid.uuid4())
            cat_id = str(uuid.uuid4())

            mock_cur.fetchone.side_effect = [
                {"id": cat_id, "kind": "expense"},
                {"id": tx_id, "created_at": datetime.now(timezone.utc)},
            ]
            mock_cur.fetchall.side_effect = [
                [{"id": acc_id, "name": "Cash", "parent_id": None, "type": "cash", "default_pocket_id": None}],
                [],
            ]
            with patch("app.routers.transactions.lock_owned_accounts", return_value={acc_id: {"id": acc_id, "type": "cash"}}), \
                 patch("app.routers.transactions.ensure_sufficient_funds", return_value=50000):
                with TestClient(app) as client:
                    res = client.post(
                        "/api/transactions",
                        json={
                            "account_id": acc_id,
                            "category_id": cat_id,
                            "type": "expense",
                            "amount": 45_000,
                            "notes": "Latte",
                            "kakeibo_type": "want",
                        },
                    )
                    assert res.status_code == 200
                    data = res.json()
                    assert data["ok"] is True
                    assert data["transaction_id"] == tx_id

        app.dependency_overrides.clear()


def test_burn_rate_cycle_forecast_and_unrounded_projection():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        user_id = str(uuid.uuid4())
        app.dependency_overrides[get_current_user] = lambda: {
            "id": user_id,
            "username": "tester",
            "currency": "IDR",
            "payday_day": 25,
        }

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch("app.routers.dashboard.db_conn", return_value=mock_conn):
            # 1. Day of week heatmap
            mock_cur.fetchall.side_effect = [
                [],  # dow_rows
                [],  # daily_rows
                [],  # cat_rows
                [],  # prev_cat_map
                [],  # kakeibo expenses
            ]
            # fetchone side_effects for analytics queries:
            mock_cur.fetchone.side_effect = [
                {"spent_7d": 50_000, "spent_30d": 50_000},  # burn_row
                {"inflow": 0, "outflow": 50_000},            # totals_row
                {"inflow": 0, "outflow": 0},                 # prev_totals_row
                {"saving_transfers": 0},                     # saving_transfers in kakeibo
            ]

            with TestClient(app) as client:
                res = client.get("/api/dashboard/analytics?timeframe=cycle&cycle_offset=0")
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True
                burn = data["burn_rate"]

                # Check unrounded 30d projection (must be 50_000, not 50_010!)
                assert burn["daily_burn_7d"] == 7_143
                assert burn["daily_burn_30d"] == 1_667
                assert burn["projected_30d_outflow"] == 50_000
                assert burn["projected_30d_outflow"] != 50_010

                # Check cycle forecast metadata
                assert "projected_cycle_outflow" in burn
                assert burn["projected_cycle_outflow"] > 0
                assert burn["cycle_elapsed_days"] >= 1
                assert burn["cycle_total_days"] >= 28
                assert "cycle_end_date" in burn

        app.dependency_overrides.clear()
