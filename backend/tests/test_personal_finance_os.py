import datetime
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.routers.dashboard import get_timeframe_window
from app.routers.goals import format_goal_row
from app.routers.obligations import format_obligation_row


def test_format_goal_row_pacing_and_percentage():
    today = datetime.date.today()
    # 6 months in the future
    future_date = datetime.date(today.year if today.month <= 6 else today.year + 1, (today.month + 5) % 12 + 1, 15)

    raw_goal = {
        "id": "11111111-1111-1111-1111-111111111111",
        "name": "Emergency Fund",
        "target_amount": 10000000,
        "current_amount": 4000000,
        "target_date": future_date,
        "color": "#10b981",
        "icon": "shield",
        "is_archived": False,
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "updated_at": datetime.datetime.now(datetime.timezone.utc),
    }

    formatted = format_goal_row(raw_goal)
    assert formatted["percentage_completed"] == 40.0
    assert formatted["remaining_amount"] == 6000000
    assert formatted["monthly_target_pace"] is not None
    assert formatted["monthly_target_pace"] > 0


def test_format_obligation_row_payoff_timeline():
    raw_obligation = {
        "id": "22222222-2222-2222-2222-222222222222",
        "name": "Laptop Loan",
        "total_amount": 12000000,
        "remaining_amount": 6000000,
        "due_date": datetime.date(2026, 12, 31),
        "minimum_payment": 1000000,
        "notes": "0% installment",
        "is_archived": False,
        "created_at": datetime.datetime.now(datetime.timezone.utc),
        "updated_at": datetime.datetime.now(datetime.timezone.utc),
    }

    formatted = format_obligation_row(raw_obligation)
    assert formatted["paid_amount"] == 6000000
    assert formatted["payoff_percentage"] == 50.0
    assert formatted["estimated_payoff_months"] == 6


def test_get_timeframe_window_options():
    ref_date = datetime.date(2026, 9, 10)
    # Test 30d
    s_utc, e_utc, s_d, e_d, label = get_timeframe_window(25, "30d", ref_date)
    assert (e_d - s_d).days == 29
    assert label == "30 Hari Terakhir"

    # Test 90d
    s_utc, e_utc, s_d, e_d, label = get_timeframe_window(25, "90d", ref_date)
    assert (e_d - s_d).days == 89
    assert label == "90 Hari Terakhir"

    # Test cycle
    s_utc, e_utc, s_d, e_d, label = get_timeframe_window(25, "cycle", ref_date)
    assert s_d == datetime.date(2026, 8, 25)
    assert e_d == datetime.date(2026, 9, 24)
    assert "Periode" in label


def test_dashboard_and_domain_routes():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "33333333-3333-3333-3333-333333333333",
            "username": "executive_user",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.goals.db_conn") as mock_goals_conn, \
             patch("app.routers.obligations.db_conn") as mock_ob_conn:

            # Mock goals list query
            mock_g_cur = MagicMock()
            mock_g_cur.fetchall.return_value = []
            mock_goals_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_g_cur

            # Mock obligations list query
            mock_o_cur = MagicMock()
            mock_o_cur.fetchall.return_value = []
            mock_ob_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_o_cur

            with TestClient(app) as client:
                res_goals = client.get("/api/goals")
                assert res_goals.status_code == 200
                assert res_goals.json()["ok"] is True
                assert "goals" in res_goals.json()

                res_ob = client.get("/api/obligations")
                assert res_ob.status_code == 200
                assert res_ob.json()["ok"] is True
                assert "obligations" in res_ob.json()

        app.dependency_overrides.clear()


def test_payday_29_and_31_cycles():
    from app.routers.pulse import get_cycle_window
    ref_date = datetime.date(2026, 9, 10)

    # Payday on 29th
    _, _, s_d, e_d, label = get_timeframe_window(29, "cycle", ref_date)
    assert s_d == datetime.date(2026, 8, 29)
    assert e_d == datetime.date(2026, 9, 28)
    assert (e_d - s_d).days == 30  # 31 total days

    # Payday on 31st (in Sep with 30 days, payday falls on Sep 30, so cycle ends Sep 29)
    _, _, s_d, e_d, label = get_timeframe_window(31, "cycle", ref_date)
    assert s_d == datetime.date(2026, 8, 31)
    assert e_d == datetime.date(2026, 9, 29)


def test_settings_routes_and_method_parity():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "44444444-4444-4444-4444-444444444444",
            "username": "payroll_user",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.auth.db_conn") as mock_auth_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = {
                "id": "44444444-4444-4444-4444-444444444444",
                "username": "payroll_user",
                "payday_day": 29,
                "currency": "IDR",
            }
            mock_auth_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

            with TestClient(app) as client:
                # Direct /api/settings PUT and PATCH
                res_put = client.put("/api/settings", json={"payday_day": 29})
                assert res_put.status_code == 200
                assert res_put.json()["user"]["payday_day"] == 29

                res_patch = client.patch("/api/settings", json={"payday_day": 29})
                assert res_patch.status_code == 200

                # /api/auth/settings PUT
                res_auth_put = client.put("/api/auth/settings", json={"payday_day": 29})
                assert res_auth_put.status_code == 200

                # Direct /api/me and /api/auth/me
                assert client.get("/api/me").status_code == 200
                assert client.get("/api/auth/me").status_code == 200

        app.dependency_overrides.clear()


def test_zero_balance_runway_calculations():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "55555555-5555-5555-5555-555555555555",
            "username": "zero_user",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        with TestClient(app) as client:
            # 1. Zero liquid balance - net-worth
            with patch("app.routers.dashboard.get_accounts_with_balances") as mock_accounts, \
                 patch("app.routers.dashboard.db_conn") as mock_dash_conn:
                mock_accounts.return_value = [
                    {"id": "a1", "name": "Cash", "type": "cash", "balance": 0, "is_archived": False}
                ]
                mock_cur = MagicMock()
                mock_cur.fetchall.return_value = []
                mock_cur.fetchone.return_value = {"spent_30d": 0}
                mock_dash_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

                res_nw = client.get("/api/dashboard/net-worth")
                assert res_nw.status_code == 200
                nw_json = res_nw.json()
                assert nw_json["total_assets"] == 0
                assert nw_json["runway"]["runway_days"] == 0
                assert nw_json["runway"]["runway_months"] == 0.0
                assert nw_json["runway"]["status"] == "zero"

            # 2. Zero liquid balance - overview
            with patch("app.routers.dashboard.get_accounts_with_balances") as mock_accounts, \
                 patch("app.routers.dashboard.db_conn") as mock_dash_conn:
                mock_accounts.return_value = [
                    {"id": "a1", "name": "Cash", "type": "cash", "balance": 0, "is_archived": False}
                ]
                mock_cur = MagicMock()
                mock_cur.fetchall.return_value = []
                mock_cur.fetchone.return_value = {
                    "inflow": 0,
                    "outflow": 0,
                    "tx_count": 0,
                    "primary_spent_30d": 0,
                    "spent_30d": 0,
                    "primary_budget": 0,
                    "total_budget": 0,
                    "monthly_commitments": 0,
                }
                mock_dash_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

                res_ov = client.get("/api/dashboard/overview")
                assert res_ov.status_code == 200
                ov_json = res_ov.json()
                assert ov_json["kpis"]["liquid_net_worth"] == 0
                assert ov_json["kpis"]["runway_days"] == 0

            # 3. Positive balance with zero burn - net-worth
            with patch("app.routers.dashboard.get_accounts_with_balances") as mock_accounts, \
                 patch("app.routers.dashboard.db_conn") as mock_dash_conn:
                mock_accounts.return_value = [
                    {"id": "a1", "name": "BCA", "type": "bank", "balance": 10000000, "is_archived": False}
                ]
                mock_cur = MagicMock()
                mock_cur.fetchall.return_value = []
                mock_cur.fetchone.return_value = {
                    "spent_30d": 0,
                    "primary_spent_30d": 0,
                    "primary_budget": 0,
                    "monthly_commitments": 0,
                }
                mock_dash_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

                res_nw = client.get("/api/dashboard/net-worth")
                assert res_nw.status_code == 200
                nw_json = res_nw.json()
                assert nw_json["total_assets"] == 10000000
                assert nw_json["runway"]["runway_days"] == 999
                assert nw_json["runway"]["status"] == "healthy"

            # 4. Positive balance with zero burn - overview
            with patch("app.routers.dashboard.get_accounts_with_balances") as mock_accounts, \
                 patch("app.routers.dashboard.db_conn") as mock_dash_conn:
                mock_accounts.return_value = [
                    {"id": "a1", "name": "BCA", "type": "bank", "balance": 10000000, "is_archived": False}
                ]
                mock_cur = MagicMock()
                mock_cur.fetchall.return_value = []
                mock_cur.fetchone.return_value = {
                    "inflow": 0,
                    "outflow": 0,
                    "tx_count": 0,
                    "primary_spent_30d": 0,
                    "spent_30d": 0,
                    "primary_budget": 0,
                    "total_budget": 0,
                    "monthly_commitments": 0,
                }
                mock_dash_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

                res_ov = client.get("/api/dashboard/overview")
                assert res_ov.status_code == 200
                ov_json = res_ov.json()
                assert ov_json["kpis"]["liquid_net_worth"] == 10000000
                assert ov_json["kpis"]["runway_days"] == 999

        app.dependency_overrides.clear()


def test_goal_multi_account_linking_and_dynamic_balance():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "11111111-1111-1111-1111-111111111111",
            "username": "goal_user",
            "currency": "IDR",
            "payday_day": 25,
        }
        app.dependency_overrides[get_current_user] = lambda: mock_user

        acc1_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        acc2_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        goal_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"

        mock_accounts = [
            {"id": acc1_id, "name": "RDPU Bibit", "type": "bank", "balance": 5000000, "is_archived": False},
            {"id": acc2_id, "name": "Tabungan Emas", "type": "bank", "balance": 7000000, "is_archived": False},
        ]

        with patch("app.routers.goals.get_accounts_with_balances") as mock_get_acc, \
             patch("app.routers.goals.db_conn") as mock_conn:
            mock_get_acc.return_value = mock_accounts
            cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

            # 1. Goal with 2 linked accounts
            cur.fetchall.side_effect = [
                # list_goals: select goals
                [
                    {
                        "id": goal_id,
                        "name": "Dana Darurat",
                        "target_amount": 20000000,
                        "current_amount": 0,  # Stored amount is 0, but linked accounts total 12,000,000
                        "target_date": None,
                        "color": "#10b981",
                        "icon": "shield",
                        "is_archived": False,
                        "created_at": None,
                        "updated_at": None,
                    }
                ],
                # get_goal_accounts_map: select goal_accounts
                [
                    {"goal_id": goal_id, "account_id": acc1_id},
                    {"goal_id": goal_id, "account_id": acc2_id},
                ],
            ]

            with TestClient(app) as client:
                res = client.get("/api/goals")
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True
                assert len(data["goals"]) == 1
                g = data["goals"][0]
                assert g["name"] == "Dana Darurat"
                # Dynamic sum of 5,000,000 + 7,000,000 = 12,000,000
                assert g["current_amount"] == 12000000
                assert g["percentage_completed"] == 60.0
                assert g["remaining_amount"] == 8000000
                assert len(g["linked_accounts"]) == 2
                assert set(g["account_ids"]) == {acc1_id, acc2_id}

            # 2. Standalone goal without linked accounts
            standalone_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
            cur.fetchall.side_effect = [
                [
                    {
                        "id": standalone_id,
                        "name": "Virtual Target",
                        "target_amount": 10000000,
                        "current_amount": 3000000,
                        "target_date": None,
                        "color": "#3b82f6",
                        "icon": "target",
                        "is_archived": False,
                        "created_at": None,
                        "updated_at": None,
                    }
                ],
                [],  # no goal_accounts mappings
            ]

            # 3. Create goal with linked accounts
            cur.fetchone.side_effect = [
                None,  # name exists check
                {
                    "id": "new-goal-id",
                    "name": "Target Tabungan Baru",
                    "target_amount": 10000000,
                    "current_amount": 0,
                    "target_date": None,
                    "color": "#10b981",
                    "icon": "target",
                    "is_archived": False,
                    "created_at": None,
                    "updated_at": None,
                },
            ]
            cur.fetchall.side_effect = [
                [{"id": acc1_id}],  # validate accounts
                [{"goal_id": "new-goal-id", "account_id": acc1_id}],  # get_goal_accounts_map
            ]

            with TestClient(app) as client:
                res = client.post(
                    "/api/goals",
                    json={
                        "name": "Target Tabungan Baru",
                        "target_amount": 10000000,
                        "account_ids": [acc1_id],
                    },
                )
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True
                assert data["goal"]["name"] == "Target Tabungan Baru"
                assert data["goal"]["current_amount"] == 5000000
                assert data["goal"]["account_ids"] == [acc1_id]

        app.dependency_overrides.clear()


def test_hierarchical_accounts_and_pockets():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "22222222-2222-2222-2222-222222222222",
            "username": "pocket_user",
            "currency": "IDR",
            "payday_day": 25,
        }
        app.dependency_overrides[get_current_user] = lambda: mock_user

        parent_id = "11111111-1111-1111-1111-111111111111"
        child1_id = "22222222-2222-2222-2222-222222222222"
        child2_id = "33333333-3333-3333-3333-333333333333"
        standalone_id = "44444444-4444-4444-4444-444444444444"

        with patch("app.routers.accounts.db_conn") as mock_conn:
            cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

            # 1. Test get_accounts_with_balances aggregation & total_balance calculation
            cur.fetchall.return_value = [
                # Parent Account (Bank Jago), direct initial/tx balance 0
                {
                    "id": parent_id,
                    "parent_id": None,
                    "name": "Bank Jago",
                    "type": "bank",
                    "initial_balance": 0,
                    "is_archived": False,
                    "created_at": None,
                    "balance": 0,
                },
                # Child Pocket 1 (Kantong Utama), balance 5,000,000
                {
                    "id": child1_id,
                    "parent_id": parent_id,
                    "name": "Kantong Utama",
                    "type": "bank",
                    "initial_balance": 0,
                    "is_archived": False,
                    "created_at": None,
                    "balance": 5000000,
                },
                # Child Pocket 2 (Kantong Darurat), balance 15,000,000
                {
                    "id": child2_id,
                    "parent_id": parent_id,
                    "name": "Kantong Darurat",
                    "type": "bank",
                    "initial_balance": 0,
                    "is_archived": False,
                    "created_at": None,
                    "balance": 15000000,
                },
                # Standalone Account (Cash), balance 1,000,000
                {
                    "id": standalone_id,
                    "parent_id": None,
                    "name": "Cash",
                    "type": "cash",
                    "initial_balance": 1000000,
                    "is_archived": False,
                    "created_at": None,
                    "balance": 1000000,
                },
            ]

            with TestClient(app) as client:
                res = client.get("/api/accounts")
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True
                # Total balance should be Bank Jago (20m) + Cash (1m) = 21m (NOT 41m double counted!)
                assert data["total_balance"] == 21000000

                accounts = data["accounts"]
                jago = next(a for a in accounts if a["id"] == parent_id)
                assert jago["is_parent"] is True
                assert jago["balance"] == 20000000
                assert len(jago["children"]) == 2

                pocket1 = next(a for a in accounts if a["id"] == child1_id)
                assert pocket1["parent_id"] == parent_id
                assert pocket1["balance"] == 5000000
                assert pocket1["is_parent"] is False

            # 2. Test 2-level depth validation on create_account
            # Attempting to create a pocket whose parent is ALREADY a child pocket should be rejected with 400
            cur.fetchone.side_effect = [
                None,  # name exists check
                {"id": child1_id, "parent_id": parent_id},  # target parent query returns an existing child pocket!
            ]

            with TestClient(app) as client:
                res = client.post(
                    "/api/accounts",
                    json={
                        "name": "Sub-sub pocket",
                        "type": "bank",
                        "parent_id": child1_id,
                    },
                )
                assert res.status_code == 400
                assert "maximum 2 levels" in res.json()["detail"]

        app.dependency_overrides.clear()


def test_investment_account_crud_and_validation():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "77777777-7777-7777-7777-777777777777",
            "username": "investor_user",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.accounts.db_conn") as mock_conn:
            cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

            # 1. Invalid type "crypto" should return 422 Unprocessable Entity
            with TestClient(app) as client:
                res_bad = client.post(
                    "/api/accounts",
                    json={"name": "Indodax", "type": "crypto"},
                )
                assert res_bad.status_code == 422

            # 2. Valid type "investment" should succeed
            cur.fetchone.side_effect = [
                None,  # name exists check
                {
                    "id": "inv-1111",
                    "user_id": mock_user["id"],
                    "parent_id": None,
                    "name": "Bibit RDPU",
                    "type": "investment",
                    "initial_balance": 50000000,
                    "is_archived": False,
                    "created_at": datetime.datetime.now(datetime.timezone.utc),
                    "updated_at": datetime.datetime.now(datetime.timezone.utc),
                },
            ]

            with TestClient(app) as client:
                res_inv = client.post(
                    "/api/accounts",
                    json={"name": "Bibit RDPU", "type": "investment", "initial_balance": 50000000},
                )
                assert res_inv.status_code == 200
                data = res_inv.json()
                assert data["account"]["type"] == "investment"
                assert data["account"]["name"] == "Bibit RDPU"

        app.dependency_overrides.clear()


def test_goal_emergency_flag_crud():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "88888888-8888-8888-8888-888888888888",
            "username": "goal_user",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        # 1. format_goal_row checks
        raw_goal = {
            "id": "g-111",
            "name": "Dana Darurat 6 Bulan",
            "target_amount": 60000000,
            "current_amount": 30000000,
            "target_date": None,
            "color": "#10b981",
            "icon": "shield",
            "is_emergency": True,
            "is_archived": False,
            "created_at": datetime.datetime.now(datetime.timezone.utc),
            "updated_at": datetime.datetime.now(datetime.timezone.utc),
        }
        fmt = format_goal_row(raw_goal)
        assert fmt["is_emergency"] is True
        assert fmt["percentage_completed"] == 50.0

        # 2. Create goal with is_emergency=True
        with patch("app.routers.goals.db_conn") as mock_conn, \
             patch("app.routers.goals.get_goal_accounts_map", return_value={}):
            cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur
            cur.fetchone.side_effect = [None, raw_goal]

            with TestClient(app) as client:
                res = client.post(
                    "/api/goals",
                    json={
                        "name": "Dana Darurat 6 Bulan",
                        "target_amount": 60000000,
                        "is_emergency": True,
                    },
                )
                assert res.status_code == 200
                assert res.json()["goal"]["is_emergency"] is True

        app.dependency_overrides.clear()


def test_category_is_primary_flag_crud():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "99999999-9999-9999-9999-999999999999",
            "username": "cat_user",
            "currency": "IDR",
            "payday_day": 25,
        }

        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.categories.db_conn") as mock_conn:
            cur = MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cur

            # Create discretionary category (is_primary = False)
            cur.fetchone.side_effect = [
                None,  # duplicate name check
                {
                    "id": "cat-hobi",
                    "user_id": mock_user["id"],
                    "name": "Hobi & Gaming",
                    "kind": "expense",
                    "icon": "gamepad",
                    "color": "#ec4899",
                    "monthly_budget": 500000,
                    "is_primary": False,
                    "is_archived": False,
                    "created_at": datetime.datetime.now(datetime.timezone.utc),
                    "updated_at": datetime.datetime.now(datetime.timezone.utc),
                },
            ]

            with TestClient(app) as client:
                res = client.post(
                    "/api/categories",
                    json={
                        "name": "Hobi & Gaming",
                        "kind": "expense",
                        "monthly_budget": 500000,
                        "is_primary": False,
                    },
                )
                assert res.status_code == 200
                data = res.json()
                assert data["category"]["is_primary"] is False

        app.dependency_overrides.clear()


def test_calculate_ketahanan_dana_logic():
    from app.routers.dashboard import calculate_ketahanan_dana

    user_id = "user-test-ketahanan"

    # Setup accounts
    accounts = [
        {"id": "acc-rdpu", "name": "Bibit RDPU", "type": "investment", "balance": 30000000},
        {"id": "acc-depo", "name": "BCA Deposito", "type": "bank", "balance": 20000000},
        {"id": "acc-emas", "name": "Antam Gold", "type": "investment", "balance": 10000000},
        {"id": "acc-cash", "name": "Dompet Cash", "type": "cash", "balance": 2000000},
    ]
    total_liquid = 62000000

    cur = MagicMock()

    # Case 1: Flagged emergency goal linked to 3 accounts (30M + 20M + 10M = 60M)
    # Primary expense: 30d spend (8M) + obligations (2M) = 10M
    # Expected: 60M / 10M = 6.0 months -> status "healthy"
    cur.fetchall.side_effect = [
        # Goals query
        [
            {
                "id": "goal-darurat",
                "name": "Dana Darurat Keluarga",
                "current_amount": 0,
                "is_emergency": True,
            },
            {
                "id": "goal-liburan",
                "name": "Liburan Jepang",
                "current_amount": 5000000,
                "is_emergency": False,
            },
        ],
        # goal_accounts query
        [
            {"goal_id": "goal-darurat", "account_id": "acc-rdpu"},
            {"goal_id": "goal-darurat", "account_id": "acc-depo"},
            {"goal_id": "goal-darurat", "account_id": "acc-emas"},
        ],
    ]
    cur.fetchone.side_effect = [
        {"primary_spent_30d": 8000000},   # primary 30d spend
        {"primary_budget": 9000000},      # primary budget fallback
        {"monthly_commitments": 2000000}, # obligations
    ]

    r1 = calculate_ketahanan_dana(cur, user_id, total_liquid, accounts)
    assert r1["is_flagged"] is True
    assert r1["emergency_fund_balance"] == 60000000
    assert r1["monthly_primary_expense"] == 10000000
    assert r1["runway_months"] == 6.0
    assert r1["runway_days"] == 182
    assert r1["status"] == "healthy"

    # Case 2: Moderate coverage (15M / 5M = 3.0 months -> status "moderate")
    cur.fetchall.side_effect = [
        [{"id": "goal-darurat", "name": "Dana Darurat", "current_amount": 15000000, "is_emergency": True}],
        [],  # no linked accounts, uses current_amount
    ]
    cur.fetchone.side_effect = [
        {"primary_spent_30d": 5000000},
        {"primary_budget": 0},
        {"monthly_commitments": 0},
    ]

    r2 = calculate_ketahanan_dana(cur, user_id, 15000000, accounts)
    assert r2["emergency_fund_balance"] == 15000000
    assert r2["monthly_primary_expense"] == 5000000
    assert r2["runway_months"] == 3.0
    assert r2["status"] == "moderate"

    # Case 3: Critical coverage (5M / 5M = 1.0 month -> status "critical")
    cur.fetchall.side_effect = [
        [{"id": "goal-darurat", "name": "Dana Darurat", "current_amount": 5000000, "is_emergency": True}],
        [],
    ]
    cur.fetchone.side_effect = [
        {"primary_spent_30d": 5000000},
        {"primary_budget": 0},
        {"monthly_commitments": 0},
    ]

    r3 = calculate_ketahanan_dana(cur, user_id, 5000000, accounts)
    assert r3["runway_months"] == 1.0
    assert r3["status"] == "critical"

    # Case 4: Zero emergency fund balance -> status "zero"
    cur.fetchall.side_effect = [
        [{"id": "goal-darurat", "name": "Dana Darurat", "current_amount": 0, "is_emergency": True}],
        [],
    ]
    cur.fetchone.side_effect = [
        {"primary_spent_30d": 5000000},
        {"primary_budget": 0},
        {"monthly_commitments": 0},
    ]

    r4 = calculate_ketahanan_dana(cur, user_id, 0, accounts)
    assert r4["emergency_fund_balance"] == 0
    assert r4["runway_months"] == 0.0
    assert r4["runway_days"] == 0
    assert r4["status"] == "zero"


def test_format_goal_row_deduplication():
    from app.routers.goals import format_goal_row

    raw_goal = {
        "id": "goal-1",
        "name": "Target Tabungan",
        "target_amount": 20000000,
        "current_amount": 0,
        "target_date": None,
        "color": "#10b981",
        "icon": "target",
        "is_emergency": False,
        "is_archived": False,
        "created_at": None,
        "updated_at": None,
    }

    parent_acc = {
        "id": "parent-bibit",
        "name": "Bibit",
        "type": "investment",
        "balance": 11400615,
        "parent_id": None,
    }
    child1_acc = {
        "id": "child-sucor",
        "name": "Sucorinvest RDPU",
        "type": "investment",
        "balance": 5712112,
        "parent_id": "parent-bibit",
    }
    child2_acc = {
        "id": "child-trim",
        "name": "TRIM Kas",
        "type": "investment",
        "balance": 5688503,
        "parent_id": "parent-bibit",
    }

    # Case 1: Only a child pocket is linked (e.g. Dana Darurat under Jago)
    single_child = {
        "id": "child-darurat",
        "name": "Dana Darurat",
        "type": "bank",
        "balance": 3500000,
        "parent_id": "parent-jago",
    }
    res_child_only = format_goal_row(raw_goal, [single_child])
    assert res_child_only["current_amount"] == 3500000
    assert len(res_child_only["linked_accounts"]) == 1

    # Case 2: Parent AND both child pockets are in linked_accounts -> MUST NOT double-count!
    res_all = format_goal_row(raw_goal, [parent_acc, child1_acc, child2_acc])
    assert res_all["current_amount"] == 11400615
    # Contributing accounts only includes the parent
    assert len(res_all["linked_accounts"]) == 1
    assert res_all["linked_accounts"][0]["id"] == "parent-bibit"

    # Case 3: Parent AND one child pocket are in linked_accounts -> parent covers both
    res_parent_and_one = format_goal_row(raw_goal, [parent_acc, child1_acc])
    assert res_parent_and_one["current_amount"] == 11400615
    assert len(res_parent_and_one["linked_accounts"]) == 1

    # Case 4: Only the two child pockets (parent NOT linked) -> sums children
    res_children_only = format_goal_row(raw_goal, [child1_acc, child2_acc])
    assert res_children_only["current_amount"] == 5712112 + 5688503  # 11400615
    assert len(res_children_only["linked_accounts"]) == 2


def test_calculate_ketahanan_dana_deduplication():
    from app.routers.dashboard import calculate_ketahanan_dana

    cur = MagicMock()
    user_id = "user-dedup"

    parent_id = "p-bibit"
    child_id = "c-sucor"

    accounts = [
        {"id": parent_id, "name": "Bibit", "balance": 11400615, "parent_id": None},
        {"id": child_id, "name": "Sucor", "balance": 5712112, "parent_id": parent_id},
    ]

    # Goal has BOTH parent and child linked in goal_accounts
    cur.fetchall.side_effect = [
        [{"id": "g-darurat", "name": "Dana Darurat", "target_amount": 20000000, "current_amount": 0, "is_emergency": True}],
        [
            {"goal_id": "g-darurat", "account_id": parent_id},
            {"goal_id": "g-darurat", "account_id": child_id},
        ],
    ]
    cur.fetchone.side_effect = [
        {"primary_spent_30d": 5000000},
        {"primary_budget": 0},
        {"monthly_commitments": 0},
    ]

    res = calculate_ketahanan_dana(cur, user_id, 11400615, accounts)
    # Must equal parent balance (11,400,615), NOT double-counted with child (17,112,727)
    assert res["emergency_fund_balance"] == 11400615


def test_obligation_auto_archive_and_reversal_in_transactions():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "11111111-2222-3333-4444-555555555555",
            "username": "debt_tester",
            "currency": "IDR",
            "payday_day": 25,
        }
        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.transactions.db_conn") as mock_tx_conn:
            mock_cur = MagicMock()
            mock_tx_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

            # Mock account query for create_transaction
            mock_cur.fetchone.side_effect = [
                {"id": "cat-1", "kind": "expense"}, # category
                {"id": "ob-1"}, # obligation check
                {"kakeibo_type": "need", "is_primary": True}, # category kakeibo query
                {"id": "tx-123", "created_at": datetime.datetime.now(datetime.timezone.utc)}, # insert returning
            ]
            mock_cur.fetchall.side_effect = [
                [{"id": "11111111-1111-1111-1111-111111111111", "name": "BCA", "parent_id": None, "type": "cash", "default_pocket_id": None}],
                [],
            ]

            with TestClient(app) as client:
                # 1. Create transaction linked to obligation
                with patch("app.routers.transactions.lock_owned_accounts", return_value={"11111111-1111-1111-1111-111111111111": {"id": "11111111-1111-1111-1111-111111111111", "type": "cash"}}), \
                     patch("app.routers.transactions.ensure_sufficient_funds", return_value=500000):
                    res = client.post(
                    "/api/transactions",
                    json={
                        "account_id": "11111111-1111-1111-1111-111111111111",
                        "category_id": "22222222-2222-2222-2222-222222222222",
                        "obligation_id": "33333333-3333-3333-3333-333333333333",
                        "type": "expense",
                        "amount": 500000,
                        "date": "2026-09-21T08:00:00Z",
                    },
                    )
                assert res.status_code == 200

                # Verify UPDATE obligations SQL includes is_archived case expression
                update_calls = [
                    call for call in mock_cur.execute.call_args_list
                    if "UPDATE obligations" in str(call[0][0])
                ]
                assert len(update_calls) == 1
                sql, params = update_calls[0][0][0], update_calls[0][0][1]
                assert "is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END" in sql
                assert params[0] == 500000
                assert params[1] == 500000

                # 2. Delete transaction linked to obligation
                mock_cur.reset_mock()
                mock_cur.fetchone.side_effect = None
                mock_cur.fetchall.side_effect = None
                mock_cur.fetchall.return_value = []
                mock_cur.fetchone.return_value = {
                    "type": "expense",
                    "amount": 500000,
                    "goal_id": None,
                    "obligation_id": "33333333-3333-3333-3333-333333333333",
                }

                res_del = client.delete("/api/transactions/44444444-4444-4444-4444-444444444444")
                assert res_del.status_code == 200

                # Verify deletion reverses obligation and un-archives if balance > 0
                del_update_calls = [
                    call for call in mock_cur.execute.call_args_list
                    if "UPDATE obligations" in str(call[0][0])
                ]
                assert len(del_update_calls) == 1
                del_sql, del_params = del_update_calls[0][0][0], del_update_calls[0][0][1]
                assert "is_archived = CASE WHEN (remaining_amount + %s) > 0 THEN false ELSE is_archived END" in del_sql
                assert del_params[0] == 500000
                assert del_params[1] == 500000

        app.dependency_overrides.clear()


def test_list_and_update_obligations_auto_archive():
    with patch("app.main.open_db_pool"), patch("app.main.close_db_pool"), patch("app.main.init_db_schema"):
        from app.main import app
        from app.services.auth import get_current_user

        mock_user = {
            "id": "11111111-2222-3333-4444-555555555555",
            "username": "debt_tester",
            "currency": "IDR",
            "payday_day": 25,
        }
        app.dependency_overrides[get_current_user] = lambda: mock_user

        with patch("app.routers.obligations.db_conn") as mock_ob_conn:
            mock_cur = MagicMock()
            mock_ob_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cur

            # 1. Query active obligations
            mock_cur.fetchall.return_value = [
                {
                    "id": "ob-active",
                    "name": "Active Debt",
                    "total_amount": 1000000,
                    "remaining_amount": 500000,
                    "due_date": None,
                    "minimum_payment": 100000,
                    "notes": None,
                    "is_archived": False,
                    "created_at": datetime.datetime.now(datetime.timezone.utc),
                    "updated_at": datetime.datetime.now(datetime.timezone.utc),
                }
            ]

            with TestClient(app) as client:
                res = client.get("/api/obligations")
                assert res.status_code == 200
                data = res.json()
                assert data["ok"] is True
                assert len(data["obligations"]) == 1

                # Verify SQL filtered for remaining_amount > 0 and is_archived = false
                active_query_call = mock_cur.execute.call_args_list[0][0][0]
                assert "is_archived = false AND remaining_amount > 0" in active_query_call

                # 2. Update obligation to remaining_amount = 0 auto-archives
                mock_cur.reset_mock()
                mock_cur.fetchone.return_value = {
                    "id": "ob-active",
                    "name": "Active Debt",
                    "total_amount": 1000000,
                    "remaining_amount": 0,
                    "due_date": None,
                    "minimum_payment": 100000,
                    "notes": None,
                    "is_archived": True,
                    "created_at": datetime.datetime.now(datetime.timezone.utc),
                    "updated_at": datetime.datetime.now(datetime.timezone.utc),
                }

                res_patch = client.patch(
                    "/api/obligations/33333333-3333-3333-3333-333333333333",
                    json={"remaining_amount": 0},
                )
                assert res_patch.status_code == 200
                update_call = next(
                    call for call in mock_cur.execute.call_args_list
                    if "UPDATE obligations" in str(call[0][0])
                )
                update_sql, update_params = update_call[0]
                assert "is_archived = %s" in update_sql
                assert update_params[2] == 0

        app.dependency_overrides.clear()
