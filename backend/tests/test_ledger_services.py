import json
import os
import pathlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")
os.environ.setdefault("SESSION_SECRET", "test-secret")

if "fastapi" not in sys.modules:
    fastapi_stub = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi_stub.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi_stub

if "fpdf" not in sys.modules:
    fpdf_stub = types.ModuleType("fpdf")

    class FPDF:  # pragma: no cover - only used as import stub for unit tests
        pass

    fpdf_stub.FPDF = FPDF
    sys.modules["fpdf"] = fpdf_stub

from fastapi import HTTPException

from app.services.ledger import (
    compute_shortfall_at_transaction,
    compute_dynamic_month_range,
    get_balance_at_transaction,
    get_balance_before,
    parse_tx_datetime,
    parse_uuid_value,
    recompute_balances_report,
    write_transaction_audit,
)


class CursorSpy:
    def __init__(self) -> None:
        self.last_sql = ""
        self.last_params = None
        self.calls: list[tuple[str, tuple | list | None]] = []

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        self.calls.append((sql, params))

    def fetchone(self):
        return {"balance": 0}

    def fetchall(self):
        return []


class RecomputeCursor:
    def __init__(self) -> None:
        self.last_sql = ""
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params

    def fetchall(self):
        if "WITH tx_running" in self.last_sql:
            return [
                {
                    "account_id": "acc-1",
                    "account_name": "Cash",
                    "transactions_count": 2,
                    "current_balance": 700_000,
                    "min_balance": 0,
                    "first_negative_at": None,
                },
                {
                    "account_id": "acc-2",
                    "account_name": "Savings",
                    "transactions_count": 1,
                    "current_balance": -50_000,
                    "min_balance": -50_000,
                    "first_negative_at": datetime(2026, 2, 3, 10, 0, tzinfo=timezone.utc),
                },
            ]
        if "FROM accounts" in self.last_sql:
            return [
                {"account_id": "acc-1", "account_name": "Cash"},
                {"account_id": "acc-2", "account_name": "Savings"},
            ]
        if "FROM transactions" in self.last_sql:
            account_id = self.last_params[0]
            if account_id == "acc-1":
                return [
                    {
                        "transaction_id": "t1",
                        "date": datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
                        "transaction_type": "debit",
                        "amount": 1_000_000,
                    },
                    {
                        "transaction_id": "t2",
                        "date": datetime(2026, 2, 2, 10, 0, tzinfo=timezone.utc),
                        "transaction_type": "credit",
                        "amount": 300_000,
                    },
                ]
            return [
                {
                    "transaction_id": "t3",
                    "date": datetime(2026, 2, 3, 10, 0, tzinfo=timezone.utc),
                    "transaction_type": "credit",
                    "amount": 50_000,
                }
            ]
        return []


class DynamicRangeCursor:
    def __init__(self, first=None, second=None) -> None:
        self.rows = [first, second]
        self.calls: list[tuple[str, tuple | list | None]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        if not self.rows:
            return None
        return self.rows.pop(0)


class BalanceCursor(CursorSpy):
    def __init__(self, balance: int) -> None:
        super().__init__()
        self._balance = balance

    def fetchone(self):
        return {"balance": self._balance}


class LedgerServiceTests(unittest.TestCase):
    def test_parse_tx_datetime_normalizes_to_seconds_and_utc(self):
        dt = parse_tx_datetime("2026-02-11T10:15:12.987654+07:00")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.microsecond, 0)
        self.assertEqual(dt.isoformat(), "2026-02-11T03:15:12+00:00")

    def test_compute_dynamic_month_range_uses_topup_windows(self):
        next_topup = datetime(2026, 2, 26, 8, 0, tzinfo=timezone.utc)
        cur = DynamicRangeCursor(
            {"date": datetime(2026, 1, 26, 7, 0, tzinfo=timezone.utc)},
            {"date": next_topup},
        )

        from_date, to_date, from_dt, to_dt = compute_dynamic_month_range(cur, "alice", "2026-02", 25, 25)

        self.assertEqual(from_date, "2026-01-26")
        self.assertEqual(to_date, "2026-02-26")
        self.assertEqual(from_dt.isoformat(), "2026-01-26T07:00:00+00:00")
        self.assertEqual(to_dt, next_topup - timedelta(microseconds=1))
        self.assertEqual(len(cur.calls), 2)
        self.assertIn("JOIN accounts", cur.calls[0][0])
        self.assertIn("a.is_payroll_source = TRUE", cur.calls[0][0])
        self.assertIn("t.is_transfer = FALSE", cur.calls[0][0])
        self.assertIn("JOIN accounts", cur.calls[1][0])

    def test_compute_dynamic_month_range_fallback_open_cycle_to_today(self):
        cur = DynamicRangeCursor(None, None)
        with patch("app.services.ledger.period.now_utc", return_value=datetime(2026, 2, 27, 3, 0, tzinfo=timezone.utc)):
            from_date, to_date, _, _ = compute_dynamic_month_range(cur, "alice", "2026-02", 25, 25)

        self.assertEqual(from_date, "2026-01-25")
        self.assertEqual(to_date, "2026-02-27")

    def test_compute_dynamic_month_range_uses_local_date_for_range_labels(self):
        cur = DynamicRangeCursor(None, None)
        # 2026-02-28 17:30 UTC equals 2026-03-01 00:30 in Asia/Jakarta
        with patch("app.services.ledger.period.now_utc", return_value=datetime(2026, 2, 28, 17, 30, tzinfo=timezone.utc)):
            from_date, to_date, _, _ = compute_dynamic_month_range(cur, "alice", "2026-03", 25, 25)

        self.assertEqual(from_date, "2026-02-25")
        self.assertEqual(to_date, "2026-03-01")

    def test_get_balance_before_filters_soft_deleted_rows(self):
        cur = CursorSpy()
        _ = get_balance_before(cur, "acc-1", datetime(2026, 2, 1, tzinfo=timezone.utc))
        self.assertIn("t.deleted_at IS NULL", cur.last_sql)

    def test_get_balance_at_transaction_can_exclude_transfer_ids(self):
        cur = BalanceCursor(120000)
        _ = get_balance_at_transaction(
            cur,
            "11111111-1111-1111-1111-111111111111",
            datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
            "22222222-2222-2222-2222-222222222222",
            exclude_transfer_ids=["33333333-3333-3333-3333-333333333333"],
        )
        self.assertIn("t.transfer_id <> ALL(%s::uuid[])", cur.last_sql)
        self.assertEqual(cur.last_params[-1], ["33333333-3333-3333-3333-333333333333"])

    def test_compute_shortfall_at_transaction_uses_negative_balance(self):
        cur = BalanceCursor(-45000)
        shortfall = compute_shortfall_at_transaction(
            cur,
            "11111111-1111-1111-1111-111111111111",
            datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
            "22222222-2222-2222-2222-222222222222",
        )
        self.assertEqual(shortfall, 45000)

    def test_parse_uuid_value_rejects_invalid_uuid(self):
        with self.assertRaises(HTTPException) as ctx:
            parse_uuid_value("not-a-uuid", "account_id")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "Invalid account_id")

    def test_write_transaction_audit_serializes_snapshot_payload(self):
        cur = CursorSpy()
        row = {
            "transaction_id": "11111111-1111-1111-1111-111111111111",
            "account_id": "22222222-2222-2222-2222-222222222222",
            "transaction_type": "debit",
            "transaction_name": "Top Up",
            "amount": 150000,
            "date": datetime(2026, 2, 11, 8, 30, 5, tzinfo=timezone.utc),
            "is_transfer": False,
            "is_cycle_topup": True,
            "transfer_id": None,
            "deleted_at": datetime(2026, 2, 11, 9, 0, 0, tzinfo=timezone.utc),
            "deleted_by": "alice",
            "delete_reason": "user_request",
        }

        write_transaction_audit(
            cur,
            username="alice",
            performed_by="alice",
            action="soft_delete",
            tx_row=row,
        )

        self.assertEqual(len(cur.calls), 1)
        _, params = cur.calls[0]
        payload = json.loads(params[5])
        self.assertEqual(payload["transaction_name"], "Top Up")
        self.assertTrue(payload["is_cycle_topup"])
        self.assertTrue(payload["date"].endswith("Z"))
        self.assertTrue(payload["deleted_at"].endswith("Z"))
        self.assertEqual(params[2], "alice")
        self.assertEqual(params[3], "alice")
        self.assertEqual(params[4], "soft_delete")

    def test_recompute_balances_report_detects_negative_account(self):
        cur = RecomputeCursor()
        report = recompute_balances_report(cur, "alice")

        self.assertEqual(report["total_asset"], 650000)
        self.assertTrue(report["has_negative"])
        accounts = {row["account_id"]: row for row in report["accounts"]}
        self.assertEqual(accounts["acc-1"]["current_balance"], 700000)
        self.assertEqual(accounts["acc-1"]["min_balance"], 0)
        self.assertEqual(accounts["acc-2"]["current_balance"], -50000)
        self.assertEqual(accounts["acc-2"]["first_negative_at"], "2026-02-03T10:00:00Z")

if __name__ == "__main__":
    unittest.main()
