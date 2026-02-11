import json
import os
import pathlib
import sys
import types
import unittest
from datetime import datetime, timezone

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

from app.services.ledger import (
    get_balance_before,
    parse_tx_datetime,
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


class LedgerServiceTests(unittest.TestCase):
    def test_parse_tx_datetime_normalizes_to_seconds_and_utc(self):
        dt = parse_tx_datetime("2026-02-11T10:15:12.987654+07:00")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.microsecond, 0)
        self.assertEqual(dt.isoformat(), "2026-02-11T03:15:12+00:00")

    def test_get_balance_before_filters_soft_deleted_rows(self):
        cur = CursorSpy()
        _ = get_balance_before(cur, "acc-1", datetime(2026, 2, 1, tzinfo=timezone.utc))
        self.assertIn("t.deleted_at IS NULL", cur.last_sql)

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
        payload = json.loads(params[4])
        self.assertEqual(payload["transaction_name"], "Top Up")
        self.assertTrue(payload["date"].endswith("Z"))
        self.assertTrue(payload["deleted_at"].endswith("Z"))
        self.assertEqual(params[2], "alice")
        self.assertEqual(params[3], "soft_delete")

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
