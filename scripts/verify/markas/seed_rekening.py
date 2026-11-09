"""Adds Rekening & Kantong's account states to a parity-seeded owner (apply-markas-rekening D22, local only).

Run after `seed_parity.py` and the migration to V54. Every addition goes through the app's own endpoints, so balances,
credit bills and gains stay the server's; the only exception is the reconciliation flag, which only the notification
path sets, so `--flag-database-url` sets it with one UPDATE on a disposable `cft_markas_*` loopback database.

    MARKAS_PASSWORD=... python scripts/verify/markas/seed_rekening.py --api http://127.0.0.1:8731 --username dimas_contoh \
        [--origin http://127.0.0.1:3750] [--language en] [--stress] [--flag-database-url postgresql://...]

The bill dates are relative to today in Asia/Jakarta, so the statuses are the same on every run day:
"Kartu Kredit Contoh" has one bill of each status (paid, paid late, overdue, due today, upcoming, open).
It prints a JSON report of every addition and every skip with its reason. Synthetic data only.
"""
from __future__ import annotations

import argparse
import calendar
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

JAKARTA = timezone(timedelta(hours=7))
LONG_POCKET = "Kantong Tabungan Pendidikan Anak dan Keperluan Sekolah Lanjut"  # 60 characters
DATABASE_PREFIX = "cft_markas_"
LOOPBACK = ("127.0.0.1", "localhost", "::1")


class Api:
    def __init__(self, base: str, origin: str) -> None:
        if urlparse(base).hostname not in LOOPBACK:
            raise SystemExit(f"refused: {base} is not a loopback backend")
        self.base = base.rstrip("/")
        self.origin = origin
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + path, data=data, method=method, headers={"Content-Type": "application/json", "Origin": self.origin},
        )
        try:
            with self.opener.open(request) as response:
                return json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"{method} {path} -> {error.code} {error.read()[:300]!r}") from error


def today() -> date:
    return datetime.now(JAKARTA).date()


def at(day: date, hour: int = 10) -> str:
    return datetime(day.year, day.month, day.day, hour, 0, tzinfo=JAKARTA).isoformat()


def months_back(day: date, months: int) -> date:
    index = day.year * 12 + day.month - 1 - months
    year, month = index // 12, index % 12 + 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def flat(accounts: list[dict]) -> dict[str, dict]:
    found: dict[str, dict] = {}
    for account in accounts:
        found[account["name"]] = account
        for child in account.get("children") or []:
            found[child["name"]] = child
    return found


def set_flag(database_url: str, username: str, account_name: str) -> str:
    """The reconciliation flag, which only a settled notification sets; disposable loopback databases only."""
    target = urlparse(database_url)
    name = target.path.lstrip("/")
    if target.hostname not in LOOPBACK or not name.startswith(DATABASE_PREFIX):
        raise SystemExit(f"refused: the flag is set only on a loopback {DATABASE_PREFIX}* database, not {target.hostname}/{name}")
    import psycopg

    with psycopg.connect(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE accounts a SET reconciliation_required = TRUE, reconciliation_reason = 'settled_notification_negative_balance',
                      updated_at = NOW()
               FROM users u WHERE u.id = a.user_id AND u.username = %s AND a.name = %s AND a.is_archived = FALSE""",
            (username, account_name),
        )
        return f"{cur.rowcount} row"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--origin", default="http://127.0.0.1:3750", help="an origin the backend allows (APP_ORIGINS)")
    parser.add_argument("--language", choices=["id", "en"], default=None, help="the owner's interface language")
    parser.add_argument("--stress", action="store_true", help="40 products, a 60-character pocket, Rp 1.234.567.890 and USD display")
    parser.add_argument("--flag-database-url", default=None, help="disposable loopback cft_markas_* database for the reconciliation flag")
    args = parser.parse_args()
    password = os.environ.get("MARKAS_PASSWORD")
    if not password:
        raise SystemExit("MARKAS_PASSWORD is required")

    api = Api(args.api, args.origin)
    api.call("POST", "/auth/login", {"username": args.username, "password": password})
    report: dict[str, list] = {"added": [], "skipped": []}

    def add(label: str, method: str, path: str, body: dict | None = None) -> dict | None:
        try:
            response = api.call(method, path, body)
            report["added"].append(label)
            return response
        except RuntimeError as error:
            report["skipped"].append({"addition": label, "reason": str(error)[:240]})
            return None

    def created_id(response: dict | None) -> str | None:
        if not response:
            return None
        account = response.get("account") if isinstance(response.get("account"), dict) else response
        return account.get("id")

    if args.language:
        add(f"interface language {args.language}", "PATCH", "/auth/settings", {"language": args.language})

    existing = flat(api.call("GET", "/accounts")["accounts"])
    if "Kartu Kredit Contoh" in existing:
        raise SystemExit("refused: this owner already has the Rekening additions")
    t = today()

    # The parity SPayLater keeps its bills (due today, Rp 434.000 left); only its bill day is set (H8 form field).
    spaylater = existing.get("SPayLater")
    if spaylater:
        add("SPayLater statement_day 25 (past bills keep their due dates)", "PATCH", f"/accounts/{spaylater['id']}", {"statement_day": 25})
    else:
        report["skipped"].append({"addition": "SPayLater statement_day", "reason": "no SPayLater in this seed"})

    # A parent with its own balance beside a pocket; it also pays the credit bills below.
    mandiri = created_id(add("parent Mandiri Contoh with its own balance", "POST", "/accounts",
                             {"name": "Mandiri Contoh", "type": "bank", "initial_balance": 2_000_000, "role": "harian"}))
    if mandiri:
        add("pocket Belanja Bulanan in Mandiri Contoh", "POST", "/accounts",
            {"name": "Belanja Bulanan", "type": "bank", "initial_balance": 400_000, "parent_id": mandiri, "role": "harian"})

    # One bill of each status on a card with payment day 5 and bill day 25; owner-set first due dates pin each bill.
    card = created_id(add("credit Kartu Kredit Contoh (limit, payment day 5, bill day 25)", "POST", "/accounts",
                          {"name": "Kartu Kredit Contoh", "type": "credit", "initial_balance": 0, "credit_limit": 5_000_000,
                           "payment_day": 5, "statement_day": 25}))
    if card and mandiri:
        paid_due, late_due = t - timedelta(days=40), months_back(t, 1)

        def purchase(label: str, amount: int, bought: date, notes: str, count: int, first_due: date) -> None:
            add(label, "POST", "/transactions", {
                "type": "expense", "account_id": card, "amount": amount, "notes": notes, "date": at(bought),
                "kakeibo_type": "want", "credit_terms": {"installment_count": count, "first_due_date": first_due.isoformat()},
            })

        purchase("paid bill: purchase due 40 days ago", 150_000, t - timedelta(days=70), "Jaket hujan", 1, paid_due)
        purchase("3-instalment purchase, first due a month ago", 300_000, late_due - timedelta(days=25), "Kursi kerja", 3, late_due)
        purchase("due today: second purchase", 200_000, t - timedelta(days=20), "Helm", 1, t)
        upcoming_due = t + timedelta(days=1)
        # Upcoming needs the bill issued (day 25 on or before the due date) by today; no such date exists on the 24th.
        issued = upcoming_due.replace(day=25) if upcoming_due.day >= 25 else months_back(upcoming_due, 1).replace(day=25)
        if issued <= t:
            purchase("upcoming bill: purchase due tomorrow", 80_000, t - timedelta(days=2), "Buku", 1, upcoming_due)
        else:
            report["skipped"].append({"addition": "upcoming bill", "reason": "today is the 24th: no due date has its bill issued yet"})
        add("payment covering the paid bill, before its due date", "POST", "/movements",
            {"source_account_id": mandiri, "target_account_id": card, "amount": 150_000, "notes": "Bayar kartu",
             "date": at(paid_due - timedelta(days=5))})
        # Settles instalment 1 a week after its due date (paid late); its Rp 15.000 fee stays unpaid (overdue).
        add("late payment of instalment 1 with a Rp 15.000 fee", "POST", "/movements",
            {"source_account_id": mandiri, "target_account_id": card, "amount": 100_000, "fee": 15_000,
             "notes": "Bayar cicilan", "date": at(late_due + timedelta(days=7))})

    add("credit Kredivo Contoh without a payment day", "POST", "/accounts",
        {"name": "Kredivo Contoh", "type": "credit", "initial_balance": -250_000, "credit_limit": 2_000_000})
    add("credit Kredit Lewat Limit Contoh over its limit", "POST", "/accounts",
        {"name": "Kredit Lewat Limit Contoh", "type": "credit", "initial_balance": -650_000, "credit_limit": 500_000,
         "payment_day": 20, "statement_day": 10})
    overpaid = created_id(add("credit Akulaku Contoh", "POST", "/accounts",
                              {"name": "Akulaku Contoh", "type": "credit", "initial_balance": 0, "payment_day": 15}))
    if overpaid and mandiri:
        add("Akulaku Contoh overpaid by Rp 12.000", "POST", "/movements",
            {"source_account_id": mandiri, "target_account_id": overpaid, "amount": 12_000, "notes": "Lebih bayar",
             "date": at(t - timedelta(days=2))})

    # A second platform: one product with a gain, one without a cost basis ("Modal belum diisi").
    pluang = created_id(add("platform Pluang Contoh", "POST", "/accounts", {"name": "Pluang Contoh", "type": "investment", "role": "investasi"}))
    if pluang:
        fund = created_id(add("product without cost basis", "POST", "/accounts",
                              {"name": "Reksa Dana Obligasi Contoh", "type": "investment", "parent_id": pluang,
                               "instrument_type": "mutual_fund", "units": 10, "role": "investasi"}))
        if fund:
            add("its price (value Rp 1.500.000, no cost)", "PATCH", f"/accounts/{fund}", {"last_price": 150_000})
        stock = created_id(add("product with a gain", "POST", "/accounts",
                               {"name": "Saham Teknologi Contoh", "type": "investment", "parent_id": pluang, "instrument_type": "stock",
                                "instrument_symbol": "TKNO", "units": 2_000, "avg_buy_price": 500, "role": "investasi"}))
        if stock:
            add("its price (value Rp 1.120.000, cost Rp 1.000.000)", "PATCH", f"/accounts/{stock}", {"last_price": 560})

    if args.flag_database_url:
        try:
            report["added"].append(f"SQL (disposable DB only): Tagihan Rumah reconciliation_required ({set_flag(args.flag_database_url, args.username, 'Tagihan Rumah')})")
        except Exception as error:  # noqa: BLE001 - reported as a skip, the run goes on
            report["skipped"].append({"addition": "reconciliation flag", "reason": str(error)[:240]})
    else:
        report["skipped"].append({"addition": "reconciliation flag", "reason": "no --flag-database-url given"})

    if args.stress:
        big = created_id(add("platform Platform Uji Contoh", "POST", "/accounts", {"name": "Platform Uji Contoh", "type": "investment", "role": "investasi"}))
        if big:
            for index in range(40):
                product = created_id(add(f"stress product {index + 1}", "POST", "/accounts", {
                    "name": f"Reksa Dana Uji Contoh {index + 1:02d}", "type": "investment", "parent_id": big,
                    "instrument_type": "mutual_fund", "units": 100 + index, "avg_buy_price": 1_000, "role": "investasi"}))
                if product:
                    add(f"stress product {index + 1} price", "PATCH", f"/accounts/{product}", {"last_price": 950 + index * 5})
        if mandiri:
            add("60-character pocket name", "POST", "/accounts",
                {"name": LONG_POCKET, "type": "bank", "initial_balance": 750_000, "parent_id": mandiri, "role": "tabungan"})
        add("Rp 1.234.567.890 balance", "POST", "/accounts",
            {"name": "Rekening Besar Contoh", "type": "bank", "initial_balance": 1_234_567_890, "role": "tabungan"})
        add("USD display currency", "PATCH", "/auth/settings", {"currency": "USD"})

    json.dump({"added": len(report["added"]), "additions": report["added"], "skipped": report["skipped"]}, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
