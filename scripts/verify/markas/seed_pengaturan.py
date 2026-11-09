"""Pengaturan seed: adds the settings states the parity seed lacks to a disposable owner (apply-markas-pengaturan D24).

Local verification tooling only. It runs after seed_parity.py on a migrated disposable database whose name starts
with ``cft_markas_`` on a loopback host, writes synthetic rows in one transaction and prints what it added:

- the parity phone becomes ready (permissions on, self-test passed, reported an hour ago);
- a second phone "Ponsel Cadangan" that last reported 30 hours ago, so it is silent;
- sender names up to eleven, one of them ambiguous, so the search field appears;
- with ``--stress``: twelve phones, sixty sender names, sixty-character names and a failed self-test.

    backend/.venv/bin/python scripts/verify/markas/seed_pengaturan.py \
        --database-url postgresql://...@127.0.0.1:<port>/cft_markas_settings --username dimas_contoh [--stress]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import psycopg

DATABASE_PREFIX = "cft_markas_"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
LONG_NAME = "Perangkat Uji Dengan Nama Yang Sangat Panjang Sekali Untuk Tata L"[:60]


def guard(url: str) -> None:
    parts = urlsplit(url)
    database = parts.path.lstrip("/")
    if parts.hostname not in LOOPBACK_HOSTS or not database.startswith(DATABASE_PREFIX):
        sys.exit(f"refused: only a loopback database named {DATABASE_PREFIX}* is seeded")


def random_hash() -> str:
    # No usable key exists for a synthetic phone.
    return hashlib.sha256(secrets.token_bytes(32)).hexdigest()


def add_phone(cur, user_id: str, name: str, now: datetime, *, reported_hours: float, access: bool = True,
              listener: bool = True, test: str = "passed", failed_step: str | None = None, version: str = "2.6.0") -> None:
    cur.execute(
        "INSERT INTO api_keys (user_id, key_hash, key_prefix, created_at, last_used_at, device_name, device_id, created_via)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, 'pairing') RETURNING id",
        (user_id, random_hash(), "cfk_" + secrets.token_hex(2), now - timedelta(days=40), now - timedelta(hours=reported_hours),
         name, "markas-settings-" + secrets.token_hex(4)),
    )
    key_id = cur.fetchone()[0]
    reported = now - timedelta(hours=reported_hours)
    cur.execute(
        "INSERT INTO device_health (api_key_id, app_version, listener_connected, notification_access, post_notifications,"
        " battery_unrestricted, self_test_status, self_test_failed_step, self_test_at, last_capture_at, queue_pending, reported_at)"
        " VALUES (%s, %s, %s, %s, true, true, %s, %s, %s, %s, 0, %s)",
        (key_id, version, listener, access, test, failed_step, None if test == "not_run" else reported, reported, reported),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--stress", action="store_true")
    args = parser.parse_args()
    guard(args.database_url)
    now = datetime.now(timezone.utc)
    added: list[str] = []
    with psycopg.connect(args.database_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE username = %s", (args.username,))
        row = cur.fetchone()
        if not row:
            sys.exit("refused: the parity owner does not exist; run seed_parity.py first")
        user_id = row[0]

        cur.execute(
            "UPDATE device_health SET notification_access = true, listener_connected = true, self_test_status = 'passed',"
            " self_test_failed_step = NULL, self_test_at = %s, reported_at = %s, last_capture_at = %s"
            " WHERE api_key_id IN (SELECT id FROM api_keys WHERE user_id = %s AND created_via = 'pairing')",
            (now - timedelta(hours=1), now - timedelta(hours=1), now - timedelta(hours=2), user_id),
        )
        added.append(f"{cur.rowcount} parity phone(s) made ready (reported 1 h ago)")
        add_phone(cur, user_id, "Ponsel Cadangan", now, reported_hours=30)
        added.append("phone 'Ponsel Cadangan', last report 30 h ago (silent)")
        if args.stress:
            for index in range(10):
                name = LONG_NAME if index == 0 else f"Ponsel Uji {index + 1}"
                add_phone(cur, user_id, name, now, reported_hours=2 + index, access=index % 3 != 1,
                          test="failed" if index % 4 == 2 else "passed", failed_step="capture" if index % 4 == 2 else None)
            added.append("10 more phones (one 60-character name, access off and failed self-tests among them)")

        cur.execute(
            "SELECT receiving_account_id, count(*) FROM notification_sender_aliases WHERE user_id = %s GROUP BY 1 ORDER BY 2 DESC LIMIT 1",
            (user_id,),
        )
        found = cur.fetchone()
        if found:
            account_id, existing = found
        else:
            cur.execute("SELECT id FROM accounts WHERE user_id = %s ORDER BY created_at LIMIT 1", (user_id,))
            account_id, existing = cur.fetchone()[0], 0
        target = 60 if args.stress else 11
        names = ["Andra Wijaya", "Bima Santoso", "Citra Lestari", "Dewi Anggraini", "Eko Prasetyo", "Fajar Nugroho",
                 "Gita Permata", "Hendra Gunawan", "Indah Sari", "Joko Susilo", "Kartika Putri"]
        for index in range(max(0, target - existing)):
            base = names[index % len(names)]
            name = ("Nama Pengirim Yang Sangat Panjang Untuk Menguji Tata Letak " + base)[:60] if args.stress and index == 0 else base
            mask = "".join(ch if i % 2 == 0 else "*" for i, ch in enumerate(base.upper().replace(" ", "")))[:12] + f"{index:02d}"
            cur.execute(
                "INSERT INTO notification_sender_aliases (user_id, institution, receiving_account_id, mask_normalized, mask_display,"
                " sender_name, state) VALUES (%s, 'bca', %s, %s, %s, %s, %s)",
                (user_id, account_id, mask.replace("*", ""), mask, name, "ambiguous" if index == 1 else "active"),
            )
        added.append(f"{max(0, target - existing)} sender names (total {max(target, existing)}, one ambiguous)")
        conn.commit()
    print(json.dumps({"added": added}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
