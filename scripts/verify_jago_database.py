"""Apply versioned migrations only to this feature's disposable loopback database."""

from pathlib import Path

import psycopg


def main():
    root = Path(__file__).resolve().parents[1]
    with psycopg.connect("postgresql://postgres@127.0.0.1:5547/ledger_test") as conn:
        exists = conn.execute("SELECT to_regclass('public.users')").fetchone()[0]
        if exists:
            raise SystemExit("Database already populated; do not replay baseline migrations.")
        paths = sorted((root / "db/migrations").glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
        for path in paths:
            conn.execute(path.read_text(encoding="utf-8"))
    print(f"Applied {len(paths)} versioned migrations to the disposable Jago test database.")


if __name__ == "__main__":
    main()
