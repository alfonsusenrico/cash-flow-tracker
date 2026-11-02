"""Belum diklasifikasi: no silent `need`, the sixth bucket, its filter and the quick fix with feedback.

Synthetic data only; reuses the monthly-report owner fixture.
"""
from datetime import datetime, timedelta
from uuid import uuid4

from test_monthly_report import TZ, as_user, insert, owners  # noqa: F401 - fixture

KAKEIBO_KEYS = ("need", "want", "culture", "unexpected", "saving", "unclassified")


def recent(minutes=5):
    return datetime.now(TZ) - timedelta(minutes=minutes)


def feedback_rows(owner, transaction_ids):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            """SELECT transaction_id, kind, before_kakeibo_type, after_kakeibo_type FROM classification_feedback
               WHERE user_id = %s AND transaction_id = ANY(%s::uuid[]) ORDER BY transaction_id""",
            (owner["user_id"], transaction_ids),
        ).fetchall()


def stored(transaction_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            "SELECT kakeibo_type, review_status, classified_by, owner_edited_at FROM transactions WHERE id = %s",
            (transaction_id,),
        ).fetchone()


def kakeibo(client):
    return client.get("/api/dashboard/overview", params={"timeframe": "month"}).json()["kakeibo"]


def test_missing_pillar_is_reported_as_unclassified_not_need(client, owners):
    owner, _ = owners
    unclassified = insert(owner, "misc", 40_000, recent())
    insert(owner, "groceries", 60_000, recent())
    insert(owner, None, 10_000, recent())
    breakdown = kakeibo(client)
    assert (breakdown["need_spent"], breakdown["unclassified_spent"]) == (60_000, 50_000)
    assert breakdown["total_allocated"] == 110_000

    row = client.get(f"/api/transactions/{unclassified}").json()["transaction"]
    assert row["kakeibo_type"] is None
    listed = client.get("/api/transactions", params={"period": "month", "type": "expense", "kakeibo_type": "unclassified"}).json()
    assert listed["summary"]["period"]["outflow"] == 50_000
    assert {item["kakeibo_type"] for item in listed["transactions"]} == {None}
    assert client.get("/api/transactions", params={"kakeibo_type": "luxury"}).status_code == 422

    categories = {row["name"]: row["kakeibo_type"] for row in client.get("/api/categories").json()["categories"]}
    assert categories["Lain-lain"] is None and categories["Belanja Dapur"] == "need"
    # The phone accepts only the five pillars or null in top_categories.
    summary = client.get("/api/dashboard/cash-flow-summary", params={"period": "month"}).json()
    assert {item["kakeibo_type"] for item in summary["top_categories"]} <= {"need", None}


def test_six_buckets_add_up_with_a_reimbursement_in_an_unclassified_category(client, owners):
    owner, _ = owners
    insert(owner, "salary", 1_000_000, recent(), tx_type="income")
    insert(owner, "misc", 100_000, recent())
    insert(owner, "misc", 300_000, recent(), pillar="want")
    insert(owner, "reimbursement", 200_000, recent(), tx_type="income", offset="misc")
    breakdown = kakeibo(client)
    # The 200.000 offset splits 1:3 across the unclassified and want shares of the category.
    assert (breakdown["unclassified_spent"], breakdown["want_spent"]) == (50_000, 150_000)
    assert breakdown["unclassified_pct"] == 5.0
    comparison = client.get("/api/dashboard/period-comparison", params={"timeframe": "month"}).json()
    pillars = {item["pillar"]: item["current"] for item in comparison["pillars"]}
    assert tuple(pillars) == KAKEIBO_KEYS
    assert sum(pillars.values()) == comparison["current"]["outflow"] == 200_000


def test_essential_cost_still_counts_unclassified_spending_and_reports_it(client, owners):
    from app.db.pool import db_conn
    from app.services.essential_cost import estimate_essential_cost

    owner, _ = owners
    today = datetime.now(TZ).date()
    previous = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    # Recorded from the month's first day, so the month counts as a completed period.
    insert(owner, "groceries", 2_000_000, datetime.combine(previous, datetime.min.time(), TZ) + timedelta(hours=8))
    insert(owner, "misc", 300_000, datetime.combine(previous.replace(day=6), datetime.min.time(), TZ))
    with db_conn() as conn:
        with conn.cursor() as cur:
            essential = estimate_essential_cost(cur, owner["user_id"])
    assert essential.amount == 2_300_000
    assert essential.periods[0]["unclassified_amount"] == 300_000


def test_a_row_in_a_category_without_pillar_stays_unclassified(client, owners):
    owner, _ = owners
    created = client.post("/api/transactions", json={"type": "expense", "account_id": owner["cash"], "category_id": owner["misc"], "amount": 12_000})
    assert created.status_code == 200, created.text
    transaction_id = created.json()["transaction_id"]
    assert stored(transaction_id)["kakeibo_type"] is None
    moved = client.patch(f"/api/transactions/{transaction_id}", json={"category_id": owner["groceries"]})
    assert moved.status_code == 200
    assert stored(transaction_id)["kakeibo_type"] == "need"  # an explicit category pillar is still copied
    back = client.patch(f"/api/transactions/{transaction_id}", json={"category_id": owner["misc"]})
    assert back.status_code == 200
    assert stored(transaction_id)["kakeibo_type"] is None


def test_category_fix_applies_to_following_rows_and_writes_feedback(client, owners):
    owner, _ = owners
    first = insert(owner, "misc", 30_000, recent(), merchant="Toko Contoh")
    second = insert(owner, "misc", 20_000, recent())
    own_pillar = insert(owner, "misc", 10_000, recent(), pillar="want")
    gaps = client.get("/api/classification/gaps").json()
    assert [(row["name"], row["unclassified_count"], row["unclassified_amount"]) for row in gaps["categories"]] == [("Lain-lain", 2, 50_000)]
    assert gaps["transaction_count"] == 2 and {row["id"] for row in gaps["transactions"]} == {first, second}

    fixed = client.post(f"/api/classification/categories/{owner['misc']}", json={"kakeibo_type": "need"})
    assert fixed.status_code == 200, fixed.text
    assert (fixed.json()["updated_transactions"], fixed.json()["corrections"]) == (2, 2)
    rows = feedback_rows(owner, [first, second, own_pillar])
    assert sorted((str(row["transaction_id"]), row["kind"], row["before_kakeibo_type"], row["after_kakeibo_type"]) for row in rows) == sorted(
        [(first, "correction", None, "need"), (second, "correction", None, "need")]
    )
    assert stored(first)["kakeibo_type"] is None and stored(first)["owner_edited_at"] is None  # the row follows the category
    assert stored(own_pillar)["kakeibo_type"] == "want"
    assert kakeibo(client)["unclassified_spent"] == 0

    again = client.post(f"/api/classification/categories/{owner['misc']}", json={"kakeibo_type": "need"})
    assert again.json()["corrections"] == 0
    assert len(feedback_rows(owner, [first, second])) == 2

    income = client.post(f"/api/classification/categories/{owner['salary']}", json={"kakeibo_type": "need"})
    assert income.status_code == 422
    invalid = client.post(f"/api/classification/categories/{owner['misc']}", json={"kakeibo_type": "unclassified"})
    assert invalid.status_code == 422


def test_row_fix_sets_many_rows_as_an_owner_edit(client, owners):
    owner, _ = owners
    rows = [insert(owner, None, 5_000 * (i + 1), recent(), review_status="auto") for i in range(3)]
    fixed = client.post("/api/classification/transactions", json={"transaction_ids": rows, "kakeibo_type": "unexpected"})
    assert fixed.status_code == 200, fixed.text
    assert fixed.json() == {"ok": True, "transaction_count": 3, "corrections": 3}
    for row_id in rows:
        row = stored(row_id)
        assert (row["kakeibo_type"], row["review_status"], row["classified_by"]) == ("unexpected", "reviewed", "owner")
    assert {(row["before_kakeibo_type"], row["after_kakeibo_type"]) for row in feedback_rows(owner, rows)} == {(None, "unexpected")}

    income = insert(owner, "salary", 1_000, recent(), tx_type="income")
    rejected = client.post("/api/classification/transactions", json={"transaction_ids": [rows[0], income], "kakeibo_type": "need"})
    assert rejected.status_code == 422 and rejected.json()["detail"]["transaction_ids"] == [income]
    assert stored(rows[0])["kakeibo_type"] == "unexpected"  # all or nothing

    moved = client.post("/api/movements", json={"source_account_id": owner["cash"], "target_account_id": owner["pocket"], "amount": 7_000})
    assert moved.status_code == 200, moved.text
    leg = next(item["id"] for item in client.get("/api/transactions", params={"type": "transfer"}).json()["transactions"] if item["amount"] == 7_000)
    assert client.post("/api/classification/transactions", json={"transaction_ids": [leg], "kakeibo_type": "need"}).status_code == 409


def test_classification_routes_are_owner_scoped(client, owners):
    owner, stranger = owners
    theirs = insert(stranger, "misc", 9_000, recent())
    insert(owner, "misc", 1_000, recent())
    assert client.post(f"/api/classification/categories/{stranger['misc']}", json={"kakeibo_type": "need"}).status_code == 404
    response = client.post("/api/classification/transactions", json={"transaction_ids": [theirs], "kakeibo_type": "need"})
    assert response.status_code == 404
    assert stored(theirs)["kakeibo_type"] is None
    gaps = client.get("/api/classification/gaps").json()
    assert theirs not in {row["id"] for row in gaps["transactions"]}
    as_user(stranger)
    assert client.get("/api/classification/gaps").json()["transaction_count"] == 1
    assert client.get("/api/classification/gaps", params={"from_date": recent(60).isoformat()}).status_code == 422
    unknown = client.post(f"/api/classification/categories/{uuid4()}", json={"kakeibo_type": "need"})
    assert unknown.status_code == 404
