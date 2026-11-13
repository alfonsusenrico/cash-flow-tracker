"""Where each Beranda and review label comes from (add-language-system D7, task 2.6).

Every line of the cycle summary, every review-queue group and every waiting line says whether its label is the
owner's data (`user`) or fixed wording (`system`), and the Kakeibo lines carry their notes as codes. `label` and
`note` keep today's Indonesian text. Every name and amount is synthetic.
"""
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from test_cycle_summary import (  # noqa: F401 - fixtures
    make_owner, owner, shift_months, today, unlimited_rate_limits, waiting_event,
)

# Today's Indonesian wording of every system label, as the summary has always sent it.
SYSTEM_LABELS = {
    "need": "Kebutuhan", "want": "Keinginan", "culture": "Budaya", "unexpected": "Tak Terduga",
    "saving": "Tabungan", "unclassified": "Belum diklasifikasi",
    "liquid": "Dana likuid", "dana_investasi": "Dana investasi", "investment": "Investasi",
    "reconciliation_pending": "Saldo minus perlu dicocokkan",
    "masuk": "Masuk", "keluar": "Keluar", "scheduled": "Terjadwal",
    "uncategorized": "Tanpa kategori",
}


def rich_owner(owner):
    """One line of every kind: buckets with both notes, user and system income, plans, goals, debts, credit."""
    owner.regular_salaries(today() - timedelta(days=5))
    owner.tx("income", 150_000, notes="Masuk tanpa kategori contoh")
    for amount, category in [(420_000, "makan"), (300_000, "jajan"), (45_000, "buku"), (30_000, "servis"), (20_000, "lain")]:
        owner.expense(amount, category)
    owner.goal("liburan", "Liburan Contoh", 6_000_000, shift_months(today(), 4).replace(day=15), accounts=["pocket"])
    owner.move("bank", "pocket", 250_000)
    owner.tx("income", 200_000, category="patungan", notes="Ganti uang contoh")
    owner.db("UPDATE transactions SET offset_category_id = %s WHERE category_id = %s", (owner["jajan"], owner["patungan"]))
    owner.plan("cicilan", "Cicilan Contoh", 600_000, today() + timedelta(days=2))
    owner.account("platform", "Platform Contoh", kind="investment", initial=3_000_000)
    owner.account("minus_pocket", "Kantong Minus", initial=-40_000, parent="bank")
    owner.db("UPDATE accounts SET reconciliation_required = TRUE WHERE id = %s", (owner["minus_pocket"],))
    rdn = owner.session.post("/api/accounts", json={
        "name": "RDN Contoh", "type": "bank", "role": "dana_investasi", "initial_balance": 700_000,
    })
    assert rdn.status_code == 200, rdn.text
    credit = owner.session.post("/api/accounts", json={
        "name": "PayLater Contoh", "type": "credit", "initial_balance": -350_000, "credit_limit": 2_000_000,
        "payment_day": 11,
    })
    assert credit.status_code == 200, credit.text
    owner.db(
        "INSERT INTO obligations (user_id, name, total_amount, remaining_amount) VALUES (%s, 'Pinjaman Contoh', 900000, 800000)",
        (owner.user_id,),
    )
    return owner


def test_every_cycle_summary_line_says_where_its_label_comes_from(owner):
    data = rich_owner(owner).summary()

    keluar = {line["key"]: line for line in data["keluar"]["lines"]}
    assert set(keluar) == {"need", "want", "culture", "unexpected", "saving", "unclassified"}
    for key, line in keluar.items():
        assert (line["label_source"], line["label"]) == ("system", SYSTEM_LABELS[key])
    assert keluar["saving"]["note_codes"] == [{"code": "includes_savings_moves", "params": {}}]
    assert keluar["saving"]["note"] == "Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan"
    assert keluar["want"]["note_codes"] == [{"code": "reduced_by_reimbursements", "params": {"amount": 200_000}}]
    assert keluar["want"]["note"] == "Sudah dikurangi ganti uang Rp 200.000"
    assert isinstance(keluar["want"]["note_codes"][0]["params"]["amount"], int)
    for key in ("need", "culture", "unexpected", "unclassified"):
        assert (keluar[key]["note_codes"], keluar[key]["note"]) == ([], None)
    # Pengeluaran and Ditabung & investasi are the same line objects, so they carry the same fields.
    assert data["spending"]["lines"] == [line for line in data["keluar"]["lines"] if line["key"] != "saving"]
    assert data["fresh_saving"]["lines"] == [keluar["saving"]]

    masuk = {line["key"]: line for line in data["masuk"]["lines"]}
    assert (masuk[owner["gaji"]]["label_source"], masuk[owner["gaji"]]["label"]) == ("user", "Gaji")
    assert (masuk["uncategorized"]["label_source"], masuk["uncategorized"]["label"]) == ("system", "Tanpa kategori")

    scheduled = data["scheduled"]["lines"]
    assert {(line["kind"], line["label"], line["label_source"]) for line in scheduled} == {
        ("plan", "Cicilan Contoh", "user"), ("goal", "Liburan Contoh", "user"),
    }

    net = {line["key"]: line for line in data["net_worth"]["lines"]}
    for key in ("liquid", "dana_investasi", "investment", "reconciliation_pending"):
        assert (net[key]["label_source"], net[key]["label"]) == ("system", SYSTEM_LABELS[key]), key
    owned = {line["label"]: line["label_source"] for key, line in net.items() if key.startswith(("debt:", "credit:"))}
    assert owned == {"Pinjaman Contoh": "user", "PayLater Contoh": "user"}

    for figure in ("keluar", "masuk", "scheduled", "net_worth"):
        for line in data[figure]["lines"]:
            assert line["label_source"] in ("user", "system"), (figure, line)
    # Rekening's net worth shares the rule, so its lines carry the same sources.
    assert owner.session.get("/api/dashboard/net-worth").json()["lines"] == data["net_worth"]["lines"]


def test_waiting_lines_mark_the_undescribed_fallback(owner):
    described = owner.expense(45_000, notes="QRIS 0812-0000", review_status="needs_review")
    by_category = owner.expense(30_000, "jajan", review_status="needs_review")
    bare = owner.expense(12_000, review_status="needs_review")
    lines = {line["transaction_id"]: line for line in owner.summary()["waiting"]["lines"]}
    assert (lines[described]["label"], lines[described]["label_source"], lines[described]["label_key"]) == (
        "QRIS 0812-0000", "user", None,
    )
    assert (lines[by_category]["label"], lines[by_category]["label_source"], lines[by_category]["label_key"]) == (
        "Jajan Contoh", "user", None,
    )
    assert (lines[bare]["label"], lines[bare]["label_source"], lines[bare]["label_key"]) == (
        "Tanpa keterangan", "system", "undescribed",
    )


def signature_event(owner, tx_id):
    owner.db(
        """INSERT INTO notification_events (user_id, device_id, package_name, post_time, payload_hash,
                                            processing_state, result_key, result_snapshot, transaction_id, interpretation)
           VALUES (%s, 'synthetic-device', 'com.example.bank', NOW(), %s, 'recorded', %s, %s, %s, %s)""",
        (owner.user_id, uuid4().hex, f"synthetic:{uuid4().hex}", Jsonb({"status": "recorded"}), tx_id,
         Jsonb({"review_reasons": [], "match_key": f"signature:{uuid4().hex[:12]}"})),
    )


def test_review_groups_say_where_their_label_comes_from(owner):
    owner.expense(25_000, "makan", merchant="Kedai Contoh", review_status="needs_review")
    owner.expense(18_000, notes="Bayar parkir contoh", review_status="needs_review")
    owner.expense(15_000, "jajan", review_status="needs_review")
    owner.expense(9_000, review_status="needs_review")
    signed = owner.expense(7_000, "makan", review_status="needs_review")
    signature_event(owner, signed)

    response = owner.session.get("/api/review/queue")
    assert response.status_code == 200, response.text
    groups = {(group["kind"], group["label"]): group for group in response.json()["groups"]}
    assert {key: (group["label_source"], group["label_key"]) for key, group in groups.items()} == {
        ("merchant", "Kedai Contoh"): ("user", None),
        ("notes", "Bayar parkir contoh"): ("user", None),
        ("category", "Jajan Contoh"): ("user", None),
        ("category", "Tanpa kategori"): ("system", "uncategorized"),
        ("signature", "Tanpa keterangan"): ("system", "undescribed"),
    }
