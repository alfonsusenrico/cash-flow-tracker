"""Server text in the account's language (localize-server-text tasks 2.1 to 2.3, 3.1 to 3.3, 4.1 to 4.3).

Every account, name and amount is synthetic.
"""
from __future__ import annotations

import ast
import copy
import json
import logging
import pathlib
import re
import string
from datetime import datetime, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

BACKEND = pathlib.Path(__file__).resolve().parents[1]
APP = BACKEND / "app"
SERVER_MESSAGES = APP / "messages"
WEB_MESSAGES = BACKEND.parent / "frontend" / "src" / "messages"
PASSWORD = "synthetic-pass-1"


def load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def placeholders(text: str) -> set[str]:
    return {name.split(".")[0].split("[")[0] for _, name, _, _ in string.Formatter().parse(text) if name}


def icu_arguments(text: str) -> set[str]:
    """The arguments of a web (ICU) message: `{name}`, `{name, plural, …}` and the like."""
    return set(re.findall(r"\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*[,}]", text))


def leaves(tree: dict, prefix: str = "") -> dict[str, object]:
    found: dict[str, object] = {}
    for key, value in tree.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict) and not is_plural(value):
            found.update(leaves(value, path))
        else:
            found[path] = value
    return found


def is_plural(value: object) -> bool:
    return isinstance(value, dict) and bool(value) and set(value) <= {"one", "other"}


# --- Server catalogue parity (task 2.2) ----------------------------------------------------------------------


def catalogue_problems(english: dict, indonesian: dict) -> list[str]:
    """Every difference that would make one language miss a message or a value (D2)."""
    problems: list[str] = []
    en, id_ = leaves(english), leaves(indonesian)
    problems += [f"{key}: missing in id" for key in sorted(set(en) - set(id_))]
    problems += [f"{key}: missing in en" for key in sorted(set(id_) - set(en))]
    for key in sorted(set(en) & set(id_)):
        en_value, id_value = en[key], id_[key]
        if is_plural(en_value) != is_plural(id_value):
            problems.append(f"{key}: plural in one language only")
            continue
        if is_plural(en_value):
            if set(en_value) != {"one", "other"}:
                problems.append(f"{key}: English count message needs one and other")
            if "other" not in id_value:
                problems.append(f"{key}: Indonesian count message needs other")
            en_names = set().union(*(placeholders(text) for text in en_value.values()))
            id_names = set().union(*(placeholders(text) for text in id_value.values()))
        elif isinstance(en_value, str) and isinstance(id_value, str):
            en_names, id_names = placeholders(en_value), placeholders(id_value)
            if not en_value.strip() or not id_value.strip():
                problems.append(f"{key}: empty message")
        else:
            problems.append(f"{key}: not text")
            continue
        if en_names != id_names:
            problems.append(f"{key}: placeholders differ {sorted(en_names)} != {sorted(id_names)}")
    return problems


def test_server_catalogues_have_the_same_keys_and_placeholders():
    assert catalogue_problems(load(SERVER_MESSAGES / "en.json"), load(SERVER_MESSAGES / "id.json")) == []


def test_parity_check_fails_on_planted_problems():
    english, indonesian = load(SERVER_MESSAGES / "en.json"), load(SERVER_MESSAGES / "id.json")

    missing = copy.deepcopy(indonesian)
    del missing["errors"]["account_not_found"]
    assert catalogue_problems(english, missing) == ["errors.account_not_found: missing in id"]

    mismatch = copy.deepcopy(indonesian)
    mismatch["questions"]["habit"]["guess"] = "Sepertinya {kategori}?"
    assert catalogue_problems(english, mismatch) == ["questions.habit.guess: placeholders differ ['category'] != ['kategori']"]

    plural_key = next(key for key, value in leaves(english).items() if is_plural(value))
    without_one = copy.deepcopy(english)
    node = without_one
    for part in plural_key.split(".")[:-1]:
        node = node[part]
    del node[plural_key.split(".")[-1]]["one"]
    assert catalogue_problems(without_one, indonesian) == [f"{plural_key}: English count message needs one and other"]


# --- Renderer and language source (task 2.1) -----------------------------------------------------------------


def test_t_renders_plurals_and_raises_on_a_missing_key_in_tests():
    from app.i18n import MessageError, t

    assert t("en", "facts.comparison.recurring", count=1) != t("en", "facts.comparison.recurring", count=2)
    with pytest.raises(MessageError):
        t("en", "errors.no_such_code_planted")
    with pytest.raises(MessageError):
        t("id", "questions.habit.guess")


def test_production_falls_back_to_indonesian_and_logs_only_the_key(monkeypatch, caplog):
    from app.i18n import catalogue as catalogue_module

    english = copy.deepcopy(catalogue_module.catalogue("en"))
    indonesian = catalogue_module.catalogue("id")
    del english["errors"]["account_not_found"]
    monkeypatch.setattr(catalogue_module, "catalogue", lambda language: english if language == "en" else indonesian)
    monkeypatch.setattr(catalogue_module, "_strict", lambda: False)
    monkeypatch.setattr(catalogue_module, "_reported", set())
    caplog.set_level(logging.WARNING, logger="app.i18n.catalogue")

    text = catalogue_module.t("en", "errors.account_not_found")

    assert text == indonesian["errors"]["account_not_found"]
    logged = [record.getMessage() for record in caplog.records]
    assert logged == ["Server message missing key=errors.account_not_found language=en"]
    assert all(text not in line for line in logged)


@pytest.mark.parametrize(
    "region,figure,inline,bare",
    [("ID", "Rp 1.234.567", "Rp1.234.567", "1.234.567"), ("US", "Rp 1,234,567", "Rp1,234,567", "1,234,567")],
)
def test_format_money_keeps_the_three_shapes(region, figure, inline, bare):
    from app.i18n import format_money

    assert format_money(1_234_567, region, "figure") == figure
    assert format_money(1_234_567, region, "inline") == inline
    assert format_money(1_234_567, region, "bare") == bare
    assert format_money(0, region, "figure") == "Rp 0"
    with pytest.raises(ValueError):
        format_money(1, region, "compact")
    with pytest.raises(ValueError):
        format_money(1, "SG", "figure")


@pytest.fixture
def browser(client, monkeypatch):
    """Fresh browsers with their own session cookies; the accounts they register are removed afterwards."""
    import app.routers.auth as auth_router
    from app.main import app

    class OpenLimiter:
        def exceeded(self, *_args) -> bool:
            return False

        def reset(self, *_keys) -> None:
            return None

    monkeypatch.setattr(auth_router, "rate_limiter", OpenLimiter())
    created: list[str] = []

    def make(language: str | None = None, region: str = "ID") -> TestClient:
        fresh = TestClient(app, base_url="https://testserver")
        fresh.headers["Origin"] = "https://testserver"
        if language:
            username = f"text_{uuid4().hex[:10]}"
            body = {"username": username, "password": PASSWORD, "invite_code": "TESTCODE",
                    "language": language, "region": region}
            response = fresh.post("/api/auth/register", json=body)
            assert response.status_code == 200, response.text
            assert fresh.post("/api/auth/login", json={"username": username, "password": PASSWORD}).status_code == 200
            created.append(username)
            fresh.username = username
        return fresh

    yield make
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE username = ANY(%s)", (created,))
        conn.commit()


def test_account_language_beats_a_conflicting_header(browser):
    english = browser("en")
    response = english.delete(f"/api/categories/{uuid4()}", headers={"Accept-Language": "id-ID,id;q=0.9"})
    assert response.status_code == 404
    assert response.json()["detail"] == {"code": "category_not_found", "params": {}, "message": "Category not found"}
    assert response.headers["content-language"] == "en"

    queue = english.get("/api/review/queue", headers={"Accept-Language": "id-ID"})
    assert queue.status_code == 200 and queue.headers["content-language"] == "en"


def test_indonesian_account_gets_the_indonesian_message(browser):
    from app.i18n import t

    response = browser("id").delete(f"/api/categories/{uuid4()}", headers={"Accept-Language": "en-US"})
    assert response.status_code == 404
    assert response.json()["detail"] == {
        "code": "category_not_found", "params": {}, "message": t("id", "errors.category_not_found"),
    }
    assert response.headers["content-language"] == "id"


def test_bearer_requests_follow_the_account(browser):
    from app.i18n import t

    owner = browser("id")
    key = owner.post("/api-key/reset").json()["api_key"]
    phone = browser()
    response = phone.delete(f"/api/categories/{uuid4()}", headers={"Authorization": f"Bearer {key}"})
    assert response.status_code == 404
    assert response.json()["detail"]["message"] == t("id", "errors.category_not_found")
    assert response.headers["content-language"] == "id"


@pytest.mark.parametrize("header,language", [("id-ID,id;q=0.9", "id"), (None, "en"), ("fr-FR", "en")])
def test_unauthenticated_requests_follow_the_header(browser, header, language):
    from app.i18n import t

    headers = {"Accept-Language": header} if header else {}
    response = browser().post("/api/auth/login", json={"username": f"none_{uuid4().hex[:8]}", "password": "wrong-pass"},
                              headers=headers)
    assert response.status_code == 401
    assert response.json()["detail"] == {
        "code": "invalid_credentials", "params": {}, "message": t(language, "errors.invalid_credentials"),
    }
    assert response.headers["content-language"] == language


def test_framework_errors_keep_the_one_shape(browser):
    unknown = browser().get("/api/no-such-route", headers={"Accept-Language": "id"})
    assert unknown.status_code == 404
    assert set(unknown.json()["detail"]) == {"code", "params", "message"}
    assert unknown.json()["detail"]["code"] == "not_found"
    wrong_method = browser().put("/api/health")
    assert wrong_method.status_code == 405 and wrong_method.json()["detail"]["code"] == "method_not_allowed"
    signed_out = browser().get("/api/review/queue", headers={"Accept-Language": "id"})
    assert signed_out.status_code == 401 and signed_out.json()["detail"]["code"] == "not_authenticated"
    assert signed_out.headers["content-language"] == "id"


def test_content_language_on_every_json_response(browser):
    assert browser().get("/api/health").headers["content-language"] == "en"
    english = browser("en")
    assert english.get("/api/auth/me").headers["content-language"] == "en"
    indonesian = browser("id")
    assert indonesian.get("/api/auth/me").headers["content-language"] == "id"


def test_a_language_switch_takes_effect_on_the_next_request(browser):
    session = browser("id")
    assert session.patch("/api/auth/settings", json={"language": "en"}).status_code == 200
    response = session.delete(f"/api/categories/{uuid4()}")
    assert response.json()["detail"]["message"] == "Category not found"
    assert response.headers["content-language"] == "en"


# --- Error contract (tasks 3.1 to 3.3) -----------------------------------------------------------------------


def python_sources() -> list[pathlib.Path]:
    return sorted(path for path in APP.rglob("*.py") if "__pycache__" not in path.parts)


def call_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    return func.id if isinstance(func, ast.Name) else ""


def is_text(node: ast.AST) -> bool:
    return isinstance(node, ast.JoinedStr) or (isinstance(node, ast.Constant) and isinstance(node.value, str))


def test_no_error_is_raised_outside_the_contract():
    """Only `app/errors.py` builds an HTTPException, and no response writes a string `detail` (D4)."""
    offenders = []
    for path in python_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative = path.relative_to(BACKEND)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and call_name(node) == "HTTPException" and path.name != "errors.py":
                offenders.append(f"{relative}:{node.lineno}: HTTPException")
            if isinstance(node, ast.Call):
                for keyword in node.keywords:
                    if keyword.arg == "detail" and is_text(keyword.value):
                        offenders.append(f"{relative}:{node.lineno}: string detail")
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    if isinstance(key, ast.Constant) and key.value == "detail" and is_text(value):
                        offenders.append(f"{relative}:{node.lineno}: string detail")
    assert offenders == []


def error_codes() -> set[str]:
    from app.errors import ErrorCode

    return {code.value for code in ErrorCode}


def used_message_keys() -> set[str]:
    """The `message_key=` values the raise sites pass."""
    keys = set()
    for path in python_sources():
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call):
                for keyword in node.keywords:
                    if keyword.arg == "message_key" and isinstance(keyword.value, ast.Constant):
                        keys.add(keyword.value.value)
    return keys


def test_every_code_and_site_message_exists_in_both_server_catalogues():
    for language in ("en", "id"):
        messages = load(SERVER_MESSAGES / f"{language}.json")
        assert set(messages["errors"]) == error_codes(), language
        assert used_message_keys() <= set(messages["error_messages"]), language
    assert used_message_keys(), "the scan found no site messages"


def sample_params(text: str) -> dict:
    values = {}
    for name in placeholders(text):
        if name == "amount" or name.endswith("_amount"):
            values[name] = 125_000
        elif name.endswith("_units") or name in {"min", "max", "count", "limit", "days"}:
            values[name] = 3
        else:
            values[name] = "Contoh"
    return values


@pytest.mark.parametrize("language", ["en", "id"])
def test_every_code_renders_with_its_params(language):
    from app.errors import ErrorCode, error_detail

    messages = load(SERVER_MESSAGES / f"{language}.json")
    for code in ErrorCode:
        params = sample_params(messages["errors"][code.value])
        detail = error_detail(code, params, language=language, region="ID")
        assert set(detail) == {"code", "params", "message"} and detail["params"] == params, code
        assert detail["message"] and "{" not in detail["message"], code
        for name, value in params.items():
            if isinstance(value, int) and (name == "amount" or name.endswith("_amount")):
                assert "Rp 125.000" in detail["message"], code
    for key, text in messages["error_messages"].items():
        assert "{" not in text.format_map(sample_params(text)), key


def test_error_with_values_keeps_its_top_level_fields(browser):
    session = browser("id")
    account = session.post("/api/accounts", json={"name": "Kas Contoh", "type": "cash", "initial_balance": 1_000})
    assert account.status_code == 200, account.text
    account_id = account.json().get("id") or account.json()["account"]["id"]
    response = session.post("/api/transactions", json={"type": "expense", "account_id": account_id, "amount": 1_001})
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["code"] == "insufficient_funds"
    assert detail["params"] == {"required_amount": 1_001, "available_amount": 1_000}
    assert (detail["required_amount"], detail["available_amount"]) == (1_001, 1_000)
    assert detail["message"] == "Saldo tidak mencukupi. Tersedia Rp 1.000 dari Rp 1.001 yang dibutuhkan."


# Web messages may use values the web derives itself from top-level fields of `detail` (lib/api.ts).
WEB_DERIVED_PARAMS = {
    "insufficient_funds": {"available", "required"},
    "insufficient_units": {"available", "required"},
}


def code_parity_problems(server: dict[str, dict], web: dict[str, dict]) -> list[str]:
    """Codes missing from a catalogue, and web values the server never sends (D4)."""
    codes = error_codes()
    problems = []
    for name, messages in {**server, **web}.items():
        problems += [f"{code}: missing in {name}" for code in sorted(codes - set(messages["errors"]))]
        problems += [f"{code}: unknown code in {name}" for code in sorted(set(messages["errors"]) - codes)]
    for name, messages in web.items():
        for code, text in messages["errors"].items():
            sent = placeholders(server["server en"]["errors"].get(code, "")) | WEB_DERIVED_PARAMS.get(code, set())
            extra = icu_arguments(text) - sent
            if extra:
                problems.append(f"{code}: {name} uses {sorted(extra)} the server never sends")
    return problems


def four_catalogues() -> tuple[dict, dict]:
    server = {f"server {language}": load(SERVER_MESSAGES / f"{language}.json") for language in ("en", "id")}
    web = {f"web {language}": load(WEB_MESSAGES / f"{language}.json") for language in ("en", "id")}
    return server, web


def test_every_code_exists_in_the_four_catalogues():
    server, web = four_catalogues()
    assert code_parity_problems(server, web) == []


def test_code_parity_names_a_planted_missing_code_and_its_catalogue():
    server, web = four_catalogues()
    del web["web en"]["errors"]["category_not_found"]
    web["web id"]["errors"]["account_not_found"] = "Rekening {nama} tidak ditemukan."
    assert code_parity_problems(server, web) == [
        "category_not_found: missing in web en",
        "account_not_found: web id uses ['nama'] the server never sends",
    ]


# --- Composed text (tasks 4.1 and 4.2) -----------------------------------------------------------------------


def test_purpose_question_for_an_english_account():
    from app.services.purpose_questions import purpose_prompt

    english = purpose_prompt("en", "ID", direction="expense", amount=187_500, account="BCA Harian", hint=None, sender=None)
    assert english == {"text": "What was this for? Rp187.500 from BCA Harian", "code": "questions.purpose.expense",
                       "params": {"amount": 187_500, "account": "BCA Harian"}}
    indonesian = purpose_prompt("id", "ID", direction="expense", amount=187_500, account="BCA Harian", hint=None, sender=None)
    assert indonesian["text"] == "Apa ini? Rp187.500 dari BCA Harian"
    assert (indonesian["code"], indonesian["params"]) == (english["code"], english["params"])


def test_question_kinds_in_both_languages():
    from tests.golden_server_text import rendered_cases

    english, indonesian = rendered_cases("en", "ID"), rendered_cases("id", "ID")
    assert english["topup.single.prompt"] == "Looks like a top-up to Reksa Dana A, like last month?"
    assert indonesian["topup.single.prompt"] == "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?"
    assert english["habit.guess"] == "Looks like Makan?"
    assert indonesian["habit.guess"] == "Sepertinya Makan?"
    assert english["capture.account.expense.source"] == "From which account?"
    assert indonesian["capture.account.expense.source"] == "Dari rekening mana?"


# Words that only Indonesian server text uses; user data in the synthetic cases is removed before the check.
INDONESIAN_WORDS = re.compile(
    r"\b(apa|ini|dari|ke|masuk|yang|dan|atau|untuk|dengan|sepertinya|kayak|bulan|lalu|sebelumnya|tanpa|kategori|"
    r"keterangan|pindah|saldo|siklus|sejak|hari|kemarin|biasanya|sering|terakhir|pengeluaran|pemasukan|tabungan|"
    r"rutin|setor|tarik|tunai|biaya|jawaban|kebiasaan|dicatat|belum|sudah|termasuk|rekening|nominal|berapa|mana)\b",
    re.IGNORECASE,
)
SYNTHETIC_USER_DATA = ("BCA Harian", "Jago · Utama", "Jago · Tabungan", "Reksa Dana A", "Reksa Dana B", "Makan",
                       "Kedai Awan", "Listrik", "Rani", "RANI ***", "Groceries", "BBCA", "Oktober 2026", "25 Okt", "24 Nov",
                       "Jajan & Nongkrong")
# Month names that are spelled the same in both languages.
SHARED_WORDS = {"windows.month_name.4", "windows.month_name.9", "windows.month_name.11", "summary.period_month"}


def test_english_account_output_holds_no_indonesian_server_text():
    """D10: the composed cases rendered for an English account contain no Indonesian catalogue text."""
    from tests.golden_server_text import inline_cases, rendered_cases

    english = {**rendered_cases("en", "US"), **inline_cases("en")}
    indonesian = {**rendered_cases("id", "ID"), **inline_cases("id")}
    found = []
    for name, text in english.items():
        if name.startswith("dashboard.narrative."):
            text = text.split("|", 1)[1]
        stripped = text
        for value in SYNTHETIC_USER_DATA:
            stripped = stripped.replace(value, "")
        if INDONESIAN_WORDS.search(stripped):
            found.append(f"{name}: {text}")
        if text == indonesian[name] and name not in SHARED_WORDS and name != "notes.investment_trade":
            found.append(f"{name}: same as Indonesian")
    assert found == []


def test_cycle_summary_system_labels_follow_the_account(browser):
    def labels(session: TestClient) -> dict[str, str]:
        body = session.get("/api/dashboard/cycle-summary").json()
        found = {}

        def walk(value):
            if isinstance(value, dict):
                if value.get("label_source") == "system" and "key" in value:
                    found[str(value["key"])] = value["label"]
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(body)
        return found

    english, indonesian = labels(browser("en")), labels(browser("id"))
    assert english and set(english) == set(indonesian)
    assert english["liquid"] == "Available cash" and indonesian["liquid"] == "Dana likuid"


# --- Stored generated text (task 4.3) ------------------------------------------------------------------------


def test_language_switch_after_a_transfer_rewrites_nothing(browser):
    from app.db.pool import db_conn
    from app.services.movement_linkage import movement_note

    session = browser("id")

    def account(name: str) -> str:
        response = session.post("/api/accounts", json={"name": name, "type": "bank", "initial_balance": 1_000_000})
        assert response.status_code == 200, response.text
        return response.json().get("id") or response.json()["account"]["id"]

    source, target = account("Kas Contoh"), account("BCA Harian")
    first = session.post("/api/movements", json={"source_account_id": source, "target_account_id": target, "amount": 10_000})
    assert first.status_code == 200, first.text
    with db_conn() as conn:
        user_id = conn.execute("SELECT id FROM users WHERE username = %s", (session.username,)).fetchone()["id"]
        before = conn.execute(
            "SELECT id, notes, updated_at FROM transactions WHERE user_id = %s ORDER BY id", (user_id,),
        ).fetchall()
        with conn.cursor() as cur:
            indonesian_note = movement_note(cur, str(user_id), target)
    assert {row["notes"] for row in before} == {"Internal Movement"}
    assert indonesian_note == "Pindah saldo ke BCA Harian"

    assert session.patch("/api/auth/settings", json={"language": "en"}).status_code == 200
    with db_conn() as conn:
        unchanged = conn.execute(
            "SELECT id, notes, updated_at FROM transactions WHERE user_id = %s ORDER BY id", (user_id,),
        ).fetchall()
    assert unchanged == before

    second = session.post("/api/movements", json={"source_account_id": source, "target_account_id": target, "amount": 5_000})
    assert second.status_code == 200, second.text
    with db_conn() as conn:
        rows = conn.execute("SELECT id, notes, updated_at FROM transactions WHERE user_id = %s", (user_id,)).fetchall()
        with conn.cursor() as cur:
            english_note = movement_note(cur, str(user_id), target)
    kept = {row["id"]: row for row in before}
    assert all(row == kept[row["id"]] for row in rows if row["id"] in kept)
    assert {row["notes"] for row in rows if row["id"] not in kept} == {"Transfer"}
    assert english_note == "Transfer to BCA Harian"


def test_success_notices_follow_the_account(browser):
    for language, expected in (("en", "Transaction deleted"), ("id", "Transaksi dihapus.")):
        session = browser(language)
        account = session.post("/api/accounts", json={"name": "Kas Contoh", "type": "cash", "initial_balance": 50_000})
        account_id = account.json().get("id") or account.json()["account"]["id"]
        created = session.post("/api/transactions", json={"type": "expense", "account_id": account_id, "amount": 1_000,
                                                          "date": datetime.now(timezone.utc).isoformat()})
        assert created.status_code == 200, created.text
        assert created.json()["message"] == {"en": "Transaction recorded successfully", "id": "Transaksi dicatat."}[language]
        transaction_id = created.json()["transaction_id"]
        deleted = session.delete(f"/api/transactions/{transaction_id}")
        assert deleted.json()["message"] == expected


# --- Copy register per language (ui-copy-register, as on the web's copyRegister.test.ts) -----------------------

SLANG_ID = re.compile(
    r"\b(saranku|buat baru|buat diterima|buat mulai|nggak|ngga|gak|udah|beres|nangkep|ketangkep|nyambung|nyambungin|"
    r"bikin|aja|banget|dimatiin|kamu|aku|nunggu|gajian|lagi bikin|cuma|nebak|dobel|ketemu|benerin|lanjutin|nuntun|"
    r"tunjukin|ngecek|kayak)\b|\b\w+-mu\b",
    re.IGNORECASE,
)
SLANG_EN = re.compile(r"\b(gonna|wanna|gotta|yep|nope|kinda|sorta|stuff|awesome|oops|hey|ok|okay)\b", re.IGNORECASE)
JARGON_EN = re.compile(r"\b(outflow|inflow|ledger|reconcil\w*|liabilit\w*|runway|cost basis)\b", re.IGNORECASE)
YOU_EN = re.compile(r"\b(you|your|yours|you're)\b", re.IGNORECASE)
# Questions and suggestion reasons talk with the owner: the conversational voice (ui-copy-register).
CONVERSATION = ("questions.", "reasons.")
# Indonesian text kept byte-identical from the base revision (D4), which names "tanggal gajian".
BASE_INDONESIAN = {"errors.payday_day_deprecated", "errors.payday_schedule_retired"}


def register_offenders(language: str, catalogue: dict) -> list[str]:
    found = []
    for key, value in leaves(catalogue).items():
        if key.startswith(CONVERSATION):
            continue
        for text in value.values() if isinstance(value, dict) else [value]:
            if language == "id" and key not in BASE_INDONESIAN and SLANG_ID.search(text):
                found.append(f"{key}: slang")
            if language == "en":
                found += [f"{key}: {name}" for name, rule in (("slang", SLANG_EN), ("jargon", JARGON_EN), ("you", YOU_EN))
                          if rule.search(text)]
    return found


@pytest.mark.parametrize("language", ["en", "id"])
def test_server_catalogue_follows_the_copy_register(language):
    assert register_offenders(language, load(SERVER_MESSAGES / f"{language}.json")) == []


def test_copy_register_fails_on_planted_lines():
    english, indonesian = load(SERVER_MESSAGES / "en.json"), load(SERVER_MESSAGES / "id.json")
    english["notices"]["account_deleted"] = "Your ledger entry is gone"
    indonesian["notices"]["account_deleted"] = "Rekening udah dihapus"
    assert register_offenders("en", english) == ["notices.account_deleted: jargon", "notices.account_deleted: you"]
    assert register_offenders("id", indonesian) == ["notices.account_deleted: slang"]


@pytest.mark.parametrize("language", ["en", "id"])
def test_error_responses_log_no_rendered_text(browser, caplog, language):
    from signed_in import leaked

    session = browser(language)
    caplog.set_level(logging.DEBUG)
    account = session.post("/api/accounts", json={"name": "Dompet Rahasia", "type": "cash", "initial_balance": 1_000})
    account_id = account.json().get("id") or account.json()["account"]["id"]
    refused = session.post("/api/transactions", json={"type": "expense", "account_id": account_id, "amount": 1_001,
                                                     "notes": "Catatan Rahasia"})
    missing = session.delete(f"/api/categories/{uuid4()}")
    assert (refused.status_code, missing.status_code) == (409, 404)
    secrets = {refused.json()["detail"]["message"], missing.json()["detail"]["message"], "1.001", "1,001", "Rahasia"}
    assert leaked([record.getMessage() for record in caplog.records], secrets) == []


def test_recurring_notes_follow_each_owners_language(browser):
    """The recurring path writes each row's note in its own owner's language (spec: Background job)."""
    from datetime import date, timedelta

    from app.db.pool import db_conn

    due = date.today() - timedelta(days=1)
    notes = {}
    for language in ("en", "id"):
        session = browser(language)
        ids = []
        for name in ("Kas Contoh", "Tabungan Contoh"):
            response = session.post("/api/accounts", json={"name": name, "type": "bank", "initial_balance": 1_000_000})
            assert response.status_code == 200, response.text
            ids.append(response.json().get("id") or response.json()["account"]["id"])
        rule = session.post("/api/recurring", json={
            "name": "Listrik Contoh", "type": "transfer", "amount": 100_000, "source_account_id": ids[0],
            "target_account_id": ids[1], "schedule_type": "monthly_day", "schedule_day": due.day,
        })
        assert rule.status_code == 200, rule.text
        rule_id = rule.json()["id"]
        with db_conn() as conn:
            conn.execute("UPDATE recurring_rules SET next_due_date = %s WHERE id = %s", (due, rule_id))
            conn.commit()
        recorded = session.post(f"/api/recurring/{rule_id}/occurrences/record", json={"scheduled_for": due.isoformat()})
        assert recorded.status_code == 200, recorded.text
        with db_conn() as conn:
            rows = conn.execute("SELECT DISTINCT notes FROM transactions WHERE recurring_rule_id = %s", (rule_id,)).fetchall()
        notes[language] = {row["notes"] for row in rows}
    assert notes == {"en": {"Recurring: Listrik Contoh"}, "id": {"Rutin: Listrik Contoh"}}


def test_period_label_for_an_english_account(browser):
    """Period labels carry no message code; they sit next to the window's dates (spec: Period label)."""
    from datetime import date

    from app.i18n import t
    from app.services.report_windows import short_local_date

    assert t("en", "periods.cycle_ongoing", start=short_local_date("en", date(2026, 10, 25))) == (
        "Pay cycle since 25 Oct · ongoing")
    assert t("id", "periods.cycle_ongoing", start=short_local_date("id", date(2026, 10, 25))) == (
        "Siklus sejak 25 Okt · berlanjut")
    body = browser("en").get("/api/dashboard/period-comparison?timeframe=month").json()
    from app.core.config import settings

    month = datetime.now(timezone.utc).astimezone(ZoneInfo(settings.tz))
    assert body["label"] == f"{t('en', f'periods.month.{month.month}')} {month.year}"
    assert {"start_date", "end_date"} <= set(body["current"])


def evidence_codes() -> set[str]:
    """Codes a notification refusal can carry: EvidenceError literals and the evidence module's error codes."""
    codes = set()
    for path in python_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and call_name(node) == "EvidenceError":
                for arg in node.args:
                    # `EvidenceError("code")` and a fallback such as `EvidenceError(found or "code")`.
                    for item in arg.values if isinstance(arg, ast.BoolOp) else [arg]:
                        if isinstance(item, ast.Constant) and isinstance(item.value, str):
                            codes.add(item.value)
            if path.name == "notification_evidence.py" and isinstance(node, ast.Assign):
                if any(isinstance(target, ast.Name) and target.id == "error" for target in node.targets):
                    codes |= {item.value for item in ast.walk(node.value)
                              if isinstance(item, ast.Constant) and isinstance(item.value, str)}
            if isinstance(node, ast.keyword) and node.arg == "error_code" and isinstance(node.value, ast.Constant):
                if isinstance(node.value.value, str):
                    codes.add(node.value.value)
    return codes


def test_every_notification_refusal_code_is_an_error_code():
    """`api_error(422, exc.code)` re-raises evidence codes, so each must be a contract code (else a 500)."""
    found = evidence_codes()
    assert {"direction_unproven", "ambiguous_amount", "unsupported_seen_twice", "legacy_review_required",
            "model_uncertain"} <= found
    assert found - error_codes() == set()


def test_settings_change_answers_in_the_new_language(browser):
    session = browser("id")
    response = session.patch("/api/auth/settings", json={"language": "en", "payday_day": 25})
    assert response.status_code == 200, response.text
    assert response.headers["content-language"] == "en"
    assert response.json()["deprecations"][0]["message"] == (
        "A fixed payday no longer sets the cycle. Confirm the received salary in Settings.")
