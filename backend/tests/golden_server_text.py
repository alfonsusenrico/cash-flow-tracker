"""Golden Indonesian server text (localize-server-text D10, task 1.3).

`capture()` renders every composed text kind the server writes with synthetic values, the four prompt files and
every literal error message, using the code of the revision it runs on. The frozen result is
`fixtures/golden_id_server_text.json`; `test_golden_indonesian_server_text.py` requires today's code to
reproduce it byte for byte. Regenerate only on the base revision: `python -m tests.golden_server_text --write`.
"""

from __future__ import annotations

import ast
import hashlib
import json
import pathlib
import re
import sys
from datetime import date, datetime, timedelta, timezone

APP = pathlib.Path(__file__).resolve().parents[1] / "app"
FIXTURE = pathlib.Path(__file__).resolve().parent / "fixtures" / "golden_id_server_text.json"
PROMPTS = ("notification_interpretation", "capture_normalization", "history_suggestions", "purpose_normalization")

# Inline text that is not reachable through a function on the base revision: the module, the expression's
# source exactly as `ast.unparse` prints it there, and synthetic values for its names. `capture()` evaluates the
# expression with those values (and the module's own helpers), so the frozen text is what that code wrote.
INLINE: dict[str, tuple[str, str, dict]] = {
    "purpose.repayment_named": ("services/purpose_questions.py", "f'Ganti uang dari {name}'", {"name": "Rani"}),
    "purpose.repayment_shared": ("services/purpose_questions.py", "'Ganti uang patungan'", {}),
    "purpose.suggestion_other": ("services/purpose_questions.py", "'Lainnya'", {}),
    "topup.this_account": ("services/topup_learning.py", "'rekening ini'", {}),
    "capture.kind_question": ("services/capture_resolution.py", "'Ini pengeluaran, pemasukan, atau pindah saldo?'", {}),
    "capture.category_question": ("services/capture_resolution.py", "'Kategorinya apa?'", {}),
    "capture.day_question": ("services/capture_resolution.py", "'Hari apa?'", {}),
    "capture.time_question": ("services/capture_resolution.py", "'Jam berapa?'", {}),
    "capture.reason_time": ("services/capture_suggestions.py", "'biasanya jam segini'", {}),
    "capture.reason_frequent": ("services/capture_suggestions.py", "'sering'", {}),
    "capture.reason_recent": ("services/capture_suggestions.py", "'terakhir'", {}),
    "history.reason_merchant": ("services/history_suggestions.py", "f'Sama dengan pilihan Anda untuk {merchant}'", {"merchant": "Kedai Awan"}),
    "history.reason_similar": ("services/history_suggestions.py", "'Sama dengan pilihan Anda untuk notifikasi bank serupa'", {}),
    "history.reason_repayment": ("services/history_suggestions.py", "'Transfer masuk menyebut patungan atau ganti uang: teman membayar kembali, bukan pemasukan'", {}),
    "history.reason_cafe": ("services/history_suggestions.py", "'Kafe, kopi atau jajanan masuk ke Jajan & Nongkrong'", {}),
    "history.note_repayment": ("services/history_suggestions.py", "'Ganti uang patungan'", {}),
    "history.fallback_undescribed": ("services/history_suggestions.py", "'Tanpa keterangan'", {}),
    "history.fallback_uncategorized": ("services/history_suggestions.py", "'Tanpa kategori'", {}),
    "comparison.category_new": ("services/monthly_comparison.py", "f\"{entry['name']}: kategori baru\"", {"entry": {"name": "Makan"}}),
    "comparison.category_above": ("services/monthly_comparison.py", "f\"{entry['name']} di atas rata-rata {len(baseline)} {unit}\"", {"entry": {"name": "Makan"}, "baseline": [1, 2, 3], "unit": "bulan"}),
    "comparison.want_share": ("services/monthly_comparison.py", "f'Porsi Keinginan {share_now}% dari pengeluaran ({previous.period.label}: {share_before}%)'", {"share_now": 34, "share_before": 28, "previous": {"period": {"label": "Oktober 2026"}}}),
    "comparison.top_merchant": ("services/monthly_comparison.py", "f\"Tempat belanja terbesar: {top['name']}\"", {"top": {"name": "Kedai Awan"}}),
    "comparison.recurring": ("services/monthly_comparison.py", "f'{count} tagihan dan langganan berulang'", {"count": 3}),
    "comparison.unexpected": ("services/monthly_comparison.py", "f'{count} pengeluaran tak terduga'", {"count": 2}),
    "windows.last_30": ("services/report_windows.py", "'30 Hari Terakhir'", {}),
    "windows.last_90": ("services/report_windows.py", "'90 Hari Terakhir'", {}),
    "windows.previous_30": ("services/report_windows.py", "'30 Hari Sebelumnya'", {}),
    "windows.previous_90": ("services/report_windows.py", "'90 Hari Sebelumnya'", {}),
    "windows.previous_cycle_missing": ("services/report_windows.py", "'Siklus sebelumnya belum tersedia'", {}),
    "windows.cycle_closed": ("services/report_windows.py", "f'Siklus {short_local_date(start_date)} – {short_local_date(end_date)}'", {"start_date": date(2026, 10, 25), "end_date": date(2026, 11, 24)}),
    "windows.cycle_open": ("services/report_windows.py", "f'Siklus sejak {short_local_date(start_date)} · berlanjut'", {"start_date": date(2026, 10, 25)}),
    "windows.cycle_needs_review": ("services/report_windows.py", "f'{month_label} · siklus gaji {reason}'", {"month_label": "November 2026", "reason": "perlu ditinjau"}),
    "windows.cycle_unavailable": ("services/report_windows.py", "f'{month_label} · siklus gaji {reason}'", {"month_label": "November 2026", "reason": "belum tersedia"}),
    "observed.cycle_open": ("services/observed_periods.py", "f'Siklus sejak {first_day}'", {"first_day": "25 Okt"}),
    "observed.cycle_closed": ("services/observed_periods.py", "f'Siklus {first_day} – {short_local_date(last.astimezone(zone).date())}'", {"first_day": "25 Okt", "last": datetime(2026, 11, 24, 5, tzinfo=timezone.utc), "zone": timezone(timedelta(hours=7))}),
    "calendar.cycle": ("services/calendar.py", "f'Siklus {short_local_date(start_date)} – {short_local_date(grid_end)}'", {"start_date": date(2026, 10, 25), "grid_end": date(2026, 11, 24)}),
    "summary.note_savings": ("services/cycle_summary.py", "'Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan'", {}),
    "summary.note_reimbursed": ("services/cycle_summary.py", "f'Sudah dikurangi ganti uang {rupiah(offset)}'", {"offset": 250000}),
    "summary.uncategorized": ("services/cycle_summary.py", "'Tanpa kategori'", {}),
    "summary.liquid": ("services/cycle_summary.py", "'Dana likuid'", {}),
    "summary.investment_cash": ("services/cycle_summary.py", "'Dana investasi'", {}),
    "summary.investments": ("services/cycle_summary.py", "'Investasi'", {}),
    "summary.negative_balance": ("services/cycle_summary.py", "'Saldo minus perlu dicocokkan'", {}),
    "summary.income": ("services/cycle_summary.py", "'Masuk'", {}),
    "summary.out": ("services/cycle_summary.py", "'Keluar'", {}),
    "summary.scheduled": ("services/cycle_summary.py", "'Terjadwal'", {}),
    "summary.period_cycle": ("services/cycle_summary.py", "f'Siklus sejak {short_local_date(start_date)}'", {"start_date": date(2026, 10, 25)}),
    "summary.period_month": ("services/cycle_summary.py", "f'{MONTH_NAMES_ID[start_date.month - 1]} {start_date.year}'", {"start_date": date(2026, 11, 1)}),
    "dashboard.note_savings": ("routers/dashboard.py", "'Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan'", {}),
    "dashboard.note_reimbursed": ("routers/dashboard.py", "f'Sudah dikurangi ganti uang {rupiah(offset)}'", {"offset": 250000}),
    "dashboard.gross": ("routers/dashboard.py", "'Belanja tercatat'", {}),
    "dashboard.gross_note": ("routers/dashboard.py", "'Pengeluaran di luar pindah saldo, investasi dan kategori yang tidak dihitung'", {}),
    "dashboard.reimbursement": ("routers/dashboard.py", "'Ganti uang'", {}),
    "dashboard.reimbursement_note": ("routers/dashboard.py", "'Uang pengganti mengurangi kategori yang dibayarnya'", {}),
    "dashboard.caveat_out": ("routers/dashboard.py", "'Pindah saldo antar rekening sendiri tidak dihitung keluar.'", {}),
    "dashboard.caveat_spending": ("routers/dashboard.py", "'Pengeluaran tidak termasuk yang ditabung atau diinvestasikan.'", {}),
    "dashboard.caveat_belanja": ("routers/dashboard.py", "'Belanja tidak termasuk yang ditabung atau diinvestasikan.'", {}),
    "review.undescribed": ("routers/review.py", "'Tanpa keterangan'", {}),
    "review.uncategorized": ("routers/review.py", "'Tanpa kategori'", {}),
    "waiting.undescribed": ("services/waiting_rows.py", "'Tanpa keterangan'", {}),
    "calendar.undescribed": ("services/calendar.py", "'Tanpa keterangan'", {}),
    "habit_list.unnamed": ("services/habit_list.py", "f'Tanpa nama toko · {institution_name}'", {"institution_name": "BCA"}),
    "notes.movement_to": ("services/notification_application.py", "f\"Pindah saldo ke {target['name']}\"", {"target": {"name": "BCA Harian"}}),
    "notes.movement": ("services/notification_application.py", "'Pindah saldo'", {}),
    "notes.stock_trade": ("services/notification_application.py", "f\"{('Beli' if parsed.investment_action == 'buy' else 'Jual')} {parsed.lots} lot {parsed.symbol}\"", {"parsed": {"investment_action": "buy", "lots": 2, "symbol": "BBCA"}}),
    "notes.stock_sale": ("services/notification_application.py", "f\"{('Beli' if parsed.investment_action == 'buy' else 'Jual')} {parsed.lots} lot {parsed.symbol}\"", {"parsed": {"investment_action": "sell", "lots": 1, "symbol": "BBCA"}}),
    "notes.linked_to": ("services/movement_linkage.py", "f'Pindah saldo ke {label}'", {"label": "Jago · Tabungan"}),
    "notes.linked_in": ("services/movement_linkage.py", "'Pindah saldo masuk'", {}),
    "notes.linked_out": ("services/movement_linkage.py", "'Pindah saldo keluar'", {}),
    "notes.linked": ("services/movement_linkage.py", "'Pindah saldo'", {}),
    "notes.paired": ("services/movement_pairing.py", "'Pindah saldo'", {}),
    "notes.transfer_fee": ("services/movement_pairing.py", "'Biaya transfer'", {}),
    "notes.movement_default": ("routers/movements.py", "'Internal Movement'", {}),
    "notes.sender_named": ("services/notification_sender.py", "f'Transfer masuk dari {name}'", {"name": "Rani"}),
    "notes.sender": ("services/notification_sender.py", "'Transfer masuk'", {}),
    "notes.recurring": ("routers/recurring.py", "f\"Rutin: {rule['name']}\"", {"rule": {"name": "Listrik"}}),
    "notes.topup": ("services/investment_topups.py", "'Top up Investasi'", {}),
    "notes.cash_deposit": ("services/text_capture.py", "'Setor tunai'", {}),
    "notes.cash_withdrawal": ("services/text_capture.py", "'Tarik tunai'", {}),
    "notes.reconciliation": ("routers/accounts.py", "'Balance Adjustment (Reconciliation)'", {}),
    "notes.investment_trade": ("routers/transactions.py", "f'Investment {payload.investment_action}'", {"payload": {"investment_action": "buy"}}),
}


class _Attributes(dict):
    """Lets an inline expression read `entry.name` style attributes from plain synthetic values."""

    def __getattr__(self, name):
        value = self[name]
        return _Attributes(value) if isinstance(value, dict) else value


def _module_globals(relative: str) -> dict:
    module = "app." + relative.removesuffix(".py").replace("/", ".")
    __import__(module)
    return vars(sys.modules[module])


def _expressions(relative: str) -> set[str]:
    tree = ast.parse((APP / relative).read_text())
    return {ast.unparse(node) for node in ast.walk(tree) if isinstance(node, (ast.Constant, ast.JoinedStr))}


def inline_text(relative: str, source: str, params: dict) -> str:
    if source not in _expressions(relative):
        raise AssertionError(f"{relative} no longer holds {source}")
    names = {key: _Attributes(value) if isinstance(value, dict) else value for key, value in params.items()}
    return eval(compile(source, relative, "eval"), dict(_module_globals(relative)), names)  # noqa: S307


def rendered_cases() -> dict[str, str]:
    """Text written through functions and constants of the base revision."""
    from app.routers import dashboard
    from app.services import (
        capture_resolution, capture_time, cycle_summary, habits, money_flow, purpose_questions, report_windows,
        topup_learning,
    )

    cases: dict[str, str] = {
        "purpose.expense": purpose_questions.purpose_prompt_text(direction="expense", amount=187500, account="BCA Harian", hint=None, sender=None),
        "purpose.expense_hint": purpose_questions.purpose_prompt_text(direction="expense", amount=187500, account="BCA Harian", hint="Groceries", sender=None),
        "purpose.income": purpose_questions.purpose_prompt_text(direction="income", amount=1250000, account="Jago · Utama", hint=None, sender=None),
        "purpose.income_sender": purpose_questions.purpose_prompt_text(direction="income", amount=1250000, account="Jago · Utama", hint=None, sender="RANI ***"),
        "habit.basis": habits.basis_text(3, 4),
        "habit.label": habits.habit_label("Makan"),
        "habit.label_bare": habits.habit_label(None),
        "habit.guess": habits.guess_label("Makan"),
        "money.inline": purpose_questions.rupiah(1234567),
        "money.figure": cycle_summary.rupiah(1234567),
        "money.bare": dashboard.format_idr_compact(1234567),
        "windows.short_date": report_windows.short_local_date(date(2026, 10, 25)),
        "windows.month": report_windows.month_window(date(2026, 2, 10), 0)[2],
        "windows.month_previous": report_windows.month_window(date(2026, 1, 31), -1)[2],
        "capture.moment_today": capture_time.moment_label(datetime(2026, 11, 6, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
        "capture.moment_yesterday": capture_time.moment_label(datetime(2026, 11, 5, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
        "capture.moment_weekday": capture_time.moment_label(datetime(2026, 10, 31, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
        "capture.moment_year": capture_time.moment_label(datetime(2025, 12, 30, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
    }
    for month in range(1, 13):
        cases[f"windows.month_name.{month}"] = report_windows.MONTH_NAMES_ID[month - 1]
    for option in capture_resolution.KIND_OPTIONS:
        cases[f"capture.kind.{option['id']}"] = option["label"]
    for reason, text in capture_resolution.AMOUNT_PROMPTS.items():
        cases[f"capture.amount.{reason}"] = text
    for (kind, role), text in capture_resolution.ACCOUNT_PROMPTS.items():
        cases[f"capture.account.{kind}.{role}"] = text
    for key, label, _field in cycle_summary.KELUAR_BUCKETS:
        cases[f"summary.bucket.{key}"] = label
    for key, label in money_flow.PILLAR_LABELS.items():
        cases[f"flow.pillar.{key}"] = label
    for node in ("src:reimbursement", "src:other", "src:unknown", "opening", "neg_opening", "neg_closing", "other_out", "waiting", "remaining"):
        cases[f"flow.node.{node}"] = money_flow._node_label(node, {}, {})

    narratives = {
        "empty": (0, 0, 0.0, 0, None, None),
        "first": (450000, 0, 0.0, -120000, None, None),
        "up_driver": (450000, 300000, 50.0, 80000, "Makan", 12.5),
        "down_new_driver": (300000, 450000, -33.3, 80000, "Makan", None),
        "stable": (300000, 300000, 0.0, 0, None, None),
    }
    for name, values in narratives.items():
        text, trend = dashboard.generate_financial_narrative(*values)
        cases[f"dashboard.narrative.{name}"] = f"{trend}|{text}"

    choices = [{"id": "p1", "name": "Reksa Dana A", "label": "Bibit · Reksa Dana A"},
               {"id": "p2", "name": "Reksa Dana B", "label": "Bibit · Reksa Dana B"}]
    products = {item["id"]: item for item in choices}
    month_before = datetime(2026, 10, 3, 3, tzinfo=timezone.utc)
    stats = {key: topup_learning.ProductStats(key, support=3, agree=3, labels=3, last_confirmed=month_before)
             for key in ("p1", "p2")}
    pattern = topup_learning.Pattern(stats=stats, products=products, learned=["p1", "p2"])
    debit = {"amount": 500000, "account_name": "BCA Harian", "date": datetime(2026, 11, 3, 3, tzinfo=timezone.utc)}
    views = {
        "choose": topup_learning.Assignment(target_id=None, choices=("p1", "p2"), rule="ask"),
        "single": topup_learning.Assignment(target_id="p1", choices=("p1",), rule="single"),
        "pair": topup_learning.Assignment(target_id="p2", choices=("p1", "p2"), rule="pair"),
    }
    for name, assignment in views.items():
        view = topup_learning.suggestion_view(debit, assignment, pattern, [{"id": "d1"}, {"id": "d2"}])
        cases[f"topup.{name}.prompt"] = view["prompt"]
        cases[f"topup.{name}.explanation"] = view["explanation"]
    earlier = {**debit, "date": datetime(2026, 12, 20, 3, tzinfo=timezone.utc), "account_name": None}
    cases["topup.earlier.prompt"] = topup_learning.suggestion_view(earlier, views["single"], pattern, [])["prompt"]
    return cases


def prompt_files() -> dict[str, dict[str, str]]:
    result = {}
    for name in PROMPTS:
        data = (APP / "services" / "prompts" / f"{name}.md").read_bytes()
        result[name] = {"sha256": hashlib.sha256(data).hexdigest(), "text": data.decode("utf-8")}
    return result


def _literal(node: ast.AST) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


# Error helpers and typed errors that take the code and the message as arguments.
TYPED_ERRORS = {"bulk_error", "_conflict", "_retired", "_unresolved", "SalaryError", "ReportError",
                "ClassificationError", "SuggestionError", "reject_movement_legs"}
SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")


def _typed_pairs(node: ast.Call) -> tuple[str, str] | None:
    literals = [text for argument in node.args if (text := _literal(argument)) is not None]
    code = next((text for text in literals if SNAKE.match(text)), None)
    message = next((text for text in literals if " " in text), None)
    return (code, message) if code and message else None


def error_messages() -> dict[str, list[dict]]:
    """Every literal error text on the base revision: plain-string details and structured `message`s."""
    plain: dict[str, set[str]] = {}
    structured: dict[str, set[str]] = {}
    for path in sorted(APP.rglob("*.py")):
        relative = str(path.relative_to(APP))
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", None)) == "HTTPException":
                detail = next((keyword.value for keyword in node.keywords if keyword.arg == "detail"), None)
                if detail is not None and (text := _literal(detail)) is not None:
                    plain.setdefault(text, set()).add(relative)
            if isinstance(node, ast.Dict):
                fields = {_literal(key): value for key, value in zip(node.keys, node.values) if key is not None}
                code, message = _literal(fields.get("code")) if "code" in fields else None, fields.get("message")
                if code and message is not None and (text := _literal(message)) is not None:
                    structured.setdefault(f"{code}|{text}", set()).add(relative)
                elif relative == "routers/dashboard.py" and node.keys and all(
                    key is not None and SNAKE.match(_literal(key) or "") and " " in (_literal(value) or "")
                    for key, value in zip(node.keys, node.values)
                ):
                    for key, value in zip(node.keys, node.values):
                        structured.setdefault(f"{_literal(key)}|{_literal(value)}", set()).add(relative)
            if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", None)) in TYPED_ERRORS:
                if pair := _typed_pairs(node):
                    structured.setdefault(f"{pair[0]}|{pair[1]}", set()).add(relative)
    return {
        "plain": [{"text": text, "modules": sorted(modules)} for text, modules in sorted(plain.items())],
        "structured": [
            {"code": key.split("|", 1)[0], "text": key.split("|", 1)[1], "modules": sorted(modules)}
            for key, modules in sorted(structured.items())
        ],
    }


def capture() -> dict:
    inline = {name: inline_text(module, source, params) for name, (module, source, params) in INLINE.items()}
    return {
        "rendered": rendered_cases(),
        "inline": inline,
        "prompts": prompt_files(),
        "errors": error_messages(),
    }


if __name__ == "__main__":
    if "--write" in sys.argv:
        FIXTURE.write_text(json.dumps(capture(), ensure_ascii=False, indent=1, sort_keys=True) + "\n")
        print(f"wrote {FIXTURE}")
    else:
        print(json.dumps(capture(), ensure_ascii=False, indent=1, sort_keys=True))
