"""Golden Indonesian server text (localize-server-text D10, task 1.3).

`fixtures/golden_id_server_text.json` was written on the base revision by commit 2cf8479 (its generator read
the base code: every composed text kind with synthetic values, the four prompt files and every literal error
message). It is frozen. `capture()` renders the same cases through today's catalogue-based code, and
`test_golden_indonesian_server_text.py` requires an Indonesian account to get the same bytes.
"""

from __future__ import annotations

import hashlib
import pathlib
from datetime import date, datetime, timezone

FIXTURE = pathlib.Path(__file__).resolve().parent / "fixtures" / "golden_id_server_text.json"
PROMPTS = ("notification_interpretation", "capture_normalization", "history_suggestions", "purpose_normalization")

# Inline text of the base revision, now rendered from the server catalogue: the message key and the synthetic
# values standing in for the names the base expression used (the base expressions are in the fixture's commit).
INLINE: dict[str, tuple[str, dict]] = {
    'purpose.repayment_named': ('notes.repayment_named', {'name': 'Rani'}),
    'purpose.repayment_shared': ('notes.repayment_shared', {}),
    'purpose.suggestion_other': ('labels.fallback.other', {}),
    'topup.this_account': ('labels.fallback.this_account', {}),
    'capture.kind_question': ('questions.capture.kind', {}),
    'capture.category_question': ('questions.capture.category', {}),
    'capture.day_question': ('questions.capture.day', {}),
    'capture.time_question': ('questions.capture.time', {}),
    'capture.reason_time': ('reasons.capture.time', {}),
    'capture.reason_frequent': ('reasons.capture.frequent', {}),
    'capture.reason_recent': ('reasons.capture.recent', {}),
    'history.reason_merchant': ('reasons.history.merchant', {'merchant': 'Kedai Awan'}),
    'history.reason_similar': ('reasons.history.similar', {}),
    'history.reason_repayment': ('reasons.history.repayment', {}),
    'history.reason_cafe': ('reasons.history.cafe', {}),
    'history.note_repayment': ('notes.repayment_shared', {}),
    'history.fallback_undescribed': ('labels.fallback.undescribed', {}),
    'history.fallback_uncategorized': ('labels.fallback.uncategorized', {}),
    'comparison.category_new': ('facts.comparison.category_new', {'category': 'Makan'}),
    'comparison.category_above': ('facts.comparison.category_above_month', {'category': 'Makan', 'count': 3}),
    'comparison.want_share': ('facts.comparison.want_share', {'share': 34, 'period': 'Oktober 2026', 'previous_share': 28}),
    'comparison.top_merchant': ('facts.comparison.top_merchant', {'merchant': 'Kedai Awan'}),
    'comparison.recurring': ('facts.comparison.recurring', {'count': 3}),
    'comparison.unexpected': ('facts.comparison.unexpected', {'count': 2}),
    'windows.last_30': ('periods.last_30', {}),
    'windows.last_90': ('periods.last_90', {}),
    'windows.previous_30': ('periods.previous_30', {}),
    'windows.previous_90': ('periods.previous_90', {}),
    'windows.previous_cycle_missing': ('periods.previous_cycle_missing', {}),
    'windows.cycle_closed': ('periods.cycle_range', {'start': '25 Okt', 'end': '24 Nov'}),
    'windows.cycle_open': ('periods.cycle_ongoing', {'start': '25 Okt'}),
    'windows.cycle_needs_review': ('periods.month_cycle_review', {'month': 'November 2026'}),
    'windows.cycle_unavailable': ('periods.month_cycle_unavailable', {'month': 'November 2026'}),
    'observed.cycle_open': ('periods.cycle_since', {'start': '25 Okt'}),
    'observed.cycle_closed': ('periods.cycle_range', {'start': '25 Okt', 'end': '24 Nov'}),
    'calendar.cycle': ('periods.cycle_range', {'start': '25 Okt', 'end': '24 Nov'}),
    'summary.note_savings': ('notes.summary.includes_savings_moves', {}),
    'summary.note_reimbursed': ('notes.summary.reduced_by_reimbursements', {'amount': 'Rp 250.000'}),
    'summary.uncategorized': ('labels.fallback.uncategorized', {}),
    'summary.liquid': ('labels.summary.liquid', {}),
    'summary.investment_cash': ('labels.summary.investment_cash', {}),
    'summary.investments': ('labels.summary.investments', {}),
    'summary.negative_balance': ('labels.summary.negative_balance', {}),
    'summary.period_cycle': ('periods.cycle_since', {'start': '25 Okt'}),
    'summary.period_month': ('periods.month_year', {'month': 'November', 'year': 2026}),
    'dashboard.note_savings': ('notes.summary.includes_savings_moves', {}),
    'dashboard.note_reimbursed': ('notes.summary.reduced_by_reimbursements', {'amount': 'Rp 250.000'}),
    'dashboard.gross': ('labels.dashboard.gross', {}),
    'dashboard.gross_note': ('labels.dashboard.gross_note', {}),
    'dashboard.reimbursement': ('labels.dashboard.reimbursement', {}),
    'dashboard.reimbursement_note': ('labels.dashboard.reimbursement_note', {}),
    'dashboard.caveat_out': ('labels.dashboard.caveat_out', {}),
    'dashboard.caveat_spending': ('labels.dashboard.caveat_spending', {}),
    'dashboard.caveat_belanja': ('labels.dashboard.caveat_belanja', {}),
    'review.undescribed': ('labels.fallback.undescribed', {}),
    'review.uncategorized': ('labels.fallback.uncategorized', {}),
    'waiting.undescribed': ('labels.fallback.undescribed', {}),
    'calendar.undescribed': ('labels.fallback.undescribed', {}),
    'habit_list.unnamed': ('labels.fallback.unnamed_merchant', {'institution': 'BCA'}),
    'notes.movement_to': ('notes.movement_to', {'account': 'BCA Harian'}),
    'notes.movement': ('notes.movement', {}),
    'notes.stock_trade': ('notes.stock_buy', {'count': 2, 'symbol': 'BBCA'}),
    'notes.stock_sale': ('notes.stock_sell', {'count': 1, 'symbol': 'BBCA'}),
    'notes.linked_to': ('notes.movement_to', {'account': 'Jago · Tabungan'}),
    'notes.linked_in': ('notes.movement_in', {}),
    'notes.linked_out': ('notes.movement_out', {}),
    'notes.linked': ('notes.movement', {}),
    'notes.paired': ('notes.movement', {}),
    'notes.transfer_fee': ('notes.transfer_fee', {}),
    'notes.movement_default': ('notes.movement_default', {}),
    'notes.sender_named': ('notes.sender_named', {'name': 'Rani'}),
    'notes.sender': ('notes.sender', {}),
    'notes.recurring': ('notes.recurring', {'plan': 'Listrik'}),
    'notes.topup': ('notes.topup', {}),
    'notes.cash_deposit': ('notes.cash_deposit', {}),
    'notes.cash_withdrawal': ('notes.cash_withdrawal', {}),
    'notes.reconciliation': ('notes.reconciliation', {}),
    'notes.investment_trade': ('notes.investment_trade', {'action': 'buy'}),
}


def rendered_cases(language: str = "id", region: str = "ID") -> dict[str, str]:
    """Text written through the composing functions, rendered for one account language and region."""
    from app.i18n import format_money, t
    from app.routers import dashboard
    from app.services import (
        capture_resolution, capture_time, cycle_summary, habits, money_flow, purpose_questions, report_windows,
        topup_learning,
    )

    def purpose(**values):
        return purpose_questions.purpose_prompt(language, region, **values)["text"]

    cases: dict[str, str] = {
        "purpose.expense": purpose(direction="expense", amount=187500, account="BCA Harian", hint=None, sender=None),
        "purpose.expense_hint": purpose(direction="expense", amount=187500, account="BCA Harian", hint="Groceries", sender=None),
        "purpose.income": purpose(direction="income", amount=1250000, account="Jago · Utama", hint=None, sender=None),
        "purpose.income_sender": purpose(direction="income", amount=1250000, account="Jago · Utama", hint=None, sender="RANI ***"),
        "habit.basis": habits.basis_text(language, 3, 4),
        "habit.label": habits.habit_label(language, "Makan"),
        "habit.label_bare": habits.habit_label(language, None),
        "habit.guess": habits.guess_label(language, "Makan")["text"],
        "money.inline": format_money(1234567, region, "inline"),
        "money.figure": format_money(1234567, region, "figure"),
        "money.bare": format_money(1234567, region, "bare"),
        "windows.short_date": report_windows.short_local_date(language, date(2026, 10, 25)),
        "windows.month": report_windows.month_window(date(2026, 2, 10), 0, language=language)[2],
        "windows.month_previous": report_windows.month_window(date(2026, 1, 31), -1, language=language)[2],
        "capture.moment_today": capture_time.moment_label(language, datetime(2026, 11, 6, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
        "capture.moment_yesterday": capture_time.moment_label(language, datetime(2026, 11, 5, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
        "capture.moment_weekday": capture_time.moment_label(language, datetime(2026, 10, 31, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
        "capture.moment_year": capture_time.moment_label(language, datetime(2025, 12, 30, 7, tzinfo=timezone.utc), datetime(2026, 11, 6, 9, tzinfo=timezone.utc), "Asia/Jakarta"),
    }
    for month in range(1, 13):
        cases[f"windows.month_name.{month}"] = t(language, f"periods.month.{month}")
    for kind in capture_resolution.KIND_OPTIONS:
        cases[f"capture.kind.{kind}"] = t(language, f"labels.kind.{kind}")
    for reason, key in capture_resolution.AMOUNT_PROMPTS.items():
        cases[f"capture.amount.{reason}"] = t(language, key)
    for (kind, role), key in capture_resolution.ACCOUNT_PROMPTS.items():
        cases[f"capture.account.{kind}.{role}"] = t(language, key)
    for key, _field in cycle_summary.KELUAR_BUCKETS:
        cases[f"summary.bucket.{key}"] = cycle_summary.bucket_label(language, key)
    for key in money_flow.PILLAR_ORDER:
        cases[f"flow.pillar.{key}"] = t(language, f"labels.pillar.{key}")
    for node in ("src:reimbursement", "src:other", "src:unknown", "opening", "neg_opening", "neg_closing", "other_out", "waiting", "remaining"):
        cases[f"flow.node.{node}"] = money_flow._node_label(node, {}, {}, language)

    narratives = {
        "empty": (0, 0, 0.0, 0, None, None),
        "first": (450000, 0, 0.0, -120000, None, None),
        "up_driver": (450000, 300000, 50.0, 80000, "Makan", 12.5),
        "down_new_driver": (300000, 450000, -33.3, 80000, "Makan", None),
        "stable": (300000, 300000, 0.0, 0, None, None),
    }
    for name, values in narratives.items():
        text, trend = dashboard.generate_financial_narrative(language, region, *values)
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
        view = topup_learning.suggestion_view(debit, assignment, pattern, [{"id": "d1"}, {"id": "d2"}],
                                              language=language, region=region)
        cases[f"topup.{name}.prompt"] = view["prompt"]
        cases[f"topup.{name}.explanation"] = view["explanation"]
    earlier = {**debit, "date": datetime(2026, 12, 20, 3, tzinfo=timezone.utc), "account_name": None}
    cases["topup.earlier.prompt"] = topup_learning.suggestion_view(
        earlier, views["single"], pattern, [], language=language, region=region,
    )["prompt"]
    return cases


def inline_cases(language: str = "id") -> dict[str, str]:
    from app.i18n import t

    return {name: t(language, key, **params) for name, (key, params) in INLINE.items()}


def prompt_files() -> dict[str, dict[str, str]]:
    """The Indonesian rendering of each prompt, read through the code path the model calls use (D7)."""
    from app.services.prompt_language import load_prompt

    result = {}
    for name in PROMPTS:
        text = load_prompt(name, "id").text
        result[name] = {"sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), "text": text}
    return result


def error_catalogue_texts(language: str) -> dict[str, set[str]]:
    """Every server error message of `language`, by code: `errors.<code>` and the site messages."""
    from app.i18n.catalogue import catalogue

    messages = catalogue(language)
    texts: dict[str, set[str]] = {code: {text} for code, text in messages["errors"].items()}
    for text in messages["error_messages"].values():
        texts.setdefault("*", set()).add(text)
    return texts


def capture(language: str = "id", region: str = "ID") -> dict:
    return {
        "rendered": rendered_cases(language, region),
        "inline": inline_cases(language),
        "prompts": prompt_files(),
    }
