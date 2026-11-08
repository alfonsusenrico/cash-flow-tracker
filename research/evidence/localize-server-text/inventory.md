# Server text inventory (localize-server-text task 1.2)

Base revision: `feat/language-system` 231f6a6 (76c63d1 plus the npm 10 lockfile fix), stacked on `main` 5eff424. The proposal's counts were taken at `feat/markas-beranda` d2805cb; `add-language-system` has since converted the sign-in, sign-up and settings errors, so the base holds fewer plain strings than the proposal's 181 / 111.

Method: an AST scan of `backend/app` for `HTTPException(...)` sites by the kind of their `detail`, and a scan of every natural-language string literal per module (two or more words, not SQL, not a docstring). Parser vocabulary (`notification_parser`, `capture_parsing`, `capture_time` regular expressions, `notification_evidence`, `merchant_memory` keys) is bank or owner input, not interface text, and stays unchanged (non-goal). Log lines and internal `RuntimeError`/`ValueError` invariants are not shown to anyone and stay English.

## 1. Error details

320 `HTTPException` sites: 169 plain-string details and 4 plain f-strings (104 unique texts), 145 structured dicts and 2 variable details. Typed errors that become HTTP errors: `SalaryError`, `ReportError`, `ClassificationError`, `SuggestionError`, `FlowKeyError`, `bulk_error`, `undo_error`, `refuse` (credit accounts, account roles, layouts), `_conflict` (payment links), `_retired` (recurring), `_unresolved` (entity names), `device_key_invalid`, `pairing_code_invalid`, `invalid_answer`, `offset_not_allowed`, `not_found` (test sessions).

| Module | Plain string | Plain f-string | Structured dict | Variable detail |
| --- | --- | --- | --- | --- |
| `app/core/errors.py` | 0 | 0 | 1 | 0 |
| `app/routers/accounts.py` | 23 | 1 | 4 | 0 |
| `app/routers/capture.py` | 0 | 0 | 2 | 0 |
| `app/routers/categories.py` | 11 | 0 | 0 | 0 |
| `app/routers/classification.py` | 0 | 0 | 1 | 1 |
| `app/routers/dashboard.py` | 0 | 0 | 1 | 0 |
| `app/routers/devices.py` | 0 | 0 | 2 | 0 |
| `app/routers/goals.py` | 13 | 0 | 0 | 0 |
| `app/routers/history_suggestions.py` | 0 | 0 | 1 | 1 |
| `app/routers/ingest.py` | 11 | 0 | 0 | 0 |
| `app/routers/investment_topups.py` | 1 | 0 | 0 | 0 |
| `app/routers/movements.py` | 11 | 0 | 1 | 0 |
| `app/routers/obligations.py` | 11 | 0 | 0 | 0 |
| `app/routers/recurring.py` | 22 | 1 | 17 | 0 |
| `app/routers/reports.py` | 0 | 0 | 2 | 0 |
| `app/routers/review.py` | 0 | 0 | 1 | 0 |
| `app/routers/salary.py` | 0 | 0 | 1 | 0 |
| `app/routers/transactions.py` | 30 | 1 | 7 | 0 |
| `app/services/account_links.py` | 0 | 0 | 1 | 0 |
| `app/services/auth.py` | 2 | 0 | 1 | 0 |
| `app/services/balance_check.py` | 1 | 0 | 2 | 0 |
| `app/services/capture_preview.py` | 0 | 0 | 1 | 0 |
| `app/services/capture_resolution.py` | 0 | 0 | 1 | 0 |
| `app/services/capture_suggestions.py` | 0 | 0 | 2 | 0 |
| `app/services/credit_accounts.py` | 0 | 0 | 1 | 0 |
| `app/services/debt_allocations.py` | 0 | 0 | 7 | 0 |
| `app/services/device_health.py` | 0 | 0 | 1 | 0 |
| `app/services/device_pairing.py` | 0 | 0 | 3 | 0 |
| `app/services/entity_names.py` | 0 | 0 | 1 | 0 |
| `app/services/investment_topups.py` | 4 | 0 | 16 | 0 |
| `app/services/ledger_mutations.py` | 6 | 0 | 3 | 0 |
| `app/services/movement_linkage.py` | 7 | 0 | 0 | 0 |
| `app/services/movement_pairing.py` | 3 | 0 | 8 | 0 |
| `app/services/notification_processing.py` | 6 | 0 | 8 | 0 |
| `app/services/notification_sender.py` | 0 | 0 | 5 | 0 |
| `app/services/notification_source_pocket.py` | 0 | 0 | 7 | 0 |
| `app/services/payment_links.py` | 0 | 0 | 3 | 0 |
| `app/services/plan_occurrences.py` | 0 | 0 | 1 | 0 |
| `app/services/purpose_questions.py` | 1 | 0 | 10 | 0 |
| `app/services/receipts.py` | 6 | 1 | 0 | 0 |
| `app/services/reimbursements.py` | 0 | 0 | 2 | 0 |
| `app/services/review_undo.py` | 0 | 0 | 1 | 0 |
| `app/services/seen_twice.py` | 0 | 0 | 2 | 0 |
| `app/services/test_sessions.py` | 0 | 0 | 2 | 0 |
| `app/services/text_capture.py` | 0 | 0 | 12 | 0 |
| `app/services/topup_learning.py` | 0 | 0 | 3 | 0 |
| **Total** | **169** | **4** | **145** | **2** |

Findings that shape the contract:
- **Languages today:** the plain strings are English except two Indonesian ones (`obligations.py` permanent delete, `notification_processing.py` "Notifikasi ini tidak menunggu konfirmasi rekening"). Structured messages are mostly Indonesian. 49 structured or typed codes carry a literal message; 9 codes carry two different messages (`already_linked`, `category_not_found`, `category_name_unresolved`, `account_name_unresolved`, `debt_unavailable`, `invalid_scheduled_occurrence`, `movement_leg_not_editable`, `pillar_not_allowed`, `scheduled_posting_retired`).
- **Codes without a message:** about 70 structured codes carry only `code`. The companion's `serverMessage` shows `detail.message` first, then `ProcessingReasons.describe(code)`, which falls back to "Perlu diperiksa (kode: …)." for unknown codes.
- **Two 401 sites** ("Not authenticated") in `services/auth.py`; Starlette's own 404/405 for unknown routes are strings too.
- FastAPI's request-validation 422 (`detail` list), which also carries the pydantic validator texts (`Each alias must be 3-150 characters`, `device_name must be …`, `Give exactly one of …`, `Catatan tidak boleh kosong.`), stays FastAPI's shape (D4).

## 2. Composed and stored text (D5, D6)

| Area | Module | String families | Count |
| --- | --- | --- | --- |
| Purpose questions | `services/purpose_questions.py` | prompt (expense, expense with bank hint, income, income with sender), suggestion fallback "Lainnya", repayment notes "Ganti uang dari {name}" / "Ganti uang patungan", the `rupiah` inline money shape | 4 prompts, 3 labels |
| Top-up suggestions | `services/topup_learning.py` | prompt (choose, single), "kayak bulan lalu" / "kayak sebelumnya", explanation (choose, single, pair), "rekening ini" | 3 prompts, 3 explanations, 3 fragments |
| Habits | `services/habits.py` | `basis_text`, `habit_label` (with and without category), `guess_label` "Sepertinya {category}?" | 4 |
| Capture questions | `services/capture_resolution.py` | kind choices (3), amount questions (3), account questions (4), kind, category, day and time questions | 14 |
| Capture labels | `services/capture_time.py` `moment_label`, `services/capture_suggestions.py` reasons | "Hari ini {clock}", "Kemarin {clock}", weekday and month abbreviations; reasons "biasanya jam segini", "sering", "terakhir" | 3 shapes, 19 names, 3 reasons |
| History suggestion reasons | `services/history_suggestions.py` | memory reasons (merchant, similar), rule reasons (repayment, café), note "Ganti uang patungan", group fallbacks "Tanpa keterangan" / "Tanpa kategori" | 4 reasons, 3 labels |
| Monthly comparison facts | `services/monthly_comparison.py` | category new, category above average (with unit), want share, top merchant, recurring count, unexpected count | 6 |
| Report windows and periods | `services/report_windows.py`, `observed_periods.py`, `calendar.py` | month names (12), short date, month label, 30/90 days current and previous, cycle closed, open, previous missing, month fallback with "perlu ditinjau" / "belum tersedia", calendar cycle label, legacy "Periode {dd Mon} - {dd Mon}" (strftime, English month abbreviations) | 12 names, 11 shapes |
| Money flow | `services/money_flow.py` | pillar labels (6), node labels (9) | 15 |
| Dashboard | `routers/dashboard.py` | legacy narrative (5 shapes, trend words, "surplus" / "defisit"), breakdown labels, notes and caveats (9), flow errors (3), `format_idr_compact` bare money | 17 |
| Cycle summary | `services/cycle_summary.py` | bucket labels (6), notes (2), uncategorized, net-worth labels (4), remaining labels (3), period labels (cycle, month) | 17 |
| Review and waiting fallbacks | `routers/review.py`, `services/waiting_rows.py`, `services/calendar.py` | "Tanpa keterangan", "Tanpa kategori" | 2 |
| Habit list | `services/habit_list.py` | "Tanpa nama toko · {institution}" | 1 |
| Stored notes | `notification_application.py`, `movement_linkage.py`, `movement_pairing.py`, `routers/movements.py`, `notification_sender.py`, `routers/recurring.py`, `investment_topups.py`, `text_capture.py`, `routers/accounts.py`, `routers/transactions.py` | "Pindah saldo ke {account}", "Pindah saldo", "Pindah saldo masuk/keluar", "Biaya transfer", "Internal Movement", "Transfer masuk (dari {name})", "Rutin: {plan}", "Top up Investasi", "Setor tunai" / "Tarik tunai", "Beli/Jual {lots} lot {symbol}", "Balance Adjustment (Reconciliation)", "Investment {buy/sell}" | 14 |
| Success messages | `routers/accounts.py`, `categories.py`, `movements.py`, `transactions.py`, `auth.py` | `message` of successful responses ("Transaction recorded successfully", "Moved {amount} from {a} to {b}", "Account reconciled with …", "Copy this key now …") | 18 |
| Prompts | `services/prompts/*.md` | `notification-interpretation-12`, `capture-normalization-2`, `history-suggestions-1`, `purpose-normalization-1`; `capture_normalization.PLACEHOLDER_DESCRIPTIONS` | 4 files |

Confirmed: `notification_sender.py` and `notification_source_pocket.py` hold no question wording. The sender module writes only the stored note "Transfer masuk" / "Transfer masuk dari {name}" and returns codes; the source-pocket module returns only codes and account labels. The companion composes both questions itself.

Names the server seeds or creates (starter categories, the on-demand system categories "Pindah Saldo" / "Penyesuaian Saldo", "Biaya Admin & Transfer", "Biaya kredit", "Harga Emas Dunia") are data written once by `add-language-system` (D15) or market data, and are not composed text.

## 3. Areas added to design.md before task 3

Not in the D5 table, added as D5 rows (see design.md):
- capture moment labels and suggestion reasons (`questions.capture.*`, `labels.moment.*`, `reasons.capture.*`);
- the habit list's unnamed-merchant label (`labels.fallback.unnamed_merchant`);
- the calendar and observed-period cycle labels (`periods.*`, shared with report windows);
- the success `message` of mutation responses (`notices.*`);
- the stored notes beyond D6's list: transfer fee, sender income notes, recurring "Rutin:", top-up, cash deposit and withdrawal, stock trade notes, the reconciliation note and the investment trade note (`notes.*`).

Golden output of every family above is frozen in `backend/tests/fixtures/golden_id_server_text.json` (task 1.3).
