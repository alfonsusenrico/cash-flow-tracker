# Design

## Context

Facts from the code at origin/main 5004b11:

- Every report figure comes from `services/report_figures.py`:
  - reportable income and expense SQL;
  - `category_breakdown`, whose rows add up to total outflow;
  - `cash_flow_totals`, which applies reimbursement offsets.

  `observed_insights.period_comparison` holds the fair comparison: both windows are cut to the shorter elapsed time, and it reports `current_to_date`. `cash_flow_history` and `recurring_charges` live in the same module. `essential_cost.estimate_essential_cost` and `dashboard.calculate_ketahanan_dana` give the essential cost and coverage.
- `observed_periods.load_period_history` lists completed periods, newest first, plus the current one. It returns calendar months, or closed salary cycles and then months before the first salary. A period that began before the first recorded transaction is left out.
- `kakeibo.effective_pillar_sql` resolves the transaction pillar, then the category pillar, and falls back to `'need'`. The same `'need'` fallback is copied by hand in nine other places:
  - `review.py` (queue);
  - `history_suggestions.py` (two);
  - `text_capture.py`;
  - `reimbursements.py` (pillar apportioning);
  - `essential_cost.py` (two);
  - `observed_insights._pillar_spending`;
  - `report_figures.category_breakdown`.

  `categories.py` derives `need`/`want` from `is_primary` when a category is read. `transactions.py` copies such a derived pillar onto a new or recategorised row.
- `merchant_memory.record_owner_corrections` writes one `classification_feedback` row per changed row. A row counts as changed when its classification differs, including `effective_kakeibo`. It also upserts owner memory and closes an open purpose question. The ledger edit, bulk review, movement edit, investment top-up and accepted-suggestion paths all call it. Because unclassified rows resolve to `need` today, setting `need` on one writes no feedback.
- The Android companion (`financial-tracker-mobile-listener` main 35dfbeb, read only) reads three things:
  - `top_categories` from the cash-flow summary;
  - `/api/categories`;
  - capture results with a `kakeibo` field.

  `ApiModels.shownTopCategories()` rejects the whole list when a non-null `kakeibo_type` is not one of the five pillars. A `null` is accepted, and `pillarLabel(null)` shows no pillar. `CapturePresentation.kakeiboLabel(null)` returns `null`.
- The web Analisis page always shows budget columns, "Tanpa Batas" chips and "Atur batas" buttons, even when no category has a budget. Category budgets are `categories.monthly_budget`. The overall `users.monthly_spending_budget` is set in Settings › Anggaran.

## Goals / Non-Goals

Goals:
- one report per period that a user reads in a few minutes on a phone;
- figures that equal the ledger;
- an honest sixth Kakeibo bucket and a fast way to fill it;
- budgets that appear only once someone sets one;
- a readiness API for the later phone reminder.

Non-goals: see proposal.

## Decisions

### D1. The report is a composition of existing services

`services/monthly_report.py` builds the report for one period window `[start, end]`. Each section comes from an existing source:

| Section | Source |
| --- | --- |
| Flow (inflow, gross outflow, offset, outflow, net) | `observed_insights._window_figures`, the same SQL and offsets as the dashboard and ledger summary |
| Income sources | reportable income grouped by category; reimbursements are not income and are left out, as everywhere |
| Fresh savings and surplus | `dashboard.get_kakeibo_breakdown`: `saving_spent` (= saving-classified expenses + net savings movements, floored at 0), with surplus = `net` shown separately |
| Spending by pillar | the same breakdown, five pillars plus `unclassified` |
| Top categories | `report_figures.category_breakdown` |
| Top merchants | `observed_insights._merchant_spending` (gross, as in Analisis) |
| Tak terduga items | reportable expenses whose effective pillar is `unexpected` |
| Recurring charges | `recurring_charges(now=period end)`, keeping charges last seen inside the period |
| Essential cost and coverage | `estimate_essential_cost` and `calculate_ketahanan_dana`, labelled "saat ini" because no balance history exists |
| Comparison | `period_comparison` against the previous period of the same basis, plus a median (D4) |

No new formula is introduced. A reconciliation test asserts that report inflow, outflow and net, every pillar, category and income source equal the ledger list summary for the same `from_date`/`to_date` window and filters.

### D2. Periods, basis and identity

- `basis=month` lists calendar months. Each month runs from its first local day to its last, in `settings.tz`.
- `basis=cycle` lists closed confirmed salary cycles only. The basis is available when `cycle_status == confirmed` and at least one cycle exists.

A report is identified by `(basis, start_date)`, where `start_date` is the local first day.

The period list:
- runs newest first;
- holds at most 24 closed periods;
- starts with the period in progress (`complete: false`, "berjalan").

The default report is the latest closed period, else the one in progress.

Unlike `load_period_history`, the list includes the period in which tracking began. It is marked `tracking_started_inside: true` with the first recorded date, so the user still sees September even though it is partial. Medians use only full periods, following the existing period rule. An unknown `start_date` answers 404 `period_not_found`. A `basis=cycle` request without cycles answers 409 `cycle_unavailable`.

Rejected alternatives:
- Arbitrary date ranges: they are not a monthly habit and are already served by the ledger.
- Mixing cycles with fallback months in one list: the switch would stop meaning anything.

### D3. "Belum diklasifikasi" is `null` on rows and `unclassified` in aggregates

`effective_pillar_sql` and `effective_pillar` now return `NULL` when neither the transaction nor its category has a pillar. A new `pillar_bucket_sql` returns `'unclassified'` instead, for grouping only.

- **Rows** (ledger, review, capture result, `top_categories`, `/api/categories`) carry `null`, never the string `unclassified`. Android rejects an unknown non-null pillar in `top_categories` but accepts `null` everywhere (Context). The phone therefore keeps working and shows no pillar for such rows.
- **Aggregates** add `unclassified_spent` and `unclassified_pct` to the Kakeibo breakdown. `total_allocated` includes them, so pillars still add up to net outflow. Fair comparison `pillars` gains a sixth entry, and reimbursement offsets apportion to an `unclassified` key too. The 50/30/20 groups are unchanged and leave the bucket out. The interface says so, and the bucket is always shown beside them.
- **Filters**: `kakeibo_type` on `GET /api/transactions` also accepts `unclassified`, matching `pillar_bucket_sql = 'unclassified'`. The ledger URL accepts `kakeibo=unclassified`.
- **Writes**: categories keep their stored pillar. Reads no longer derive `need`/`want` from `is_primary`. Category creation still derives a pillar from `is_primary` when none is sent, because that is an explicit stored value on an existing API contract. A transaction recorded or recategorised into a category without a pillar keeps `kakeibo_type = NULL`, so a later category fix applies to it.
- **Forms**: the web ledger edit form and the Analisis category form offer "Belum diklasifikasi" as the current value and send `null`. Saving a note or a budget therefore no longer turns the row or category into "Kebutuhan". Quick Capture keeps its visible preselected pill, because the user sees and confirms it.

Typed-capture defaults (`capture_resolution.py`, `capture_normalization.py`) still write `need` and are left to their owner (Follow-ups).

Rejected alternatives:
- A literal `unclassified` value on rows: it breaks Android `shownTopCategories()`.
- A migration that stamps `need` on old rows: it would invent classifications.

### D4. Fair previous comparison and three-period median

- **Previous period**: the report calls `period_comparison(current=report window, previous=previous period of the same basis)`. That keeps the existing rule that both windows are cut to the shorter length. `previous_partial: true` is added when tracking began inside the previous period.
- **Median**:
  1. Take the three most recent full periods before the report period, with the same basis, after tracking began.
  2. Let `D` be the minimum day count among them and the report period.
  3. Measure each of them and the report period over its first `D` local days.
  4. Report the median of inflow, of outflow and of net (`median_rupiah`), each computed separately.

  With fewer than three such periods the median is unavailable and reports how many periods it has. No percentage is shown, following `observed-insights`.

Rejected alternative: averaging, which a single unexpected month distorts. The median is already the essential-cost rule.

### D5. Data quality counts

All counts are owner-scoped and limited to the report window:

| Count | Rule | Link |
| --- | --- | --- |
| Pending Tinjau | rows with `review_status` `needs_review` or an open purpose question (the Tinjau badge's `attention`), plus `auto` rows | `/review` |
| Unclassified spending | count and amount of reportable expenses in the `unclassified` bucket | ledger `kakeibo=unclassified&type=expense` and "Lengkapi klasifikasi" |
| Possible duplicates | expense or income rows with another row of the same account, amount and type within ±3 minutes; movement legs and movement fees are excluded | ledger `possible_duplicates=true` (URL `dup=1`), which uses the same SQL fragment |
| Days without records | local days of the window, through today for a period in progress, with no transaction of any type | the dates are listed; the action is Catat |

### D6. Essential cost counts unclassified spending, labelled

Before this change, unclassified expenses counted as `need` in the essential cost. Excluding them now would shrink the cost and raise Ketahanan Dana only because a pillar is missing. Per the pause rule, the conservative option is kept:
- per-period essential spending is `need` + `unclassified` (same scope and reimbursement shares);
- each period reports `unclassified_amount`;
- the coverage card says how much is unclassified and links to it.

The figure does not change for any existing user. The `need`-budget fallback likewise keeps counting budgets of categories without a pillar. This is recorded under Open decisions.

### D7. Quick fix "Lengkapi klasifikasi" writes feedback through the owner-edit path

`GET /api/classification/gaps?from_date&to_date` lists:
- active, reportable expense categories without a pillar, with the count and amount of their unclassified rows in the window;
- up to 200 unclassified rows, newest first;
- the totals.

There are two write routes. Both take the advisory lock that bulk review and notification processing use (`notification:<user>`).

- `POST /api/classification/categories/{id}` `{kakeibo_type}` sets the category pillar. Its rows with no own pillar follow the new pillar. Rows with their own pillar are untouched. The route snapshots the affected rows before the change and calls `record_owner_corrections` after it. The feedback rows match those of every other owner edit, with `kind = correction`, before `null` and after the pillar. Owner memory is upserted as usual.

  Open purpose questions of those rows are not closed (`close_questions=False`). The owner classified the category, not each row's purpose. The row itself is not edited, so `review_status` and provenance stay as they are.
- `POST /api/classification/transactions` `{transaction_ids (1–200), kakeibo_type}` sets the row pillar. It uses the bulk-review owner-edit path, all or nothing:
  - lock the rows;
  - reject movement legs (409) and non-expense rows (422);
  - set `kakeibo_type`, `review_status = reviewed` and `OWNER_EDIT_ASSIGNMENTS`;
  - call `record_owner_corrections`.

Rejected alternatives:
- Reusing `PATCH /api/categories/{id}` silently: it would make every Settings pillar change write feedback, which is a behaviour change outside this scope (Follow-ups).
- A new feedback `kind`: the habit learner reads `correction` rows.

### D8. Reflection note storage (V40)

```sql
CREATE TABLE IF NOT EXISTS monthly_reflections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  basis VARCHAR(10) NOT NULL CHECK (basis IN ('month', 'cycle')),
  period_start DATE NOT NULL,
  period_end DATE NOT NULL CHECK (period_end >= period_start),
  note VARCHAR(1000) NOT NULL CHECK (char_length(btrim(note)) BETWEEN 1 AND 1000),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, basis, period_start)
);
```

- `PUT /api/reports/monthly/reflection` `{basis, start_date, note}` upserts. A blank note answers 422. The period must be a listed one (404 otherwise).
- `DELETE /api/reports/monthly/reflection?basis&start_date` removes it (404 when absent).
- The report returns `reflection` for the period and `previous_reflection`, the note of the period before it, so last month's intention is shown again.
- Notes are personal data: they are never logged and are returned only to their owner.

Rollback: the table is additive. Dropping it loses only notes. The application code does not depend on it existing outside the reports routes.

### D9. Readiness API

`GET /api/reports/monthly/latest?basis=month|cycle` (default: `cycle` when available, else `month`) returns:

```json
{"ok": true, "ready": true, "basis": "month",
 "period": {"label": "Oktober 2026", "start": "...", "end": "...", "start_date": "2026-10-01", "end_date": "2026-10-31"},
 "pending_review": 3, "unreviewed": 5,
 "url": "/laporan?basis=month&start=2026-10-01"}
```

- `pending_review` is the Tinjau badge count over all rows. `unreviewed` is the `auto` count.
- Without a closed period the response has `ready: false` and `period: null`.
- It accepts session or Bearer auth. Values are owner-scoped, and an unknown key gets 401.

### D10. Optional budgets

- **Report**: the report returns `budget: null` unless an active expense category has `monthly_budget > 0` or the user has `monthly_spending_budget > 0`. With a budget it returns one row per budgeted category (budget, net spent in the period, remaining) and the overall budget against total outflow. A salary cycle is compared with the monthly amount as is, and the interface says so.
- **"Atur anggaran"**: a dialog lists the active expense categories with a money input each and saves changed ones through `PATCH /api/categories/{id}` `monthly_budget`.
- **Analisis**: when no category has a budget, the category table drops the budget, remaining and status columns and the "Atur batas" buttons, and is titled "Pengeluaran per Kategori". The category edit form keeps its optional budget field.

### D11. Layout

Layout considered at 390 px (primary, the owner reads on the phone through the web) and at 1280 px:

| Option | Shape | Verdict |
| --- | --- | --- |
| A. Question story | Header with the period picker and basis switch; four numbered sections in fixed order ("1 Berapa uang masuk?" … "4 Apa yang mau diperbaiki bulan depan?"), each opening with its answer amount, then evidence. On desktop a 2-column grid: questions on the left (8/12), and on the right (4/12) the comparison, "Kualitas data", budget and the past-report list. | Chosen |
| B. Tabs per question | Four tabs, one answer visible at a time | Rejected: hides three answers. A monthly read should scan top to bottom without remembering what is behind a tab. Tabs also fail the "fixed, scannable structure" goal on desktop. |
| C. KPI grid dashboard | Eight cards of totals, then charts | Rejected: it is the Analisis pattern the owner already does not read. AGENTS.md forbids decorative KPI cards. It does not answer the four questions in order. |

Within A:
- the period picker is a native `<select>` of past reports;
- "Kualitas data" is shown first on the phone when any count is non-zero, because fixing data makes the rest true;
- pillar amounts use a stacked list with a label, the amount and a share bar, and the share is text as well as the bar, so meaning does not rely on colour;
- the unclassified bucket uses a neutral token (`--color-kakeibo-unclassified`), checked for contrast in both themes.

Copy is plain Indonesian without verdicts. For example: "Uang keluar Rp X lebih tinggi dari September pada 30 hari pertama", not "boros".

## API summary

| Route | Auth | Result |
| --- | --- | --- |
| `GET /api/reports/monthly/periods?basis=` | session/Bearer | `{basis, cycle_available, periods[]}` |
| `GET /api/reports/monthly?basis=&start_date=` | session/Bearer | report (D1), 404 `period_not_found`, 409 `cycle_unavailable` |
| `GET /api/reports/monthly/latest?basis=` | session/Bearer | readiness (D9) |
| `PUT /api/reports/monthly/reflection` | session/Bearer | `{reflection}`; 422 blank or too long; 404 unknown period |
| `DELETE /api/reports/monthly/reflection?basis=&start_date=` | session/Bearer | `{ok}`; 404 when absent |
| `GET /api/classification/gaps?from_date=&to_date=` | session/Bearer | categories and rows without a pillar |
| `POST /api/classification/categories/{id}` | session/Bearer | `{updated_transactions, corrections}`; 404; 422 income category |
| `POST /api/classification/transactions` | session/Bearer | `{transaction_count, corrections}`; 404; 409 movement leg; 422 income |
| `GET /api/transactions?possible_duplicates=true` | session/Bearer | the ledger filtered to possible duplicates |

Every route that takes a `basis` rejects any value other than `month` or `cycle` with 422.

## Risks / Trade-offs

- **The meaning of the "need" totals changes for users with unclassified rows.** Kebutuhan drops by the unclassified amount, which appears beside it. Totals and the essential cost are unchanged (D6).
- **Pending suggestions superseded by the change.** History-suggestion supersede checks compare the stored `before_kakeibo_type`. Stored values were written with the old default, so the check treats `null` and `need` as equal and does not supersede them.
- **Large categories.** A category fix on a category with many unclassified rows writes one feedback row per row, which is the learning signal the habit model needs. It runs in one transaction under the advisory lock.

## Open decisions for owner

1. **Essential cost and unclassified spending (D6).** Unclassified spending stays in the essential cost (conservative) instead of being excluded. Owner decision needed on whether to exclude it once classification is complete.
2. **Category pillar edits elsewhere.** `PATCH /api/categories/{id}` (Settings and the Analisis form) still writes no feedback when a pillar changes. Should it use the D7 path too?
3. **Typed-capture default.** `capture_resolution.py` and `capture_normalization.py` still store `need` when the category has no pillar. These files are owned by the concurrent Catat change.
4. **Category creation default.** `POST /api/categories` without `kakeibo_type` still stores `need`/`want` from `is_primary`. Should a new category start unclassified?
5. **Starter categories without a pillar.** `seed_user_defaults` (`backend/app/db/init_db.py`) gives every new user seven expense categories without a pillar: Makanan & Minuman, Belanja Harian, Transportasi, Tagihan & Utilitas, Belanja, Hiburan and Kesehatan. Only Jajan & Nongkrong has one. This is the main source of unclassified spending, the owner's included. A new user's spending now shows as "Belum diklasifikasi" until they set the pillars once in "Lengkapi klasifikasi". Seeding default pillars, or asking for them at onboarding, would change approved new-user defaults, so it was left out.

## Follow-ups

- The phone reminder that reads `GET /api/reports/monthly/latest`.
- Items 2 to 5 of the open decisions.
- The shared phone dock (`BottomNav`) paints inactive labels in `--muted` on its always-dark `.glass-dock`. In the light theme that is about 2.4:1, and axe reports `color-contrast` for them on `/laporan`. This change did not touch that markup (verification.md).
- `PATCH /api/categories/{id}` and the Settings category form write no feedback for a category pillar change (open decision 2).
