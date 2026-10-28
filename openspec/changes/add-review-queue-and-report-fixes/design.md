## Context

See `proposal.md`. State on the base (f55dd46, phase 5 archived and merged into main as a723b4c):

- Rows carry `review_status` (`auto`, `needs_review`, `reviewed`), `merchant`, `classified_by` and the
  owner-edit columns (V28). Review reasons live in the recording event's
  `interpretation.review_reasons` (`uncertain_category`, `model_fallback`, `low_confidence`,
  `model_flagged`, `ambiguous_movement`, `purpose_question`, `awaiting_counterpart`).
- Owner-edit hooks: `routers/transactions.classification_change`, `provenance.OWNER_EDIT_ASSIGNMENTS`,
  `merchant_memory.snapshot_rows` and `record_owner_corrections` (one `correction` feedback row per
  changed row, owner merchant memory, closes an open purpose question).
- `GET /api/ingest/purpose-questions` lists open questions; `POST
  /api/ingest/notifications/{id}/answer-purpose` answers one. Both use `get_current_user`, which accepts
  the session cookie, and every cookie-authenticated write passes the global origin check in
  `main.csrf_middleware`. No session-safe copy of the route is needed (verified by a test, task 2.4).
- `POST /api/movements/complete` creates a missing leg; `POST /api/movements/merge` links two equal legs.
- Reports: `dashboard.py` overview and analytics, the ledger summary in `GET /api/transactions`. Each
  had its own expense predicate; analytics `total_outflow` did not leave out "Internal Movement" and
  "Investasi" by name while the overview did. Category breakdowns listed active, non-excluded categories
  only. Daily series, heatmap, burn rates and the ledger outflow were gross (phase 2 D7, open decision 6).
- Windows: `get_timeframe_window` (cycle, 30d, 90d) lived in `dashboard.py`.

## Goals / Non-Goals

**Goals:** one place where the owner confirms everything; group actions that teach memory exactly like
single edits; report figures that add up and lead to their rows.

**Non-Goals:** see the proposal.

## Decisions

### D1. Review queue endpoints

`routers/review.py`, prefix `/review` (mounted under `/api`, `/v1` and bare like the other routers):

- `GET /review/summary` → `{"ok", "attention", "unreviewed"}`. `attention` counts expense and income
  rows with `needs_review` or an open purpose question; `unreviewed` counts `auto` rows without one. The
  navigation badge shows `attention`.
- `GET /review/queue` → `{"ok", "truncated", "counts", "questions", "groups", "awaiting", "ambiguous"}`:
  - reads at most 1 000 expense/income rows with `review_status` `auto` or `needs_review`, newest first
    (`truncated` when more exist), plus the open questions from `list_open_questions` (max 200);
  - each row goes to exactly one section, in this order: a row with an open question is answered in
    `questions`; an unlinked `needs_review` row with the reason `ambiguous_movement` and at least one
    candidate (or not filed as "Internal Movement") goes to `ambiguous`; an unlinked "Internal Movement"
    row goes to `awaiting`; everything else goes to `groups`;
  - `ambiguous` candidates come from `movement_pairing.load_candidates`: opposite unlinked legs of the
    same amount within `MOVEMENT_REPAIR_MAX_GAP_SECONDS`, on another account, nearest first, at most 5.

*Rejected:* computing the queue in the browser from `GET /transactions?review_status=…`. The ledger
list has no review reasons, no grouping key and pages of 200; the browser would need many calls.

### D2. Grouping

Group key, first that applies:

1. `movement` for linked legs (one group "Pindah saldo antar rekening", accept only);
2. `<type>|merchant:<normalised merchant>` (phase 3 `merchant_match_key`);
3. `<type>|signature:…` — the event's phase 3 match key when the row has no merchant;
4. `<type>|notes:<normalised notes>`;
5. `<type>|category:<id or none>`.

The type is part of the key, so every group can be recategorised into one category kind. A group
returns `count`, `total` (outgoing legs only for movements), `first_date`, `last_date`,
`needs_review_count`, `auto_count`, `reasons` (union, open reasons only, D3), `category` when all rows
share one, `editable` and its rows. Groups with `needs_review` rows sort first, then by size.

### D3. Open reasons

Stored reasons are never removed when a later event resolves them. The queue hides
`awaiting_counterpart`, `ambiguous_movement` and `purpose_question` on linked legs, and
`purpose_question` when no question is open. The browser shows Indonesian labels
(`lib/review.ts`); an unknown code is shown as sent.

### D4. Bulk endpoint

`POST /review/bulk`:

```json
{"action": "accept" | "recategorise" | "rename" | "keep_separate",
 "transaction_ids": ["<uuid>", ...],          // 1–200, no repeats
 "category_id": "<uuid>", "kakeibo_type": "want",   // recategorise only
 "merchant": "Kedai Awan", "notes": "makan siang"}  // rename only, at least one
```

Validation (422): unknown action, empty or more than 200 ids, repeated ids, a field the action does not
take, `recategorise` without `category_id`, `rename` without a field, merchant over 120 characters after
normalisation, notes over 500, unknown pillar.

Order, in one database transaction under the owner's notification advisory lock (the lock notification
processing and movement completion take, because the same rows, questions and memory change):

1. lock all rows `FOR UPDATE` in id order; any id that is not the owner's → 404
   `transactions_not_found` with the missing ids; nothing changes;
2. `accept`: `review_status = reviewed` for all, movement legs included; classifier untouched;
3. `recategorise` / `rename`: a movement leg → 409 `movement_leg_not_editable` with the leg ids;
   `recategorise` also checks the category (owned, active → else 404 `category_not_found`), its kind
   against every row (422 `category_type_mismatch`) and a pillar only for expense (422
   `pillar_not_allowed`);
4. for every row, `classification_change` decides whether the owner changed it (same comparison as the
   PATCH path). Changed rows: snapshot (`snapshot_rows`), update with `OWNER_EDIT_ASSIGNMENTS` and
   `review_status = reviewed`, then `record_owner_corrections` writes one `correction` feedback row per
   changed row, upserts owner memory and closes an open question. Unchanged rows only become reviewed;
5. `keep_separate`: a linked row → 409 `already_linked`; else `owner_split_at` (kept if set) and
   `reviewed`, so automatic pairing never links the leg again (phase 5 D7).

Recategorise stores the requested pillar, or NULL so the row follows the new category's pillar.
An income row keeps its offset only when the new category offsets spending.

Response: `{"ok", "action", "transaction_count", "corrections"}`. Logs: none.

The browser sends groups larger than 200 rows in batches; each batch is atomic and a partial failure is
reported with the number saved.

*Rejected:* N `PATCH /transactions/{id}` calls (no atomicity, N round trips, N advisory locks);
recategorising movement legs (a movement's category is "Internal Movement" by definition).

### D5. Calendar month windows

`services/report_windows.py` now owns `get_timeframe_window` and `get_previous_timeframe_window`
(re-exported by `dashboard.py`). New timeframe `month`: the calendar month of today shifted by
`cycle_offset` months, label in Indonesian ("Oktober 2026"); the previous window is the previous month.
Dashboard overview and analytics accept `timeframe=month`. The ledger list accepts `period=cycle|month`
and `period_offset` (−36…12), resolved by the same function into `from_date`/`to_date`; combining it
with `from_date`/`to_date` is 422 `period_with_dates`. The ledger summary returns a `period` block
(`basis`, `offset`, `label`, `start`, `end` and the filtered figures) whenever a period or date range
filters the list.

### D6. One reportable-expense rule and the complete breakdown

`services/report_figures.py`:

- `reportable_expense_sql`: an expense whose category is missing or neither excluded from budget nor
  named "Internal Movement"/"Investasi"; `reportable_income_sql`: the income counterpart, without
  reimbursements.
- `category_breakdown`: active expense categories (also when 0), archived categories with spending in
  the window (`is_archived: true`) and a "Tanpa kategori" bucket (`id: null`, `is_uncategorized: true`,
  colour `#64748b`, the colour the fee category already uses) when uncategorised spending exists;
  `spent = gross − applied offset`. Because the offset rule already excludes non-reportable categories,
  `Σ spent = total_outflow` exactly.

Used by overview (`categories`) and analytics (`category_variance`). Rows gain `is_archived` and
`is_uncategorized`.

### D7. Drill-down and URL filters

The ledger reads `period`, `offset`, `category` (uuid or `uncategorized`), `kakeibo`, `type`, `account`
and `review` from the URL (`lib/ledgerFilters.ts`); invalid values fall back to "all". Changing a filter
replaces the URL. `uncategorized=true` is a new list filter (`t.category_id IS NULL`; with
`category_id` → 422 `uncategorized_with_category`). Insights category and pillar figures link to
`/ledger?period=<cycle|month>&offset=<n>&type=expense&category=…|kakeibo=…`, matching the insights
window; rolling 30/90-day windows get no links (no ledger equivalent). The ledger lists active filters as
chips and keeps a period selector (all / payday cycle / calendar month) with a stepper.

The ledger's period is independent of the top-bar stepper, which keeps driving the dashboard and
insights; the insights page gains a payday-cycle / calendar-month switch that sets the shared
`timeframe`.

`/ledger?edit=<id>&return=review` opens the existing edit dialog for that row (fetched by id) and, on
close or save, returns to `/review`.

*Rejected:* extracting the 600-line edit dialog into a shared component for the review page. It carries
debt allocations, receipts and top-up conversion; a deep link reuses it unchanged.

### D8. Ledger marks

Rows show "Perlu ditinjau" or "Otomatis" (reviewed rows carry no mark). List items gain
`movement_fee` (`{transaction_id, amount}` on movement legs with a fee) and `fee_movement`
(`{movement_id, source_account_name, target_account_name, amount}` on fee rows); the ledger shows
"Biaya pindah saldo Rp … · A → B" on the fee row and "+ biaya transfer Rp …" on the movement.

### D9. Reimbursement-consistent figures

Applied offsets are attributed to days: each category's applied offset is spread over the days that
category was spent on in the window, in proportion to that day's spending, by largest remainder. No day
goes negative and the days sum to `Σ A`.

| Figure | Before | Now |
|---|---|---|
| Analytics `total_outflow`, `prev_outflow` | expense not excluded by flag | reportable expense (adds the name rule) − offsets |
| Analytics `total_inflow`, `prev_inflow` | income not excluded by flag, no reimbursements | reportable income (adds the name rule) |
| Analytics `daily_cadence[].expense` | gross, name rule | reportable − attributed offsets |
| Analytics `day_of_week_heatmap` | gross per weekday | sum of the net daily figures; `tx_count` unchanged |
| Analytics `burn_rate.daily_burn_7d`, `daily_burn_30d`, `projected_30d_outflow` | gross, flag only | reportable − offsets of the same rolling window |
| Analytics `category_variance` | active categories | D6 breakdown |
| Overview `cumulative_trendline`, `burn_cadence` expense | gross, flag only | reportable − attributed offsets |
| Overview `categories` | active categories | D6 breakdown |
| Ledger summary `inflow` | not Internal Movement, not excluded, not reimbursement | reportable income (adds "Investasi") |
| Ledger summary `outflow` (cycle, cumulative, period) | gross | reportable − offsets, see below |

Ledger outflow subtracts offsets only when the filtered rows are what a report figure covers: no
account, goal, debt, source, review, search or uncategorised filter, type empty or expense, and not both
category and pillar. Then: all applied offsets of the window, or the category's, or the pillar's
(floored at 0). Otherwise the outflow stays gross and `reimbursement_offset` is 0. New fields:
`gross_outflow`, `reimbursement_offset`.

Unchanged: pulse `today_spent`, pulse insights histogram and the emergency-fund runway (follow-up).

## Risks / Trade-offs

- [Figures change for owners with unflagged "Investasi"/"Internal Movement" rows] → Intended: one rule;
  listed in D9 and tested.
- [Large groups] → batches of 200; partial success is reported, never claimed as full.
- [A wrong group recategorisation teaches memory for a merchant] → the same as a single edit; every
  correction stays in feedback and a later edit overwrites the memory.
- [Queue cost] → one indexed query (`idx_transactions_review_queue`) plus a candidate query per
  ambiguous leg; bounded by 1 000 rows.
- [Rollback] → no schema change; redeploying the previous release removes the routes and the figures
  return to the old definitions.

## Migration Plan

No migration. Merge; the GitHub workflow deploys. Post-deploy (owner authority): `/review` lists the
open questions the phone shows; the insights category list adds up to its total.

## Open decisions for owner

1. **Badge counts attention only** (`needs_review` rows and open questions); unreviewed automatic rows
   are counted on the page, not on the badge, so the badge does not grow with every normal payment.
2. **Accept writes no feedback row.** `classification_feedback.kind` allows `correction` and
   `purpose_answer`; a confirmation kind needs a migration. `review_status = reviewed` already marks the
   row owner-confirmed for phase 3 similar history. Accepting or rejecting a purpose suggestion is
   recorded through the existing answer endpoint.
3. **Ambiguous legs link only equal amounts** from the review page (the merge contract); fee-different
   pairs are not offered.
4. **"Tetap terpisah" sets `owner_split_at`**: the leg is never auto-paired again, as after a split.
5. **Movement legs are accepted, never recategorised or renamed** in bulk (409).
6. **Row edits open the ledger dialog through `?edit=`** instead of an embedded copy of the dialog.
7. **The ledger period does not follow the top-bar stepper**; insights links pass their period
   explicitly.
8. **Ledger outflow is net only for report-shaped filters** (D9); other filters show gross.
9. **The badge is shown in the sidebar, the phone drawer and on the phone menu button**; the bottom
   dock keeps its four entries.
10. **Insights' rolling 30/90-day windows get no drill-down links** (not reachable in the current UI).
11. **Pulse figures and the runway stay gross** (follow-up).
12. **Pre-existing contrast findings** (white on the coral active nav pill, emerald/rose figures, the
    bottom dock labels) are left as they are; the owner asked not to restyle. Listed in verification.md.
