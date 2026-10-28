## Context

See `proposal.md` for the motivation. The relevant state on `origin/main` (6cb8c9f, phase 1 merged):

**Insert paths.** Seven `INSERT INTO transactions` statements exist in `backend/app`:
- `routers/transactions.py` (manual create);
- `services/ledger_mutations.create_bilateral_movement` (two legs; called by movements, trades,
  recurring transfers, payroll, investment top-ups and notification movements and broker trades);
- `routers/recurring.py` (expense or income occurrence);
- `routers/accounts.py` (reconcile adjustment);
- `services/investment_topups.py` (inbound leg of a converted expense);
- `services/notification_application.record_observed_leg` (one observed leg).

**Provenance hints.** `notification_events.transaction_id` links an event to the row it recorded.
Idempotency keys use the prefixes `notification:<event>`, `recurring:<rule>:<date>`,
`payroll:<rule>:<date>` and `investment-topup:<id>`, with `:out` and `:in` suffixes on movement legs.
`notification_events.interpretation->>'source'` is `ai`, `deterministic`, `deterministic_fallback` or
`manual`; broker trades store no source. The model's confidence is not stored anywhere. No trigger
maintains `transactions.updated_at`; movement linking does not change it.

**Reports.** Income and spending totals are computed in `dashboard.py` (overview, analytics,
Kakeibo), `pulse.py` (pulse, insights) and the ledger list summary. Each excludes
`is_excluded_from_budget` categories and, in most places, the names "Internal Movement" and
"Investasi".

## Goals / Non-Goals

**Goals:**
- Stable provenance columns that later phases can build on without another migration.
- A backfill that is deterministic, explainable rule by rule, and runs exactly once.
- One definition of a reimbursement and one offset calculation shared by every report.

**Non-Goals:** see the proposal. In particular no merchant extraction, no confidence gate, no
review queue, and no change to balances.

## Decisions

### D1. Columns, constraints and defaults

`transactions` gains:

| Column | Type | Rule |
|---|---|---|
| `source` | `VARCHAR(20) NOT NULL DEFAULT 'manual'` | check: manual, notification, recurring, payroll, movement, reconcile, investment, import |
| `merchant` | `VARCHAR(120) NULL` | check: NULL or trimmed length 1–120 |
| `review_status` | `VARCHAR(20) NOT NULL DEFAULT 'reviewed'` | check: auto, needs_review, reviewed |
| `classified_by` | `VARCHAR(10) NULL` | check: rule, ai, owner, system |
| `classification_confidence` | `NUMERIC(4,3) NULL` | check: 0 ≤ value ≤ 1 |
| `owner_edited_at` | `TIMESTAMPTZ NULL` | — |
| `offset_category_id` | `UUID NULL REFERENCES categories(id) ON DELETE SET NULL` | check: NULL or `type = 'income'` |

Index `idx_transactions_review_queue ON transactions(user_id, review_status, date DESC)`.

`categories` gains `offsets_spending BOOLEAN NOT NULL DEFAULT FALSE` with the check
`NOT offsets_spending OR kind = 'income'`.

The application never relies on the defaults: every insert names the three provenance columns, and a
static test fails if any `INSERT INTO transactions` in `backend/app` omits one.

The defaults exist for rollback. If the previous release is redeployed after V28, its inserts do not
name the new columns; without defaults every insert would fail. With them, rows written by old code
read as manual and reviewed, which is wrong only in provenance and never in money.

*Rejected:*
- No defaults. Strictest, but makes a code rollback break every insert, including notification
  ingestion.
- A PostgreSQL `ENUM`. Changing the value set later needs more ceremony than a check constraint, and
  the project already uses checks for `type` and the pillar.
- A separate `description` column. `notes` already is the human description and every reader uses it;
  a second text column would need its own backfill and a rule for which one the UI shows. The plan's
  "description" is therefore `notes`, and `merchant` is the new field.

### D2. Backfill rules, applied once

Source, first matching rule wins:

1. `notification` — a `notification_events` row of the same user links to it by `transaction_id`, or
   its idempotency key starts with `notification:`.
2. `recurring` — key starts with `recurring:`.
3. `payroll` — key starts with `payroll:`.
4. `recurring` — `recurring_rule_id` is set.
5. `investment` — `movement_id` is the id of an `investment_topups` row of the same user, or it is a
   movement leg and either leg's account is an investment position (`type = 'investment'` or
   `instrument_type` set). The second half identifies Beli/Jual trades, which new code also records
   as `investment` (D3); without it historical trades would read as plain movements.
6. `reconcile` — its category is the user's "Penyesuaian Saldo".
7. `movement` — `movement_id` is set.
8. `manual` — everything else.

The ordering puts direct evidence (event links, keys) before structural evidence (rule, movement and
category links). A converted top-up therefore keeps `notification` on its outbound leg (the bank
debit came from a notification) and gets `investment` on its inbound leg.

Classifier:
- `notification` rows: the event is the one whose id is named by the idempotency key, otherwise the
  earliest linked event. Its `interpretation->>'source'` decides: `ai` → `ai`; `manual` → `owner`;
  anything else, including no interpretation, → `rule`.
- `manual` rows → `owner`; all others → `system`.

Review status:
- `notification` rows with `updated_at <= created_at + interval '1 minute'` and classifier other than
  owner → `auto`;
- everything else → `reviewed`.

`classification_confidence`, `merchant`, `owner_edited_at` and `offset_category_id` stay NULL: the
confidence was never stored, merchants are phase 3 and 7 work, and history cannot prove which past
edits were classification edits by the owner.

Known limits, accepted:
- Payroll legs executed without a rule have neither a key nor a rule link and read as `movement`.
- An owner-merged movement of two notification rows keeps `auto`, because merging never changed
  `updated_at`.
- A manual row the owner later moved into "Penyesuaian Saldo" reads as `reconcile`.

The backfill updates only the new columns, so `updated_at` and every money field are untouched.

### D3. Provenance for each insert path

| Path | source | review_status | classified_by | confidence |
|---|---|---|---|---|
| `POST /transactions`, ordinary | manual | request value or `reviewed` | owner | — |
| `POST /transactions`, Beli/Jual trade | investment | reviewed | system | — |
| `POST /movements` | movement | reviewed | system | — |
| recurring occurrence (expense, income, transfer, top-up) | recurring | reviewed | system | — |
| payroll batch | payroll | reviewed | system | — |
| reconcile | reconcile | reviewed | system | — |
| investment top-up, conversion inbound leg | investment | reviewed | system | — |
| notification, AI interpretation | notification | auto | ai | model confidence |
| notification, deterministic or fallback, broker trade | notification | auto | rule | — |
| notification, owner resolution | notification | reviewed | owner | — |

`services/provenance.py` defines a frozen `Provenance` value (validated against the vocabularies)
and the shared constants. `create_bilateral_movement` takes it as a required keyword, so a new caller
cannot forget it. The notification application derives it from the interpretation source it already
threads through (`signature["source"]`) and from `proposal.confidence`.

The deterministic interpretation carries a fixed `confidence = 1.0`. It is not a measured
confidence, so rule rows store none; phase 3's gate treats a rule hit as certain anyway.

System-created rows are `reviewed` because their classification comes from something the owner
configured (a rule, a movement, a reconcile). `review_status` describes classification uncertainty,
not whether the owner has scrolled past the row. Phase 3 sets `needs_review` for uncertain
notification rows.

*Rejected:* `auto` for recurring auto-posts. The category is the owner's rule; asking to review it
every month would train the owner to ignore the queue.

### D4. Owner edits

Classification fields are category, pillar, notes, merchant, type and offset category. When an owner
action changes at least one of them (compared after normalisation), the row gets
`owner_edited_at = NOW()`, `classified_by = 'owner'`, `classification_confidence = NULL` and
`review_status = 'reviewed'`, unless the request itself sets `review_status`.

Owner actions covered: `PATCH`/`PUT /transactions/{id}`, `PATCH /movements/{id}` (notes compared per
leg), `POST /movements/merge` (category always changes), converting an expense into a top-up, and
editing a top-up's notes. The confidence is cleared because it described the previous classifier.

Automatic pairing in notification processing calls the same linking function with owner tracking
off. Splitting a movement restores the pre-link classification and is not an owner classification.

A `PATCH` whose only field is `review_status` is a review action: it changes `review_status` and
`updated_at` only, is allowed on movement legs (other edits of legs stay 409), and leaves
`classified_by` alone so an accepted AI classification stays visible as AI-made.

`review_status` from clients may be `reviewed` or `needs_review`; `auto` is reserved for automation
and returns 422.

*Rejected:* a separate `POST /transactions/{id}/review` endpoint. The review-only PATCH is the same
operation without new routing; phase 6 bulk review will need its own endpoint anyway.

### D5. Merchant normalisation

`normalize_merchant` trims, collapses inner whitespace and keeps case (it is a display name). An
empty result becomes NULL; more than 120 characters is a 422. Search (`q`) also matches merchant.

### D6. Reimbursement category and offset link

V28 upserts "Patungan & Ganti Uang" (income, icon `users`, colour `#0d9488`) for every user with
`offsets_spending = TRUE`, not excluded from budget and not archived. New users get it from
`seed_user_defaults`.

It is deliberately *not* `is_excluded_from_budget`: that flag means "a movement, neither income nor
spending", and reports would then also drop it from the ledger's income filter and the category
lists. A separate flag keeps the meaning explicit.

`offset_category_id` validation, in the transactions router:
- non-NULL offset requires the final row to be income in an offsetting category, else 422
  `offset_not_allowed`;
- the offset must be an expense category of the same user, not archived, not excluded and not named
  "Internal Movement" or "Investasi" (reports leave those out by name, and new users' seeded
  "Internal Movement" is not flagged excluded until the first movement), else 422
  `invalid_offset_category` (422 rather than 404, because the request is well-formed but invalid);
- on update, when the final row is no longer eligible and the request does not mention the offset,
  the offset is cleared. An existing offset whose category was later archived is kept while the
  request does not touch it.

Categories API: list returns `offsets_spending` and `is_excluded_from_budget`; changing the kind of
an offsetting category, or of an expense category used as an offset, is 409; deleting a category used
as an offset archives it, as with any referenced category.

### D7. Report arithmetic

`services/reimbursements.py` owns the rule. For a window W:

- `R(X)` = sum of reimbursements in W whose offset is X; `U` = sum of reimbursements in W without
  offset.
- `G(X)` = X's expenses in W when X is reportable (not excluded and not named "Internal Movement" or
  "Investasi"), else 0.
- Applied `A(X) = min(G(X), R(X))`; excess `E(X) = R(X) − A(X)`.
- Pillar `p(X)` = the effective-pillar rule for an expense with no own pillar in X:
  `COALESCE(X.kakeibo_type, 'need')`, via `kakeibo.effective_pillar`.

Applied in the reports:

| Figure | Rule |
|---|---|
| Income totals (overview, analytics, both comparisons, daily series, pulse, insights, ledger summary) | exclude every reimbursement |
| `total_outflow` (overview, analytics, both comparisons; insights `total_expense`) | gross − ΣA |
| Category breakdowns (overview categories, analytics variance, previous-period driver map, insights categories) | `spent = G − A`; additive `gross_spent`, `reimbursed` |
| Kakeibo pillar spent | `max(0, gross pillar − Σ A(X) for p(X) = pillar)`; saving is reduced before net saving transfers are added |
| Pulse `cycle_spent` | `max(0, gross living − Σ A(X) for p(X) ≠ saving)`; safe-to-spend and pace use it |
| Today's spending, daily and cumulative series, heatmap, burn rates, runway | gross (unchanged) |

New response fields: overview `kpis` and analytics `comparison` gain `gross_outflow`,
`reimbursement_offset` (ΣA), `reimbursement_unallocated` (U) and `reimbursement_excess` (ΣE); pulse
gains `cycle_reimbursed`.

**Excess.** A repayment larger than the category's spending in the window (typical when a friend
repays in the next cycle and little was spent there) floors the category and pillar at zero. The
remainder is reported as `reimbursement_excess`; it is not income, does not reduce another category,
and does not carry into another window. This is the conservative choice: it can understate a
reduction but never invents negative spending.

Floors and totals: because `A(X) ≤ G(X)` and `G(X)` is part of gross spending, `total_outflow` cannot
go negative. A pillar floor can only bind when expenses in X carry their own different pillar; then
the pillar total and the total spending differ by the floored amount, which is accepted and tested.

*Rejected:*
- Treating a repayment as negative spending on its own date without a category floor. Simple, but a
  late repayment would produce negative category and pillar figures.
- Matching each repayment to the individual expense it repays. Exact, but it needs a new link and a
  UI the owner did not ask for; the category link is enough for monthly figures.
- Netting the daily series and burn rates. That needs a per-day attribution rule; phase 6 reworks
  these reports.

### D8. Jajan & Nongkrong

V28 inserts the expense category "Jajan & Nongkrong" (pillar `want`, icon `coffee`, colour
`#d97706`) with `ON CONFLICT DO NOTHING`, so an existing category of that name keeps its settings.
`DEFAULT_CATEGORIES` in `init_db.py` gains it with its pillar and the reimbursement category with
its flag; the seed insert now writes `kakeibo_type` and `offsets_spending` when given. No row moves.

### D9. V28 and the startup mirror

`V28__transaction_provenance_and_reimbursements.sql`:
1. one `DO` block guarded by "`transactions.source` does not exist" that adds every column and
   constraint, runs the backfill and inserts both categories;
2. `CREATE INDEX IF NOT EXISTS` for the review queue.

The guard makes the file safe to run on every API start: Flyway applies it once, and the identical
mirror `backend/app/db/transaction_provenance_and_reimbursements.sql`, run last by `init_db.py`, is
then a no-op. A non-guarded backfill would rewrite owner review decisions at each restart.

Categories are created only inside the guard, so an owner who later deletes "Jajan & Nongkrong" does
not get it back at the next restart.

### D10. Frontend

- `lib/provenance.ts` holds the Indonesian labels:
  - source: Dicatat manual, Notifikasi bank, Transaksi rutin, Alokasi gaji, Pindah saldo,
    Penyesuaian saldo, Investasi, Impor;
  - review status: Otomatis, belum ditinjau / Perlu ditinjau / Sudah ditinjau;
  - classifier: Aturan, AI, Anda, Sistem.
- Ledger edit dialog (ordinary rows): a definition list at the top with source, review status and
  classifier; a "Tandai sudah ditinjau" button when not reviewed, sending only `review_status`; a
  merchant text input (`maxLength` 120) before notes; and, for income in an offsetting category, a
  "Mengurangi pengeluaran" select. The save payload sends `merchant` and, for income,
  `offset_category_id`.
- Quick capture: the same select for income in an offsetting category; the payload sends
  `offset_category_id` only then.
- All new controls are native inputs, selects and buttons with visible labels, at least 44 px high,
  using the existing field classes and tokens. No new colours.
- Insights and dashboard read the backend figures; no new cards in this phase.

## Risks / Trade-offs

- [Rollback to the previous release] → Columns have defaults (D1). Old code ignores the new columns
  and counts reimbursements as income again until redeployed; balances are unaffected.
- [Backfill misclassifies a row] → Each rule is tested with synthetic rows. Only provenance columns
  change; a wrong value is corrected by an owner edit.
- [Reports change for owners with existing "Patungan" rows] → None exist before V28; the category is
  new. Rows the owner already filed under other income categories stay income until recategorised.
- [Offset category changed later] → Reports use the current link and the current category pillar,
  matching how category pillars already apply to history.
- [Report totals stop matching the sum of daily series] → Documented in D7; phase 6 reworks the
  series.

## Migration Plan

1. Merge. GitHub Actions deploys; Flyway applies V28 (adds columns, backfills, creates categories).
   The API starts; the mirror is a no-op.
2. Verify on production, as the owner or with an explicit instruction: counts per `source` and
   `review_status`, both categories per user, and that `updated_at` did not change (compare
   `max(updated_at)` before and after).
3. Rollback: redeploy the previous release; V28 needs no reversal. To remove it entirely (owner
   authority only): drop the index, the new columns and `categories.offsets_spending`, and delete the
   two categories where no transaction references them.

## Open decisions for owner

1. **Owner-resolved notification rows in the backfill** are classified `owner` and `reviewed`, a
   refinement of "ai if the interpretation source is ai, else rule": the owner chose their category,
   so calling them rule-classified would mislabel training data for phase 9.
2. **Historical trades** are backfilled as `investment` by account type (D2 rule 5), an addition to
   the listed rules, so new and old trades agree.
3. **Rule rows store no confidence** although the deterministic interpretation carries 1.0 (D3).
4. **Notification rows get no merchant yet**; phase 3 fills it with normalisation and memory.
5. **Excess reimbursement** is reported, not carried forward (D7). If the owner prefers carry-over
   to the next window, that is a later change.
6. **Ledger summary outflow** stays gross; only inflow drops reimbursements (D7).
7. **Column defaults** `manual`/`reviewed` stay until phase 8 for rollback safety (D1).
