## Context

The code facts were read on `origin/main` `5eff424`, after #88 `add-category-system-keys`. The research and sources are in `cash-flow-tracker-prototypes/research/2026-11-06-credit-spending-flow.md`.

### What exists

- **Credit account** (`add-credit-accounts`, V50):
  - `accounts.type = 'credit'`, top-level, with nullable `credit_limit` and `payment_day`;
  - the balance is negative while money is owed, and `credit_owed(balance) = max(-balance, 0)`;
  - role `utang` is fixed by the type (`account_roles.py`).
- **Purchase:** an `expense` on the credit account. A refund is an `income` on it. Neither needs a funds check.
- **Payment:** a bilateral movement, with the outbound leg on the cash account and the inbound leg on the credit account. A movement out of a credit account is a cash advance. A credit-to-credit movement is refused (`ensure_generic_movement_accounts`).
- **Fee:** booked by `book_credit_fee` as an expense on the credit account. It has `fee_for_movement_id` set and the category "Biaya kredit", which is found or created **by name** (`ensure_credit_fee_category`).
- **Ledger account filter:** `GET /api/transactions?account_id=` selects every row whose `account_id` is the account, together with its pockets. For a credit account that means purchases, refunds, fees, cash-advance legs and payment inbound legs, so the per-provider view already exists as a filter.
- **Language:** V53 is `account_language_and_region` (`add-language-system`). `localize-server-text` brings the error contract `{code, params, message}`, the server catalogues and `label_source` on cycle-summary lines. Neither adds another migration.

### How today's figures treat credit (`services/cycle_summary.py`, `routers/dashboard.py`)

- **Aman dipakai:** `remaining = masuk − keluar − terjadwal`.
- **Keluar:** `get_kakeibo_breakdown().total_allocated`, which is reportable expenses by pillar plus net fresh saving.
- **The purchase:** a credit purchase is a reportable expense, so it counts in Pengeluaran and lowers Aman dipakai on its date.
- **A payment from an account that is not a savings destination** (for example role `harian`): it is a movement. `reportable_expense_sql` leaves movement legs out through the `internal_movement` key. The saving query sees source and target both outside savings, and role `utang` is not a savings destination. So Masuk, Keluar and Aman dipakai do not change. `test_paying_the_bill_from_the_pocket_is_a_movement_not_spending` asserts this.
- **A payment from a savings destination** (role `tabungan` or a goal-linked account): `out_of_savings` grows, so the saving bucket, Keluar and "Ditabung & investasi" fall by the payment, and Aman dipakai rises by it. Moving money into such an account had raised the saving bucket. Within one cycle the two cancel. Across cycles, one cycle shows the saving and the next shows the payment. No test covers this path for credit (D10).
- **Terjadwal:** `unpaid_plan_lines` counts every active plan that is not income, transfers included (`plan_occurrences_in_window`: `r.type <> 'income'`). A transfer plan into a credit account therefore subtracts the bill amount. The purchases it pays are already in Keluar, so the same money is subtracted twice until the occurrence is met (D9). Expense plans paid from a credit account are not counted twice: they are future spending until recorded. The overview's `upcoming_items` reads expense plans only, so it is unaffected.
- **Net worth** counts credit owed as utang. It is unaffected.

### How providers bill (research note §1)

- **SPayLater** bills once a month. The bill comes out on a fixed day, for example on the 1st for a due date on the 11th, or on the 25th for a due date on the 5th. The owner pays from that day until the due date, and may pay before the bill comes out. Plans run 1, 3, 6 or 12 times, and 0% plans exist on eligible products. Paying late costs 5% per month of the bill.
- **Kredivo** has "30 hari" and instalment plans. Sources disagree on whether a 30-day purchase is due 30 days after the purchase or on a fixed monthly date. Kredivo's own disclosure gives the late interest and fees.
- **Settings in common:** a bill day, a due day and a number of instalments. Late fees vary by provider. The design therefore stores the two days and the instalment count, and never computes a fee.

## Goals / Non-Goals

**Goals:**
- Terms and due dates for credit purchases.
- Bills per credit account, computed and always consistent with what the account owes.
- Settlement facts for the owner's own analysis.
- Two new informational figures, and no double count in Aman dipakai.
- An API that each Markas+ phase can render without touching the backend.

**Non-goals:** see the proposal. In short: no UI in this change, no notification capture, no interest model, no advice and no special pocket rules.

## Decisions

### D1. Terms per purchase in a side table

V54 adds this table:

```sql
credit_purchase_terms (
  transaction_id UUID PRIMARY KEY REFERENCES transactions(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  installment_count SMALLINT NOT NULL DEFAULT 1 CHECK (installment_count BETWEEN 1 AND 36),
  first_due_date DATE NULL,
  first_due_date_set_by VARCHAR(8) NULL CHECK (first_due_date_set_by IN ('owner', 'system')),
  created_at, updated_at,
  CHECK ((first_due_date IS NULL) = (first_due_date_set_by IS NULL))
)
```

- **No row:** the purchase is paid in full, with its due date derived (D2). Existing purchases need no backfill.
- **Who may have a row:** only an `expense` whose account is `credit`. The API enforces it:
  - `credit_terms_not_credit_purchase` (422);
  - `credit_terms_count_range` (422);
  - `credit_terms_due_before_purchase` (422).
- **Keeping rows valid:**
  - Changing the purchase's account to one that is not credit, or its type to anything but expense, deletes the row in the same transaction.
  - Changing its date clears a `system` first due date. It also clears an `owner` date that would fall before the new purchase date, and the response says `credit_terms_reset: true`.
  - Deleting the transaction deletes the row (cascade).

Rejected alternatives:
- **One stored row per instalment:** it must be rewritten on every amount, date or day edit, and it can drift from the purchase.
- **An obligation per instalment purchase:** the cost was shown in `add-credit-accounts` D1. It means manual debts, a split record, and payments counted as spending (a cash basis).
- **Columns on `transactions`:** every row would carry credit-only fields, and the "only on credit expenses" rule would sit in a 40-column table that every write path touches.

### D2. Due dates from the bill day and the payment day

V54 adds `accounts.statement_day SMALLINT NULL CHECK (1..31)`, which is credit-only like `payment_day` (`credit_fields_not_allowed`). The UI calls it "Tagihan terbit tiap tgl" / "Bill issued on day".

For a purchase whose local date (Asia/Jakarta) is `d`:
1. **Bill day:** `S = statement_day`, or 1 when it is null. A calendar month is the common default, and SPayLater's 11th-due plan uses the 1st.
2. **Statement close:** `close` is the first date after `d` whose day is `S`. A short month clamps the day to its last day. A purchase made on the bill day goes into the next bill.
3. **First due date:** the first date on or after `close` whose day is `payment_day` (clamped). Without a `payment_day` there is no due date. Bills are then not computed and the API says `due_dates_unavailable`.
4. **Owner's date:** an owner `first_due_date` replaces step 3.
5. **Later instalments:** instalment k (from 0) is due on `payment_day` k months after the first, clamped. An owner first date shifts the whole series.
6. **Fee:** due on its own date. A fee booked with a payment is settled by that payment.
7. **Cash advance:** due like a purchase.
8. **Opening debt** (a negative `initial_balance`): due on the first `payment_day` on or after the account's creation date.

The worked examples are in the spec, for example S=1 and P=11: a purchase on 5 Nov is due on 11 Dec.

**Freezing:** changing `payment_day` or `statement_day` would move every derived due date, and with it the on-time history. So the account `PATCH` first writes `system` first due dates for every purchase whose derived first due date is on or before today, in the same transaction. Only open and future bills move.

Rejected alternatives:
- **A due date stored on every purchase at creation:** every write path would need it (capture, edit, pairing completion, recurring, later notifications), and one missed path would fail silently. With derivation, a missed path is harmless.
- **Kredivo's "30 days after purchase" as a third mode:** the sources conflict, and the owner's app shows the real rule. A per-account billing mode stays an open question (O1). Until then the owner's first due date covers it.

### D3. Spending counts in full at purchase

A credit purchase, instalments included, stays a reportable expense on its own date. This is the accrual view: an expense is recognised when it is incurred, not when it is paid (AccountingTools). YNAB, Monarch, Copilot and Rocket Money all treat the purchase as spending and the card payment as a transfer, "because it would be double counting" (Rocket Money). The owner accepted this on 2026-11-05 and again on 2026-11-06.

What the purchase day does not show, "no cash left yet", is carried by labels and by the cash figure of D11, not by moving the spending.

Rejected alternatives:
- **A cash basis** (spending at payment): it contradicts the approved credit-accounts rule. A payment would need to be split back into categories and pillars, and a purchase would not appear in Kakeibo until it is paid.
- **Spreading an instalment purchase over its instalments:** Pengeluaran would change, and the category and pillar would split across months. The owner rejected it ("A credit purchase counts as Pengeluaran").

### D4. Bills are computed at read time, not stored

`services/credit_bills.py` builds an account's schedule from its rows on each request. Rows are loaded once per credit account per request, and the cost is bounded by the account's history (hundreds of rows per year). The schedule contains:
- **Items:** purchase instalments, fees, cash advances and the opening debt.
- **Credits:** payments, which are inbound movement legs, and refunds, which are incomes.
- **Bills:** items grouped by `(account, due_date)`. A bill's `statement_date` is the latest date on or before its due date whose day is `S`.

**Invariant:** `Σ items − Σ credits = −balance`, where balance is the account's ledger balance including `initial_balance`. So left to pay is `credit_owed`, and a negative remainder is `credit_balance` ("Lebih bayar"). A violated invariant raises an error and is never patched up.

Rejected alternatives:
- **A stored statement table:** it must be recomputed after every edit, delete, undo or merge anyway, and a stale statement is a wrong money figure.
- **Stored payment allocations:** `transaction_obligation_allocations` and `reverse_allocations` show the cost. Every delete, split, merge and undo path needs its own reversal.

### D5. Payments settle the oldest due items first

Credits are applied in date order to items sorted by `(due_date, purchase date, transaction id, instalment number)`.

An item's `settled_on` is the date of the credit at which the running credit total first covers the running item total through that item. It is never earlier than the item's own date, so a payment made before a purchase gives 0 days. From that:
- `days_to_settle = settled_on − purchase local date`;
- `settled_on_time = settled_on <= due_date`.

The result handles every payment pattern:
- partial payments;
- early payment in full of every instalment;
- overpayment, which is carried into later items.

Providers apply a payment to the current bill, so oldest-due-first matches what the owner sees in the provider's app.

Bill status, in order:
1. `paid` when nothing is left and every item was settled by the due date;
2. `paid_late` when nothing is left but some item was settled after it;
3. `open` when today is before `statement_date`;
4. `upcoming` when the due date is in the future;
5. `due_today` on the due date;
6. `overdue` after it.

Rejected alternatives:
- **The owner links each payment to a bill:** more work for the owner, and wrong when the provider applies the payment differently. It may come later as an override, if needed.
- **Settle by purchase date:** it ignores instalments that are due later.

### D6. A refund settles the oldest due item too

An income on a credit account is a credit applied by the same order as a payment. It is labelled `refund`, and it is never counted as cash paid to credit.

Rejected for now: reducing the refunded purchase's last instalments. That needs a refund-to-purchase link that the ledger does not have (risk R2).

### D7. Instalment amounts in whole rupiah

Each instalment is `floor(amount / n)`. The remainder `amount − n·floor` is added to the first instalment, so the first bill shows the odd amount, and the sum is always exact.

Placing the remainder on the last instalment stays open (O2). The provider's own bill decides.

### D8. The credit fee category gets a system key

V54 adds `credit_fee` to `categories_system_key_check`. It backfills `system_key = 'credit_fee'` on categories whose name is exactly "Biaya kredit" and whose kind is `expense`, only while the owner has no keyed row yet.

- `ensure_credit_fee_category` becomes `ensure_system_category(cur, user_id, CREDIT_FEE, 'expense')`. Its seed name comes from the account's language: "Biaya kredit" in Indonesian, "Credit fee" in English. This follows the stored-text rule of `localize-server-text`.
- `credit_fee` is **reportable**. It is not in `NON_REPORTABLE_KEYS`, so fees stay spending.
- In the schedule, a row is a fee item when it is a credit-account expense with `fee_for_movement_id` set or with category key `credit_fee`, so a late fee recorded by the owner counts as a fee in the totals.

Rejected alternative: keep finding the category by name. #88 removed name-based rules because a rename moved figures. Here a rename would move fee totals, and an English account would get a second category.

### D9. F1 (FLAGGED, approved 2026-11-06): Terjadwal counts a credit-bill plan only for debt that never was spending

The owner approved F1 explicitly on 2026-11-06 ("ok").

A plan is a credit-bill plan when its type is `transfer` and its target account's type is `credit`. For each unpaid occurrence of one:
- `cycle_summary` counts `min(plan amount, the account's opening debt still left to pay)` (D4 item kind `opening`);
- the rest is marked `counted_elsewhere`.

So the line keeps three fields:
- `plan_amount`, the full plan;
- `amount`, the counted part;
- `counted_elsewhere`, the rest, which is already in Pengeluaran as purchases, fees or spent cash advances.

`plans_total` and Terjadwal sum `amount`. Rencana, Target & Tagihan and the calendar still show the line, with its full plan amount.

Why this is a double count today: the purchases already lowered Aman dipakai. Aman dipakai does not carry a cycle's leftover into the next one, so the cash kept for the bill is outside the next cycle's figure. Subtracting the bill again in Terjadwal counts it twice. The research note works this through with numbers.

**Opening debt** (owed before the account was tracked) was never Pengeluaran, so a plan that pays it still reserves money, as today. A cash advance is treated like a purchase: the cash it brought was not Masuk, and spending it is already Pengeluaran.

For an account with no opening debt left, the counted part is 0.

Rejected alternatives:
- **Keep counting the plans in full** (the fallback if F1 is rejected): it is the status quo, with a notice on the plan.
- **Leave every credit-bill plan out of Terjadwal:** it would stop reserving for opening debt, which was never spending.
- **Add the computed credit bills to Terjadwal:** the same double count, built in.
- **Refuse transfer plans into credit:** existing plans would still count, and a reminder is useful.

### D10. F2 (unchanged): payments from a savings destination

A movement from a role-`tabungan` or goal-linked account into a credit account keeps lowering the saving bucket, because the general rule says savings went down.

Changing it would need either an exception in `get_kakeibo_breakdown`, which changes "Ditabung & investasi" for every owner, or a new movement class. Both alter approved `organize-accounts-by-use` D3 and D4 figures and need their own review. This change only:
- documents the behaviour;
- adds a characterisation test;
- shows "Tagihan kredit dibayar" (D11), so the cash out is visible next to it.

No rule depends on any particular pocket.

### D11. Two informational credit figures in the cycle summary

`spending.credit` is new. It contains:
- **`spent_on_credit`** ("Pengeluaran pakai kredit" / "Spending on credit"): reportable expenses on credit accounts in the period, net of applied reimbursements as Pengeluaran is. It is a part of `spending.amount`, never added to it.
- **`paid_to_credit`** ("Tagihan kredit dibayar" / "Credit bills paid"): the outbound amounts of movements in the period whose inbound leg is a credit account and whose outbound leg is not. It is the cash that left, including a booked fee.
- **`lines`:** one per credit account with either figure, with `key: credit:<id>`, `label` the account name and `label_source: user`.

`paid_to_credit` is not spending and is never added to Keluar. No existing total changes.

### D12. Order of work and where each piece of UI lands

**Order:**
1. `add-language-system`
2. `localize-server-text`
3. **this change**: backend and API only. It can be built in parallel with phase 4 (UI only) and merges before phase 5 starts.
4. Phases 5 to 11 render it. Each phase adds the hand-off block below to its own change, as ADDED requirements in `credit-accounts` (or the spec it already modifies), with tasks.
5. The capture change (D13) waits for samples and can come after this change in any order.

Phases 4 to 12 keep their "backend untouched" gate, because every field they need exists after step 3. Phase 5's approved change (`apply-markas-transaksi`) gains block H5. This is a scope addition to that change, recorded here and surfaced before phase 5 starts.

Every requirement below is at most 500 characters. Its wording follows the glossary (D14).

**H5. Phase 5 Transaksi (`apply-markas-transaksi`)**
- *Requirement: Credit Purchase Rows.* A credit purchase row SHALL carry the meta "dibayar nanti · {account} · jatuh tempo {date}" ("pay later · {account} · due {date}"). With instalments it SHALL read "cicilan {k}/{n}" ("installment {k} of {n}"), and once settled "lunas {date}" ("paid {date}"). It SHALL keep the expense amount in the ink.
  - Scenario: a Rp 120.000 SPayLater dinner on 5 Nov with S=1 and P=11 SHALL read "dibayar nanti · SPayLater · jatuh tempo 11 Des".
- *Requirement: Credit Payment Rows.* A movement into a credit account SHALL keep its route "A → B" and add "bayar tagihan" ("bill payment") to its meta.
  - Scenario: Jago → SPayLater Rp 470.000 SHALL read "Jago → SPayLater · bayar tagihan".
- *Requirement: Provider Filter.* The Rekening filter SHALL list credit accounts in a "Kredit" group. With one selected, the page SHALL show that account's Utang, next due bill, "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" for the period, from `GET /api/credit/accounts/{id}`.
  - Scenario: SPayLater selected SHALL list its purchases, fees, refunds and payments only.
- *Requirement: Credit Terms in the Transaction Dialog.* The dialog of a credit purchase SHALL show its terms, its instalments with due dates and status, and its settlement date and days. "Ubah transaksi" SHALL change the instalment count and the first due date through `PUT /api/transactions/{id}/credit-terms`.

**H8. Phase 8 Rekening (`apply-markas-rekening`)**
- *Requirement: Credit Account Tile.* A credit tile SHALL show Utang, "Sisa limit {amount}" ("Limit left") or "Melewati limit", and the next bill "jatuh tempo {date} · {left}".
- *Requirement: Credit Account Detail.* The credit dialog SHALL list the bills per due date with their amount, paid, left and status (open, upcoming, due today, overdue, paid, paid late), and link to Transaksi filtered by the account.
- *Requirement: Credit Days in the Account Form.* The account form SHALL edit "Tagihan terbit tiap tgl" and "Jatuh tempo tiap tgl", and SHALL say that past bills keep their due dates.

**H9. Phase 9 Target & Tagihan (`apply-markas-target`), with Beranda and the bell**
- *Requirement: Credit Bills Beside Plans.* "Tagihan 30 hari" SHALL list unpaid credit bills from `GET /api/credit/bills` as "Tagihan {account}" with the due date and what is left. It SHALL mark them "sudah dihitung di Pengeluaran" ("already counted in Spending") and SHALL NOT add them to Terjadwal. A credit-bill plan SHALL show its full amount and the part counted elsewhere.
- *Requirement: Credit in Utang & kredit.* Each credit account SHALL show its Utang and its next due bill.
- *Requirement: Due Credit Bills Need Action.* Beranda's "Perlu tindakan" and the bell SHALL list overdue credit bills, then bills due today, then bills due within two days, ranked with plans of the same urgency. Selecting one SHALL open Catat's Perpindahan into that account with the amount left.
- *Requirement: Credit Lines in the Pengeluaran Breakdown.* Beranda's Rincian of Pengeluaran SHALL show "Pengeluaran pakai kredit" as a part of Pengeluaran, and "Tagihan kredit dibayar" as cash out that is not counted again.

**H11. Phase 11 Catat (`apply-markas-catat-dialogs`)**
- *Requirement: Paying With Credit.* When an expense's account is a credit account, Catat SHALL show "Bayar pakai kredit" ("Pay with credit") with "Sisa limit", a choice of "Sekali bayar · 3x · 6x · 12x · Lainnya" ("Pay in full · 3 · 6 · 12 · Other"), and the first due date. It SHALL send `credit_terms`.
- *Requirement: Paying a Credit Bill.* Perpindahan into a credit account SHALL offer "Bayar sisa tagihan {amount}" ("Pay the amount left {amount}") for the next bill, and SHALL keep the "Biaya / bunga" field.

Rejected alternatives:
- **Build the web UI in this change, on today's pages:** each surface would be rebuilt again by its Markas+ phase.
- **Keep this change open until phase 11:** a half-implemented change would sit on `main` unarchived, which breaks the archive-with-implementation rule.

### D13. Notification capture hook (pending samples)

The later capture change `capture-credit-notifications` will do three things:
1. allow the notification mapping to point at a credit account;
2. book a recognised purchase as an expense on it, through the same create path as `POST /api/transactions`, so default terms and derived due dates apply. If the text names a plan ("3x", "cicilan 3 bulan"), deterministic code sets `credit_terms.installment_count`. The model never sets terms.
3. let a recognised bill payment ("Pembayaran tagihan SPayLater berhasil") complete the cash leg as a movement into the credit account, as Tinjau's completion does today.

Kredivo app capture belongs to the companion's own OpenSpec root.

**Status:** pending. It waits for real purchase, instalment and payment notifications from the owner's device (the owner's phone database is the sample source). The owner is asked again when this change is merged, or when such samples are first seen.

### D14. Terms in both languages

US spelling. Plain words. One word per concept, following the language research §7.

| Concept | Indonesian | English | Rejected |
| --- | --- | --- | --- |
| Credit account type | Kredit | Credit | "Credit line": jargon |
| Purchase on credit (row status) | dibayar nanti | pay later | "Deferred": jargon. "BNPL": acronym |
| Expense paid by credit (Catat) | Bayar pakai kredit | Pay with credit | "Charge": card jargon |
| Instalment | Cicilan | Installment (US) | "Instalment": UK spelling |
| Paid in full | Sekali bayar | Pay in full | "One-time": ambiguous with one-off spending |
| Credit bill | Tagihan {account} | {account} bill | "Statement": banking jargon. "Invoice": business |
| Paying the bill (action, row) | Bayar tagihan | Bill payment | "Settlement": accounting and banking jargon. "Repayment": loan jargon |
| Due | Jatuh tempo | Due | "Past due": formal (glossary) |
| Bill issued on day | Tagihan terbit tiap tgl | Bill issued on day | "Statement date": jargon |
| Left to pay | Sisa tagihan | Left to pay | "Outstanding": formal |
| Limit left | Sisa limit | Limit left | "Available credit": card jargon |
| Over the limit | Melewati limit | Over limit | — |
| Paid / paid late / paid early | Lunas / lunas terlambat / lunas lebih awal | Paid / paid late / paid early | "Settled": jargon |
| Days to pay | Hari sampai lunas | Days to pay | "Settlement period": jargon |
| Spending on credit (figure) | Pengeluaran pakai kredit | Spending on credit | "Credit expenses": "expense" is banned as a figure word |
| Credit bills paid (figure) | Tagihan kredit dibayar | Credit bills paid | "Money out for credit": direction words belong in row text |
| Credit fee category | Biaya kredit | Credit fee | "Interest": not every fee is interest |
| Owed (on the account) | Utang | Owed | "Balance": ambiguous for credit |
| Overpaid | Lebih bayar | Overpaid | "Credit balance": confusing next to "credit" |

The brief's "Uang keluar untuk bayar kredit" becomes the figure label "Tagihan kredit dibayar". The direction phrase "uang keluar …" stays allowed in explanation text ("uang yang benar-benar keluar untuk tagihan kredit"). This keeps the figure-label rule of `ui-copy-register` (no "Keluar" figure).

### D15. Migration V54 and a number clash

`db/migrations/V54__credit_purchase_terms.sql` is additive and safe to run twice, as V50 is. It:
- adds `statement_day` with its CHECK;
- creates the terms table with its index `(user_id)`;
- drops and re-adds `categories_system_key_check` with `credit_fee`;
- runs the guarded backfill, with V52's duplicate check pattern.

V53 is used by `add-language-system`, and `localize-server-text` adds none, so V54 is the next free number.

**If V54 is taken when this branch rebases on `main`:**
- rename this file to the next free version before merge;
- check that `db/migrations` has unique versions (test 1.4);
- never renumber after a release has applied it.

Flyway runs in order, so the release preflight (`scripts/flyway_v14_preflight.py`) needs no change.

### D16. Approval and implementation notes (2026-11-06)

**Approval.** The change was approved on 2026-11-06 under the owner's standing instruction "complete all until finished". The owner approved F1 explicitly the same day ("ok"). O1–O5 and every other decision take their recommended defaults.

**Notes from implementation.** None changes how an existing figure is computed.
- **A stored first due date anchors its series.** D2 keeps the due dates of purchases already due when the bill day or payment day changes, but stores only the first due date. An instalment purchase that is partly due therefore keeps its day for its remaining instalments, as an owner first due date does (D2.5), so no paid instalment is ever re-dated. Every purchase not yet due follows the new days.
- **Opening debt follows the current payment day.** Its due date is derived from the account's creation date and has no stored date, so a payment-day change can move it (risk R5).
- **`spent_on_credit` is the credit part of Pengeluaran.** It counts every pillar but saving, as `spending_split` does. A reimbursement offset of a category and pillar is shared between the accounts that hold that spending, in proportion to it (largest remainder), so the figure never exceeds Pengeluaran. The overview's `spent_on_credit` uses the same rule.
- **Errors.** New refusals use `{code, params, message}` through `credit_terms.credit_error`, with English and Indonesian messages, and the web `errors` namespace has every code. The new codes besides D's list are `credit_period_invalid` (a period whose start is after its end), `account_not_found` and `transaction_not_found` (404). The existing `add-credit-accounts` codes, `credit_fields_not_allowed` among them, keep their current shape until `localize-server-text` converts every error site.
- **The transaction dialog.** `GET /api/transactions/{id}` carries the same `credit` block as the ledger row, with `installments` added for H5's dialog.
- **The fee category.** `ensure_system_category` creates `credit_fee` as an unexpected-pillar expense that counts in budgets, and never repairs the owner's later classification of it. The fee row's note is the fee's seed name in the account's language. A parser hint may still reach the fee category by its name, as before it had a key.
- **Test owners.** The synthetic owner of `test_cycle_summary.make_owner` is now an Indonesian account, as every name it creates is. It had become English only through the V53 column default.

## Data, API and failure behaviour

- **`GET /api/credit/accounts/{id}`** (another owner's account or a non-credit account gives 404 / 422 `credit_account_required`).
  - Response:
    - `account`: id, name, owed, credit_limit, limit_left, over_limit, statement_day, payment_day, credit_balance;
    - `next_due`: due_date, amount, left_to_pay and status, or null;
    - `bills[]`: due_date, statement_date, amount, paid, left_to_pay, status, settled_on, `items[]`;
    - `totals` for `from`/`to` (default: the current period): spent_on_credit, fees, refunds, paid_to_credit, items_settled, items_settled_on_time.
  - Each item has: kind (`purchase`, `fee`, `cash_advance`, `opening`), transaction_id, date, due_date, installment {number, count} or null, amount, left_to_pay, settled_on, days_to_settle, settled_on_time.
  - Without `payment_day`: `bills: []` and `due_dates_unavailable: true`, while owed and limit still show.
- **`GET /api/credit/bills?from&to&status`:** bills across the owner's credit accounts without items, including `account_id` and `account_name`. Target & Tagihan, Perlu tindakan and the bell use it.
- **`PUT /api/transactions/{id}/credit-terms`** with `{installment_count, first_due_date?}`: returns the purchase's schedule. **`DELETE`** returns it to paid in full with a derived date. `POST /api/transactions` accepts the same `credit_terms`.
- **`GET /api/transactions`** rows gain `credit`. It is null for rows not on a credit account and for movement legs whose partner is not credit. Otherwise it holds:
  - `role`: `purchase`, `fee`, `refund`, `payment` or `cash_advance`;
  - account_id and account_name;
  - for a purchase: installment_count, installments_settled, next_due_date, left_to_pay, settled_on, days_to_settle and settled_on_time.
  Movement rows into credit carry `role: payment`.
- **`GET /api/accounts`** credit rows gain `statement_day`, `limit_left` (`credit_limit − owed`, may be negative) and `next_due`.
- **Cycle summary:** `spending.credit`, plus `plan_amount` and `counted_elsewhere` on credit-bill plan lines in `scheduled.lines`.
- **Errors:** 422 with the `localize-server-text` contract. The codes are:
  - `credit_terms_not_credit_purchase`;
  - `credit_terms_count_range`;
  - `credit_terms_due_before_purchase`;
  - `credit_account_required`;
  - `credit_fields_not_allowed` (extended to `statement_day`).
  Each code exists in both catalogues and in the web `errors` namespace (the contract test of `localize-server-text`).
- **Concurrency:** terms writes lock the purchase row. The account `PATCH` freezes due dates inside its own transaction, with the account row locked.

## Compatibility, migration and rollback

- **Additive only:** existing clients ignore the new fields. `credit` is null wherever no credit is involved.
- **Rollback:** redeploy the previous release. The column, the table and the key stay, unused.
  - The older release's `ensure_credit_fee_category` finds "Biaya kredit" by its unique `(user_id, name, kind)`, and the key does not hinder it.
  - Terms written by the new release are ignored.
  - After a rollback, a credit-bill plan counts in full in Terjadwal again.

## Risks / Trade-offs

- **R1. Derived due dates can differ from the provider's**, for example Kredivo's 30-day rule or a provider's cut-off time. Mitigation: the owner sets the first due date, the account days are editable, past bills are frozen, and O1 stays open.
- **R2. Refund allocation:** a partial refund on an instalment purchase settles the oldest item, not the refunded purchase's last instalments. Totals and what is left stay exact, but the per-purchase settlement dates can differ from the provider's.
- **R3. Read-time cost:** the schedule is computed per credit account per request. A test with 2,000 synthetic rows on one account keeps `GET /api/credit/accounts/{id}` under 300 ms locally. If it fails, cache per request; a table is not the fix.
- **R4. F1 changes a figure:** it is reviewed before implementation (proposal), and test 3.4 proves the exact change.
- **R5. Opening debt has no stored due date:** a payment-day change moves its due date, and with it its on-time fact. It matters only for the first bill after an account is added. Mitigation: the owner sets the payment day when adding the account; a stored opening due date would be a follow-up.

## Open questions (provisional defaults)

- **O1. Kredivo billing.** Is a 30-day purchase due 30 days after the purchase, or on a fixed monthly date? Default: a fixed monthly date from `payment_day`, with the owner's first due date as the fix. Revisit when the owner reads the rule in the Kredivo app. A per-account mode would be a follow-up change.
- **O2. Remainder placement.** First instalment (default) or last? Revisit when the first real instalment bill is compared.
- **O3. Instalment cap.** 36 (default), which covers 3/6/12/18/24 with headroom.
- **O4. Default bill day** when `statement_day` is empty: 1 (default).
- **O5. Bell window.** Bills due within two days, the same as plans (default).
