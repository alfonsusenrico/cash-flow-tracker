## Why

On 2026-11-06 the owner described how they use credit. They mostly use SPayLater and Kredivo for simple things: eating out, or something they want. Instalments of 3, 6 or 12 months are occasional. Every plan is 0% and free of charge, and they pay before the due date.

What the owner wants from the app:
- "I can track in the ledger about credits transaction might be grouped per provider (Spaylater/kredivo/anything else), then the settlement also. Later I can analyze myself how efficient is the money management strategy."
- A credit purchase counts as Pengeluaran, and the UI makes clear that "the day the credit transaction made, physically there's no money out. The real money out activity is when I pay the settlement, either it's in one month payment, 3 or 6 or 12 months."

`add-credit-accounts` (2026-11-05) made the ledger correct:
- a purchase is an expense on a `credit` account;
- paying is a movement into it;
- a fee is a "Biaya kredit" expense.

The ledger still cannot answer the owner's questions:
- which bill a purchase belongs to and when it is due;
- how a purchase splits into instalments;
- which payment settled which bill, and whether it was on time;
- how much this period was spent on credit, and how much cash actually went out to pay credit.

A recurring transfer plan used as a bill reminder also counts the same money twice in "Aman dipakai" (design D9).

## Expected Outcome

- **Purchase terms:** a purchase on a credit account carries terms: paid in full (the default) or split into N monthly instalments. Each instalment has a due date that follows the account's bill (statement) day and payment day. The owner can correct the first due date.
- **Bills per provider:** for every credit account, the server returns its bills per due date. Each bill has its amount, the amount paid, what is left, and a status: open, upcoming, due today, overdue, paid or paid late. Paying is still an ordinary movement into the credit account. Payments and refunds settle the oldest due instalments first, so partial payments and early payment in full both work without the owner linking anything.
- **Analysis data:** each purchase and instalment exposes when it was settled, how many days after the purchase, and whether that was by its due date. These are data for the owner's own analysis, never advice.
- **Figures:**
  - Pengeluaran still counts a credit purchase in full on the day it is made.
  - The cycle summary adds two figures inside Pengeluaran's explanation: "Pengeluaran pakai kredit" (spending on credit in the period) and "Tagihan kredit dibayar" (cash paid to credit accounts in the period).
  - No other total changes, except the flagged Terjadwal rule (F1).
- **Rows and filter:** ledger rows say that no cash left yet ("dibayar nanti · SPayLater · jatuh tempo 11 Des"). The existing account filter is the per-provider view: purchases, fees, refunds and payments of one credit account.
- **UI timing:** the web shows all of this through the Markas+ phases that rebuild each surface (design D12), so nothing is built twice.

## Approval

Approved 2026-11-06 under the owner's standing instruction; decisions take their recommended defaults.

The owner gave the standing instruction "complete all until finished" on 2026-11-06, and said this credit work may go in whatever order is best and most efficient.

- **F1:** the owner explicitly approved flag F1 on 2026-11-06 ("ok"). A transfer plan into a credit account counts in Terjadwal only up to the account's opening debt still left to pay, and the rest is shown as already counted in Pengeluaran.
- **O1–O5 and every other decision:** they take their recommended defaults, under the same standing instruction of 2026-11-06.

### Flagged decisions: they change how a figure is computed and are reviewed before implementation (F1 approved 2026-11-06)

- **F1 (D9):** "Terjadwal" counts an unpaid transfer plan into a credit account only up to the account's opening debt still left to pay. That debt was owed before tracking, and it never was Pengeluaran.
  - Why: today such a plan, used as a "Bayar SPayLater" reminder, subtracts the bill from "Aman dipakai" a second time, because the purchases are already in Pengeluaran.
  - The plans are still listed in full, with what is already counted.
  - This changes "Terjadwal" and "Aman dipakai" only for owners who have such a plan.
  - Fallback if rejected: keep counting them in full, and show a notice on the plan that it counts the purchases twice.

### Reviewed and left unchanged (no figure changes)

- **F2 (D10):** a payment from an account with role Tabungan, or one linked to a goal, keeps lowering "Ditabung & investasi". This is the general savings rule, unchanged. Moving money into such an account counts as saving, so it lowers "Aman dipakai" next to the purchase until the payment brings it back. This is documented and covered by a characterisation test.
- **D3:** an instalment purchase stays Pengeluaran in full on the purchase day (owner OK, 2026-11-06), not spread over the instalments.
- **D8:** the new `credit_fee` category key does not change any figure. A parity test proves it.

## What Changes

- **Migration V54 (additive):**
  - `accounts.statement_day` (nullable, 1–31, credit only);
  - table `credit_purchase_terms` (instalment count 1–36, an optional first due date and who set it);
  - the system category key `credit_fee`, backfilled on expense categories named exactly "Biaya kredit".
- **Purchase terms:** set at creation (`POST /api/transactions` accepts `credit_terms`), and changed or reset later (`PUT` and `DELETE /api/transactions/{id}/credit-terms`). Terms are only for an expense on a credit account. Removing a purchase from a credit account removes its terms.
- **Due dates:**
  - computed from the purchase date, `statement_day` (default 1) and `payment_day`;
  - instalment k falls k months after the first;
  - amounts are split into whole rupiah, with the remainder on the first instalment.
  - Changing an account's payment or bill day freezes the due dates that have already passed.
- **Bills at read time:** one service builds, per credit account:
  - the items: instalments, fees, cash advances and the opening debt;
  - the credits: payments and refunds, applied oldest due date first;
  - the bills per due date.
  It guarantees that what is left to pay equals what the account owes. New endpoints: `GET /api/credit/accounts/{id}` and `GET /api/credit/bills`. No statement table is added.
- **Ledger rows:** `GET /api/transactions` rows gain a nullable `credit` block: role, account, due date, instalment number and count, status, settlement date, days to settle.
- **Accounts:** credit rows in `GET /api/accounts` gain `statement_day`, `limit_left` and `next_due`.
- **Cycle summary:** gains `spending.credit` (spending on credit, cash paid to credit accounts, and per-account lines with `label_source: user`). Terjadwal follows F1.
- **Fee category:** "Biaya kredit" is found by the `credit_fee` key instead of its name. A new one is created in the account's language ("Credit fee" in English).
- **Errors:** every new refusal uses the error contract of `localize-server-text`, with codes in both catalogues.
- **Phase hand-off:** design D12 assigns each presentation requirement to the Markas+ phase that rebuilds its surface: 5, 8, 9 and 11, plus Beranda's Perlu tindakan and the bell in phase 9.

## Non-goals

- **Web UI in this change.** Phases 5, 8, 9 and 11 deliver it (D12). This change is server and API only.
- **Notification capture** of SPayLater and Kredivo purchases and payments. It is blocked on real samples. The hook is planned (D13), and it stays pending.
- **No interest model:**
  - no interest schedules, minimum payments or late-fee calculation;
  - an actual fee stays a "Biaya kredit" expense, booked by the owner or with a payment.
- **No link with obligations.** Kredivo's earlier debt stays an obligation.
- **No special handling of any particular pocket**, and no rule about where the bill money is kept.
- **No advice** or efficiency scores. The owner analyses the data.
- **No spreading** of an instalment purchase across months in Pengeluaran (D3).
- **No production action.** Verification is local, on synthetic data in disposable PostgreSQL. Release goes through the approved GitHub workflow after the owner accepts.

## Capabilities

### New Capabilities

None. Credit terms and bills belong to the existing `credit-accounts` capability.

### Modified Capabilities

- `credit-accounts`:
  - the account type gains the bill day;
  - new requirements for purchase terms, due dates, bills and settlement, settlement facts per item, ledger row facts, the account overview, and the credit fee identity.
- `beranda-cycle-summary`:
  - "Scheduled Amount" follows F1;
  - new credit figures in the cycle summary API.
- `custom-category-management`: "System Category Identity" gains the `credit_fee` key.

Project capabilities do not change. The change exercises the declared standards:
- `database`: additive migration, migration test, rollback;
- `money`: the figure parity and double-count tests;
- `multi-user`: owner scoping and 404 for another owner's data;
- `api-consumers`: additive fields, one error shape.

## Impact

- **Database:** V54, additive. An older release ignores the column and the table. `credit_fee` is outside the older release's non-reportable keys, so fees still count as spending there. Rollback: redeploy the previous release, and leave the column, the table and the key.
- **Backend:**
  - new `services/credit_terms.py` (due dates, schedules) and `services/credit_bills.py` (items, credits, allocation, bills);
  - new `routers/credit.py`;
  - changes to `routers/transactions.py` (terms on create and edit, the row `credit` block), `routers/accounts.py` (fields, freezing on day change), `services/credit_accounts.py` (fee by key), `services/system_categories.py`, `services/cycle_summary.py` (credit figures, F1) and `services/plan_occurrences.py` (F1 marker);
  - the message catalogues of `localize-server-text`.
- **API:** additive fields and four endpoints. No field is removed or renamed.
- **Order:** this change goes after `add-language-system` and `localize-server-text` (V53, the error contract and catalogues). It can be built in parallel with phase 4, and it merges before phase 5 starts (D12).
- **Android companion:** unchanged.
