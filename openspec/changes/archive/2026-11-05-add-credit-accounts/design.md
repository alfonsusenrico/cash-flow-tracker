## Context

The touchpoint map was read from code on `dd67c58` and spot-checked.

**Liquid-only rules:** nearly every rule tests "liquid" (`type IN cash, bank, wallet, ewallet` with no instrument) or "investment". Today a `credit` account would:
- fall into `investment_balance` (`summarize_account_balances`);
- be refused by every liquid-only picker and pairing rule;
- be impossible to open with an existing debt (`initial_balance ge=0`).

**Other facts:**
- `accounts.type` has no database CHECK. The API pattern `^(cash|bank|wallet|investment)$` is the only gate.
- Beranda's Masuk, Pengeluaran and "Aman dipakai" are computed from cycle transactions, whatever the account type. A credit expense therefore already behaves as the owner wants.
- Ditabung counts a movement only when `movement_kakeibo` tags it saving: one side investment, or a goal link. Bank → credit is not tagged.
- Obligations are a separate liability list feeding net worth, essential cost and Terjadwal.

## Goals / Non-Goals

**Goals:**
- A credit account whose balance is the amount owed.
- Purchases that count as spending when made.
- Payments that are movements.
- Fees that are expenses.
- Net worth with credit as utang.

**Non-goals:** see the proposal.
- No notification wording.
- No reminders.
- No obligation linking.
- No "Cocokkan saldo" for credit.

## Decisions

### D1. A new account type, not an obligation

`credit` is an account type with a signed balance, negative while owed, booked through ordinary transactions and movements.

Rejected alternatives:
- **An obligation per purchase:** the owner's past SPayLater obligations show the cost. Every purchase is a manual debt row paid off by hand, the purchase category and the debt are separate records, and notifications cannot land on a debt.
- **A flag on wallet accounts:** every liquid rule would then need an exception for flagged wallets, and the credit account would count as cash in Ketahanan Dana.

### D2. Columns and validation

V50 adds `credit_limit BIGINT NULL CHECK (credit_limit > 0)` and `payment_day SMALLINT NULL CHECK (payment_day BETWEEN 1 AND 31)`.

The API enforces the rest:
- type `credit` is accepted;
- a credit account has no parent and no pockets;
- `initial_balance <= 0` for credit and `>= 0` for every other type;
- limit and payment day are allowed only on credit;
- changing the type to or from `credit` is allowed only while the account has no transactions.

The UI asks for "Utang saat ini" as a positive amount and stores its negative.

A database CHECK on `type` is not added. Existing rows include the legacy `ewallet` and `regular`, so a CHECK risks a failed migration in production for no gain.

### D3. Balance buckets

`summarize_account_balances` sends credit to a new bucket, `credit_owed`: the sum of `max(-balance, 0)` over credit accounts.

An overpaid credit account (positive balance) counts as 0 owed and is not added to any asset bucket. The account card shows "Lebih bayar Rp X". Rejected: adding the overpayment to `liquid_balance`. That money cannot be spent as cash, so counting it would overstate "Dana likuid". Overpayment is rare, and the card keeps it visible.

`investment_balance` no longer receives credit accounts.

### D4. Net worth and utang

`cycle_summary` net worth: `total_liabilities = obligations + credit_owed`, and `lines` gains one `credit:<id>` line per credit account that owes money.

The Beranda caption "aset Rp … · utang Rp …" therefore includes credit. Pulse's raw `total_net_worth` and Ketahanan Dana keep using `liquid_balance` only, so credit headroom is never counted as an emergency fund.

### D5. Purchases and the limit

Expenses on credit skip `ensure_sufficient_funds`, which already skips non-liquid types; the credit type is added explicitly so the intent is visible.

After a save, `over_limit = credit_limit IS NOT NULL AND owed > credit_limit`. It is returned and shown, never refused. The limit is the provider's business, and a real purchase that already happened must be recordable, including one that came from a notification.

### D6. Payments, advances and fees

- **Movements:** credit is allowed as a movement source or target. `ensure_generic_movement_accounts` already allows it.
- **Funds check:** for the liquid side only, as today.
- **Fee:** a payment's optional fee is booked as an expense on the credit account in the category "Biaya kredit" (Kakeibo `unexpected`). It is created on first use for the owner, as `ensure_fee_category` does for transfer fees. The movement amount is the cash that left the bank. The amount owed falls by the movement amount minus the fee, because the fee expense is booked on the same account.
- **Tinjau:** completing a one-sided leg, merging two legs and the server pairing check accept a credit account as the non-liquid side. A movement needs at least one liquid leg.

Rejected alternative: a dedicated "Bayar tagihan" endpoint. The movement already carries the money, and a second path would need its own undo, history and edit rules.

### D7. Pickers

`lib/accountOptions.ts` gains a "Kredit" group, offered where D5 and D6 allow:
- the expense account in QuickCapture, the transaction edit form and recurring expenses;
- movement source and target;
- one-sided completion.
It is not offered for:
- income other than refunds (the edit form still allows a refund income);
- investment funding and top-ups;
- obligation payments;
- "Cocokkan saldo".

The server refuses credit in those places with 422, so the browser is not the only gate.

### D8. Cocokkan saldo

It is hidden for credit accounts. This also fixes today's mismatch, where the dialog shows the action for any non-investment account. Comparing a provider's bill with the owed balance is a follow-up, with its own wording ("Tagihan di aplikasi").

### D9. No notification lands on credit yet

The notification pipeline places money only on cash accounts in about six places:
- interpretation;
- processing;
- application;
- context;
- the mapping and its AI proposals.

Opening only the mapping to credit would let an owner point an alias at SPayLater with nothing following from it. So mapping, recognising "Pembayaran SPayLater" and Kredivo texts, and capturing the Kredivo app are one follow-up change, built from real samples.

A Jago debit that pays a credit bill still arrives as a one-sided leg. The owner completes it with the credit account in Tinjau (D6).

## Data, API and failure behaviour

- **Accounts:** `GET /accounts` rows gain `credit_limit`, `payment_day`, `credit_owed` and `over_limit` (null or false for non-credit). Create and edit accept the new fields under D2's rules. Errors use the existing 422 shape with codes `credit_parent_not_allowed`, `credit_balance_sign`, `credit_fields_not_allowed` and `credit_type_change_has_transactions`.
- **Net worth:** the response gains `credit_owed`, and its `lines` include credit.
- **Rollback:** the release can be reverted while no credit account exists. Once one exists, an older release treats it as investment, mislabelled but with nothing lost. To roll back, first archive the credit accounts. The V50 columns stay, unused.

## Risks / Trade-offs

- **[Liquid sets repeated in ~15 places]:** each is decided explicitly, and tests cover the money paths: purchase, payment, fee, net worth, pickers and refusals. Unlisted places keep treating credit as non-liquid, which is the safe side: refused, not counted as cash.
- **[Owner enters the owed amount with the wrong sign]:** the UI asks for a positive "Utang saat ini". The API rejects a positive `initial_balance` for credit.

## Open questions

None blocking. The follow-ups are listed in the proposal's Non-goals.

## Implementation notes (2026-11-05)

- **Fee field:** new movements are created in QuickCapture's "Perpindahan" tab, so the "Biaya / bunga" field lives there. It appears only when the target is a credit account. The movement edit modal accepts credit sides but has no fee, because `PATCH /movements` takes none.
- **"Sesuaikan saldo" (reconcile):** it is hidden for credit, and the server refuses it with `credit_account_not_allowed`. Its form takes a cash balance, not an owed amount. Checking a credit balance belongs to the "Cocokkan saldo" follow-up (D8).
- **Goal funding:** goal account linking refuses a credit account on the server (`credit_account_not_allowed`) and leaves it out of the web picker. A goal is funded by money held, not by credit owed.
- **Default colour:** the default colour for credit is Slate `#475569` on both the server and the web.
