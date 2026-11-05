## Why

The owner's money framework changed on 2026-11-04. Most spending now goes on credit ("utilize credits as my colateral"):
- SPayLater, paid on the 11th of each month;
- Kredivo, which already carries an existing debt.

After each credit purchase, the owner moves the cash for it into a Jago pocket, "Bayar Kredit".

Today the app has no way to hold this:
- **Account types:** accounts are cash, bank, wallet or investment.
- **Starting balance:** it cannot be negative.
- **Utang:** the only debt is an obligation row.

A paylater purchase can only be booked on a cash account (wrong balance), or as an obligation that the owner pays off by hand. Earlier SPayLater debts were kept this way, as three paid obligations.

The owner's verdict: "I don't think this need to be applied for the feature as this is my specific case sequence, but I think adding credit account and capture the notification for that is good, and maybe a feature that can be expanded from the credit feature."

## Expected Outcome

- **Credit account:** the owner can add "SPayLater" as a **Kredit** account, with what is owed today, an optional limit and a payment day (11). Rekening & Kantong shows it with "Utang Rp X", the limit used and "dibayar tiap tgl 11".
- **Purchases:** a purchase paid with SPayLater is recorded as a **Pengeluaran** on that account, with its real category and on the day it was made. Pengeluaran, Kakeibo and "Aman dipakai" change at purchase time, and the amount owed grows.
- **Moving cash to "Bayar Kredit":** this stays an ordinary Pindah saldo.
- **Paying the bill:** the payment is a **Pindah saldo from "Bayar Kredit" into SPayLater**. The amount owed falls, and nothing is counted as spending a second time. A fee or interest on the bill is recorded as its own expense, "Biaya kredit", on the credit account.
- **Kekayaan bersih:** it counts what credit accounts owe as **utang**, next to obligations, never as an asset or as investment.

## Approval

On 2026-11-05 the owner answered "defaults ok" to the credit questions:
- a credit purchase counts as Pengeluaran at purchase time;
- "Aman dipakai" falls at purchase time;
- Kredivo's existing debt stays an obligation, and new Kredivo spending uses a credit account;
- which apps notify is found from the phone;
- a bill reminder is a follow-up;
- the two earlier purchases are recorded later.

**Plan approval, 2026-11-05:** the owner answered "approved, defaults ok".

## What Changes

- **Account type:** a new type `credit` (label "Kredit"). It is a top-level account with no pockets. Its balance is negative while money is owed.
- **Migration:** V50 adds `credit_limit` and `payment_day`, both nullable, each with a CHECK. `initial_balance` may be negative for a credit account only.
- **Balance summary:** `summarize_account_balances` gains a `credit_owed` bucket (positive, the sum owed). Credit leaves the "Investasi" bucket.
- **Net worth:** `total_liabilities` includes `credit_owed`, with one line per credit account. Pulse and Ketahanan Dana keep using liquid funds only.
- **Expenses:** an expense, or a refund income, may be booked on a credit account. No sufficient-funds check applies. Going over the limit shows a warning but is never refused.
- **Movements:** a movement may have a credit account as its target (payment) or its source (cash advance). One-sided legs in Tinjau can be completed with a credit account. A payment may carry a fee, which becomes a "Biaya kredit" expense on the credit account.
- **Pickers:** the web pickers for expense accounts, movement targets and sources, and one-sided completion list credit accounts in their own "Kredit" group. Investment funding, obligation payments and "Cocokkan saldo" keep excluding them.
- **Notifications:** no notification lands on a credit account yet. Mapping, recognising SPayLater and Kredivo wording, and companion capture are one follow-up change, made once real samples exist (design D9).

## Non-goals

- **SPayLater and Kredivo notification recognition, and companion capture of the Kredivo app.** No real sample exists yet, so a follow-up change will cover this from captured samples.
- **Bill reminder, statement cycle and minimum payment.** Follow-up.
- **"Cocokkan saldo" for credit accounts.** Follow-up. In this change the action is hidden for credit (D8).
- **Linking credit accounts with obligations, and converting Kredivo's obligation.** Kredivo's existing debt stays an obligation.
- **"Bayar Kredit" earmarking logic.** It stays an ordinary pocket.
- **No production action.** Production is only checked read-only after release.

## Capabilities

### New Capabilities

- `credit-accounts`: credit account type, owed balance, limit and payment day, purchases, payments and fees, net-worth treatment, pickers.

### Modified Capabilities

- `net-worth-analytics`: "Net Worth & Liquidity Aggregation" includes credit owed in `total_liabilities`.

No project capability is added. Money, database and multi-user standards apply: owner scoping, migration safety and regression tests on a migrated PostgreSQL.

## Impact

- **Database:** V50, additive. An older release ignores the columns. A `credit` account written by the new release reads as "not liquid" in an older release, so roll back only before credit accounts exist, or hide them first (design, Rollback).
- **Backend:**
  - `routers/accounts.py`;
  - `services/ledger_mutations.py`;
  - `services/cycle_summary.py`;
  - `routers/movements.py`, `movement_pairing.py` and `movement_linkage.py` (completion and merge);
  - `services/payment_links.py` (obligation payments);
  - `services/account_colours.py`.
- **Frontend:**
  - account types, dialogs and the Rekening card;
  - `lib/accountOptions.ts` and the pickers;
  - the Beranda net-worth caption;
  - `BalanceCheckDialog`.
- **API:** account payloads gain `credit_limit`, `payment_day` and `credit_owed`. Net worth gains credit lines. All fields are additive.
