## Why

On 2026-11-13 the owner asked for three things after reviewing Beranda:

1. Beranda's supporting figures (Masuk, Pengeluaran, Ditabung & investasi) should open the transactions behind them, already filtered. After an audit of the figures against production on 2026-11-13, the owner dropped Terjadwal as a supporting figure ("do A"); it stays in the split bar and the Rincian.
2. A new Beranda figure for ongoing credit bills. Credit accounts can be more than two, so selecting it should show a breakdown per account, each with its transactions, including the payments made.
3. Transaction titles prefer the merchant over the note, so a row with the note "Top up saldo fal.ai untuk development" reads only "fal.ai". The note should be the title.

The owner accepted the proposed defaults on 2026-11-13 ("use the defaults" → "ok do it").

## Expected Outcome

- Each supporting figure is a link: Masuk, Pengeluaran and Ditabung & investasi open Transaksi with the same period and type. Terjadwal is no longer a supporting figure; it stays in the split bar and the Rincian, where its bills are listed.
- An owner with a credit account sees a fourth figure, "Tagihan kredit": the total owed on all credit accounts, with the nearest due date. Selecting it opens a centered dialog with one section per credit account, most owed first: owed, limit left and next bill, then the account's transactions from its oldest unpaid purchase to today, oldest first, purchases showing unpaid or paid and payments listed, with "Lihat semua di Transaksi".
- A transaction's title is its note, then its merchant, then its category. When the note is the title, the merchant joins the meta line unless the note already names it.

## What Changes

- `FigureBlock` gains an optional link; `BerandaHero` passes one per figure and adds the credit figure.
- New `CreditBillsDialog` reading `GET /accounts`, `GET /credit/accounts/{id}` and `GET /transactions?account_id=&from_date=`.
- `rowTitle` and `rowMeta` follow the title rule; the edit dialog's subtitle follows it too.
- `en` and `id` messages.

## Non-goals

- No backend change, no new endpoint, no migration.
- The credit figure is information only: credit purchases already count in Pengeluaran when made, so it is not subtracted from Aman dipakai and takes no part of the split bar.
- No change to what the figures count, nor to the ledger's own filters.
- Merchant grouping in Tinjau and merchant memory are unchanged.

## Capabilities

- Modified: `beranda-cycle-summary` (figures are links; the credit figure), `credit-accounts` (the credit bills dialog), `transaction-ledger-management` (the title rule).
- No new project capability.

## Impact

- `frontend/src/components/workspace/figures.tsx`, `frontend/src/components/beranda/*`, new `frontend/src/components/beranda/CreditBillsDialog.tsx`, `frontend/src/components/transactions/rows.tsx`, `frontend/src/components/transactions/TransactionEditDialog.tsx`, their tests and the message catalogues.
