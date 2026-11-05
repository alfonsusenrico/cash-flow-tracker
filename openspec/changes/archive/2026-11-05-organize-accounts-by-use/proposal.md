## Why

A parent account in the app stands for three different things at once. The research note `research/2026-11-05-accounts-institutions-apps.md` (prototypes repository) lists them:
- where the money is held;
- which app reports it;
- what the money is for.

When these disagree, the figures mislead. These are the owner's real cases, read read-only in production on 2026-11-05:
- **RDN BCA** is cash that can only buy securities through the broker. It counts in "Saldo tersedia" and in Ketahanan Dana's fallback.
- **The BCA card** mixes cash (ATM) with gold and mutual funds in one total.
- **GoPay Tabungan by Jago** is a Bank Jago pocket, but it stands as a separate wallet. A move from it to Makan-Jalan is a pocket move inside Jago.
- **Savings pockets** count as "Ditabung" only when a goal is linked to them.
- **The Rekening page** is a list of cards per parent. It cannot show the money by use, or the composition of each rekening.

## Expected Outcome

- **Kegunaan:** every kantong and produk has a use, one of **Uang harian**, **Tabungan**, **Dana investasi**, **Investasi** or **Utang**. The owner sets it in the account dialog.
- **Saldo tersedia** counts Uang harian and Tabungan. **Dana investasi** counts in Kekayaan bersih as its own line, but not in Saldo tersedia or Ketahanan Dana.
- **Saving moves:**
  - a move into a Tabungan pocket is **Ditabung**;
  - a move out of one is **Diambil dari tabungan**;
  - a purchase paid from one stays Pengeluaran and is **Pengeluaran dari tabungan**.

  Beranda states the savings change in words: **"Tabungan bertambah Rp X"**, or **"Tabungan berkurang Rp X"** when more left savings than came in.
- **Rekening & Kantong is redesigned** from the accepted preview `accounts-roles/wallet.html`:
  - "Kartu" (default) and "Grafik" tabs;
  - "Per kegunaan" and "Per rekening" grouping;
  - a summary with Kekayaan bersih, Saldo tersedia, Investasi, untung/rugi and Utang;
  - a small ring per use, or composition bars per rekening;
  - one row of equal cards per use or per rekening;
  - Investasi broken down into Emas, Reksa dana and Saham.
- **Capital gain** is shown wherever it exists: on each product, each investment type, each rekening and in total, in rupiah and percent.
- **Order:** the owner drags rows and cards. The order is saved to the account, separately for each grouping, and is the same on every device.
- **Restructuring:** the owner moves "Gopay" under Bank Jago as the pocket "GoPay Tabungan", and "RDN BCA" under BCA, with the existing "Pindahkan ke rekening" action. Notifications keep recording correctly after the move.

## Approval

The direction comes from the owner's answers on 2026-11-05.
- **Structure:**
  - GoPay becomes a Jago pocket only;
  - gold sits under BCA;
  - RDN sits under BCA, the bank that holds it.
- **Savings:**
  - a move into a Tabungan pocket counts as Ditabung;
  - both Uang harian and Tabungan count in Saldo tersedia.
- **Scope:** two changes. This change covers roles, structure and the page. The per-account app list ("Dilaporkan oleh") comes later.
- **Rekening page:**
  - concept 2 (Dompet) is the default;
  - per-rekening composition is added;
  - "Lihat sebagai tabel" is removed;
  - the ring is kept both as a small ring and as a tab ("keep both");
  - capital gain is shown everywhere;
  - drag ordering is kept.

**Labels, 2026-11-05.** The owner asked for terms that are "easier to understand, easier to notice, not technical, firm". Applied: Kegunaan, Dana investasi ("belum dibelikan"), Saldo tersedia, Untung/Rugi, Modal belum diisi, Tabungan bertambah/berkurang, Masuk ke tabungan, Diambil dari tabungan, Pengeluaran dari tabungan, Kartu / Grafik, Urutan gagal disimpan, and Pindahkan ke rekening (which renames the existing "Jadikan kantong di bawah").

**Plan approval, 2026-11-05:** the owner answered "approved, defaults ok". OD-A, OD-B and OD-C take their defaults (design.md).

## What Changes

- **Database (V51):** additive.
  - `accounts.role`, with a CHECK and a backfill: credit → utang, investment or instrument → investasi, goal-linked → tabungan, everything else → harian.
  - A table `account_layouts`, holding the owner's section and card order per grouping.
- **Figures:**
  - `summarize_account_balances` gains a `dana_investasi` bucket, which is taken out of `liquid_balance`.
  - Net worth gains a "Dana investasi" line.
  - Ketahanan Dana's fallback and Saldo tersedia exclude Dana investasi.
- **Savings:**
  - The net fresh saving classification treats role `tabungan` as a savings destination, alongside goal links and investment products.
  - It is classified at read time from the accounts' current roles (OD-B).
  - Expenses paid from a Tabungan pocket reduce the savings change (OD-A).
  - Keluar stays Pengeluaran + the savings change, and the savings change may be negative.
- **API:**
  - Account rows carry `role`, `kind` (emas, reksa dana, saham, other), and gain totals per rekening and per kind.
  - New `GET` and `PUT /accounts/layout?grouping=use|rekening`.
  - Account create and edit accept `role`, refused where it contradicts the type: credit is always utang, and an investment product is always investasi.
- **Web:**
  - a new Rekening page (Kartu and Grafik tabs, grouping toggle, rails and cards, a mini ring and composition bars, drag with the layout API);
  - a role field in the account dialogs;
  - the savings change explanation on Beranda;
  - neutral standard Indonesian copy.
- **Notifications:** no rule change. Regression tests cover the owner's real flows with GoPay Tabungan as a Jago pocket and RDN under BCA.

## Non-goals

- **The app list per account ("Dilaporkan oleh").** It belongs to the second change.
- **Automatic restructuring of the owner's data.** The owner moves GoPay and the RDN and sets the roles in the app, after release.
- **Touch drag on phones.** Phones keep the existing "Ubah urutan" mode with move buttons.
- **No change to notification recognition, credit accounts or obligations.**
- **No production action** beyond read-only checks after release.

## Capabilities

### New Capabilities

- `account-roles`: the "Kegunaan" role, its effect on Saldo tersedia, Dana investasi and savings, and its validation.
- `accounts-wallet-view`: the Rekening page's Kartu and Grafik tabs, the two groupings, the summary, rails and cards, capital gain display and the saved layout.

### Modified Capabilities

- `kakeibo-reconciliation`: "Net Fresh Savings Calculation" adds role `tabungan`, expenses paid from savings, and a negative net.
- `net-worth-analytics`: "Net Worth & Liquidity Aggregation" adds the `dana_investasi` bucket.

No project capability is added. Money, database and multi-user standards apply.

## Impact

- **Database:** V51, additive and idempotent. Rollback: an older release ignores `role` and the layout table, and its figures return to the previous rules.
- **Backend:**
  - `routers/accounts.py` (models, rows, summary buckets, layout API);
  - `services/cycle_summary.py`;
  - `routers/dashboard.py` (saving bucket, Ketahanan Dana);
  - `services/ledger_mutations.py` (`movement_kakeibo`);
  - `services/money_flow.py` (saving nodes);
  - `routers/reports.py`, `services/monthly_report.py` and `monthly_comparison.py`, which follow the saving figure.
- **Frontend:**
  - `components/accounts/*` (the page is rebuilt);
  - `AccountDialogs` (role field);
  - Beranda explanations;
  - tests in `app/accounts/page.test.tsx`, Beranda and analysis.
- **Figures that change for the owner** once roles are set:
  - Saldo tersedia falls by the RDN balance;
  - net worth shows a Dana investasi line;
  - Ditabung includes moves into Tabungan pockets, in past cycles too under OD-B.
