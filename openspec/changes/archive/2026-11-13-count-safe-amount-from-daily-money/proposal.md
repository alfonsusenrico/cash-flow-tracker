## Why

Beranda's hero, "Aman dipakai sampai gajian", is the salary view: Masuk − Pengeluaran − Ditabung & investasi − Terjadwal for the current cycle. On 2026-11-13 it showed Rp 674.415 while the owner's Makan-Jalan pocket alone held Rp 715.609 and their daily pockets about Rp 4,4 million. The figure leaves out money carried over from before the salary, money friends returned for last cycle's spending, and credit purchases whose cash is already set aside. Each is correct by the salary rule and wrong for the owner's question.

The owner's question when opening the app is "how much money do I have left that I can safely use, from everything I have", not "how much of this salary is left". On 2026-11-13 they chose the cash-based figure ("use your recommended").

## Expected Outcome

- The hero reads "Aman dipakai": the money in daily-money pockets, minus what is still owed on credit, minus plans and set-asides paid from daily money before the next salary. For the owner on 2026-11-13: Rp 4.420.334 − Rp 3.046.014 − Rp 0 = Rp 1.374.320.
- It never shows less than the owner can actually spend for reasons of period bookkeeping, and it never counts money already committed.
- Rincian shows the three parts, each opening its own lines: every daily pocket, every credit account, every plan.
- Masuk, Pengeluaran, Ditabung & investasi and Tagihan kredit stay as this cycle's facts.

## What Changes

- `cycle_summary`: a `safe_amount` block (daily money by account, credit owed by account, scheduled lines from daily money, the amount and any shortfall) replaces `remaining`; `safe_to_spend.per_day` reads it.
- The scheduled amount for the safe figure leaves out transfer plans into credit accounts (their debt is already subtracted in full) and plans paid from savings.
- Beranda: the hero, its line, the split bar and Rincian follow the new figure; the Tagihan kredit dialog says its total is subtracted from Aman dipakai.
- `en` and `id` messages.

## Non-goals

- No change to Masuk, Pengeluaran, Ditabung & investasi, the Kakeibo pillars, reports or Analisis.
- No new account setting: daily money is the existing role "Uang harian" (harian).
- No change to how credit bills, plans or goals are stored.

## Capabilities

- Modified: `beranda-cycle-summary`.
- Removed from `credit-accounts`: "Credit Lines in the Pengeluaran Breakdown", whose Rincian line no longer exists; credit spending stays in the Tagihan kredit dialog and Transaksi · Kredit.
- No new project capability. No migration.

## Impact

- `backend/app/services/cycle_summary.py`, `backend/app/routers/dashboard.py` (response), their tests.
- `frontend/src/components/beranda/*` (hero, explanations, credit dialog copy, types), `frontend/src/app/page.test.tsx`, message catalogues.
- `GET /dashboard/cycle-summary` drops `remaining` and gains `safe_amount`; Beranda is its only consumer.
