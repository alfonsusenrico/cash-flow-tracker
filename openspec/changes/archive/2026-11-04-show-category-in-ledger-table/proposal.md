## Why

On 2026-11-04 the owner reviewed the production Transaksi table and said: "better change the "Dicatat" with category names it's more useful". The "Dicatat" column repeats provenance such as "Pengguna · Notifikasi bank" on every row. The "Kategori" column shows only the pillar ("Kebutuhan"), so the category itself ("Makanan & Minuman") is not visible without opening the row.

## Expected Outcome

The desktop Transaksi table shows each row's category name in place of "Dicatat". The pillar column keeps its dot and text under the heading "Pilar". Provenance stays in the transaction detail.

## Approval

**Owner request, 2026-11-04.** The owner asked for this change directly in the session, and it has no open scope questions. The heading "Pilar" for the existing pillar column is the agent's default (design D2). During the work the owner added two more requests: "only Masuk the amount colored in green, add also for pengeluaran (red), and pindah saldo (blue)" for the day headings, and "The belanja harian charts, make it hoverable and show how much the amount".

**Acceptance, 2026-11-04.** After the summary with its verification, the owner said "accepted, give the PR url".

## What Changes

- The `Dicatat` column of the desktop ledger table becomes `Kategori` and shows the category name.
- The existing pillar column is headed `Pilar`.
- Provenance is shown only in the transaction detail, which already lists "Dicatat" and "Diklasifikasikan".
- The day headings colour Pengeluaran red and Pindah saldo blue, as Masuk is already green.
- Each Belanja harian bar shows its date and amount on hover or tap.
- The phone feed's columns, filters, search and every API stay unchanged.

## Non-goals

- No change to category data, classification, the API, the phone feed or the detail dialog.

## Capabilities

- Modified: `transaction-ledger-management` (new requirements for the table columns and the day heading colours) and `analysis-workspace` (Ringkasan Daily Spending).
- No new project capability.

## Impact

- `frontend/src/components/transactions/LedgerList.tsx`, `frontend/src/components/beranda/activity.tsx` (a `rowCategory` helper), and their tests.
- `frontend/src/app/globals.css`, `frontend/src/components/analysis/RingkasanTab.tsx` and `analysis.module.css`, with their tests.
