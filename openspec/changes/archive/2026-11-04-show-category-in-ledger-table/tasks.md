## 1. Table

- [x] 1.1 Add `rowCategory(row)` beside `rowStatus` in `beranda/activity.tsx`, implementing design D1. Evidence: unit tests for expense, income, uncategorised, movement, top-up and waiting rows.
- [x] 1.2 In `LedgerTable`, head the pillar column "Pilar" and replace the provenance column with "Kategori" using `rowCategory`, truncated with `title`. Remove imports and CSS that become unused. Evidence: the LedgerList and ledger page tests assert the new headings and cells.

Expected Result: the table reads Transaksi, Jam, Jenis, Pilar, Kategori, Jumlah.

## 2. Day headings and chart

- [x] 2.1 Colour the day-heading Pengeluaran and Pindah saldo amounts (design D4). Evidence: the ledger page test asserts the classes.
- [x] 2.2 Show the hovered or tapped day's amount in Belanja harian (design D5). Evidence: a component test for hover, leave, tap and Esc, and a browser screenshot.

Expected Result: the day figures read by colour and label, and every bar shows its amount on demand.

## 3. Verification

- [x] 3.1 Frontend `npm ci`, type-check, lint, tests and build. `openspec validate --all --strict`. `git diff --check`. Evidence: verification.md.
- [x] 3.2 After the owner accepts: archive, push the branch, and report the PR link.
