# Verification: open-beranda-figures

Date: 2026-11-13 · Branch `feat/beranda-figure-details` · Base `origin/main` `61f2602` · Code under test `3655b76`.
Defaults accepted by the owner on 2026-11-13 ("ok do it").

## What changed

- Titles (`49ed95c`): the owner's note, then the merchant, then a generic notification note, then the category. The
  merchant leads the meta when the note is the title and does not name it. `GENERIC_ROW_NOTES` is checked against both
  backend catalogues by a test. The edit dialog's subtitle follows the same order.
- Figures (`b9aeb03`): Masuk, Pengeluaran and Ditabung & investasi link to Transaksi with Beranda's period and type,
  Terjadwal to `/goals#tg-bills`; "Tagihan kredit" (new glossary term `terms.creditBills`) opens the credit bills dialog.
- Dialog fixes after the first captures (`0507224`): opening debt listed from its bill item, in date order with the
  rows; an account without a due day says so; the credit figure takes a full row wherever the legend has two columns.

## Terjadwal dropped as a figure (owner, 2026-11-13)

After an audit of the figures against production (read-only: every Beranda figure and the Kakeibo pillars matched the
owner's transactions to the rupiah, movements included), the owner chose to drop only Terjadwal as a supporting figure.
It stays in the split bar and the Rincian. The legend is back to four columns; "Tagihan kredit" takes the slot, and its
caption is "jatuh tempo {date}" so it fits one line in a four-column cell (the first build's "jatuh tempo 13 Nov · Rp
434.000" wrapped at 1440).

## Automated checks

| Check | Result |
| --- | --- |
| Frontend tests (`vitest run`) | 1105 passed (an earlier run had a load timeout in `BudgetDialog.test.tsx`, which passed alone and in every later full run) |
| Frontend lint, type check, build | passed |
| `openspec validate --all --strict` | 85 passed |
| Backend | not run: no backend file changed |

New tests: the title rule (note and merchant, a note naming the merchant, merchant only, generic notes, catalogue
parity); figure links in a cycle and a month; the credit figure's sum and nearest due, its absence without a credit
account, nothing owed and hidden balances; the dialog's order, header facts, rows from the oldest unpaid item oldest
first, opening debt in date order, a missing due day, one section failing, axe and focus return.

## Browser checks (task 3.3)

Production build (`next build`, `next start` behind a loopback proxy) over the synthetic Markas seed (`seed_parity.py`
at V51, migrated to V54, then `seed_catat.py`: SPayLater, Kartu Kredit Contoh and Kredivo Contoh as credit accounts).
24 captures: Beranda, the credit dialog, Transaksi · Masuk and Transaksi · Tabungan & investasi, at 1440, 1024 and 390
in light and dark. 0 console errors, 0 serious or critical axe violations, 0 horizontal overflow.

- Layout: Masuk, Pengeluaran, Ditabung & investasi and Tagihan kredit in one row at 1440 and 1024, each caption on one
  line; at 390 the phone shows Pengeluaran and Ditabung side by side and "Tagihan kredit" on its own row.
- Figure-to-Transaksi parity through the API for the cycle: Masuk 9.518.400 = `type=income` inflow; Pengeluaran
  3.472.990 = `type=expense` spending; Ditabung & investasi 2.250.000 = `type=saving` saving_net.
- The scenarios lived in a scratch copy of `scripts/verify/markas/`; they are not committed.

Owner's answer: pending.

## Not verified

- Real production data: the dialog was checked on synthetic data only.
- Transaksi's own hero is unchanged: opened from Masuk it still leads with "Pengeluaran Rp 0" and shows Masuk beside it.
