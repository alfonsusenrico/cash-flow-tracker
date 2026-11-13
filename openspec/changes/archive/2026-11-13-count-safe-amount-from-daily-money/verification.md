# Verification: count-safe-amount-from-daily-money

Date: 2026-11-13 · Branch `feat/cash-based-safe-amount` · Base `origin/main` `07d1db7`. Plan approved by the owner on
2026-11-13 ("ok").

## Why, from production (read-only)

On 2026-11-13 the salary-view hero read Rp 674.415 while the owner's daily pockets held about Rp 4,4 million. A
read-only check of production explained the gap to the rupiah: money carried over from before the salary (Rp 398.836),
reimbursements for last cycle's spending that cannot reduce this cycle (Rp 300.000), a credit purchase whose cash was
not yet set aside (Rp 47.619) and balance adjustments (Rp 1.069). The same day's figures give the cash-based amount:
Rp 4.420.334 − Rp 3.046.014 − Rp 0 = Rp 1.374.320.

## Server (tasks 1.1, 1.2)

- `safe_amount` in `build_cycle_summary`; `remaining` and the three server labels it used are removed (both
  catalogues and the frozen golden text file lose those three entries).
- New database tests: the owner's 13 November figures as synthetic data (Rp 1.374.320), savings, investment cash and
  investments outside daily money, a pocket below zero counting as money short, money from before the salary
  counting, plans paid from savings and plans into credit not counted, and a shortfall. The shared summary check now
  requires the safe amount's parts to add up and `amount`/`short_by` to follow the formula.
- Existing tests that compared the salary figure now compare the safe amount. The changes in meaning: a credit purchase
  still lowers it by its amount; paying the bill from daily money leaves it unchanged; an overpayment lowers it by the
  part beyond what was owed; paying credit from savings raises it.
- Full backend suite as CI runs it (Postgres 16, Flyway V1→V54): **2275 passed**.

## Beranda (tasks 2.1, 2.2)

- Hero "Aman dipakai" with the shortfall line; the split bar divides daily money into Tagihan kredit, Terjadwal and Aman
  dipakai; Rincian lists Uang harian, Tagihan kredit and Terjadwal, each opening its pockets, accounts or plans, with
  a plan counted elsewhere saying where. The Masuk, Pengeluaran (with its credit lines), Ditabung and waiting-row
  breakdowns left the Rincian with their messages; the credit-lines requirement is removed in the spec delta.
- The bar's Terjadwal segment is sky (`--tr-solid`), not the warning orange: beside the credit amber the orange measured
  OKLab ΔE 8.0 normal and 1.5 protan in light. Measured with sky: credit|Terjadwal 25.7 / 24.0 / 25.4 (light) and
  34.0 / 34.6 / 28.5 (dark); Terjadwal|Aman dipakai 17.2 / 16.5 / 17.1 and 18.1 / 17.1 / 17.2 (normal / deutan /
  protan, Machado 2009). The dataviz skill's validator script was not on disk, so these were computed directly.
- Frontend: **1103 tests passed**, lint and type check pass, build passes.
- Browser: production build over the synthetic Markas seed (V51 seed, migrated to V54, `seed_catat.py`), Beranda and the
  open Rincian at 1440, 1024 and 390 in light and dark: 12 captures, 0 console errors, 0 serious or critical axe
  violations, 0 horizontal overflow. Hero Rp 5.099.186 = 11.680.520 − 1.443.000 − 5.138.334.
- Keyboard (`beranda-keys.mjs`): **33/33**, including Rincian as the hero's first stop, Enter opening it with focus
  inside, a line opening with Enter and closing with Space, Tab trapped, and Esc returning focus.
- Verification tooling under `scripts/verify/markas/` follows the new names and Rincian lines; `seed_parity.py` no longer
  compares the prototype's salary-view `safeToSpend`; `beranda-keys.mjs` now recognises the native `<dialog>` the
  transaction dialog uses (its row check failed on that before this change).

Owner's answer on 2026-11-13: "ok". Accepted.

## Not verified

- Production data: the new figure was checked on synthetic data; the owner's own figure is expected to read about
  Rp 1.374.320 on 13 November, less anything spent since.
- The screenshot harness did not leave a Rincian line expanded in its captures; expansion is covered by the unit tests
  and the keyboard check.
