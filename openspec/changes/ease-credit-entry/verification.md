# Verification: ease-credit-entry

Date: 2026-11-13 · Branch `fix/credit-capture-entry` · Base `origin/main` `0d37f69`. Plan approved by the owner on
2026-11-13 ("add both for the new release").

## Reproduction (task 1.1)

On `0d37f69`, `parse_time` with a device time of 13 Nov 2026 09:00 WIB: "tgl 11 kemarin, beli pedigree spaylater kredit
47619", "tanggal 11 kemarin beli", "11 november kemarin beli" and "tgl 20 kemarin beli" raised `CaptureDateError`
`several_dates`, which the app shows as "Tanggal tidak valid. Pilih hari lain.". "tgl 11, beli pedigree 47619" and
"tanggal 11 yang lalu beli" resolved to 11 Nov.

## A past word after a date (task 1.2)

Commit `f6e2592`. Six of the new parser cases failed with the change stashed and pass with it. Capture tests
(`test_capture_time.py`, `test_capture_parsing.py`, `test_capture_time_flow.py`, `test_capture_preview.py`): 172 passed,
32 skipped without a database, and all run in the full suite below. The owner's sentence reads as 11 Nov, Rp 47.619,
expense, with Spaylater as the only account mentioned.

## Paying later in Catat (tasks 2.1, 2.2)

- `AccountSelectOptions` `creditOnly`: new component test, axe clean.
- `ManualView` "Kapan bayar": tests for the default (Bayar sekarang, cash accounts only, axe clean), arrow-key switching
  to Bayar nanti (picks the first credit account and shows "Bayar pakai kredit"), back to Bayar sekarang (first cash
  account, no credit option, no `credit_terms`), no choice for income and transfer, and no choice without a credit
  account. The existing credit tests now reach credit through the choice.
- The first label, "Cara bayar", clashed with the instalment block's own "Cara bayar" in the browser captures, and the
  English "How you pay" failed the copy register. The choice is now "Kapan bayar" / "When to pay".

## Owner's check (task 2.3)

Captures of a production build (`next build`, `next start` behind a loopback proxy) over the synthetic Markas seed
(`seed_parity.py` at V51, migrated to V54, then `seed_catat.py`): Catat manual on Bayar sekarang and on Bayar nanti at
1440 and 390, light and dark. 8 captures: 0 console errors, 0 serious or critical axe violations, 0 horizontal
overflow. The two scenarios lived in a scratch copy of `scripts/verify/markas/`; they are not committed.

Owner's answer: pending.

## Checks (task 3.1)

| Check | Result |
| --- | --- |
| Full backend suite as CI runs it (Postgres 16, Flyway V1→V54) | 2270 passed |
| Frontend tests (`vitest run`) | 1095 passed |
| Frontend lint, type check, build | passed |
| `openspec validate --all --strict` | 85 passed |

Data: synthetic only.

## Not verified

- The edit dialog keeps its "Kredit" group and has no "Kapan bayar" (out of scope).
- With several credit accounts, Bayar nanti picks the first in the owner's account order, not the last one used.
