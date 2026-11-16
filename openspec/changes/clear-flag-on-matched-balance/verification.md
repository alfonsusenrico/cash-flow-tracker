# Verification: clear-flag-on-matched-balance

Tested on 2026-11-16, branch `fix/matched-balance-clears-flag` on base `9e5be43`.

## Diagnosis (production, read-only)

- Tabungan has two matched points at Rp 82.515 (08:16:39 and 08:16:51 WIB) while it was still flagged.
- The flag was cleared at 08:17:33 by "Sesuaikan saldo" with no difference; no adjustment row was written.

## Checks

| Check | Result |
| --- | --- |
| `test_saldo_cocok_clears_the_pockets_warning` on base | failed (flag still set), as reported |
| `tests/test_balance_check.py` after the fix | 14 passed |
| Backend, full suite (Postgres 16, Flyway V1→V56) | 2356 passed |
| `openspec validate --all --strict` | 86 passed, 0 failed (86 items) |

## Scenario evidence

| Scenario | Test |
| --- | --- |
| Matching a flagged pocket clears its warning | `test_saldo_cocok_clears_the_pockets_warning`; the API also shows the flag cleared |
| A refused match keeps the warning | `test_a_refused_match_keeps_the_warning` |
| A matching total keeps a pocket's warning | `test_a_matching_total_keeps_a_flagged_pockets_warning` |
| Next check starts at the match (unchanged) | `test_saldo_cocok_starts_the_next_window` |

The web dialog already invalidates the accounts query after a match (`BalanceCheckDialog`), so no frontend change is needed. That was not checked in a browser.
