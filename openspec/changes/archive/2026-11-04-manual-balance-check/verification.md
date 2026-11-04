# Verification: manual-balance-check

**Revisions:** the plan is `8fdacb3`, the backend `156e187` and the web `ca03131`. Every check below ran on the tree of `ca03131`. The commit holding this record adds only this file and tasks.md.

**Environment:** local only.
- Postgres 16 in `cft-balance-pg` on 55622, migrated by Flyway 10 to V48 (V47 does not exist on this branch).
- For the browser: the synthetic seed of the earlier routes change restored into `ledger_browser` and migrated to V48, the API on 8322, `next start` on 3323 behind the loopback proxy on 3322, and headless Brave on 9622.
- Synthetic owner `raka_contoh`.
- No production access was used.

## Automated checks

| Check | Result |
| --- | --- |
| New backend tests, `test_balance_check.py` | 10 passed. Without the routes, 9 fail (404/405). The other-owner 404 passes either way, because a missing route also answers 404. |
| Backend full suite | 1808 passed, 0 failed (1798 on `main` plus 10) |
| Frontend `npm run lint` and `type-check` | exit 0 |
| Vitest | 75 files, 679 passed, 0 unhandled errors. The 6 new tests are in `BalanceCheckDialog.test.tsx`. |
| `npm run build` | compiled successfully |
| `openspec validate --all --strict` | exit 0 |
| `git diff --check` | clean |

**What the backend tests cover:**
- an exact match writes nothing;
- a Jago income mirrored by a GoPay income 2 s away explains −Rp 500.000 as a double booking, with its twin;
- an unpaired leg explains +Rp 75.000;
- a held second observation explains +Rp 120.000 and links to Tinjau "dua";
- an unexplained difference has no explanation, and a queued report with no account is listed apart;
- "Saldo cocok" refuses a mismatch with `balance_mismatch` and the difference, and the next window starts at the match;
- the reconcile request and response are unchanged, and an adjustment starts the window;
- a parent is checked as its group, Rp 960.000, with no adjustment offered;
- products answer `balance_check_not_liquid`;
- unknown and other-owner accounts answer 404;
- a negative balance answers 422.

**Unchanged writes:**
- The backend diff against `main` only adds lines, with 0 removed in `accounts.py` and no existing write path touched.
- `test_reconcile_is_unchanged_and_also_starts_the_window` asserts the reconcile response (`ok`, `diff`, `message`).
- The frontend's existing reconcile callers pass one argument as before. `openReconciliation`'s second argument is optional.

## Browser check

Account: Jago · Main Pocket, recorded Rp 7.088.299.
- **First check, Rp 6.968.299 typed:** shows −Rp 120.000, explained by the seed's unpaired incoming leg "Pindah uang dari BCA". Its row link is `/ledger?edit=…`.
- **Second check, Rp 7.088.299 typed:** shows "Cocok" and offers "Saldo cocok".

| Cell | Input starts empty | Overflow | axe serious/critical (explained / match) | Console errors | Targets under 24 px | Tab escapes in 25 presses | After Esc |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1440 light | yes | 0 | 0 / 0 | 0 | 0 | 0 | focus on "Cocokkan saldo", the detail still open |
| 1440 dark | yes | 0 | 0 / 0 | 0 | 0 | 0 | same |
| 390 dark | yes | 0 | 0 / 0 | 0 | 0 | 0 | same |

**Screenshots:** in the session scratchpad, `balance-web/shots/`: `check-explained-*`, `check-match-*` and `report.json`. All were read. The dialog is a centered 672 px modal on desktop and a sheet at 390 px.

## Not verified

- A screen reader.
- A real phone.
- A real browser click on "Saldo cocok" and "Sesuaikan saldo". Both are covered by Vitest with the API mocked, and by the backend tests.

## Integration with `reconcile-linked-account-reports`, before merge

After acceptance, `feat/linked-account-reports` was merged into this branch, because it must deploy first (V47 before V48).
- **The fix:** that change marks a skipped second report with `reporting_link_id`. Skip patterns are retired. The check now treats `reporting_link_id` as "skipped", as it does the older `skip_pattern_id`. Without this, a report skipped by a linked account would have been left out of the causes.
- **Test:** `test_a_report_skipped_by_a_linked_account_counts_as_skipped` fails without the fix and passes with it.
- **Checks on the merged tree (V48 on a fresh database):** backend 1810 passed. Frontend type-check and lint exit 0, Vitest 75 files with 675 passed, and the build compiled.
