# Verification

All data is synthetic. No production host, production database or real financial record was used.

## Revisions

- Backend and web: `ec135c2` on `feat/financial-reporting-consistency` (base `b255833`). The working tree matched this commit when the checks below ran.
- Android companion: `2ea4b5e` on `feat/ledger-home-summary` (base `7468256`), version 2.2.0, versionCode 21.

## Baseline before this delivery (Codex working tree on `b255833`, 2026-10-30)

| Check | Result |
| --- | --- |
| Backend, fresh PostgreSQL 16 migrated V1→V36 | 1 failed, 1091 passed (`projected_30d_outflow` returned `null`) |
| Frontend `npm test` | 228 passed |
| Frontend lint | 0 errors, 1 existing warning (`src/app/review/page.tsx` `_done`) |
| Frontend type-check | passed |
| Backend without PostgreSQL (CI equivalent) | not run at baseline; on the first run during this delivery 4 mock tests failed because the salary-cycle lookup bypassed their mocks |

## Final checks on `ec135c2`

| Check | Result |
| --- | --- |
| Flyway migrate of a newly created database | V1→V37 applied, 37 migrations |
| Backend `pytest backend/tests` on that database | 1111 passed, 0 failed, 0 skipped |
| Backend without PostgreSQL (database cases skip, as in CI) | 621 passed, 490 skipped, 0 failed |
| Frontend `npm test` | 48 files, 239 passed |
| Frontend `npm run lint` | 0 errors, 1 existing warning (unchanged) |
| Frontend `npm run type-check` | passed |
| Frontend `npm run build` | compiled successfully |
| `git diff b255833 --check` | clean |
| `openspec validate audit-financial-calculations-and-automation --strict` | valid |
| `openspec validate --all --strict` | 62 passed, 0 failed |
| MODIFIED deltas against main specs (script: every scenario name kept) | no dropped scenario |
| Android `testDebugUnitTest --rerun-tasks` on `2ea4b5e` | 306 tests, 0 failed, 0 errors, 0 skipped (reports 2026-10-30T08:17Z) |
| Android `clean assembleDebug --no-build-cache` + dexdump | versionCode 21, versionName 2.2.0, no `2.1.0` string in dex (run by the delegated Android check) |

Two database tests failed once each in earlier full runs and passed on immediate re-runs and in the final run (`test_settled_external_movement_marks_negative_source_for_reconciliation`, `test_broker_explicit_retry_uses_deterministic_worker_without_ai`). Neither touches the changed code; they are recorded as existing order or timing sensitivity.

## Synthetic end-to-end scenarios

| Scenario | Evidence |
| --- | --- |
| Duplicate capture versus a plan | `test_expectations.py`: same-amount captures are listed, a manual record is refused with `possible_duplicate` until they are ruled out, replay is idempotent |
| One payment, one debt effect | links repeated, concurrent (two clients) and from both the plan and the debt; a payment cannot meet two occurrences or two debts |
| Edits and deletion | deleting a linked payment restores and reactivates the debt; the occurrence can be relinked; salary receipt edits and deletions move or remove the cycle boundary (`test_salary_receipts.py`, `test_salary_cycle_consumers.py`) |
| Salary gaps and timezone | receipt at 00:30 local: spending 45 minutes earlier stays in the previous cycle across pulse, overview, analytics and ledger; no receipt or an unavailable step falls back to a labelled calendar month; the old payday setting is answered with `Deprecation: true` and creates no boundary |
| Kakeibo reconciliation | `test_kakeibo_reconciliation.py`: mixed-pillar category reimbursement split 1:3, fee row, operational transfer, goal deposit and withdrawal, stock purchase; expense pillars equal net reportable outflow and fresh savings count only the net savings movement |
| Migration and retry | `test_v37_converts_rules_keeps_history_and_can_be_retried`: interrupted run leaves no change, repeated run is unchanged, history and ledger rows identical, the check constraint refuses `auto_post = true` |
| Rollback | The previous release's backend (`b255833`, extracted to a scratch folder) was run against a V37 database with a formerly automatic rule due yesterday: its scheduler processed 0 rules, creating or updating a rule with `auto_post = true` failed with `CheckViolation chk_recurring_rules_non_posting` (HTTP 500), and no transaction was created |
| Emergency coverage unchanged | existing runway and coverage tests pass unchanged; no emergency calculation was edited |

## Browser checks (headless Brave over CDP, local API and `next dev` on loopback)

Seeded through the API with one synthetic user. Viewports 390 and 1280 px, light and dark.

- Home: due-plan panel, observed cash-flow card (inflow, outflow, net, budget remaining, plans for 30 days) and five Kakeibo classifications; no "Sisa Aman", "aman terkendali", "Alokasi Gaji", "/ hari" or "Proyeksi" text.
- Analisis: five classifications with percentages, saving components, surplus note and the collapsed optional 50/30/20 reference; no forecast or average tiles.
- Target: "Kaitkan pembayaran" opens the link-first dialog with the ambiguous-match note; no payoff-time estimate.
- Settings: salary section with source, review and confirmed lists. During the run the background detector auto-confirmed a second synthetic income that matched the source and said "Gaji", as the owner rule specifies.
- No horizontal page overflow at any width; console errors: 0.
- Keyboard: Tab order through the due-plan panel is Kelola rencana → each same-amount "Kaitkan" → "Transaksi lain…" disclosure → Lewati kali ini → manual record; the focused control matches `:focus-visible`.
- The first run found the candidate rows clipped at 390 px (a `fieldset` minimum width); fixed by listing same-amount matches separately and collapsing other transactions, then re-run with the results above.

## Not verified

- Android instrumented `LedgerSummaryDeviceTest` on the Realme: the run reached the device but failed with "No compose hierarchies found" because the phone was asleep and locked; the agent did not wake it. To finish, unlock the Realme, keep the screen on and run `ANDROID_SERIAL=3646fbc7 ./gradlew clean -PcompanionVerification connectedDebugAndroidTest --no-build-cache -Pandroid.testInstrumentationRunnerArguments.class=com.alfonsusenrico.financialtracker.listener.LedgerSummaryDeviceTest`.
- `LedgerSummaryBackendTest` (device against a local backend) and the same-figures comparison on a device: not run.
- Android `assembleRelease`: not run.
- Production deployment, production data and the TECNO phone: out of scope and untouched.

## Remaining risks

- Plans with the legacy `payday` schedule still date their next occurrence from the deprecated `payday_day` (labelled as an estimate).
- Candidate matching is deliberately simple (exact amount within a window); a changed bill amount appears under "Transaksi lain" and must be linked by hand.
- The background salary detector confirms any matching income that says `gaji` or `salary` without a bonus or refund marker; a mislabelled bonus would need the owner to revoke it.
- Unused web components remain until the owner approves deleting them (see design.md, Open decisions for owner).
