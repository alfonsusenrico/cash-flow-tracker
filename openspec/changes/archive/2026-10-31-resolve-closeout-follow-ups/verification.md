# Verification — 2026-10-31

Branch `fix/closeout-follow-ups` from origin/main 02f4468. All checks ran locally on synthetic data. No production host, phone or real AI was used.

## Baseline (02f4468, clean `git archive` export, separate disposable database)

- Backend on PostgreSQL 16 migrated V1→V38: 5 full runs each gave 1143 passed and 1 failed. The failure was `test_observed_insights.py::test_comparison_endpoint_returns_its_windows`, which depends on the calendar day (design D9). `test_jago_transfer_to_bca_pairs_with_the_bca_credit` passed in all 5 runs.
- Flaky test alone: 50 runs (100 parametrised executions), 50 passed and 0 failed.
- CI-style (`cd backend && python -m pytest tests/`, no database): 627 passed, 517 skipped.
- Frontend `vitest run`: 48 files, 249 tests passed.

## C7 reproduction

- Widened confirmation gap (a test plugin sleeps 1.5 s after confirm-source-pocket). Before the fix, 2 of 2 failed with `TypeError: 'NoneType' object is not subscriptable` at `claimed["id"]`: the lifespan worker had claimed the event. After the fix, 2 of 2 passed.
- No `queued`, `processing` or `failed` notification events were left after repeated full runs (H2 rejected).

## Final evidence (working tree of the four implementation commits)

- **Flyway:** a fresh `postgres:16`, `flyway/flyway:10 migrate` reported "Successfully applied 38 migrations … now at version v38".
- **Backend on that database** (`SKIP_TEST_MIGRATIONS=1`): 1169 passed, 0 failed, 0 skipped.
- **Five more full runs** on the same database: 1169 passed each time.
- **Flaky test alone after the fix:** 50 runs (100 parametrised executions), 50 passed and 0 failed.
- **CI-style without a database:** 639 passed and 530 skipped (`cd backend && python -m pytest tests/`). The skips are the database cases.
- **New backend tests:** 25. That is 23 in `test_entity_name_resolution.py` (10 unit cases, 13 database cases) and 2 in `test_notification_lifecycle.py`.
- **Frontend:**
  - `npm ci`: exit 0.
  - `npm test`: 48 files and 252 tests passed (+3: two accounts-page flag tests and one greeting-name test).
  - `npm run lint`: exit 0, with the one existing warning (`review/page.tsx:111` `_done`).
  - `npm run type-check`: exit 0.
  - `npm run build`: exit 0.
- **Browser** (headless Brave over CDP, uvicorn and `next dev` on loopback, a synthetic owner):
  - Setup: `BACKGROUND_JOBS_ENABLED=false` and AI disabled. The flag was set directly on one pocket and one account.
  - At 390 and 1280 px, in light and dark: the pocket button "Perlu rekonsiliasi: sesuaikan saldo Kantong Utama", the parent label "1 kantong perlu rekonsiliasi" and the account button on "Kas Contoh" were all visible at 32 px high. The unflagged "Dompet Contoh" had no flag, and there was no horizontal overflow.
  - axe-core in the real browser (WCAG 2 A/AA, 2.1 AA, 2.2 AA, including colour contrast) on both flagged cards in all four combinations: 0 violations.
  - Keyboard at 390 px: 20 Tab presses reached the pocket flag, with `:focus-visible` and a 2 px outline. Enter opened "Sesuaikan Saldo: Kantong Utama" with focus inside. Escape closed it and returned focus to the flag.
  - Console errors: 0. Screenshots are in the scratch area, not tracked.
- **OpenSpec:** before the archive, `openspec validate --all` exited 0 (63 passed). `--strict` failed only on the known `transaction-provenance` long-requirement warning. The results after the archive are recorded in the archive commit's check (see "Archive" below).
- **`git diff --check`:** clean for every commit (`git diff --cached --check`).

## Archive

- `openspec archive resolve-closeout-follow-ups` produced + 1, ~ 3, - 1. It added the account-reconciliation flag requirement; modified idempotent-public-transactions, tactile-pulse-ui and transaction-ledger-management; and removed one notification-filtering requirement. No new capability was created, so no Purpose placeholder exists.
- After the archive, `openspec validate --all` exited 0 (62 passed). `--strict` reported 61 passed and 1 failed: only the known `transaction-provenance` long-requirement warning.
- Main-spec names went from 358 requirements and 929 scenarios before to 358 and 937 after. The only removals are the intended C2 requirement "Discard Non-Financial Notifications at Listener Entrance" and its two scenarios. The additions are the new flag requirement with its 4 scenarios and 6 new name-resolution scenarios.
