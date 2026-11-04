## 0. Base

- [x] 0.1 After `reconcile-linked-account-reports` and `manual-balance-check` merge, rebase this branch on `main`, confirm V47 and V48 exist, and refresh the `currency-conversion-and-settings` delta against the main spec. Keep every scenario name. Evidence: `git log`, `openspec validate --all --strict` exit 0.

Expected Result: the change sits on the merged linked-accounts and balance-check work.

## 1. Data

- [x] 1.1 V49: `notification_test_sessions`, `notification_events.test_session_id` (`ON DELETE CASCADE`) and `test_case`, the D3 check, the partial unique index for one open session per owner, and the lookup index. Evidence: migration test on a fresh database and on a V48 database with existing events, applied twice. The check rejects a tagged event with a transaction.

Expected Result: the schema enforces that tagged events never book.

## 2. Backend

- [x] 2.1 Regression tests first, failing on the code before 2.2–2.4:
  - an active session books nothing, for myBCA plus GoPay, a Jago pocket move and a Stockbit report;
  - a late upload of a session notification stays capture-only;
  - a pre-session notification uploaded during the session books;
  - capture resumes after the end;
  - the worker, the deferred pairing pass, the linked-report rule, retry, resolve and confirmations leave tagged events untouched;
  - owner isolation (404);
  - the Bearer key gets 403.

  Evidence: the test names and their failure output on the old code.
- [x] 2.2 `services/test_sessions.py`: start (closing an expired session), end, the window lookup by `post_time`, list, events, label, CSV export, delete. Evidence: unit tests.
- [x] 2.3 `notification_processing.accept_event`: the membership branch right after the duplicate lookup, before the ignore branch, the source-pocket gate, locks and interpretation, writing the D3 fields. Reason-code logging only. Evidence: 2.1 passes, and a test that no `notification_ai` provider call happens.
- [x] 2.4 `routers/ingest.py`: the D5 endpoints under session auth, with validation and errors. Evidence: API tests for 200, 403, 404, 409 and 422.
- [x] 2.5 Companion contract: the response shape for a session event, checked against `financial-tracker-mobile-listener` (`DirectSync.pollResults`, `RowPresentation.status`, `HomeFigures.SETTLED`, `ActivityFilter.UNSETTLED`). Evidence: a contract test with the companion's fixture shape, and a note in verification.md naming the companion revision read.

Expected Result: sessions capture without booking, and normal capture is unchanged.

## 3. Web

- [x] 3.1 Pengaturan section `sesi-uji`:
  - duration, "Mulai sesi" and "Akhiri sesi";
  - the list of past sessions;
  - the report with case number and note editing, "Unduh CSV" and "Hapus data sesi" with a confirmation.

  Evidence: Vitest for each state and the 409 message.
- [x] 3.2 The banner on every page while a session is open, with "Akhiri sesi". Evidence: Vitest, plus the AppLayout test.
- [x] 3.3 Browser check:
  - Pengaturan Sesi uji (idle, running, report) and the banner;
  - widths and themes: 1440 light, 1440 dark and 390 dark;
  - axe serious/critical 0, overflow 0, console errors 0, targets of at least 24 px, keyboard reachable;
  - screenshots read by the agent, using synthetic data.

  Evidence: verification.md.

Expected Result: the owner can run and label a session alone.

## 4. Replay tooling

- [x] 4.1 `scripts/verify/export_test_session.py`: read-only recipe only, and refuses an output path inside the repository. Evidence: a test that it refuses a repository path, and a run against a local database.
- [x] 4.2 `scripts/verify/replay_test_session.py`: loopback-only guard, structure load, events through `accept_event` in `post_time` order, a deterministic worker run, plus `--drop-app` and `--ai`. Evidence: a test that it refuses a non-loopback URL, and a dry run on a synthetic export modelled on the 1 Nov sequence, on `main` and on this branch.
- [x] 4.3 `scripts/verify/score_test_session.py`: per-case pass/fail against an expected file. Evidence: the synthetic dry run scored.

Expected Result: a session can be replayed and scored outside production.

## 5. Verification

- [x] 5.1 Checks:
  - backend pytest, full;
  - frontend `npm ci`, type-check, lint, tests and build;
  - unchanged writes against `main` outside a session;
  - `openspec validate --all --strict` and `git diff --check`.

  Evidence: verification.md.

## 6. Acceptance and session

- [ ] 6.1 Present to the owner: the verification, OD150–OD155, and the session-day steps. The owner accepts. Record their words and the date in proposal.md.
- [ ] 6.2 After acceptance: archive the change on its branch with `openspec archive notification-test-session -y`, confirm `openspec validate --all --strict` exits 0, push the branch, and report the PR link. The owner merges, and the agent checks the deploy read-only.
- [ ] 6.3 After the owner runs the session:
  - export, replay on `main` and on the merged linked-accounts code, with the drop-app variants from the case script;
  - score;
  - report per case;
  - delete the local copy.

  Evidence: the report in the research note, with no personal data.
