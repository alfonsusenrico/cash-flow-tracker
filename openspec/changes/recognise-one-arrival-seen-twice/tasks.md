# Tasks

The owner approved this plan on 2026-11-04 (proposal.md, "Approval").

Evidence for every task goes into `verification.md`.

**Data:** synthetic only, with fictional names. The incident's shape is rebuilt as:
- a bank account "BCA Harian";
- a Jago parent "Bank Jago" with the default pocket "Kantong Utama" and a pocket "Tabungan";
- an e-wallet "GoPay".

**Backend tests:** run as `.github/workflows/checks.yml` does, on a disposable PostgreSQL 16 migrated by Flyway with `REQUIRE_TEST_DATABASE=1`.

**Local processes:** every one uses this change's own ports and is stopped by PID or port. Record `ps`, `lsof` and `docker ps` at the end.

**Production:** read-only and only on the owner's request (D7). No production writes.

## 1. Reproduce

- [x] 1.1 Write the regression test first: the incident sequence (D8) in arrival order and in reverse order. Evidence: it fails on `main` for the expected reason (two incomes; the myBCA debit flagged `ambiguous_movement`), with the output recorded.
- [x] 1.2 Add the failing tests for the transfer answer and for transfer memory. Evidence: they fail on `main` (row `reviewed` with no `awaiting_counterpart`; memory written for `signature:jago|income|unknown`).

Expected Result: the failing state is pinned before any fix.

## 2. Deterministic guard

- [x] 2.1 Add `same_arrival` (D1) and call it from `record_observed_leg` and `repair_owner`. Record `account_basis` in the signature. Evidence: 1.1 passes in arrival order, and the guard's negative-case tests pass.
- [x] 2.2 Keep the right leg (D2), including the guarded swap and OD119. Evidence: 1.1 passes in reverse order; an owner-edited earlier leg is never removed (test).
- [x] 2.3 Exclude `duplicate_observation` events from candidate discovery and pairing. Make the compact result reuse the kept leg. Evidence: tests for candidates, the companion result, and a repeated deferred pass that changes nothing.

Expected Result: one money movement books once in either order.

## 3. Answers and memory

- [x] 3.1 Transfer answers pair at once or stay `awaiting_counterpart` (D4), with undo. Evidence: 1.2 passes, and undo tests pass.
- [x] 3.2 No memory from transfer answers or corrections. Retire the leg's `ai` entry on link; owner entries are untouched. Evidence: memory tests for automatic pairing, the owner merge and completion.

Expected Result: a transfer answer can't hide a missing side or teach a wrong habit.

## 4. Time-aware interpretation

- [x] 4.1 Add `neighbour_context` with its setting, budget order and incomplete flag (D5). Evidence: context tests for bounds, order, sanitization (no other event text), owner isolation, the invalid setting, and the incomplete flag on query failure and over budget.
- [x] 4.2 Bump the prompt to `notification-interpretation-11` and add `same_money_as` to the schema and validation (`invalid_neighbour_reference`). Apply it per D5 and OD116. Evidence:
  - provider tests with recorded responses: confirmed, unconfirmed (held, nothing booked), unknown id, null with guard match, model off;
  - `prompt_version` stored on events.
- [x] 4.3 Add the sequence case to `evaluation/notification_cases.py` (`synthetic-notifications-4`): myBCA debit, GoPay credit "Tabungan by Jago", Jago "received from GoPay", and the Jago pocket hop two minutes later, each replayed with its neighbours. Score `same_money_as` precision and recall, and false duplicates as a safety failure. Evidence: `test_notification_cases.py` and `test_notification_benchmark.py` pass offline.
- [ ] 4.4 Live benchmark of `notification-interpretation-11` against `-10` on the synthetic corpus. This runs only with the owner's go-ahead and within the per-run cap. Evidence: the report with prompt and fixture versions, safety gate, `same_money_as` precision and recall, latency and tokens. Or recorded as "not run: awaiting approval".

Expected Result: the model sees time and can only propose. The guard decides.

## 5. Tinjau and migration

- [x] 5.1 Add `V45__duplicate_observation_review.sql` (D6). Evidence: Flyway migrate on a copy of the test database; the full suite passes after it.
- [x] 5.2 Add `seen_twice` to the queue, the acknowledge endpoint, and "Catat terpisah" through manual resolution (D3). Evidence: API tests for the section, counts (OD117), acknowledge, record-separately with undo, and another owner's 404.
- [x] 5.3 Add the Tinjau "Terlihat dua kali" section, its two actions, and labels for `possible_duplicate_observation`. Evidence: Vitest; a browser check at 1440 and 390 px, light and dark, with 0 serious axe findings.

Expected Result: every swallowed or held observation is visible and reversible.

## 6. Detector

- [x] 6.1 Add `scripts/verify/find_duplicate_arrivals.py` (D7). Evidence: a test on the synthetic database that lists the planted cases, prints no notification text, and fails to write (read-only transaction).
- [x] 6.2 Only if the owner asks: run it read-only against production for the owner's account. Report the cases for the owner to handle in the app. Evidence: the summary (counts, dates, amounts, ids), or "not run".

Expected Result: past cases are found without touching production data.

## 7. Verification

- [x] 7.1 Run the full suites as CI does: backend pytest; frontend `npm ci`, type-check, lint, Vitest and build. Evidence: counts with 0 failures at the tested revision.
- [x] 7.2 Check money identities: balances, overview and pulse figures are unchanged on a seed without duplicates, and correct on the incident seed. Evidence: the identity table.
- [x] 7.3 Run `openspec validate --all --strict`, `git diff --check` and the clean-up proof. Evidence: outputs.

Expected Result: the change is proven locally.

## 8. Acceptance

- [ ] 8.1 Present to the owner:
  - verification.md;
  - the Tinjau screenshots;
  - OD116–OD120 with their applied defaults.

  Record the acceptance with the owner's words and the date in proposal.md. Evidence: the quote.

## Workflow follow-up

- After acceptance:
  - archive the change on the branch with `openspec archive recognise-one-arrival-seen-twice -y`;
  - confirm `openspec validate --all --strict` exits 0;
  - push the branch.
- The owner opens and merges the pull request. The release runs only through `deploy.yml`, which applies V45.
- After the deploy, check production read-only (health, Flyway at V45, the queue answers 401 without a session), then delete the branch locally and on the remote.
