## 1. Regression tests first

- [x] 1.1 Backend tests that fail on `main` (missing table, column or behaviour):
  - the 1 Nov sequence with a link: one BCA → GoPay movement, Kantong Utama unchanged, no Tinjau item;
  - without a link, "Terlihat dua kali" asks;
  - a real Jago income with no GoPay report records;
  - reverse arrival order with the swap, and an edited first leg that waits;
  - a late report at 4 minutes is skipped, and at 6 minutes it is a doubt;
  - a two-bank transfer in opposite directions still pairs;
  - unrelated same-amount reports minutes apart stay separate;
  - possible transfer is a doubt;
  - two top-ups absorb once each;
  - the deferred pass is idempotent;
  - acknowledgement learns the link (idempotent, nothing learned from a typed kept leg);
  - link API isolation (404) and companion key (403);
  - V47 converts a resolvable pattern and stops an unresolvable one.

  Evidence: the run on the tests-only commit, with its failures recorded in verification.md.

## 2. Data and rule

- [x] 2.1 V47 `account_report_links`, `notification_events.reporting_link_id`, the pattern conversion and the stop. Evidence: the migration applied on a V46 database with patterns, twice, with the expected rows.
- [x] 2.2 `services/account_links.py`: active links, labels, learn, stop and list. Evidence: unit tests.
- [x] 2.3 `movement_pairing`: `link_arrival` replaces `pattern_arrival` (D2–D4), `ArrivalDecision.reporting_link_id`, `mark_duplicate_observation` with the link, and the deferred pass. Evidence: the 1.1 tests pass.
- [x] 2.4 `notification_application` and `seen_twice` use the link. Acknowledgement learns a link. "Catat terpisah" and its undo keep the link. Evidence: the 1.1 tests and the review tests.
- [x] 2.5 Remove `skip_patterns.py`, `/api/ingest/skip-patterns` and their tests. Add `GET` and `DELETE /api/ingest/account-links`. Evidence: the API tests.

Expected Result: mirrors of a linked account book once in any arrival order, and patterns no longer act.

## 3. Web

- [x] 3.1 Tinjau:
  - the "Rekening yang sama" fact on seen-twice items;
  - the toast with "Batalkan" stopping the link;
  - "Dilewati otomatis" naming the link.

  Evidence: Vitest.
- [x] 3.2 Rekening detail: "Juga dilaporkan" with "Lepaskan" (two-step). Evidence: Vitest.
- [x] 3.3 Remove the Pengaturan section "Notifikasi ganda", its API helpers, its query key and its tests. Evidence: the settings tests, and the copy-register test.

Expected Result: the owner answers once in Tinjau and sees and stops links on the account.

## 4. Verification

- [x] 4.1 Backend full suite. Frontend `npm ci`, type-check, lint, tests and build. The offline benchmark. `openspec validate --all --strict`. `git diff --check`. Evidence: verification.md.
- [x] 4.2 Browser check of Tinjau (seen twice, skipped) and the Rekening detail at 1440 light, 1440 dark and 390 dark: axe serious/critical 0, overflow 0, console errors 0, targets ≥ 24 px, and screenshots read by the agent. Evidence: verification.md.
- [x] 4.3 Unchanged writes and reads against `main` on the same seed without links. Evidence: verification.md.
- [x] 4.4 verification.md section "Owner phone test cases". Evidence: the section.

## 5. Acceptance

- [x] 5.1 Present to the owner: the verification, OD129–OD134, and the phone test cases. Record their acceptance in proposal.md.
- [x] 5.2 After acceptance: archive, push, and report the PR link.
