## 1. Regression tests first

- [x] 1.1 Add database tests to `backend/tests/test_duplicate_arrivals.py` (fictional names, synthetic accounts) that fail on `main`:
  - (a) the incident sequence (GoPay, Jago 0.13 s later, myBCA 3.4 s later) with the pattern (jago, income, gopay): the Jago event is a `duplicate_observation` with `skip_pattern_id` and `reviewed_at`, no Tinjau `seen_twice` item, myBCA paired with GoPay, balances unchanged;
  - (b) the reverse order (Jago first) with the pattern: the Jago transaction is removed and the Jago event is the skipped duplicate of the GoPay leg;
  - (c) the reverse order with the Jago leg edited by the owner: the GoPay event is an ordinary duplicate waiting in `seen_twice`.

  Evidence: the test run on `main` fails for the expected reasons, recorded in verification.md.
- [x] 1.2 Add the negative cases:
  - without a pattern, the guard still makes the Jago report a waiting duplicate;
  - a Jago income with no GoPay report records normally;
  - a different amount, a different direction, an unresolved remote and two possible kept reports each skip nothing;
  - a stopped pattern doesn't match;
  - another owner's pattern doesn't match.

  Evidence: tests in the same file.

Expected Result: the behaviour is pinned before any code changes.

## 2. Remove the model's time context

- [x] 2.1 Remove:
  - `neighbour_context`, the `neighbours`, `neighbours_incomplete` and `_neighbour_ids` context keys, and the neighbour trimming in `notification_context.py`;
  - `same_money_as` and `invalid_neighbour_reference` in `notification_interpretation.py`;
  - `hold_possible_duplicate` and the `same_money_event_id` parameter in `notification_application.py`;
  - the `notification_neighbour_window_seconds` setting in `config.py`, `docker-compose.yml` and `.env.example`;
  - `backend/tests/test_notification_neighbours.py`.

  Evidence: `rg "neighbour|same_money|possible_duplicate_observation|invalid_neighbour_reference"` over `backend/app`, `frontend/src`, `docker-compose.yml` and `.env.example` finds nothing; the backend suite passes.
- [x] 2.2 Remove the neighbours paragraph from `prompts/notification_interpretation.md` and set `PROMPT_VERSION = "notification-interpretation-12"`. Evidence: the prompt and schema tests assert the version and that the schema has no `same_money_as`.
- [x] 2.3 Remove `possible_duplicate_observation` from `SEEN_TWICE_SQL`, `_held_original` and the held branch of `seen_twice_items`. In the frontend, remove the held variant in `SeenTwiceCard.tsx`, its reason label in `lib/review.ts` and `kind: "held"` in `lib/triage.ts`. Evidence: review queue and Tinjau tests updated and passing.

Expected Result: the model reads one notification at a time, and no model output can hold or skip a report.

## 3. Patterns and migration

- [x] 3.1 Write `db/migrations/V46__notification_skip_patterns.sql` (design D2, D6):
  - the table `notification_skip_patterns` with its checks and the unique partial index on active patterns;
  - `notification_events.skip_pattern_id` with a partial index;
  - the requeue of events held with `possible_duplicate_observation`;
  - a rollback note in the header.

  Evidence: Flyway migrates a fresh database and a database restored at V45; running the requeue twice changes nothing the second time.
- [x] 3.2 Add `backend/app/services/skip_patterns.py`:
  - `active_patterns`, `create_pattern`, `stop_pattern`, `learn_from_acknowledgement` and `seen_institutions`;
  - owner-scoped queries, and institutions validated against `SUPPORTED_PACKAGES`.

  Evidence: unit tests for validation, idempotence and owner isolation.
- [x] 3.3 Add the pattern branch to `same_arrival` and `record_observed_leg`, including the reverse-order swap through `replaceable_leg` and `drop_observation` (design D3). Do the same in `repair_duplicate_arrivals`. Evidence: tasks 1.1 and 1.2 pass, and a repeated deferred pass changes nothing.
- [x] 3.4 In `acknowledge`, learn the pattern (design D4) and return `pattern: {id, created}`. Evidence: tests for a first confirmation, a repeat, and a kept leg that is not from a notification.

Expected Result: confirmed or added patterns skip their repeats with no ledger effect.

## 4. API

- [x] 4.1 Add `GET`, `POST` and `DELETE /api/ingest/skip-patterns` in `routers/ingest.py` (design D7), with session auth. Evidence: API tests for:
  - 200, 201, 409 `skip_pattern_exists` and 422 `invalid_skip_pattern`;
  - 404 `skip_pattern_not_found` for another owner and for an unknown id;
  - an idempotent delete;
  - a request with only the companion API key is refused.
- [x] 4.2 In `GET /api/review/queue`, add `skipped` and `counts.skipped` (design D5). Let `record_separately` and its undo handle skipped events, with `skip_pattern_id` in `RESTORED_EVENT_FIELDS`. Evidence: tests for:
  - the queue shape and counts;
  - "Catat terpisah" and "Urungkan" on a skipped event;
  - another owner gets 404.

Expected Result: the web app can list, add and stop patterns and see what was skipped.

## 5. Web

- [x] 5.1 Tinjau:
  - a folded "Dilewati otomatis" list in `TriageList.tsx` with its count, opening a skipped item in `SeenTwiceCard.tsx` with the kept leg, the pattern and "Catat terpisah";
  - after "Benar, satu transaksi", the toast "Laporan Bank Jago serupa akan dilewati otomatis." with "Batalkan" (stops the pattern).

  Evidence: Vitest for the fold, the item, the toast, "Batalkan" and the axe check.
- [x] 5.2 Pengaturan:
  - the section "Notifikasi ganda" (`sections.ts`, `app/settings/notifikasi-ganda/page.tsx`, `components/settings/SkipPatterns.tsx`) with the list, "Berhenti melewati" through `ConfirmRemove`, "Tambah pola", the empty state and error messages (design D7);
  - labels in neutral standard Indonesian (`ui-copy-register`).

  Evidence: Vitest for listing, adding, the 409 and 422 messages, stopping, and the section order and address.
- [x] 5.3 Browser check on a local stack with synthetic data:
  - Tinjau "Dilewati otomatis" and Pengaturan "Notifikasi ganda" at 1440 light, 1440 dark, 1024 light and 390 dark;
  - axe serious and critical 0, overflow 0, console errors 0, targets at least 24 px;
  - keyboard reach of the form;
  - screenshots read by the agent;
  - the stack stopped by PID or port afterwards.

  Evidence: verification.md and the screenshot paths.

Expected Result: the owner can add the known pattern by hand and see every skip.

## 6. Evaluation

- [x] 6.1 Corpus and report:
  - `notification_cases.py` becomes `synthetic-notifications-5`, with the sequence cases replayed without neighbours and expecting plain observed legs;
  - `notification_benchmark.py` drops the `same_money` metric.

  Evidence: the offline benchmark and `test_notification_benchmark.py` and `test_notification_cases.py` pass.
- [ ] 6.2 Live benchmark: `-12` on this branch against `-11` at `62285dd`, `gpt-6-luna`, `none` and `low`, three repeats, about $0.10. It runs only with the owner's go-ahead (OD127) and with the key the owner provides. Evidence: the report with prompt and fixture versions, safety gate, composite, latency and tokens in verification.md, or "not run" with the reason.

Expected Result: the prompt change is measured, and `none` stays unless the numbers say otherwise.

## 7. Verification

- [x] 7.1 Run:
  - the backend full suite on a fresh V46 database;
  - frontend `npm ci`, type-check, lint, tests and build;
  - money identities on a seed without duplicates, read through `main` and this branch;
  - `openspec validate --all --strict` and an archive rehearsal;
  - `git diff --check`.

  Evidence: verification.md with counts tied to the tested revision.
- [x] 7.2 Before the release, count read-only in production the events held with `possible_duplicate_observation` that V46 will requeue. Evidence: the count in verification.md.

## 8. Acceptance

- [ ] 8.1 Present the change to the owner: the verification, the open decisions OD122–OD128, and the steps to add the Bank Jago / GoPay pattern in Pengaturan after the deploy. The owner accepts. Record their words and the date in proposal.md.
- [ ] 8.2 After acceptance:
  - archive with `openspec archive skip-redundant-notifications -y`;
  - write a real Purpose for any new main spec;
  - confirm `openspec validate --all --strict` exits 0;
  - push the branch and give the PR link.

  After the merge and deploy:
  - verify the deploy read-only;
  - confirm the owner's pattern is listed;
  - delete the branch and worktree.
