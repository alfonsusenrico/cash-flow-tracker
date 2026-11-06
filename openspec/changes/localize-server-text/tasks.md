## 1. Approval, base and golden Indonesian output

- [ ] 1.1 Record the owner's approval and the answers to OD1 and OD2, with the date, in `proposal.md` (replacing "Plan approval pending") and in `PROJECT_STATE.md`. Create `feat/localize-server-text` from `main` after `add-language-system` has merged and move this change folder to it. Evidence: both files name the date and each decision's choice, `git log` shows the `add-language-system` merge, and `openspec validate localize-server-text --strict` passes on the new branch.
- [ ] 1.2 Inventory every server-written text on the base revision: composed questions, reasons, labels, facts, periods, notes, error details and prompts. Confirm the D5 table, including that `notification_sender.py` and `notification_source_pocket.py` hold no question wording. Evidence: `research/evidence/localize-server-text/inventory.md` lists each module, string family and count (it must cover all 181 plain-string `detail` sites). Any area not in the D5 table is added to `design.md` before task 3.
- [ ] 1.3 Record the golden Indonesian outputs (D10): a pytest fixture that renders every composed text kind, every error detail and the four prompt renderings on the parity seed and the synthetic cases, and stores them with the SHA-256 of each prompt file. Evidence: the fixture file and its generating commit hash. The test passes on the base revision.

Expected Result: everything the server writes today is listed and frozen byte for byte before any edit.

## 2. Language source, catalogues and money helper

- [ ] 2.1 Add `app/i18n/` (D1, D2): `language_for_user`, `language_for_request` (reusing the `Accept-Language` parser), the catalogue loader and `t()`, and the `Content-Language` middleware. Evidence: unit tests for an authenticated request with a conflicting header (the account wins), an unauthenticated request (the header wins), a missing key (it raises in tests; in production it falls back to Indonesian and logs only the key path), and `Content-Language` on text responses.
- [ ] 2.2 Add `backend/app/messages/{en,id}.json` and the server catalogue parity pytest. Evidence: the test fails on a planted missing key, a planted placeholder mismatch, and an English count message without `.one` (each checked once and removed).
- [ ] 2.3 Add `format_money(amount, region, style)` (D3) and replace the three helpers. Evidence: tests for the `figure`, `inline` and `bare` shapes in regions `ID` and `US`, and the golden fixture still passes.

Expected Result: the server knows each request's language, renders from catalogues, and formats money exactly as today.

## 3. Error contract

- [ ] 3.1 Add `app/errors.py` (`ErrorCode`, `api_error`) (D4) and convert the typed service errors (`SalaryError`, `refuse`, and the existing structured codes) to it, keeping existing codes and top-level fields. Evidence: the existing error tests pass, and new tests assert `params` and a `message` in both languages for each converted code.
- [ ] 3.2 Convert the 181 plain-string sites, router by router, including the 401 `not_authenticated`. Evidence: per-router API tests assert `{code, params, message}`, a scan test fails if any `HTTPException(... detail="...")` string remains, and the golden fixture's Indonesian messages are byte-identical where a plain string was Indonesian.
- [ ] 3.3 Add every code to the web's `errors` namespace in both languages, render `params` in `lib/api.ts`, and add the four-catalogue code parity pytest (D4). Evidence: the parity test passes and fails on a planted missing code. `lib/api.ts` tests render an English and an Indonesian error from `code`, with `params`.
- [ ] 3.4 Companion compatibility check: run the companion's JVM tests that parse error bodies (`ServerMessage` and `PurposeAnswers` tests) against responses captured from this branch. Evidence: the test run output and the captured bodies (synthetic) in `research/evidence/localize-server-text/companion/`.

Expected Result: every endpoint returns one error shape, every code renders in both languages, and the companion still shows messages.

## 4. Composed text and stored notes

- [ ] 4.1 Questions (D5): purpose, top-up, habit and capture questions and choices are rendered in the account's language with `code` and `params`. Evidence: tests per question kind in both languages, the golden fixture byte-identical for Indonesian, English text matching the D5 voice, and stored `active_question` and `text_captures.question` keeping their creation language after a switch.
- [ ] 4.2 Reasons, facts, periods and labels (D5): history-suggestion template reasons, monthly comparison facts, report windows and month names, money flow, the dashboard narrative and cash-flow summary, the cycle-summary system `label` and `note`, and the review fallbacks. Evidence: tests per area in both languages, the golden fixture byte-identical for Indonesian, and a scan of an English account's fixture output finding no `id.json` value outside user data.
- [ ] 4.3 Stored generated notes (D6): movement notes, generic descriptions and the movement default note in the account's language at creation. Evidence: the "Language switch after a transfer" scenario test, plus a test that a language switch updates no stored row (row count and `updated_at` unchanged).
- [ ] 4.4 Logs (D8): extend the capture log leak test to purpose questions, capture follow-ups, history runs and error responses in both languages. Evidence: the test passes, and it fails on a planted log line that contains a rendered prompt.

Expected Result: an English account receives English server text everywhere, with codes, and an Indonesian account receives exactly today's text.

## 5. LLM output language

- [ ] 5.1 Turn the four prompt files into templates with an output language and per-language placeholder lists (D7). Add the `-en` prompt versions, the English placeholder guards and `LLM_ENGLISH_OUTPUT_ENABLED` (default `false`). Evidence: the SHA-256 test proves each Indonesian rendering is byte-identical to the file before this change. Mocked-provider tests show English output used only when the flag is on and the account is English, and that "Payment" is rejected like "Pembayaran".
- [ ] 5.2 Add English-output cases to `evaluation/notification_cases.py` and offline English tests for the capture, purpose and history prompts. Evidence: `pytest backend/tests/test_notification_benchmark.py backend/tests/test_notification_cases.py` and the new prompt tests pass offline, with no network call (a test asserts that the provider is mocked).
- [ ] 5.3 Paid benchmark, waiting for the owner. Present the estimate from `estimated_run_cost` for the English development and held-out runs (cap $0.25 per configuration) and wait for the owner's explicit OK before any provider call. With the OK, run the development partition, then the held-out partition, with the English rendering. Evidence: the owner's OK quoted with its date, and the benchmark report in `openspec/changes/localize-server-text/benchmark-report.md` with quality, latency and cost against the same gates as the Indonesian benchmark. Without the OK, this task stays open, the flag stays `false`, and that is reported.
- [ ] 5.4 Decide the flag from the result: enable `LLM_ENGLISH_OUTPUT_ENABLED` in the release configuration only if the held-out run passes. Record the decision either way. Evidence: the configuration diff (or none) and the decision in `verification.md` and `PROJECT_STATE.md`.

Expected Result: English AI output exists, is measured, and is used only when the owner-approved evaluation passes.

## 6. Verification

- [ ] 6.1 Figures untouched: `parity.mjs` and the API snapshots before and after on the same seed, in both languages. Evidence: zero differing values in `verification.md`.
- [ ] 6.2 Web check of server text in English: the review queue questions, a Rincian note and the bell's report name in an English account, at 1440 and 390, in light and dark. Evidence: captures in `research/evidence/localize-server-text/`, with no Indonesian server text outside user data and no layout regression in the phase 3 checklist.
- [ ] 6.3 Repository checks on the exact revision:
  - the full backend suite on freshly migrated disposable PostgreSQL (V1 to V53), with skipped tests listed and explained;
  - `npm test`, `npm run type-check`, `npm run lint`, `npm run build`;
  - `git diff --check`, `openspec validate --all --strict`;
  - `git diff --stat origin/main -- db` empty.

  Evidence: counts and the commit hash in `verification.md`.
- [ ] 6.4 Present the verification to the owner: what changed, the golden-fixture result, the benchmark status and flag decision, the error-shape note for API clients, and what was not verified. Evidence: the owner's acceptance, quoted with its date in `verification.md`.
- [ ] 6.5 After the owner's acceptance: run `openspec archive localize-server-text`, then `openspec validate --all --strict`, commit with a subject-only Conventional Commit, push `feat/localize-server-text`, open the pull request against `main` and report its URL. Record the post-merge production checks (`Content-Language` present, the owner's questions unchanged, the flag value) in `PROJECT_STATE.md`. Evidence: the archive path, the passing validation, the pushed revision and the pull request URL.

Expected Result: all server text follows the account's language on an accepted, archived revision, so phases 4 to 12 can keep the backend untouched.
