## 1. Schema

- [x] 1.1 Add `db/migrations/V29__merchant_memory_and_purpose_questions.sql` and its identical mirror `backend/app/db/merchant_memory_and_purpose_questions.sql`, run last by `init_db.py` (design D3, D15).
  - Evidence: a migration test that applies V29 and the mirror twice, asserts the tables, the unique keys and checks, and that an `UPDATE` of a feedback row fails while inserts and user deletion succeed.

Expected Result: both stores exist on a freshly migrated database and the mirror is idempotent.

## 2. Record and flag

- [x] 2.1 Deterministic path records uncategorised rows; model fallback, low confidence, model review reason and ambiguous pairing flag rows `needs_review` with stored reasons; automatic pairing resets both legs to `auto`; the confidence threshold setting (D1, D8).
  - Evidence: database tests for each recorded case (uncategorised, model `needs_review` fallback, invalid output fallback, confidence 0.6, ambiguous counterparts) and each held case (two amounts, unknown pocket, Jago source pocket, ambiguous existing movement); a settings test for an invalid threshold.

Expected Result: settled money with a resolved account always gets a ledger row, and only money or account doubts hold an event.

## 3. Merchant memory and learning

- [x] 3.1 `services/merchant_memory.py`: match keys, placeholders, signatures, feedback insert, memory upsert, owner memory lookup (D2–D4).
  - Evidence: unit tests for keys over the real corpus (merchant-less vs named) and for normalisation.
- [x] 3.2 Owner edit hooks write feedback and memory and close open purpose questions: transaction PATCH/PUT, movement rename, merge, top-up conversion, top-up notes (D4).
  - Evidence: database tests: recategorise writes one feedback row with features and an owner memory entry; amount-only edit writes nothing; movement rename writes feedback but no memory; feedback update rejected.
- [x] 3.3 Classification order with memory hits in deterministic, AI and fallback paths; AI memory entries never overwrite owner entries; merchant column filled (D5, D6).
  - Evidence: tests: memory hit records without calling the provider, `classified_by = rule`, hit count 1; archived category disables the hit; signature entries do not short-circuit; AI entry not replacing an owner entry; merchant stored.

Expected Result: an owner correction changes how the next notification of that merchant is recorded.

## 4. Model context and prompt

- [x] 4.1 `load_context` adds memory and similar history (replacing recent history) and the byte budget; prompt version 9 with the `merchant` field and placeholder rules (D7, D8).
  - Evidence: tests: similar history ordering prefers owner-reviewed same-key rows and excludes unrelated institutions; budget trimming keeps the input under 60 000 bytes; claimed events store `notification-interpretation-9`; existing context tests updated.

Expected Result: the model sees the owner's own labels for similar payments.

## 5. Purpose questions

- [x] 5.1 Question creation and suggestion ranking in `services/purpose_questions.py` (D9, D10).
  - Evidence: tests: question for myBCA category-only, QRIS without merchant and masked incoming; none for named merchant or paired legs; ranking by time and amount; repayment suggestion; cold start.
- [x] 5.2 Answer endpoint, normaliser provider method and prompt, list endpoint, version gating (D11–D14).
  - Evidence: tests: suggestion answer; text answer with mocked normaliser; provider failure; idempotent replay; conflicting replay 409; wrong `question_id` 409; other owner 404; invalid bodies 422; ledger edit closes the question; 1.8.0 vs 1.7.0 result fields; list endpoint scoping.

Expected Result: every merchant-less payment can be explained with one tap or a short text, from the phone or the web.

## 6. Verification and delivery

- [x] 6.1 Run the full backend suite on a freshly recreated disposable PostgreSQL migrated V1→V29 with `SKIP_TEST_MIGRATIONS=1`, including the real-corpus and benchmark tests.
  - Evidence: pass and skip counts in `verification.md`.
- [x] 6.2 Run `openspec validate add-ingestion-memory-and-purpose-questions`, `openspec validate --all`, both with `--strict`, and `git diff --check`.
  - Evidence: zero errors; strict warnings recorded as follow-ups.
- [x] 6.3 Write `verification.md` with revision, commands, counts, synthetic-data note and what was not verified.
  - Evidence: the file is complete.
- [x] 6.4 Check open changes for conflicting deltas, archive, re-run `openspec validate --all`, commit, push `feat/ingestion-memory-and-purpose` and report the compare link.
  - Evidence: archive exit status 0, validation output, pushed branch.
