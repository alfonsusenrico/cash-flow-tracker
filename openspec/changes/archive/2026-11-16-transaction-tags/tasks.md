## 1. Data and API

- [x] 1.1 Migration V58: `tags`, `transaction_tags`. Evidence: Flyway to V58; cascade tests.
- [x] 1.2 Tag service and router (list, create, rename, merge, archive); `tag_ids` and `new_tags` on transaction create and update; `tags` on responses. Evidence: API tests for every scenario of "Owner-Created Tags" and "Managing Tags".
- [x] 1.3 Ledger `tag` filter with summary totals. Evidence: the test "What an event cost"; the existing ledger statement-count tests still pass.

Expected Result: tags exist, attach, filter and total.

## 2. AI text capture

- [x] 2.1 Context with active tags, schema `tags`, backend checks, tags recorded on send and removed by undo when the capture created them; prompt `capture-normalization-3` in both languages. Evidence: tests with a fake provider for every scenario of "Tags in AI Text Capture", including a foreign ID, a span not in the text and a duplicate new name.
- [x] 2.2 Synthetic benchmark cases (prefix, hashtag, instruction, near-duplicate, none) in `backend/evaluation/capture_tag_benchmark.py`, then one paid run with the owner's OK. Evidence: scores and cost in `verification.md`; existing capture cases do not regress.

Expected Result: the AI reuses tags and never creates one without the owner.

## 3. Web

- [x] 3.1 Tag picker with create in Catat (manual form) and the edit dialog; tags on the recorded AI result; chips on rows and in the detail; ledger filter; Pengaturan "Tag" section; messages in both languages. Evidence: component tests; keyboard and axe checks; type check, lint, tests and build.
- [x] 3.2 Browser check on a production build with synthetic data at 1440 and 390, light and dark: create from Catat, tags on an AI result, filter with totals, merge. Evidence: captures shown to the owner.

Expected Result: tagging takes a few taps, and the filter answers "what did this cost".

## 4. Verify and deliver

- [x] 4.1 Full backend and frontend suites; `openspec validate --all --strict`; `verification.md`.
- [x] 4.2 After the owner accepts, archive, push, open the PR and report its URL.
