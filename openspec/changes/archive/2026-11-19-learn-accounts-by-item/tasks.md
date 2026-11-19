## 1. Learning

- [x] 1.1 Migration V59 `capture_item_groups`. Evidence: Flyway to V59; a test that a recorded capture stores its group once.
- [x] 1.2 Item group of a phrase (stored, first word, model, first word). Evidence: tests for every scenario of "Item Groups of Captures".
- [x] 1.3 Account by item group in `account_for`, and the preselection for a question or an ambiguous mention. Evidence: tests for every scenario of "Accounts Learned by Item"; the existing account tests still pass.
- [x] 1.4 Bare clock time after a day word. Evidence: the scenario test and the existing "Capture Time Reading" scenarios.

Expected Result: the account follows the owner's habit per item, and the time no longer leaks into the item.

## 2. Model and preview

- [x] 2.1 `item_group` in the capture's structured output with code checks; prompt `capture-normalization-4` in both languages; `item_groups` in the model context only when needed; `CAPTURE_AI_REASONING_EFFORT`. Evidence: fake-provider tests for an existing group, a new group, an unproven group and a span not in the text; golden prompt fixture refreshed.
- [x] 2.2 Preview's learned account ("Cash · dari kebiasaan") on the web. Evidence: preview API test and `SentenceView` test.

Expected Result: new wording joins the right item without an extra model call, and the owner sees the learned account before sending.

## 3. Benchmark

- [x] 3.1 Synthetic grouping benchmark (same item at a new place, synonyms, brand vs item, unrelated items, existing category cases) at `none` and `low`; one paid run with the owner's OK. Evidence: scores, latency and cost in `verification.md`; the owner picks the production effort.

Expected Result: the grouping is measured before it reaches production.

## 4. Verify and deliver

- [x] 4.1 Full backend and frontend suites; `openspec validate --all --strict`; browser check of the preview; `verification.md`.
- [x] 4.2 After the owner accepts: archive, push, open the PR and report its URL; after the merge set `CAPTURE_AI_REASONING_EFFORT` only if the owner chose a value other than the default.
