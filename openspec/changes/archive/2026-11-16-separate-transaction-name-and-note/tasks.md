## 1. Data and API

- [x] 1.1 Migration V57: rename `transactions.notes` to `name`, add `note`. Evidence: Flyway V1→V57 on Postgres 16; a migration test for the scenario "Migrated rows".
- [x] 1.2 Transaction models and routers: `name` and `note` in responses and requests, with the `notes` alias. Evidence: API tests for "Notes Alias for Older Clients" and for both fields on create, update and detail.
- [x] 1.3 Every backend reader and writer of the old column moves to `name`; search includes `note`. Evidence: the full backend suite; a grep check that no `t.notes` or `transactions.notes` reference is left; the test "Search finds a note".

Expected Result: the server speaks name and note, and old clients still work.

## 2. AI and learning

- [x] 2.1 Notification interpretation, text capture, purpose answers, merchant memory and habits write and learn `name`, and never `note`. Prompt wording updated and versions bumped if the text changes. Evidence: the test "The AI never writes the note", including the model input; existing learning tests pass.

Expected Result: automatic writers fill the name only.

## 3. Web and companion

- [x] 3.1 Catat and the edit dialogs: "Nama transaksi" and "Catatan (opsional)"; rows and detail show the note as a second line; messages in both languages; copy-register tests. Evidence: component tests for "A remark on an event's spending"; type check, lint, tests and build.
- [x] 3.2 Browser check on a production build with synthetic data at 1440 and 390, light and dark: Catat with a note, a ledger row, the detail and search. Evidence: captures shown to the owner.
- [x] 3.3 Companion (`mobile/`): the row title uses the name; the detail shows the note. Evidence: companion unit and UI tests, lint and build.

Expected Result: the owner can name a transaction and add a remark on web and phone.

## 4. Verify and deliver

- [x] 4.1 Full backend, frontend and companion suites; `openspec validate --all --strict`; `verification.md`.
- [x] 4.2 After the owner accepts, archive, push, open the PR and report its URL. After deploy, check read-only that the row counts are unchanged and that the notes now sit in `name`; then release the companion.
