# history-suggestions Specification

## Purpose
Fix past automatically classified rows with suggestions the owner decides on: owner-started runs with a
cost estimate, memory and rule proposals before bounded model batches, grouped review on the web, and
acceptance that behaves exactly like an owner edit while rejections are never proposed again.

## Requirements

### Requirement: Eligible History Rows
A history suggestion run SHALL consider only the owner's expense and income rows within the requested local-date window that were classified automatically (`classified_by` `ai` or `rule`) and never edited by the owner (`owner_edited_at` is NULL), and that are not movement legs, transfer-fee rows, rows linked to a goal, a debt or a debt allocation, or rows in "Internal Movement" or "Investasi". At most `HISTORY_SUGGESTION_MAX_ROWS` (default 300) rows SHALL be processed per run, newest first.

#### Scenario: Owner rows and movements are left alone
- **GIVEN** in the window an AI row, an owner-classified row, an owner-edited row, a system row, both legs of a movement and a fee row of that movement, all naming "Kedai Awan"
- **WHEN** the owner asks for an estimate
- **THEN** only the AI row SHALL be counted as eligible and only it SHALL receive a suggestion.

#### Scenario: Row limit
- **GIVEN** five eligible rows and a maximum of three
- **WHEN** the owner asks for an estimate
- **THEN** it SHALL report five eligible, three selected and `truncated: true`.

### Requirement: Estimate Before Start
`POST /api/review/suggestions/runs` with `from_date` and `to_date` SHALL return an estimate without creating a run: eligible and selected counts, rows covered by memory and by rules, rows and calls the model would need (calls = rows divided by `HISTORY_SUGGESTION_BATCH_SIZE`, default 20, rounded up), the batch size, the row limit, and whether the model is available with the reason when it is not.

#### Scenario: Estimate creates nothing
- **WHEN** the owner posts a window without `confirm`
- **THEN** the response SHALL contain the estimate, no run SHALL exist and no suggestion SHALL be written.

#### Scenario: Confirmed start
- **WHEN** the owner posts the same window with `confirm: true`
- **THEN** a run SHALL be created and processed in the background and `GET /api/review/suggestions/runs/latest` SHALL report its status and counts.

#### Scenario: Invalid window
- **WHEN** the window ends before it starts, ends in the future, spans more than 366 days, or the body has an unknown field
- **THEN** the response SHALL be 422 and nothing SHALL be created.

### Requirement: One Active Run Per Owner
An owner SHALL have at most one running run. A start while one is running SHALL fail with 409 `suggestion_run_active`. A run holds a lease that every step renews; a running run whose lease expired SHALL be marked failed with `run_interrupted` before the next start or status read.

#### Scenario: Second start
- **GIVEN** a running run with a live lease
- **WHEN** the owner confirms another run
- **THEN** the response SHALL be 409 `suggestion_run_active`.

#### Scenario: Interrupted run
- **GIVEN** a running run whose lease expired
- **WHEN** the owner reads the latest run or starts a new one
- **THEN** the old run SHALL be failed with `run_interrupted` and a new run SHALL be allowed.

### Requirement: Proposal Order
For each selected row the run SHALL propose, in this order: (1) the owner's merchant memory, source `memory`; (2) the repayment rule or the café rule, source `rule`; (3) otherwise one model call per batch of rows, source `ai`.

#### Scenario: Memory before the model
- **GIVEN** owner memory "Kedai Awan" → "Jajan & Nongkrong", want, notes "Ngopi di Kedai Awan"
- **WHEN** a run covers an AI row "Pengeluaran makanan dan minuman" at Kedai Awan in "Makanan & Minuman"
- **THEN** a `memory` suggestion with those values SHALL be stored and the model SHALL not be called.

#### Scenario: Friend repayment
- **WHEN** a run covers an income row "Transfer masuk tf makan" in "Transfer Masuk"
- **THEN** the suggestion SHALL move it to the offsetting category "Patungan & Ganti Uang" with offset "Makanan & Minuman", source `rule`.

#### Scenario: Café
- **WHEN** a run covers an expense at "Kopi Senja" in "Makanan & Minuman"
- **THEN** the suggestion SHALL move it to "Jajan & Nongkrong" with pillar want.

#### Scenario: Model batch
- **GIVEN** three generic rows, a batch size of two and a model that returns one valid item and one with an unknown category per batch
- **WHEN** the run completes
- **THEN** there SHALL be two model calls, two `ai` suggestions with their confidence, and no suggestion for the invalid item.

#### Scenario: Equal or low-confidence proposals
- **WHEN** the model proposes a row's current values, or a proposal with confidence 0.3
- **THEN** no suggestion SHALL be stored and the equal one SHALL be counted as unchanged.

### Requirement: Degraded Run Without the Model
When the model is not configured or a model call fails, the run SHALL complete with the memory and rule suggestions already stored, stop further model calls, and record the reason as `error_code` (`ai_disabled`, a configuration code, or the provider error code). An unexpected failure SHALL mark the run failed with `run_failed`.

#### Scenario: Provider unavailable
- **GIVEN** one row covered by memory and one row that needs the model
- **WHEN** the provider fails with `provider_unavailable`
- **THEN** the run SHALL be completed with `error_code = provider_unavailable` and exactly the memory suggestion stored.

#### Scenario: AI disabled
- **WHEN** a run starts while the model is disabled
- **THEN** it SHALL complete with `error_code = ai_disabled`, no model and no prompt version, and keep its memory suggestions.

### Requirement: Suggestion Storage and Privacy
Each suggestion SHALL store the transaction, the run, the source (`memory`, `rule` or `ai`), the proposed merchant, notes, category, pillar and offset, the confidence (model only), a short Indonesian reason, the row's values when proposed, and a status `pending`, `accepted`, `rejected` or `superseded`.

#### Scenario: Redacted model input
- **GIVEN** a row whose notes contain the owner's name
- **WHEN** the row is sent to the model
- **THEN** the input SHALL contain the redaction marker instead of the name.

#### Scenario: Logs
- **WHEN** a run completes
- **THEN** its log line SHALL contain counts and codes and no merchant, notes or model output.

### Requirement: Listing and Supersession
`GET /api/review/suggestions` SHALL list the owner's pending suggestions (at most 1 000, newest row first) grouped by direction, proposed merchant (or notes), category and offset category, each group with count, total, date range, the distinct before values with their counts, the proposed values, the sources and its suggestions with before and after values, reason and confidence.

#### Scenario: Later edit
- **GIVEN** a pending suggestion for a row
- **WHEN** the owner changes the row's notes in the ledger
- **THEN** the suggestion SHALL be superseded, leave the list, and an accept of it SHALL apply nothing and report it as skipped.

#### Scenario: Review accept keeps the suggestion
- **WHEN** the owner accepts the row as reviewed in the phase 6 queue without changing it
- **THEN** its suggestion SHALL stay pending.

### Requirement: Accepting Applies an Owner Edit
`POST /api/review/suggestions/accept` with 1–200 distinct suggestion ids SHALL, in one database transaction under the owner's notification lock, apply every pending suggestion through the owner-edit path of the bulk review and mark the suggestions accepted.

#### Scenario: Accept a group
- **WHEN** the owner accepts both "Kedai Awan" suggestions
- **THEN** both rows SHALL carry the proposed values, be owner-classified and reviewed, two feedback rows of kind `accepted_suggestion` SHALL name the suggestions, and the memory for "kedai awan" SHALL hold the accepted values with source `owner`.

#### Scenario: Repeat accept
- **WHEN** the same ids are accepted again
- **THEN** nothing SHALL change, every id SHALL be skipped and no feedback row SHALL be added.

#### Scenario: Another owner's suggestion
- **WHEN** a user accepts or rejects suggestion ids of another owner
- **THEN** the response SHALL be 404 `suggestions_not_found` and the suggestions SHALL stay pending.

### Requirement: Rejecting Is Remembered
`POST /api/review/suggestions/reject` with 1–200 distinct ids SHALL mark pending suggestions rejected without changing the rows or writing feedback, report skipped ids, and fail with 404 for ids that are not the owner's. A later run SHALL not store a proposal equal to a rejected one for the same row and SHALL count it as rejected before.

#### Scenario: Rerun after rejection
- **GIVEN** two rejected "Kedai Awan" suggestions
- **WHEN** the owner runs the same window again
- **THEN** no pending suggestion SHALL be created for those rows and the run SHALL report two rejected before.

#### Scenario: Invalid decision body
- **WHEN** the body has no ids, a repeated id, more than 200 ids or an invalid id
- **THEN** the response SHALL be 422.

### Requirement: Saran Perbaikan Riwayat Section
The `/review` page SHALL show, in Indonesian and regardless of the rest of the queue, a "Saran perbaikan riwayat" section with a period, an estimate before any run, progress while it runs, the last run, and the pending groups with their accept and reject actions.

#### Scenario: Estimate then confirm by keyboard
- **WHEN** the owner presses Enter on "Hitung perkiraan", tabs to "Mulai buat saran" and presses Enter
- **THEN** the first request SHALL carry no `confirm`, the second SHALL carry `confirm: true`, and progress SHALL show the processed and selected counts.

#### Scenario: Reject one row
- **WHEN** the owner opens a group's rows and activates "Tolak" on one row
- **THEN** one reject request SHALL carry only that suggestion's id and the status SHALL name the group and the count.

#### Scenario: Failure
- **WHEN** a decision request fails
- **THEN** an alert SHALL show the server's message and no success message SHALL appear.

### Requirement: Confirmed Suggestion Run Start
Only a suggestion run request with `confirm: true` SHALL create a run and start it in the background.

#### Scenario: Unconfirmed request
- **WHEN** the owner posts a window with `confirm: false`
- **THEN** the response SHALL be an estimate and no run SHALL be created.

### Requirement: Suggestion Window Validation
The suggestion window SHALL end no later than today, not before it starts, and span at most 366 days; unknown fields SHALL be rejected (422).

#### Scenario: Window ending tomorrow
- **WHEN** the owner posts a window whose `to_date` is tomorrow
- **THEN** the response SHALL be 422 and nothing SHALL be created.

### Requirement: Invalid Suggestion Settings Disable Runs
An invalid `HISTORY_SUGGESTION_MAX_ROWS` or `HISTORY_SUGGESTION_BATCH_SIZE` setting SHALL disable runs with 503 `suggestion_configuration_invalid`.

#### Scenario: Invalid batch size
- **GIVEN** an invalid `HISTORY_SUGGESTION_BATCH_SIZE`
- **WHEN** the owner requests an estimate
- **THEN** the response SHALL be 503 `suggestion_configuration_invalid`.

### Requirement: Memory Proposal Keys
A memory proposal SHALL use the owner's merchant memory for the row's merchant or for its notification's match key (merchant or signature key).

#### Scenario: Signature key
- **GIVEN** owner memory stored under the signature key of a merchant-less myBCA payment
- **WHEN** a run covers another automatic row recorded from a notification with that signature
- **THEN** a `memory` suggestion with the remembered values SHALL be stored.

### Requirement: Repayment Rule
Income whose notes, merchant or recording notification mention a repayment ("patungan", "urunan", "ganti", "bayar", "split", "tf makan", "uang makan") SHALL be proposed into the owner's active offsetting income category with notes "Ganti uang patungan" and, as offset, the expense category the owner most often chose for repayments, else a meal category when the text mentions a meal; source `rule`.

#### Scenario: Owner's usual offset
- **GIVEN** the owner's repayments most often offset "Hiburan"
- **WHEN** a run covers an income row "Ganti uang nonton" in "Transfer Masuk"
- **THEN** the suggestion SHALL propose "Patungan & Ganti Uang", notes "Ganti uang patungan" and offset "Hiburan", source `rule`.

### Requirement: Café Rule
An expense whose text names a café, coffee or snack SHALL be proposed into "Jajan & Nongkrong" with that category's pillar; source `rule`.

#### Scenario: Coffee merchant
- **WHEN** a run covers an expense at "Kopi Pagi" in "Belanja"
- **THEN** the suggestion SHALL propose "Jajan & Nongkrong" with pillar want, source `rule`.

### Requirement: Redundant Proposals Are Not Stored
A proposal equal to the row's current values, to its pending suggestion, or to a suggestion the owner rejected for that row SHALL not be stored.

#### Scenario: Previously rejected proposal
- **GIVEN** the owner rejected a suggestion for a row
- **WHEN** a later run proposes the same values for that row
- **THEN** no suggestion SHALL be stored.

### Requirement: Model Item Validation
Model items SHALL reference only rows of the batch and the owner's active categories of the row's direction; a pillar only for expenses; an offset only for income in an offsetting category and from the eligible offset list. Items that fail these checks or have confidence below 0.5 SHALL be dropped.

#### Scenario: Item for a row outside the batch
- **WHEN** the model returns an item for a transaction that is not in its batch
- **THEN** that item SHALL be dropped and no suggestion SHALL be stored for it.

### Requirement: One Pending Suggestion per Transaction
A transaction SHALL have at most one pending suggestion.

#### Scenario: Second run over the same row
- **GIVEN** a pending suggestion for a row
- **WHEN** another run covers the same row
- **THEN** the row SHALL still have at most one pending suggestion.

### Requirement: Suggestion Reasons Without Notification Text
Suggestion reasons SHALL never quote notification text.

#### Scenario: Rule suggestion for a notification row
- **GIVEN** a row recorded from a notification
- **WHEN** a rule suggestion is stored for it
- **THEN** its reason SHALL NOT contain the notification text.

### Requirement: History Model Input
The model input SHALL contain the rows' stored merchant, notes, category, pillar, amount, local date, account label and bank category, the owner's active categories with default pillars, owner memory and owner-classified history, all through the existing redaction, and never the notification text.

#### Scenario: Notification text stays out
- **GIVEN** a row recorded from a notification
- **WHEN** its batch is sent to the model
- **THEN** the input SHALL contain the row's stored values and SHALL NOT contain the notification text.

### Requirement: Suggestion Run Logs
Suggestion run logs SHALL contain counts and codes only.

#### Scenario: Provider failure
- **WHEN** a run ends with a provider error
- **THEN** its log line SHALL contain the error code and counts and no merchant, notes or model output.

### Requirement: Stale Suggestions Are Superseded
Before listing, accepting or rejecting, a pending suggestion SHALL become `superseded` when its row was edited by the owner, became a movement leg or fee row, or no longer has the values it had when proposed, or when its proposed category is archived or of the other kind.

#### Scenario: Archived proposed category
- **GIVEN** a pending suggestion whose proposed category the owner has since archived
- **WHEN** the suggestions are listed
- **THEN** the suggestion SHALL be superseded and SHALL NOT be listed.

### Requirement: Reviewing a Row Keeps Its Suggestion
Marking a row reviewed without changing it SHALL not supersede its suggestion.

#### Scenario: Bulk accept of the row
- **GIVEN** a pending suggestion for an automatic row
- **WHEN** the owner marks the row reviewed with the bulk `accept` action
- **THEN** the suggestion SHALL stay pending.

### Requirement: Accepted Suggestion Values
Accepting a suggestion SHALL set the row's merchant, notes, category, pillar (NULL when it equals the category's) and offset, `review_status = reviewed`, `classified_by = owner` and `owner_edited_at`, and SHALL clear the confidence.

#### Scenario: Pillar equal to the category default
- **GIVEN** a suggestion proposing "Jajan & Nongkrong" with pillar want, the category's own pillar
- **WHEN** the owner accepts it
- **THEN** the row SHALL have that category, no row-level pillar, `review_status = reviewed`, `classified_by = owner` and no confidence.

### Requirement: Accepted Suggestion Learning
Accepting a suggestion SHALL write one `accepted_suggestion` feedback row per changed row, upsert owner merchant memory, and close an open purpose question of the row.

#### Scenario: Open purpose question
- **GIVEN** a row with an open "Apa ini?" question and a pending suggestion
- **WHEN** the owner accepts the suggestion
- **THEN** the question SHALL be closed.

### Requirement: Accept Reporting and Ownership
Accept ids that are not pending SHALL be reported as skipped. Any id that is not the owner's SHALL fail the whole request with 404 `suggestions_not_found`. The response SHALL report accepted, skipped and the number of feedback rows.

#### Scenario: One pending and one rejected id
- **WHEN** the owner accepts one pending and one rejected suggestion that changes its row
- **THEN** the response SHALL report one accepted, one skipped and one feedback row.

### Requirement: Suggestion Period and Estimate Controls
The section SHALL offer a period (default the last three months) checked in the browser like the backend, and an estimate shown before any run with a "Mulai buat saran" confirmation and a "Batal" that returns focus.

#### Scenario: Cancel the estimate
- **WHEN** the owner activates "Batal" on a shown estimate
- **THEN** no run request SHALL be sent and focus SHALL return to "Hitung perkiraan".

### Requirement: Suggestion Progress and Last Run
The section SHALL show progress from the run's real counts while it runs, and the last run's window, counts and the reason when suggestions were limited.

#### Scenario: Model unavailable
- **GIVEN** the last run completed without the model
- **WHEN** the section is shown
- **THEN** it SHALL show that run's window, its counts and why model suggestions were skipped.

### Requirement: Suggestion Group Actions
The section SHALL show the pending groups with a before → after preview, their sources, "Terima semua" and "Tolak semua", and per row "Terima" and "Tolak". Groups above 200 suggestions SHALL be sent in batches.

#### Scenario: Large group
- **GIVEN** a group of 250 suggestions
- **WHEN** the owner activates "Terima semua"
- **THEN** the suggestions SHALL be sent in two accept requests of 200 and 50 ids.

### Requirement: Accessible Suggestion Feedback
Every section control SHALL be operable by keyboard, results SHALL be reported in a live region and failures in an alert, skipped ids SHALL be reported, and the section SHALL never claim success for a failed request.

#### Scenario: Skipped decisions
- **WHEN** an accept reports two skipped suggestions
- **THEN** the live region SHALL state that two suggestions were skipped.
