# review-queue Specification

## Purpose
Let the owner confirm or correct every automatically recorded or doubtful row from one web page:
answer purpose questions, accept, recategorise or rename whole merchant groups, and finish one-sided
or ambiguous transfers, with every correction teaching merchant memory.

## Requirements

### Requirement: Review Summary
`GET /api/review/summary` SHALL return, for the authenticated owner, `attention` (expense and income rows with `review_status = needs_review` or an open purpose question) and `unreviewed` (`auto` rows without an open question). The navigation SHALL show a "Tinjau" entry linking to `/review` with the `attention` count as a badge whose number is also announced in words.

#### Scenario: Badge count
- **GIVEN** one open purpose question, two rows awaiting a counterpart, one ambiguous credit and two automatic merchant rows
- **WHEN** the summary is requested
- **THEN** `attention` SHALL be 4 and `unreviewed` 2.

### Requirement: Review Queue
`GET /api/review/queue` SHALL return the owner's open purpose questions, groups of `auto` and `needs_review` expense/income rows, unlinked "Internal Movement" rows awaiting a counterpart, and ambiguous movement legs with their candidate counterparts. Every row SHALL appear in at most one section. Groups SHALL be keyed by type and merchant, else the phase 3 text signature, else notes, else category; linked movement legs SHALL form one non-editable group. Each group SHALL report its count, total, first and last date, counts per review status and its open review reasons. At most 1 000 rows SHALL be read and `truncated` SHALL say when more exist.

#### Scenario: Sections
- **GIVEN** a merchant-less payment with an open question, two automatic payments at "Kedai Awan" and two GoPay sends competing for one BCA credit
- **WHEN** the queue is requested
- **THEN** the question SHALL be listed under `questions` and not in any group
- **AND** one merchant group "Kedai Awan" SHALL hold both payments with their total
- **AND** the credit and both sends SHALL be listed under `ambiguous`, each with the others as candidates.

#### Scenario: Ambiguous leg without candidates
- **GIVEN** the credit of the previous scenario was kept separate
- **WHEN** the queue is requested
- **THEN** both sends SHALL be listed under `awaiting` and `ambiguous` SHALL be empty.

#### Scenario: Resolved reasons are hidden
- **WHEN** a linked movement leg stores the reason `awaiting_counterpart`, or a row stores `purpose_question` without an open question
- **THEN** the queue SHALL not show that reason.

### Requirement: Bulk Review Actions
`POST /api/review/bulk` SHALL apply one action (`accept`, `recategorise`, `rename`, `keep_separate`) to 1–200 distinct transaction ids of the owner in one database transaction. Any id that is not the owner's SHALL reject the whole request with 404 `transactions_not_found` and change nothing. `accept` SHALL mark rows reviewed without changing their classifier. `recategorise` and `rename` SHALL reject movement legs with 409 `movement_leg_not_editable`, SHALL use the same owner-edit rule as editing one row, and SHALL write exactly one owner `correction` feedback row and owner merchant memory for each row whose classification changed. `keep_separate` SHALL mark unlinked legs reviewed and never pair them automatically again, and SHALL reject linked legs with 409 `already_linked`.

#### Scenario: Recategorise a merchant group
- **GIVEN** two automatic "Kedai Awan" payments in another category
- **WHEN** the owner recategorises both into "Jajan & Nongkrong" with pillar want
- **THEN** both rows SHALL be reviewed, classified by the owner, with that category and pillar
- **AND** exactly two correction feedback rows SHALL be written, one per row
- **AND** owner memory for `merchant:kedai awan` SHALL hold the new category
- **AND** repeating the request SHALL write no further feedback.

#### Scenario: Accept
- **WHEN** the owner accepts the two payments
- **THEN** both SHALL be reviewed, still classified by the rule, and no feedback SHALL be written.

#### Scenario: Wrong category kind
- **WHEN** the owner recategorises expense rows into an income category
- **THEN** the response SHALL be 422 `category_type_mismatch` and no row SHALL change.

#### Scenario: Foreign row in the batch
- **WHEN** the batch contains another user's transaction id
- **THEN** the response SHALL be 404 `transactions_not_found` naming that id and the owner's rows SHALL be unchanged.

#### Scenario: Movement legs in the batch
- **WHEN** a batch with a manual payment and both legs of a movement is recategorised
- **THEN** the response SHALL be 409 `movement_leg_not_editable` listing the legs and nothing SHALL change
- **AND** accepting the same batch SHALL mark all three reviewed and keep the movement linked.

#### Scenario: Invalid bodies
- **WHEN** the body has no ids, more than 200 ids, repeated ids, an unknown action, a field the action does not take, `recategorise` without a category or `rename` without a merchant or notes
- **THEN** the response SHALL be 422.

### Requirement: Tinjau Page
The `/review` page SHALL show, in Indonesian: open "Apa ini?" questions with suggestion chips and a text answer sent to the existing answer endpoint; groups with "Terima semua", "Ubah kategori semua" (category and Kakeibo pillar) and "Ganti nama" (merchant and notes) actions; each row's link to the ledger edit dialog; awaiting transfers with an account or pocket picker that completes them through `POST /api/movements/complete`; and ambiguous legs whose counterpart is linked through `POST /api/movements/merge` or kept separate. Every action SHALL be operable by keyboard, SHALL report success or failure in text, and SHALL never claim success for a failed request. Groups larger than 200 rows SHALL be sent in batches and a partial failure SHALL state how many rows were saved.

#### Scenario: Keyboard group accept
- **WHEN** the owner tabs to "Terima semua" of a group and presses Enter
- **THEN** one bulk request SHALL accept every row of the group and a status message SHALL name the group and the count.

#### Scenario: Answer by text
- **WHEN** the owner types an answer in "Jawab sendiri" and presses Enter
- **THEN** the answer SHALL be posted with the question id and a fresh reply id, and the question SHALL leave the list.

#### Scenario: Session answer passes the origin check
- **GIVEN** a browser session cookie
- **WHEN** the answer endpoint is called without an Origin header or from a foreign origin
- **THEN** the response SHALL be 403
- **AND** with the app's origin it SHALL apply the answer.

#### Scenario: Complete an awaiting transfer
- **WHEN** the owner chooses "BCA Harian" for a GoPay send and submits
- **THEN** the page SHALL call the completion endpoint with `target_account_id` of BCA Harian.
