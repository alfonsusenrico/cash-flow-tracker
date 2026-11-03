# review-undo Specification

## Purpose
Let the owner take back a Tinjau answer within ten minutes on the server, exactly and safely: the ledger, movement legs, investment products, open questions and everything the app learned return to their state before the answer, and nothing a later change wrote is ever overwritten.

## Requirements

### Requirement: Undo Token on Review Actions
Bulk accept, recategorise, rename and keep separate; movement complete and merge; top-up conversion from a transaction; "Bukan top up"; purpose answers; and history-suggestion accept and reject SHALL return `undo_token` (opaque, 43 characters) and `expires_at` (10 minutes after the action), in addition to their existing fields. Both SHALL be `null` when the action changed nothing.

#### Scenario: Token after a change
- **WHEN** the owner recategorises two automatic "Kedai Awan" rows
- **THEN** the response SHALL keep its existing fields and carry a non-null `undo_token` and an `expires_at` 10 minutes after the request

#### Scenario: No token when nothing changed
- **WHEN** the owner accepts rows that are already reviewed, replays a completed movement completion, or repeats a "Bukan top up" that is already stored
- **THEN** `undo_token` and `expires_at` SHALL be `null`

#### Scenario: Refused action issues nothing
- **WHEN** a completion is answered with 409 `counterpart_candidates`, or a conversion with 409 `transaction_not_convertible`
- **THEN** no undo record SHALL be stored

#### Scenario: Older clients
- **WHEN** a client that does not know the new fields calls any covered endpoint
- **THEN** every field it read before SHALL have the same name, type and meaning

### Requirement: Undo Endpoint
`POST /api/review/undo` with `{"undo_token"}` SHALL restore, in one database transaction, the state that the token's action changed. It SHALL answer 200 with `kind`, `already_undone: false`, `transaction_ids`, `removed_transaction_ids` and the restored rows. It SHALL accept session and Bearer authentication, and session requests SHALL pass the app's origin check.

#### Scenario: Undo an accept
- **GIVEN** two automatic rows accepted by the owner a minute ago
- **WHEN** the owner posts the accept's token
- **THEN** both rows SHALL have their earlier review status and classifier, and the response SHALL list both ids as restored

#### Scenario: Session request without origin
- **GIVEN** a browser session cookie
- **WHEN** the undo is posted without an Origin header or from a foreign origin
- **THEN** the response SHALL be 403 and nothing SHALL change

### Requirement: Undo Refusals
The undo endpoint SHALL answer 404 `undo_not_found` for an unknown token, 410 `undo_expired` once `expires_at` has passed, 409 `undo_conflict` with the changed `transaction_ids` when any row, event or record the action touched changed since, and 422 for a missing or malformed token. A refusal SHALL change nothing.

#### Scenario: Expired
- **GIVEN** an accept made 10 minutes ago
- **WHEN** its token is posted
- **THEN** the response SHALL be 410 `undo_expired` and the rows SHALL stay reviewed

#### Scenario: Unknown token
- **WHEN** a token that was never issued is posted
- **THEN** the response SHALL be 404 `undo_not_found`

#### Scenario: Malformed body
- **WHEN** the body has no token, or a token that is not 43 base64url characters
- **THEN** the response SHALL be 422

### Requirement: Single-Use Undo
A token SHALL restore at most once. A repeat after a successful undo SHALL answer 200 with `already_undone: true` and change nothing. Two concurrent undos of one token SHALL result in one restoration.

#### Scenario: Retry after a lost response
- **GIVEN** an undo that succeeded
- **WHEN** the same token is posted again
- **THEN** the response SHALL be 200 with `already_undone: true` and no row SHALL change

#### Scenario: Two requests at once
- **WHEN** two undo requests with the same token arrive together
- **THEN** exactly one SHALL restore the rows and the other SHALL answer `already_undone: true`

### Requirement: Undo Is Owner-Scoped
A token SHALL only be usable by the owner whose action issued it. Another owner's token SHALL be answered with 404 `undo_not_found`, never with 403, and every restoration SHALL write only the issuing owner's rows.

#### Scenario: Another owner's token
- **GIVEN** owner A's recategorise token
- **WHEN** owner B posts it
- **THEN** the response SHALL be 404 `undo_not_found` and A's rows SHALL stay recategorised

### Requirement: Classification Undo Restores the Earlier State
Undoing an accept, recategorise, rename, keep separate, purpose answer or history-suggestion decision SHALL restore each touched row's category, pillar, notes, merchant, offset category, review status, classifier, confidence, owner-edit time and owner-split time. It SHALL reopen a purpose question the action closed, with its original question id, and return a decided suggestion to pending.

#### Scenario: Recategorise undone
- **GIVEN** an AI-classified automatic row recategorised by the owner into "Jajan & Nongkrong"
- **WHEN** the owner undoes it
- **THEN** the row SHALL have its earlier category, pillar, `review_status = auto` and `classified_by = ai`, and its owner-edit time SHALL be empty again

#### Scenario: Purpose answer undone
- **GIVEN** a merchant-less payment answered with a suggestion
- **WHEN** the owner undoes the answer
- **THEN** the row SHALL have its earlier values, the event's purpose question SHALL be open again with the same `question_id`, and the row SHALL be listed under "Apa ini?" again

#### Scenario: Keep separate undone
- **GIVEN** an ambiguous leg kept separate
- **WHEN** the owner undoes it
- **THEN** the leg SHALL have no owner-split time and its earlier review status, and automatic pairing MAY link it again

#### Scenario: Suggestion accept undone
- **GIVEN** an accepted history suggestion
- **WHEN** the owner undoes the accept
- **THEN** the row SHALL have its earlier values and the suggestion SHALL be pending again

### Requirement: Movement Undo Restores Both Legs
Undoing a movement completion or merge SHALL unlink both legs and restore their earlier values and review state, SHALL delete a leg the completion created, and SHALL restore the notification results the action rewrote. No other row SHALL be written, and every involved account's balance SHALL equal its balance before the action.

#### Scenario: Completion with a new leg undone
- **GIVEN** a GoPay send of Rp 50.000 completed to "BCA Harian", which created the income leg
- **WHEN** the owner undoes the completion
- **THEN** the created leg SHALL no longer exist, the send SHALL be unlinked with its earlier category, notes and review status, both balances SHALL equal their balances before the completion, and the send SHALL be listed under "Pindah saldo belum lengkap" again

#### Scenario: Completing again after undo
- **WHEN** the owner completes the same send again after the undo
- **THEN** a new leg SHALL be created and linked

#### Scenario: Merge undone
- **GIVEN** an ambiguous send merged with a listed credit
- **WHEN** the owner undoes the merge
- **THEN** both rows SHALL be unlinked with their earlier categories, notes and review state, and neither SHALL be marked as split by the owner

### Requirement: Top-up Conversion Undo
Undoing a top-up conversion SHALL remove the top-up through the existing top-up deletion. That deletes its inbound leg, restores the debit as an expense and takes the amount off the product's value and cost basis. It SHALL also restore the debit's review state and classifier. When the product is unchanged since, its tracking mode, value, cost basis and estimate flag SHALL equal their earlier values.

#### Scenario: Conversion undone
- **GIVEN** a Rp 112.590 awaiting debit from "RDN Contoh" converted into "Bibit Contoh · Reksa Dana A"
- **WHEN** the owner undoes the conversion
- **THEN** the top-up SHALL be deleted, its inbound leg SHALL no longer exist, the debit SHALL be an unlinked expense with its earlier category, notes, review status and classifier, the RDN balance SHALL be unchanged, and the product's value and cost basis SHALL equal their values before the conversion

#### Scenario: Product valued in between
- **GIVEN** the owner entered a newer valuation for the product after the conversion
- **WHEN** the conversion is undone
- **THEN** the product's value and cost basis SHALL each decrease by Rp 112.590, as deleting the top-up does

#### Scenario: Notification retry after undo
- **WHEN** the debit's original notification is sent again after the undo
- **THEN** no new transaction SHALL be recorded

### Requirement: Undo Never Overwrites a Later Change
Before writing anything, the undo SHALL compare every row, event and top-up the action touched or created with their state right after the action. Any difference SHALL refuse the undo with 409 `undo_conflict` naming the changed transactions, and nothing SHALL be written.

#### Scenario: Edited in Transaksi meanwhile
- **GIVEN** a row recategorised in Tinjau and then edited in Transaksi
- **WHEN** the Tinjau action is undone
- **THEN** the response SHALL be 409 `undo_conflict` naming the row and the Transaksi edit SHALL stay

#### Scenario: Two tabs
- **GIVEN** tab A recategorised a group and tab B then renamed the same rows
- **WHEN** tab A undoes its recategorise
- **THEN** it SHALL be refused with 409 and tab B's undo SHALL still succeed

#### Scenario: Paired by a notification meanwhile
- **GIVEN** a leg kept separate and then changed by notification processing
- **WHEN** the keep-separate is undone
- **THEN** the response SHALL be 409 and the leg SHALL keep its current state

### Requirement: Undone Labels Are Not Evidence
Undo SHALL append one `undo` feedback row per feedback row the action wrote, naming the reverted row, and SHALL never update a feedback row. Habits, purpose suggestions, the repayment-offset preference and the weekly question rate SHALL ignore undo rows and the rows they revert. A transaction's latest remaining label SHALL count again.

#### Scenario: Correction undone
- **GIVEN** a merchant-less habit key with two agreeing owner answers and a third payment the owner corrected
- **WHEN** the correction is undone
- **THEN** the habit evidence SHALL be the two answers only, exactly as before the correction

#### Scenario: Earlier label counts again
- **GIVEN** a transaction with a purpose answer and a later correction
- **WHEN** the correction is undone
- **THEN** the purpose answer SHALL count for that transaction again

#### Scenario: Redo counts once
- **WHEN** the owner undoes a correction and then makes the same correction again
- **THEN** exactly one label SHALL count for that transaction

#### Scenario: Append-only
- **WHEN** any undo runs
- **THEN** no existing `classification_feedback` row SHALL be updated or deleted, and a feedback row SHALL be reverted at most once

### Requirement: Memory After Undo
Undo SHALL return each merchant-memory entry the action wrote to its earlier value, or remove it if it did not exist, but only while the entry is unchanged since the action. A newer entry SHALL stay.

#### Scenario: Memory restored
- **GIVEN** owner memory for `merchant:kedai awan` written by a recategorise
- **WHEN** the recategorise is undone and nothing else wrote that key
- **THEN** the entry SHALL equal its value before the recategorise, or SHALL not exist if it did not exist before

#### Scenario: Newer memory kept
- **GIVEN** a later answer that rewrote the same memory key
- **WHEN** the earlier action is undone
- **THEN** the later entry SHALL stay

### Requirement: Undo Leaves Debt and Goal Progress Unchanged
No covered review action changes debt or goal progress, and undo SHALL never write obligations, debt allocations or goals.

#### Scenario: Debt-linked row
- **GIVEN** a debt payment row in a review group
- **WHEN** the owner recategorises the group and undoes it
- **THEN** the debt's remaining amount, archived state and allocations, and every goal's current amount, SHALL be unchanged throughout

### Requirement: Undo Snapshot Retention
The stored before and after state SHALL be cleared as soon as the token is used or expires, and the undo record SHALL be deleted 24 hours after the action. Logs SHALL carry only the action kind and outcome code, never merchants, notes, amounts or snapshots.

#### Scenario: After expiry
- **GIVEN** an action 11 minutes ago whose token was never used
- **WHEN** the cleanup has run
- **THEN** no merchant, note or amount of that action SHALL remain in its undo record, and the token SHALL be answered with 410

#### Scenario: After a day
- **WHEN** the token of an action from 25 hours ago is posted
- **THEN** the response SHALL be 404 `undo_not_found`
