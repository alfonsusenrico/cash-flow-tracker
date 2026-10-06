# Spec Delta

## MODIFIED Requirements

### Requirement: Bank Jago Child Pocket Transfer Resolution
The system SHALL resolve source and destination child pockets when ingesting internal movement notifications from Bank Jago (`com.jago.digitalbanking` / `com.jago.digitalBanking`):
1. When a notification matches pocket-to-pocket movement with `source_pocket` and `target_pocket`, the ingestion engine SHALL search the user's active child accounts (`parent_id = bank_jago_id`) for matching names.
2. The matching algorithm SHALL support exact name matching, normalized name matching (ignoring the redundant word "Pocket"), and case-insensitive substring matching.
3. If `source_pocket` matches a child pocket, `transactions.account_id` SHALL be assigned that child pocket's ID; otherwise, for a named internal movement whose source cannot be mapped, it SHALL preserve the existing review/fallback behavior without inventing a different child pocket.
4. If `target_pocket` matches a child pocket, `transactions.transfer_target_account_id` SHALL be assigned that child pocket's ID.
5. The created transaction SHALL have `type = 'transfer'` and `kakeibo_type = NULL`.
6. A verified settled Jago outbound transfer that has no source-pocket evidence SHALL enter the source-pocket confirmation flow before any Jago balance or movement record is written. The system SHALL offer the owner all current eligible child pockets under the owned Bank Jago parent, including the configured main pocket, without selecting one by default.
7. The source choice SHALL be scoped to the event and SHALL NOT create or update a generic account alias, `default_pocket_id`, or future default for unnamed transfers.
8. After an authenticated owner confirms an eligible pocket, the system SHALL resume the normal transaction and bilateral-pairing pipeline using that selected account. An unanswered, stale, invalid, or conflicting choice SHALL remain pending or enter review and SHALL NOT fall back to the main pocket.

#### Scenario: Moving funds between Main Pocket and GoPay Tabungan Pocket
- **WHEN** Bank Jago sends a notification *"Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket"*
- **THEN** the system SHALL create a transaction with `type = 'transfer'`
- **AND** `account_id` SHALL match the child pocket "Kantong Utama" / "Main" or parent Bank Jago
- **AND** `transfer_target_account_id` SHALL match the child pocket "GoPay Tabungan"
- **AND** `kakeibo_type` SHALL be `NULL`

#### Scenario: Moving funds between custom pockets
- **WHEN** Bank Jago sends a notification *"Rp50.000 has been moved from your Jajan Pocket to your Tabungan Pocket"*
- **THEN** the system SHALL link `account_id` to the child pocket "Jajan" and `transfer_target_account_id` to the child pocket "Tabungan"
- **AND** `type` SHALL be `'transfer'` and amount SHALL be `50000`

#### Scenario: Unnamed outbound transfer is held before recording
- **WHEN** a verified Jago notification says the owner transferred Rp125.000 to an external recipient and contains no source-pocket evidence
- **THEN** the event SHALL be `needs_confirmation` with a `source_pocket` question
- **AND** the question SHALL contain the event question ID, proven amount/currency, Jago parent context, and every current eligible Jago pocket
- **AND** no transaction, movement, reconciliation flag, or consumed counterpart role SHALL be written for the Jago event
- **AND** no pocket SHALL be preselected

#### Scenario: Owner selects a non-main pocket for an unnamed transfer
- **WHEN** the owner submits a valid event question ID, unique reply ID, and the active Jago child pocket "Dana Darurat"
- **THEN** the answer SHALL be accepted idempotently and the event SHALL resume normal processing
- **AND** the recorded expense SHALL use "Dana Darurat" as its account
- **AND** the answer SHALL not change Jago defaults or aliases used by a later unnamed transfer

#### Scenario: Unnamed transfer is paired only after source confirmation
- **WHEN** an independently observed BCA leg with the same proven transfer evidence arrives before the owner answers the Jago source question
- **THEN** the BCA event MAY record its own observed leg through its normal path
- **AND** the Jago event SHALL remain unrecorded and SHALL not create or link a bilateral movement
- **WHEN** the owner subsequently selects the eligible Jago source
- **THEN** the system SHALL pair the two legs only if all existing uniqueness, timing, reference, and ownership checks pass
- **AND** the movement source SHALL be the selected Jago pocket

#### Scenario: Source answer is stale or no longer eligible
- **WHEN** a reply references a superseded question, another owner's event, an archived pocket, a non-Jago account, or a pocket outside the event's owner
- **THEN** the API SHALL reject it with a bounded conflict or authorization response
- **AND** the event SHALL remain pending or enter review
- **AND** the system SHALL not debit the main pocket or any other account

#### Scenario: Existing named Jago movement remains automatic
- **WHEN** a Jago notification explicitly names an active source pocket and target pocket
- **THEN** the existing exact, normalized, or case-insensitive matching behavior SHALL continue to resolve those named endpoints
- **AND** the new source-pocket question SHALL not be shown

### Requirement: Multi-App Notification Ingestion Scope
The system SHALL support push notification ingestion from the 6 active registered companion apps:
1. `myBCA` (`com.bca.mybca.omni.android`, `id.co.bca.mybca.omni.android`)
2. `BCA mobile` (`com.bca`)
3. `Bank Jago` (`com.jago.digitalbanking`, `com.jago.digitalBanking`)
4. `GoPay` (`com.gojek.gopay`, `com.gojek.app`, `com.gopay.wallet`)
5. `ShopeePay` (`com.shopeepay.id`)
6. `Stockbit` (`com.stockbit.android`)
The system SHALL NOT process unsupported apps (such as Blu by BCA) and SHALL postpone Bibit ingestion until real notification payload samples are provided.

#### Scenario: Ingesting valid financial notification from supported app
- **WHEN** an incoming batch contains a notification event from `com.gojek.app` for an outbound payment
- **THEN** the system SHALL parse the event, map it to the user's GoPay account, and record the transaction in the ledger

#### Scenario: Ingesting notification from unmapped source
- **WHEN** an incoming notification is received without a matching configured institution
- **THEN** the system SHALL fall back to the user's primary liquid account without crashing
