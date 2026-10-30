# notification-ingestion Specification

## Purpose
Define supported banking notification ingestion and resolve named Bank Jago pocket movement endpoints.

## Requirements

### Requirement: Bank Jago Child Pocket Transfer Resolution
The system SHALL resolve Jago movement endpoints from the user's active registered accounts and child pockets under the same Jago parent.

#### Scenario: Required behavior and constraints
- **WHEN** a Jago internal movement notification is ingested
- **THEN** the following required behavior and constraints hold:

  The system SHALL resolve Jago movement endpoints from the user's active registered accounts and child pockets under the same Jago parent. Exact and normalized names, including redundant `Pocket`/`Kantong` terms and supported aliases, SHALL be considered; substring or semantic matching SHALL NOT resolve an ambiguous name arbitrarily. An implicit main endpoint SHALL resolve only to that parent's explicitly configured valid default pocket, uniquely identified registered main pocket, or valid parent main balance. Unknown named pockets SHALL NOT be created or replaced with a parent fallback. A verified complete movement SHALL atomically create the canonical linked expense/income pair, preserving amount and original timestamp, and apply the canonical movement Kakeibo rule rather than a legacy `transfer` transaction type. The processor SHALL NOT wait for a second notification for an explicit Jago pocket movement. A verified settled Jago outbound transfer without source-pocket evidence SHALL enter the source-pocket confirmation flow before any Jago balance or movement record is written; the owner's choice SHALL be scoped to that event, and an unanswered, stale, invalid, or conflicting choice SHALL NOT fall back to the main pocket.

#### Scenario: Moving funds between Main Pocket and GoPay Tabungan Pocket
- **WHEN** Jago reports `Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket` and both endpoints resolve uniquely within the registered Jago hierarchy
- **THEN** the backend records exactly one linked bilateral movement for 500000 IDR between the effective main endpoint and the registered GoPay Tabungan pocket

#### Scenario: Moving funds between custom pockets
- **WHEN** Jago reports movement from the registered Jajan pocket to the registered Tabungan pocket for 50000 IDR
- **THEN** both records share a movement identifier, both endpoints belong to the same Jago parent, and the account balances reflect only that one movement

#### Scenario: Rejecting an unknown named pocket
- **WHEN** a named Jago movement endpoint is absent or ambiguous in the user's registered hierarchy
- **THEN** no liquid pocket is created and no guessed movement is recorded
- **AND** the event waits for an account-mapping confirmation or review

#### Scenario: Unnamed outbound transfer is held before recording
- **WHEN** a verified Jago notification says the owner transferred Rp125.000 to an external recipient and contains no source-pocket evidence
- **THEN** the event SHALL be `needs_confirmation` with a `source_pocket` question
- **AND** the question SHALL carry the event question ID, proven amount/currency and Jago parent context, and `GET /api/ingest/notifications/{event_id}/source-pockets` SHALL list every current eligible Jago pocket
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
- **THEN** the existing exact or normalized name matching SHALL continue to resolve those named endpoints
- **AND** the new source-pocket question SHALL not be shown

### Requirement: Multi-App Notification Ingestion Scope
The system SHALL continue to support authenticated backend ingestion from myBCA (`com.bca.mybca.omni.android`, `id.co.bca.mybca.omni.android`), BCA mobile (`com.bca`), Bank Jago (`com.jago.digitalbanking`, `com.jago.digitalBanking`), GoPay (`com.gojek.gopay`, `com.gojek.app`, `com.gopay.wallet`), ShopeePay (`com.shopeepay.id`, `com.shopee.id`), and Stockbit (`com.stockbit.android`).

#### Scenario: Required behavior and constraints
- **WHEN** a companion application submits a notification for ingestion
- **THEN** the following required behavior and constraints hold:

  The system SHALL continue to support authenticated backend ingestion from myBCA (`com.bca.mybca.omni.android`, `id.co.bca.mybca.omni.android`), BCA mobile (`com.bca`), Bank Jago (`com.jago.digitalbanking`, `com.jago.digitalBanking`), GoPay (`com.gojek.gopay`, `com.gojek.app`, `com.gopay.wallet`), ShopeePay (`com.shopeepay.id`, `com.shopee.id`), and Stockbit (`com.stockbit.android`). Unsupported apps, including Blu and deferred Bibit ingestion, SHALL NOT create ledger effects. Source institutions SHALL resolve from the authenticated user's registered data and supported package evidence; missing or ambiguous mapping SHALL NOT fall back to an unrelated primary or first liquid account. Existing deterministic broker trade processing SHALL remain isolated from generic AI liquid operations. Settlement, direction, and balance cue recognition SHALL cover outbound transfer wording (`sent`, `dikirim`, `mengirim`) and balance phrases (`remaining balance`, `saldo kamu`), and myBCA outbound `Account Transfer` notifications SHALL be treated as settled outgoing candidates.

#### Scenario: Ingesting valid financial notification from supported app
- **WHEN** a supported GoPay event confirms an outbound payment and resolves uniquely to an owned active GoPay account
- **THEN** the system validates and records the observed payment using that account

#### Scenario: Ingesting notification from unmapped source
- **WHEN** a supported package has no unambiguous registered source institution
- **THEN** no unrelated liquid account receives its financial effect
- **AND** the event waits for an account-mapping confirmation or review instead

#### Scenario: Receiving an unsupported package
- **WHEN** a notification comes from an unsupported app
- **THEN** the result is ignored without ledger effects or an external inference call

#### Scenario: Outbound transfer wording is a settled expense
- **WHEN** a Bank Jago notification reads "You've sent Rp1.500.000 to [owner alias]."
- **THEN** the backend treats it as a settled outgoing candidate rather than `settlement_unproven`, and an unnamed Jago source then waits for source-pocket confirmation

#### Scenario: Second amount in a balance phrase is ignored
- **WHEN** a notification states the transaction amount and "Your remaining balance is Rp950.000"
- **THEN** only the transaction amount is used as evidence

### Requirement: Companion Ledger Edit Identity
The backend SHALL distinguish an event identifier and logical result key from the actual ledger identifier used for editing.

#### Scenario: Required behavior and constraints
- **WHEN** the companion opens a ledger edit from a notification result
- **THEN** the following required behavior and constraints hold:

  The backend SHALL distinguish an event identifier and logical result key from the actual ledger identifier used for editing. Recorded ordinary expense/income results SHALL optionally expose `transaction_id` only when a current owned ordinary transaction still exists and is not a movement leg or trade. Movement/trade, deleted, queued, ignored, review, and failed results SHALL NOT expose an editable ordinary transaction identifier. The field SHALL be additive to the approved compact result contract and SHALL NOT change record keys, amounts, financial effects, or owner isolation. Existing clients SHALL remain able to ignore it.

#### Scenario: Ordinary recorded result
- **WHEN** an owned ordinary expense or income has committed and remains an ordinary ledger record
- **THEN** its batch/lookup result includes the actual editable transaction UUID separately from `event_id` and `record_key`

#### Scenario: Movement or deleted record
- **WHEN** the result identifies a movement/trade or its ordinary ledger row no longer exists
- **THEN** the result does not provide an editable ordinary transaction UUID and no transaction is recreated

#### Scenario: Foreign event or pending work
- **WHEN** a caller requests another owner's event or a record has not committed
- **THEN** no editable transaction identity is revealed

#### Scenario: Older companion compatibility
- **WHEN** a companion ignores the additional identity field
- **THEN** existing batch counters and compact financial result fields keep their approved behavior

### Requirement: Concurrency-Safe Notification Application
The system SHALL claim each `(user, payload_hash)` event before applying financial effects and SHALL associate that event with at most one logical ledger operation. Concurrent or repeated deliveries SHALL return the existing result without creating additional transactions, positions, pockets, or account effects.

#### Scenario: Receiving the same notification concurrently
- **WHEN** two authenticated requests submit the same user's payload hash at the same time
- **THEN** exactly one notification event and one corresponding logical ledger operation are committed

### Requirement: Durable Idempotent Notification Processing
The system SHALL persist accepted financial candidates and processing status before external inference, keyed uniquely by authenticated user and payload hash.

#### Scenario: Required behavior and constraints
- **WHEN** a financial candidate is accepted, processed or redelivered
- **THEN** the following required behavior and constraints hold:

  The system SHALL persist accepted financial candidates and processing status before external inference, keyed uniquely by authenticated user and payload hash. It SHALL expose `queued`, `processing`, `recorded`, `needs_review`, `needs_confirmation`, `ignored`, or `failed` as appropriate. Repeated deliveries, simultaneous workers, expired worker ownership, and process restarts SHALL NOT duplicate ledger effects. A per-event failure SHALL NOT roll back another event's accepted state. Ordinary redelivery SHALL return existing state, not overwrite source facts or replay a completed ledger operation.

#### Scenario: Concurrent delivery of the same candidate
- **WHEN** two requests submit the same user and payload hash simultaneously
- **THEN** exactly one accepted event exists and no more than one corresponding logical ledger operation is committed

#### Scenario: Recovering a worker after restart
- **WHEN** a worker stops after claiming an accepted event
- **THEN** a replacement worker can recover it after ownership expires without duplicating a previously committed operation

#### Scenario: Isolating failure inside a batch
- **WHEN** one event cannot be interpreted or applied while other batch events are valid
- **THEN** each event retains its own durable state and the failed event does not erase the accepted or recorded results of the others

### Requirement: Notification Movement Provenance and Counterpart Deduplication
The system SHALL distinguish a leg observed directly in a notification from a counterpart synthesized for an explicit complete movement. A later notification that uniquely confirms an existing unconsumed movement role SHALL attach to that role without another financial effect. A movement role SHALL NOT consume multiple unrelated notifications. Historical movements without sufficient provenance SHALL NOT be rewritten or replayed automatically.

#### Scenario: Receiving a second notification for an already recorded Jago movement
- **WHEN** a notification uniquely confirms an unconsumed role of an explicit Jago movement within the supported evidence/time window
- **THEN** it links to the existing logical operation and creates zero additional transactions

#### Scenario: Handling an ambiguous historical pair
- **WHEN** a historical movement lacks enough evidence to establish whether a new event confirms it
- **THEN** the backend does not silently reuse, rewrite, or double-record that movement and retains the ambiguity for review

### Requirement: Compact Committed Notification Results
Batch ingestion SHALL preserve `ok`, `received`, `inserted`, `updated`, and `created_transactions` and add one result per submitted event in input order.

#### Scenario: Required behavior and constraints
- **WHEN** the backend returns results for a notification batch
- **THEN** the following required behavior and constraints hold:

  Batch ingestion SHALL preserve `ok`, `received`, `inserted`, `updated`, and `created_transactions` and add one result per submitted event in input order. Each result SHALL identify its submitted `payload_hash` and its `status`. A committed result SHALL include stable `record_key`, `type`, concise `description`, source/target display strings, numeric `amount`, and `currency`, with `target` null when inapplicable. `event_id` SHALL support lookup for durably accepted asynchronous events. `recorded` SHALL be returned only after commit; queued acceptance or provider success alone SHALL NOT indicate transaction success. Redelivery SHALL reuse the committed key and results; a reconciled movement's notifications SHALL identify the same logical operation. Counters SHALL describe the current HTTP request, not eventual asynchronous ledger writes.

#### Scenario: Acknowledging queued work
- **WHEN** ingestion durably accepts an event that requires asynchronous processing
- **THEN** the response identifies its payload hash, event ID, and `queued` status without presenting it as a recorded transaction

#### Scenario: Returning an ordinary recorded expense
- **WHEN** a validated expense has committed
- **THEN** its compact result contains the transaction description, source, amount, currency, `type = expense`, and `target = null`

#### Scenario: Returning a reconciled movement
- **WHEN** two observed legs are linked into a movement
- **THEN** both event lookups return `type = internal_movement`, consistent source/target/amount, and the stable logical-operation key assigned to the first committed observed leg

#### Scenario: Redelivering a recorded notification
- **WHEN** the mobile app resubmits an already recorded hash
- **THEN** the backend returns its existing logical-operation key and creates no new ledger effect

### Requirement: Owner-Scoped Processing Inspection and Retry
The backend SHALL expose the same compact event result through `GET /api/ingest/notifications/{event_id}/result`, and SHALL expose safe processing/error metadata through existing owner-scoped notification inspection. Explicit retry SHALL be limited to owned unresolved events, SHALL preserve source facts, and SHALL reject replay of recorded or ignored events. Label edits alone SHALL NOT silently create or rewrite ledger effects. Cross-user lookup and retry SHALL reveal no event contents.

#### Scenario: Looking up completion with the companion Bearer key
- **WHEN** the authenticated owner looks up an accepted event after processing
- **THEN** the result reflects its current durable state and provides the approved compact record only after commit

#### Scenario: Attempting another user's event lookup
- **WHEN** an authenticated user requests lookup or retry for another user's event
- **THEN** the backend returns not found without revealing that event or its processing details

#### Scenario: Retrying after configuration repair
- **WHEN** the owner explicitly retries an unresolved event after fixing registered mapping or provider availability
- **THEN** the same event is queued for revalidation without overwriting its captured source payload

#### Scenario: Retrying while optional AI is disabled
- **WHEN** the owner retries an unresolved event originally assigned to deterministic processing while AI is disabled
- **THEN** the event retains deterministic processing and can complete through the available worker

#### Scenario: Rejecting AI retry without a worker
- **WHEN** the owner retries an unresolved AI event while AI processing is unavailable
- **THEN** the backend rejects the retry without changing its durable state or reporting it queued

### Requirement: Owner-Authorized Development Notification Dry Run
The backend SHALL provide owner-authenticated dry-run interpretation only in an explicitly enabled development environment. `POST /api/ingest/notifications/dry-run` SHALL accept a normal notification payload without persisting it, and `POST /api/ingest/notifications/{event_id}/dry-run` SHALL read exactly one owned stored event.

#### Scenario: Required behavior and constraints
- **WHEN** an owner requests notification interpretation through a development dry-run route
- **THEN** the following required behavior and constraints hold:

  The backend SHALL provide owner-authenticated dry-run interpretation only in an explicitly enabled development environment. `POST /api/ingest/notifications/dry-run` SHALL accept a normal notification payload without persisting it, and `POST /api/ingest/notifications/{event_id}/dry-run` SHALL read exactly one owned stored event. Both routes SHALL use current active owner context and the same fact, provider, redaction, and trusted-validation boundaries as AI processing, but SHALL create no notification event, transaction, movement, lease, retry, or balance/state mutation. They SHALL not start or require the normal processing worker. Responses SHALL contain only a safe dry-run status, proposed compact financial fields when available, and a safe validation/provider error code; raw notification text and provider diagnostic bodies SHALL not be returned or persisted.

#### Scenario: Testing a direct local mobile payload without a write
- **WHEN** an authenticated owner submits a supported notification to the enabled development dry-run route
- **THEN** the response reports the validated proposed description, type, source/target, amount, category, and Kakeibo when available, and no event or ledger row is created

#### Scenario: Testing one stored local notification
- **WHEN** an authenticated owner requests dry run for one of their locally stored notification event identifiers
- **THEN** only that event is read with current owner context, its durable event state and all ledger balances remain unchanged, and no other queued event is processed

#### Scenario: Rejecting dry run outside local development
- **WHEN** the route is called without explicit dry-run enablement or outside the development environment
- **THEN** the backend returns not found before loading event/context or sending notification data to OpenAI

#### Scenario: Preventing an external call for deterministic routes
- **WHEN** a dry-run payload is ignored by the entrance filter or belongs to deterministic Stockbit processing
- **THEN** the response identifies the deterministic dry-run outcome and no OpenAI request is made

### Requirement: Settled Notification Balance Reconciliation Preservation
The processor SHALL retain the existing ingestion exception for verified settled events that exceed the tracked liquid balance: record the real-world financial effect and flag the affected account for reconciliation. Model availability or insufficient tracked balance SHALL NOT authorize fabricated funds, relaxed user-initiated balance rules, or a successful result before commit.

#### Scenario: Recording a settled expense against a stale balance
- **WHEN** verified notification evidence reports a 200000 IDR expense against a tracked balance of 150000 IDR
- **THEN** the expense is recorded once, the affected account is flagged for reconciliation, and ordinary manual overdraft protection remains unchanged

### Requirement: Owner Identity Aliases and Masked Name Matching
The system SHALL maintain per-owner name aliases (defaulting to the profile name) editable through the settings API, and SHALL treat notification text as naming the owner when any alias matches literally or through bank masking, where masked words align by position with alias words, asterisks stand for single letters, and at least two literal letters are present.

#### Scenario: Masked BCA sender matches the owner
- **WHEN** a BCA income names the sender "RA*A PU***MA" and an alias "Raka Purnama" is configured
- **THEN** the event is treated as a self-transfer candidate and the sender is redacted as `[SELF]` in model context

#### Scenario: Aliases updated through settings
- **WHEN** the owner saves up to ten aliases through the settings endpoint
- **THEN** subsequent processing uses the new list without a restart

### Requirement: Deterministic Fallback and Failure State Separation
When AI interpretation returns `needs_review` or a non-transient provider error occurs, the system SHALL attempt the deterministic interpretation and record it when it validates, marking the provenance as a fallback. Transient and configuration provider failures SHALL leave the event `failed` with an escalating retry schedule (capped attempts within 48 hours) that the worker resumes automatically, never `needs_review`. Uncertain mapping waits in `needs_confirmation` or `needs_review`.

#### Scenario: Model uncertainty on a deterministic-resolvable event
- **WHEN** the model returns `needs_review` for a ShopeePay income whose amount, direction, and effective account are proven
- **THEN** the event is recorded through the deterministic path and its result shows no error code

#### Scenario: Provider outage recovers without owner action
- **WHEN** the provider is unavailable for twenty minutes
- **THEN** affected events stay `failed` with a future retry time and are recorded once the provider returns

#### Scenario: Genuine mapping ambiguity is not retried as an outage
- **WHEN** the observed pocket cannot be resolved uniquely
- **THEN** the event waits in `needs_confirmation` or `needs_review` with the mapping reason and is not retried automatically

### Requirement: Owner-Scoped Manual Resolution
The backend SHALL expose an owner-scoped resolve operation that records exactly one expense or income for an unresolved notification event, marks the event recorded with manual provenance, and runs the pairing pass.

#### Scenario: Required behavior and constraints
- **WHEN** an owner resolves a notification event requiring review
- **THEN** the following required behavior and constraints hold:

  The backend SHALL expose an owner-scoped resolve operation for events in `needs_review`, `needs_confirmation` or `failed` that records exactly one expense or income using the event's single proven amount (or an owner-entered amount only when the notification has no single proven amount) and its captured timestamp, the owner-chosen liquid account and matching-kind category, and an idempotency key derived from the event, then marks the event recorded with manual provenance and runs the pairing pass. It SHALL refuse with 409 `source_pocket_confirmation_required` while a Jago source-pocket answer is pending, reject recorded or ignored events with 409, and answer 404 for a foreign event without revealing its contents.

#### Scenario: Resolving a stuck event from the phone
- **WHEN** the owner resolves a `needs_review` event with an owned account and a category of the same kind
- **THEN** one transaction is created, the event becomes `recorded`, and a later counterpart can still pair with it

#### Scenario: Owner supplies the amount for a fee-and-total notification
- **WHEN** a `needs_review` event states a payment, a fee, and a total, and the owner resolves it with the total
- **THEN** one transaction for the owner-entered amount is recorded; an owner amount that contradicts a single proven amount is rejected

#### Scenario: Resolving twice is idempotent
- **WHEN** the same resolve request is repeated
- **THEN** no second transaction is created and the recorded result is returned

### Requirement: Neutral Naming for Movements and Self-Transfers
When the system links two notification-recorded legs into one movement, automatically or through a manual merge that includes a notification-recorded leg, both transactions SHALL be described as "Pindah saldo ke <destination account>". A leg recorded alone whose counterparty is the owner SHALL be described as "Pindah saldo masuk" or "Pindah saldo keluar" instead of the owner's name. Notes on manually entered transactions SHALL NOT be rewritten by a merge.

#### Scenario: Top-up legs linked into a movement
- **WHEN** a BCA debit labelled "Belanja" by the interpreter is linked with a ShopeePay top-up
- **THEN** both transactions read "Pindah saldo ke ShopeePay"

#### Scenario: Owner-named incoming transfer without its counterpart
- **WHEN** Jago reports "<owner> has sent Rp7.491.557 to you" and no counterpart exists yet
- **THEN** the income is recorded under Internal Movement with the description "Pindah saldo masuk"

#### Scenario: Manual merge of hand-entered transactions
- **WHEN** the owner merges two manually entered transactions
- **THEN** their notes are unchanged

### Requirement: Institution Balances Hosted as Pockets
When no top-level account is named after a notification's institution, the system SHALL map that institution's notifications to the single active pocket whose name contains the institution name, so a balance shown in one app but held by another bank (GoPay Tabungan held in Bank Jago) is one account.

#### Scenario: Required behavior and constraints
- **WHEN** an institution notification must be mapped to a registered account or pocket
- **THEN** the following required behavior and constraints hold:

  When no top-level account is named after a notification's institution, the system SHALL map that institution's notifications to the single active pocket whose name contains the institution name, so a balance shown in one app but held by another bank (GoPay Tabungan held in Bank Jago) is one account. A top-level account named after the institution SHALL keep precedence. The owner SHALL be able to move an existing stand-alone liquid account without pockets under another top-level account, keeping its transaction history.

#### Scenario: GoPay payment debits the GoPay Tabungan pocket in Jago
- **WHEN** the owner has moved the GoPay account under Jago as "GoPay Tabungan" and the GoPay app reports a QRIS payment
- **THEN** the expense is recorded on "Bank Jago · GoPay Tabungan"

#### Scenario: Jago pocket move into the hosted pocket
- **WHEN** Jago reports "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
- **THEN** a movement from Main Pocket to GoPay Tabungan is recorded

### Requirement: Securities Fund Account Deposits Reported Twice
The system SHALL record a deposit into the owner's securities fund account (RDN) once when both the bank (myBCA "RDN earning … at … category") and the broker (Stockbit "Dana kamu senilai Rp… sudah dapat digunakan") report it.

#### Scenario: Required behavior and constraints
- **WHEN** bank and broker notifications describe a deposit into the same securities fund account
- **THEN** the following required behavior and constraints hold:

  The system SHALL record a deposit into the owner's securities fund account (RDN) once when both the bank (myBCA "RDN earning … at … category") and the broker (Stockbit "Dana kamu senilai Rp… sudah dapat digunakan") report it. Whichever report is processed first SHALL record an own-account transfer into the RDN; a report from the other source with the same amount within 48 hours SHALL attach to that record without creating a transaction. Stockbit deposit reports SHALL map to the broker's configured funding account. Stockbit trade notifications SHALL keep the trade path.

#### Scenario: Broker confirms hours after the bank
- **WHEN** myBCA reports "RDN earning of IDR 225,180.00 at Account Transfer category." at 23:59 and Stockbit reports "Dana kamu senilai Rp225,180 sudah dapat digunakan" at 03:01
- **THEN** exactly one income of Rp225.180 into the RDN exists, filed as Internal Movement, and both events show the same recorded result

#### Scenario: Only the broker report arrives
- **WHEN** only the Stockbit deposit report is captured
- **THEN** it records the deposit into the broker's funding account

### Requirement: Splitting an Automatically Paired Movement
The system SHALL let the owner split a non-trade internal movement back into two independent transactions that keep their accounts, dates, and notes and their amounts, except that a transfer fee taken off the outbound leg at pairing returns to it, restoring each leg's notification-derived category where known and detaching the notification events from the movement.

#### Scenario: Undoing a wrong pairing
- **WHEN** the owner splits a movement created by automatic pairing
- **THEN** both transactions remain with their original balance effects and can be merged again manually
