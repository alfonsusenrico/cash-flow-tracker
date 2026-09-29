# Spec Delta

## MODIFIED Requirements

### Requirement: Bank Jago Child Pocket Transfer Resolution
The system SHALL resolve Jago movement endpoints from the user's active registered accounts and child pockets under the same Jago parent. Exact and normalized names, including redundant `Pocket`/`Kantong` terms and supported aliases, SHALL be considered; substring or semantic matching SHALL NOT resolve an ambiguous name arbitrarily. An implicit main endpoint SHALL resolve only to that parent's explicitly configured valid default pocket, uniquely identified registered main pocket, or valid parent main balance. Unknown named pockets SHALL NOT be created or replaced with a parent fallback. A verified complete movement SHALL atomically create the canonical linked expense/income pair, preserving amount and original timestamp, and apply the canonical movement Kakeibo rule rather than a legacy `transfer` transaction type. The processor SHALL NOT wait for a second notification for an explicit Jago pocket movement.

#### Scenario: Moving funds between Main Pocket and GoPay Tabungan Pocket
- **WHEN** Jago reports `Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket` and both endpoints resolve uniquely within the registered Jago hierarchy
- **THEN** the backend records exactly one linked bilateral movement for 500000 IDR between the effective main endpoint and the registered GoPay Tabungan pocket

#### Scenario: Moving funds between custom pockets
- **WHEN** Jago reports movement from the registered Jajan pocket to the registered Tabungan pocket for 50000 IDR
- **THEN** both records share a movement identifier, both endpoints belong to the same Jago parent, and the account balances reflect only that one movement

#### Scenario: Rejecting an unknown named pocket
- **WHEN** a named Jago movement endpoint is absent or ambiguous in the user's registered hierarchy
- **THEN** the event is retained for review, no liquid pocket is created, and no guessed movement is recorded

### Requirement: Multi-App Notification Ingestion Scope
The system SHALL continue to support authenticated backend ingestion from myBCA (`com.bca.mybca.omni.android`, `id.co.bca.mybca.omni.android`), BCA mobile (`com.bca`), Bank Jago (`com.jago.digitalbanking`, `com.jago.digitalBanking`), GoPay (`com.gojek.gopay`, `com.gojek.app`, `com.gopay.wallet`), ShopeePay (`com.shopeepay.id`), and Stockbit (`com.stockbit.android`). Unsupported apps, including Blu and deferred Bibit ingestion, SHALL NOT create ledger effects. Source institutions SHALL resolve from the authenticated user's registered data and supported package evidence; missing or ambiguous mapping SHALL NOT fall back to an unrelated primary or first liquid account. Existing deterministic broker trade processing SHALL remain isolated from generic AI liquid operations.

#### Scenario: Ingesting valid financial notification from supported app
- **WHEN** a supported GoPay event confirms an outbound payment and resolves uniquely to an owned active GoPay account
- **THEN** the system validates and records the observed payment using that account

#### Scenario: Ingesting notification from unmapped source
- **WHEN** a supported package has no unambiguous registered source institution
- **THEN** the event produces a review outcome and no unrelated liquid account receives its financial effect

#### Scenario: Receiving an unsupported package
- **WHEN** a notification comes from an unsupported app
- **THEN** the result is ignored without ledger effects or an external inference call

## ADDED Requirements

### Requirement: Durable Idempotent Notification Processing
The system SHALL persist accepted financial candidates and processing status before external inference, keyed uniquely by authenticated user and payload hash. It SHALL expose `queued`, `processing`, `recorded`, `needs_review`, `ignored`, or `failed` as appropriate. Repeated deliveries, simultaneous workers, expired worker ownership, and process restarts SHALL NOT duplicate ledger effects. A per-event failure SHALL NOT roll back another event's accepted state. Ordinary redelivery SHALL return existing state, not overwrite source facts or replay a completed ledger operation.

#### Scenario: Concurrent delivery of the same candidate
- **WHEN** two requests submit the same user and payload hash simultaneously
- **THEN** exactly one accepted event exists and no more than one corresponding logical ledger operation is committed

#### Scenario: Recovering a worker after restart
- **WHEN** a worker stops after claiming an accepted event
- **THEN** a replacement worker can recover it after ownership expires without duplicating a previously committed operation

#### Scenario: Isolating failure inside a batch
- **WHEN** one event cannot be interpreted or applied while other batch events are valid
- **THEN** each event retains its own durable state and the failed event does not erase the accepted or recorded results of the others

### Requirement: Conservative Automatic Movement Reconciliation
For cross-account notifications, the system SHALL record only the observed financial leg until a unique supported opposite leg exists. Automatic linkage SHALL require the same owner, equal positive source-supported amounts and currency, opposite directions, distinct eligible liquid accounts, original event timestamps strictly less than 30 seconds apart, compatible transfer/endpoint evidence, and exactly one mutually eligible counterpart. Model confidence or amount/time coincidence alone SHALL NOT prove a movement. Associated goal/debt/recurring/trade records, unrelated manually recorded transactions, and consumed movement roles SHALL NOT be reassigned automatically. Linking two existing observed legs SHALL preserve their identities, timestamps, amounts, notes, receipts, and balance effects, while assigning canonical movement roles/classifications atomically. Manual merge and display-only legacy inference SHALL retain their existing contracts.

#### Scenario: Receiving opposite legs in separate batches
- **WHEN** a supported outgoing event and its unique supported incoming counterpart arrive in different batches with event timestamps 29 seconds apart
- **THEN** the backend links exactly those two observed records into one durable internal movement without creating another pair or changing their amounts or balance effects

#### Scenario: Processing notifications out of order
- **WHEN** the incoming notification is processed before its outgoing counterpart
- **THEN** the same validation links the pair once the counterpart is available

#### Scenario: Refusing an exact boundary or ambiguous match
- **WHEN** event timestamps differ by exactly 30 seconds, or either leg has multiple eligible counterparts
- **THEN** no automatic movement linkage occurs and explicit manual merging remains available under its existing rules

#### Scenario: Refusing unrelated equal-value payments
- **WHEN** two records have equal amounts and nearby timestamps but lack compatible internal-transfer evidence
- **THEN** they remain separate and no movement is inferred merely from model confidence

### Requirement: Notification Movement Provenance and Counterpart Deduplication
The system SHALL distinguish a leg observed directly in a notification from a counterpart synthesized for an explicit complete movement. A later notification that uniquely confirms an existing unconsumed movement role SHALL attach to that role without another financial effect. A movement role SHALL NOT consume multiple unrelated notifications. Historical movements without sufficient provenance SHALL NOT be rewritten or replayed automatically.

#### Scenario: Receiving a second notification for an already recorded Jago movement
- **WHEN** a notification uniquely confirms an unconsumed role of an explicit Jago movement within the supported evidence/time window
- **THEN** it links to the existing logical operation and creates zero additional transactions

#### Scenario: Handling an ambiguous historical pair
- **WHEN** a historical movement lacks enough evidence to establish whether a new event confirms it
- **THEN** the backend does not silently reuse, rewrite, or double-record that movement and retains the ambiguity for review

### Requirement: Compact Committed Notification Results
Batch ingestion SHALL preserve `ok`, `received`, `inserted`, `updated`, and `created_transactions` and add one result per submitted event in input order. Each result SHALL identify its submitted `payload_hash` and state. A committed result SHALL include stable `record_key`, `type`, concise `description`, source/target display strings, numeric `amount`, and `currency`, with `target` null when inapplicable. `event_id` SHALL support lookup for durably accepted asynchronous events. `recorded` SHALL be returned only after commit; queued acceptance or provider success alone SHALL NOT indicate transaction success. Redelivery SHALL reuse the committed key and results; a reconciled movement's notifications SHALL identify the same logical operation. Counters SHALL describe the current HTTP request, not eventual asynchronous ledger writes.

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
