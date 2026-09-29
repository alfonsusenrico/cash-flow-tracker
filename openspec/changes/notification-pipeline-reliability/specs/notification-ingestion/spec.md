# Spec Delta

## ADDED Requirements

### Requirement: Evidence-Bounded Automatic Movement Pairing
The system SHALL link an observed notification leg to a counterpart as one internal movement when both are notification-recorded transactions on distinct owned liquid accounts under different root institutions, with equal positive amount and currency, opposite directions, no goal, debt, recurring, or trade association, and original event timestamps within the configured pairing window (default 900 seconds, configurable between 30 and 86400 seconds). A leg whose parsed counterparty names a third party that matches none of the owner's identity aliases SHALL NOT be paired under this rule. When more than one counterpart qualifies, the system SHALL record the leg alone and flag it `ambiguous_movement`. Linking SHALL preserve both legs' identities, timestamps, amounts, notes, and balance effects.

#### Scenario: Top-up recognised across institutions without transfer wording
- **WHEN** a BCA expense "You spent IDR 7,490,557.00 at Shopping." and a ShopeePay income "Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu." arrive 3 seconds apart
- **THEN** the backend links them into one internal movement from the BCA account to ShopeePay and neither leg keeps a spending category

#### Scenario: Counterparts minutes apart
- **WHEN** opposite legs with equal amount on Jago and ShopeePay arrive 4 minutes apart within the configured window
- **THEN** they are linked into one movement regardless of arrival order

#### Scenario: Payment to a third party is not paired
- **WHEN** an outgoing GoPay leg names a recipient that matches no owner alias and an unrelated equal-amount income arrives within the window
- **THEN** the legs remain separate

#### Scenario: Ambiguous counterparts are flagged
- **WHEN** two qualifying counterparts exist for one leg
- **THEN** no link is created and the leg's result carries `ambiguous_movement` for manual merge

### Requirement: Owner Identity Aliases and Masked Name Matching
The system SHALL maintain per-owner name aliases (defaulting to the profile name) editable through the settings API, and SHALL treat notification text as naming the owner when any alias matches literally or through bank masking, where masked words align by position with alias words, asterisks stand for single letters, and at least two literal letters are present.

#### Scenario: Masked BCA sender matches the owner
- **WHEN** a BCA income names the sender "ALFO**US ***ICO *O" and an alias "Alfonsus Enrico Soebijanto" is configured
- **THEN** the event is treated as a self-transfer candidate and the sender is redacted as `[SELF]` in model context

#### Scenario: Aliases updated through settings
- **WHEN** the owner saves up to ten aliases through the settings endpoint
- **THEN** subsequent processing uses the new list without a restart

### Requirement: Deterministic Fallback and Failure State Separation
When AI interpretation returns `needs_review` or a non-transient provider error occurs, the system SHALL attempt the deterministic interpretation and record it when it validates, marking the provenance as a fallback. Transient and configuration provider failures SHALL leave the event `failed` with an escalating retry schedule (capped attempts within 48 hours) that the worker resumes automatically; only evidence, mapping, or ambiguity outcomes SHALL leave an event in `needs_review`.

#### Scenario: Model uncertainty on a deterministic-resolvable event
- **WHEN** the model returns `needs_review` for a ShopeePay income whose amount, direction, and effective account are proven
- **THEN** the event is recorded through the deterministic path and its result shows no error code

#### Scenario: Provider outage recovers without owner action
- **WHEN** the provider is unavailable for twenty minutes
- **THEN** affected events stay `failed` with a future retry time and are recorded once the provider returns

#### Scenario: Genuine mapping ambiguity stays reviewable
- **WHEN** the observed pocket cannot be resolved uniquely
- **THEN** the event is `needs_review` with the mapping error code and is not retried automatically

### Requirement: Owner-Scoped Manual Resolution
The backend SHALL expose an owner-scoped resolve operation for events in `needs_review` or `failed` that records exactly one expense or income using the event's single proven amount (or an owner-entered amount only when the notification has no single proven amount) and its captured timestamp, the owner-chosen liquid account and matching-kind category, and an idempotency key derived from the event, then marks the event recorded with manual provenance and runs the pairing pass. It SHALL reject recorded, ignored, or foreign events without revealing their contents.

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
When no top-level account is named after a notification's institution, the system SHALL map that institution's notifications to the single active pocket whose name contains the institution name, so a balance shown in one app but held by another bank (GoPay Tabungan held in Bank Jago) is one account. A top-level account named after the institution SHALL keep precedence. The owner SHALL be able to move an existing stand-alone liquid account without pockets under another top-level account, keeping its transaction history.

#### Scenario: GoPay payment debits the GoPay Tabungan pocket in Jago
- **WHEN** the owner has moved the GoPay account under Jago as "GoPay Tabungan" and the GoPay app reports a QRIS payment
- **THEN** the expense is recorded on "Bank Jago · GoPay Tabungan"

#### Scenario: Jago pocket move into the hosted pocket
- **WHEN** Jago reports "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
- **THEN** a movement from Main Pocket to GoPay Tabungan is recorded

### Requirement: Splitting an Automatically Paired Movement
The system SHALL let the owner split a non-trade internal movement back into two independent transactions that keep their accounts, amounts, dates, and notes, restoring each leg's notification-derived category where known and detaching the notification events from the movement.

#### Scenario: Undoing a wrong pairing
- **WHEN** the owner splits a movement created by automatic pairing
- **THEN** both transactions remain with their original balance effects and can be merged again manually

## MODIFIED Requirements

### Requirement: Multi-App Notification Ingestion Scope
The system SHALL support push notification ingestion from the 6 active registered financial companion apps:
1. `myBCA` (`com.bca.mybca.omni.android`, `id.co.bca.mybca.omni.android`)
2. `BCA mobile` (`com.bca`)
3. `Bank Jago` (`com.jago.digitalbanking`, `com.jago.digitalBanking`)
4. `GoPay` (`com.gojek.gopay`, `com.gojek.app`, `com.gopay.wallet`)
5. `ShopeePay` (`com.shopeepay.id`)
6. `Stockbit` (`com.stockbit.android`)
The system SHALL NOT process unsupported apps (such as Blu by BCA) and SHALL postpone Bibit ingestion until real notification payload samples are provided. Settlement, direction, and balance cue recognition SHALL cover outbound transfer wording (`sent`, `dikirim`, `mengirim`) and balance phrases (`remaining balance`, `saldo kamu`), and myBCA outbound `Account Transfer` notifications SHALL be recorded as outgoing transfers.

#### Scenario: Ingesting valid financial notification from supported app
- **WHEN** an incoming batch contains a notification event from `com.gojek.app` for an outbound payment
- **THEN** the system SHALL parse the event, map it to the user's GoPay account, and record the transaction in the ledger

#### Scenario: Ingesting notification from unmapped source
- **WHEN** an incoming notification is received without a matching configured institution
- **THEN** the system SHALL fall back to the user's primary liquid account without crashing

#### Scenario: Outbound transfer wording is a settled expense
- **WHEN** a Bank Jago notification reads "You've sent Rp1.500.000 to [owner alias]."
- **THEN** the backend treats it as a settled outgoing candidate rather than `settlement_unproven`

#### Scenario: Second amount in a balance phrase is ignored
- **WHEN** a notification states the transaction amount and "Your remaining balance is Rp950.000"
- **THEN** only the transaction amount is used as evidence
