# Spec Delta

## ADDED Requirements

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
