# Spec Delta

## MODIFIED Requirements

### Requirement: Backend Rejection of Non-Financial Payloads
The backend ingestion endpoint (`POST /api/ingest/notifications`) SHALL classify each notification from a supported package before it can reach the ledger or the review queue, and SHALL answer ignored notifications with status `ignored`.

#### Scenario: Non-financial or noise event payloads are acknowledged without database storage
- **GIVEN** an ingestion request to `POST /api/ingest/notifications` for a notification that the rules classify as `ignored`
- **WHEN** the request is processed
- **THEN** the API SHALL NOT insert the event into the `notification_events` table
- **AND** the API SHALL NOT create any ledger transaction
- **AND** the item result SHALL report status `ignored`.

#### Scenario: Settled payment mentioning cashback is recorded
- **GIVEN** a GoPay notification "Pembayaran berhasil. Kamu baru aja bayar Rp45.000 ke Kedai Contoh pake GoPay. Dapat cashback!"
- **WHEN** it is ingested
- **THEN** it SHALL NOT be classified as `ignored`
- **AND** it SHALL be processed as an expense of Rp 45.000.

#### Scenario: Promotional message with a bonus offer is ignored
- **GIVEN** a Jago notification "Pay with Jago and get up to 30K BONUS before it's too late"
- **WHEN** it is ingested
- **THEN** the response SHALL report `ignored`
- **AND** no notification event and no transaction SHALL be stored.

#### Scenario: Foreign-currency promotion is ignored rather than reviewed
- **GIVEN** a notification "Get up to $6.000 CREDIT BONUS with Digital Pro Card!"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`, not `needs_review` with `unsupported_currency`.

#### Scenario: Market news with headline figures is ignored
- **GIVEN** a Stockbit notification "IHSG naik +1,12%, Emiten Contoh dapat komitmen investasi US$555 Juta" or "OJK mengenakan denda Rp1,5 T dan Rp500 M"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

#### Scenario: Payment instruction with a deadline is ignored
- **GIVEN** a GoPay notification "Transfer Rp350.000 ke BCA Virtual Account sebelum 11 Oct 2026, 22:55"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

#### Scenario: In-progress transfer is ignored
- **GIVEN** a GoPay notification "Kami lagi memproses pengiriman Rp350.000 ke Tabungan Contoh"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

#### Scenario: Card authorisation request is ignored
- **GIVEN** a Jago notification "MERCHANT_CONTOH is requesting USD 0. Please confirm or reject"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

#### Scenario: Amount-less spending summary is ignored
- **GIVEN** a notification "You spent on 3 categories in September. Learn more"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`, not `needs_review` with `missing_amount`.

#### Scenario: Settled foreign-currency payment is still reviewed
- **GIVEN** a Jago notification "You've paid USD 10.00 using your debit card"
- **WHEN** it is ingested
- **THEN** it SHALL NOT be `ignored`
- **AND** it SHALL be held as `needs_review` with `unsupported_currency`.

#### Scenario: Existing noise in the review queue is cleared
- **GIVEN** a stored notification event in `needs_review` with no linked transaction, whose text the current rules classify as `ignored`
- **WHEN** the backend starts
- **THEN** the event SHALL move to `ignored`
- **AND** events in any other state, or with a linked transaction, SHALL NOT change.

#### Scenario: Required behavior and constraints
- **WHEN** the backend receives a notification payload
- **THEN** the following required behavior and constraints hold:

  The backend ingestion endpoint `POST /api/ingest/notifications` SHALL prevent ledger creation for unsupported packages, empty notifications, promotions, pending/failed transactions, OTPs, and other non-settled or non-financial events. Definite unsupported/empty/noise events SHALL be acknowledged as ignored without external inference or event persistence. A supported plausibly financial event SHALL NOT be discarded merely because a known regex did not recognize its syntax; it SHALL be persisted before inference and classified as recorded, ignored, or requiring review according to independently validated settlement and amount evidence. Mobile expected fields and a model's assertion of financial status SHALL NOT bypass this gate. Companion-app entrance filtering remains unchanged.

#### Scenario: Retaining an unfamiliar financial candidate
- **WHEN** a supported notification plausibly describes a settled payment with monetary evidence but does not match an existing parsing pattern
- **THEN** the backend retains the candidate for constrained interpretation instead of losing it because of the regex miss

#### Scenario: Rejecting a failed or pending payment with a positive amount
- **WHEN** notification text clearly reports a failed or pending payment despite mobile or model fields marking it financial
- **THEN** no ledger effect is created and the result is ignored rather than recorded

### Requirement: Bank Jago Pocket Movement Notifications
The backend SHALL recognize supported single-pocket and dual-pocket Jago movement syntax while preserving companion-app behavior.

#### Scenario: Required behavior and constraints
- **WHEN** the backend interprets a Jago pocket movement notification
- **THEN** the following required behavior and constraints hold:

  The backend SHALL recognize supported single-pocket and dual-pocket Jago movement syntax while preserving companion-app behavior. Named endpoints SHALL resolve only within the user's registered active Jago hierarchy. Single-pocket syntax SHALL use an unambiguous valid default main endpoint; unresolved named pockets or main endpoints SHALL become review items rather than newly created pockets or guessed transfers. A complete verified interpretation SHALL be applied as a canonical bilateral movement through the notification ingestion contract.

#### Scenario: Single-pocket movement out of a pocket
- **WHEN** Jago reports `You've moved Rp500.000 out of your My Emergency Fund Pocket` and the named pocket and configured main endpoint resolve uniquely
- **THEN** the backend recognizes a 500000 IDR movement from that registered pocket to the registered main endpoint

#### Scenario: Single-pocket movement into a pocket
- **WHEN** Jago reports `You've moved Rp500.000 into your Tabungan Pocket` with uniquely resolved named and main endpoints
- **THEN** the backend recognizes a 500000 IDR movement from the registered main endpoint into that registered pocket

#### Scenario: Dual-pocket movement between two pockets
- **WHEN** Jago reports `Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket`
- **THEN** the backend validates both registered endpoints and one supported amount before recording one canonical bilateral movement

#### Scenario: Missing or ambiguous main endpoint
- **WHEN** single-pocket syntax lacks enough registered context to determine the effective main endpoint
- **THEN** no endpoint is invented and the event is retained for review
