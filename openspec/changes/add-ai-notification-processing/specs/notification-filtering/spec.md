# Spec Delta

## MODIFIED Requirements

### Requirement: Backend Rejection of Non-Financial Payloads
The backend ingestion endpoint `POST /api/ingest/notifications` SHALL prevent ledger creation for unsupported packages, empty notifications, promotions, pending/failed transactions, OTPs, and other non-settled or non-financial events. Definite unsupported/empty/noise events SHALL be acknowledged as ignored without external inference or event persistence. A supported plausibly financial event SHALL NOT be discarded merely because a known regex did not recognize its syntax; it SHALL be persisted before inference and classified as recorded, ignored, or requiring review according to independently validated settlement and amount evidence. Mobile expected fields and a model's assertion of financial status SHALL NOT bypass this gate. Companion-app entrance filtering remains unchanged.

#### Scenario: Non-financial or noise event payloads are acknowledged without database storage
- **WHEN** backend entrance checks establish that the submitted event is an empty foreground notification or definite promotional noise
- **THEN** the API returns an ignored result, `inserted = 0`, and creates no notification event, inference request, or ledger transaction

#### Scenario: Retaining an unfamiliar financial candidate
- **WHEN** a supported notification plausibly describes a settled payment with monetary evidence but does not match an existing parsing pattern
- **THEN** the backend retains the candidate for constrained interpretation instead of losing it because of the regex miss

#### Scenario: Rejecting a failed or pending payment with a positive amount
- **WHEN** notification text clearly reports a failed or pending payment despite mobile or model fields marking it financial
- **THEN** no ledger effect is created and the result is ignored rather than recorded

### Requirement: Bank Jago Pocket Movement Notifications
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
