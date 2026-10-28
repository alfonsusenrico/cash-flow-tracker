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

## ADDED Requirements

### Requirement: Recognised Transaction Definition
A notification SHALL be a **recognised transaction** when either a known institution transaction template matches with an amount, or the text contains exactly one Rupiah amount, settlement wording and a resolvable direction.

#### Scenario: Unknown template with settlement evidence
- **GIVEN** a notification from a supported package that matches no institution template but quotes exactly one Rupiah amount, says the payment succeeded and names its direction
- **WHEN** it is ingested
- **THEN** it SHALL be treated as a recognised transaction and SHALL NOT be classified as `ignored`.

### Requirement: Hard-Noise Notifications Are Ignored
The backend SHALL classify a notification as `ignored`, without storing it and without creating a ledger transaction, when it contains hard-noise wording that cannot describe a completed transaction: a one-time or verification code, failure, pending or in-progress processing, a future transfer, a card authorisation request awaiting confirmation, or a payment instruction with a deadline.

#### Scenario: Failed payment
- **GIVEN** a notification "Pembayaran Rp25.000 ke Toko Contoh gagal"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored` and no notification event or transaction SHALL be stored.

### Requirement: Promotional Wording Without a Transaction Is Ignored
The backend SHALL classify a notification as `ignored` when it contains promotional or market-information wording and is not a recognised transaction. Promotional wording SHALL NOT, on its own, cause a recognised transaction to be ignored.

#### Scenario: Top-up promotion
- **GIVEN** a notification "Top up sekarang dan dapatkan bonus Rp10.000!"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

### Requirement: Headline Figures Without a Transaction Are Ignored
The backend SHALL classify a notification as `ignored` when every Rupiah figure in it is a headline figure written with a magnitude word (for example thousand, million, billion or trillion) and it is not a recognised transaction.

#### Scenario: Earnings headline
- **GIVEN** a notification "Laba Emiten Contoh naik menjadi Rp2,1 triliun"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

### Requirement: Amount-Less Notifications Are Ignored
The backend SHALL classify a notification as `ignored` when it contains no monetary amount in any currency and it is not a recognised transaction.

#### Scenario: Account notice without an amount
- **GIVEN** a notification "Rekening kamu sudah aktif. Selamat bertransaksi"
- **WHEN** it is ingested
- **THEN** it SHALL be `ignored`.

### Requirement: Remaining Notifications Pass the Evidence Checks
Notifications that are not ignored SHALL continue through the existing evidence checks, which may hold them for review: unsupported currency, ambiguous amount, unproven settlement, and conflicting mobile hints.

#### Scenario: Settled payment reaches the evidence checks
- **GIVEN** a settled payment notification with one Rupiah amount
- **WHEN** it is ingested
- **THEN** it SHALL NOT be `ignored` and its amount and settlement SHALL be checked before any ledger effect.
