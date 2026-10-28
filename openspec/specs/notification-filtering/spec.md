# notification-filtering Specification

## Purpose
Reject non-financial notifications while retaining supported bank and pocket movement notifications.

## Requirements

### Requirement: Discard Non-Financial Notifications at Listener Entrance
The companion mobile notification listener SHALL discard any status bar notification that is not a settled financial transaction before database persistence or network synchronization.

#### Scenario: Promotional or marketing notifications are dropped immediately
- **GIVEN** a notification from a registered package (e.g. `com.gojek.app` or `com.shopeepay.id`)
- **WHEN** the notification contains promotional keywords, marketing slogans, ride discounts, or lacks a confirmed transaction amount (e.g. "GoRide harga pelajar", "Kabar baik buat Alfonsus!", "Fix irit ongkos")
- **THEN** the listener SHALL log the rejection at debug level
- **AND** the listener SHALL NOT insert the notification into the local Room database
- **AND** the listener SHALL NOT schedule or enqueue any background sync job.

#### Scenario: Verified transaction notifications are captured and synced
- **GIVEN** a notification from a registered package (e.g. `com.gojek.app` or `com.bca`)
- **WHEN** the notification matches a verified transaction pattern with a positive numeric amount (e.g. "Kamu berhasil transfer ke TABUNGAN BY JAGO", "Rp500.000 udah dikirim ke BCA", "Pengeluaran sebesar IDR 500,000.00")
- **THEN** the listener SHALL extract the structured fields (`event_class`, `expected_amount`, `expected_direction`, `expected_counterparty`)
- **AND** the listener SHALL persist the entity to the local database with `is_financial = true`
- **AND** the listener SHALL schedule immediate synchronization to the backend.

### Requirement: Backend Rejection of Non-Financial Payloads
The backend ingestion endpoint (`POST /api/ingest/notifications`) SHALL classify each notification from a supported package before it can reach the ledger or the review queue.

A notification is a **recognised transaction** when either:
- a known institution transaction template matches with an amount; or
- the text contains exactly one Rupiah amount, settlement wording and a resolvable direction.

The backend SHALL classify the notification as `ignored`, without storing it and without creating a ledger transaction, when any of these holds:
1. It contains hard-noise wording that cannot describe a completed transaction: a one-time or verification code, failure, pending or in-progress processing, a future transfer, a card authorisation request awaiting confirmation, or a payment instruction with a deadline.
2. It contains promotional or market-information wording and is not a recognised transaction.
3. Every Rupiah figure in it is a headline figure written with a magnitude word (for example thousand, million, billion or trillion), and it is not a recognised transaction.
4. It contains no monetary amount in any currency, and it is not a recognised transaction.

Promotional wording SHALL NOT, on its own, cause a recognised transaction to be ignored. Other notifications SHALL continue through the existing evidence checks, which may hold them for review: unsupported currency, ambiguous amount, unproven settlement, and conflicting mobile hints. The API SHALL answer ignored notifications with status `ignored`.

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

### Requirement: Bank Jago Pocket Movement Notifications
The mobile companion app and the backend notification ingestion pipeline SHALL recognize and parse all Bank Jago pocket transfer notifications (both single-pocket and dual-pocket syntax) as internal financial transfers.

#### Scenario: Single-pocket movement out of a pocket
- **GIVEN** a Bank Jago notification with text "You've moved Rp500.000 out of your My Emergency Fund Pocket."
- **WHEN** evaluated by the whitelist parser
- **THEN** it SHALL be recognized as a verified financial transaction with `event_class = "transfer"`, `expected_amount = 500000`, `expected_direction = "internal"`, and `source_pocket = "My Emergency Fund"`
- **AND** the destination SHALL default to the main account balance ("Kantong Utama").

#### Scenario: Single-pocket movement into a pocket
- **GIVEN** a Bank Jago notification with text "You've moved Rp500.000 into your Tabungan Pocket."
- **WHEN** evaluated by the whitelist parser
- **THEN** it SHALL be recognized as a verified financial transaction with `event_class = "transfer"`, `expected_amount = 500000`, `expected_direction = "internal"`, and `target_pocket = "Tabungan"`
- **AND** the source SHALL default to the main account balance ("Kantong Utama").

#### Scenario: Dual-pocket movement between two pockets
- **GIVEN** a Bank Jago notification with text "Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."
- **WHEN** evaluated by the whitelist parser
- **THEN** it SHALL be recognized as a verified financial transaction with `event_class = "transfer"`, `expected_amount = 500000`, `expected_direction = "internal"`, `source_pocket = "Main Pocket"`, and `target_pocket = "GoPay Tabungan"`.
