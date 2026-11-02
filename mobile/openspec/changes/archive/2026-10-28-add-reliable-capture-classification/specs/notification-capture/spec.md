## ADDED Requirements

### Requirement: Every capture gets an outcome before any network call
The listener SHALL give each notification from an enabled app one outcome before anything is sent: drop, send, or keep on the phone. Each stored row SHALL record its outcome and a reason code. Notification channels SHALL NOT decide the outcome, because the same channel carries both offers and real transactions.

#### Scenario: Outcome and reason are recorded
- **WHEN** GoPay posts "Pembayaran berhasil" with "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay"
- **THEN** the stored row SHALL have the send outcome and a reason code.

#### Scenario: Promotional channel does not drop a transaction
- **WHEN** a payment confirmation arrives on a channel named `promotional_notifications`
- **THEN** the listener SHALL send it.

### Requirement: Dropped notifications are never stored or sent
The listener SHALL neither store nor send:
- one-time codes;
- ongoing or foreground-service notifications;
- group summaries without text of their own;
- notifications without any text;
- the listener's own notifications.

It SHALL count each drop for capture health.

#### Scenario: Ongoing notification is dropped
- **WHEN** an enabled app posts an ongoing notification such as a running-transfer progress
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: Empty group summary is dropped
- **WHEN** an enabled app posts a group summary whose title and text are empty
- **THEN** the listener SHALL NOT store or send it, even if it lists its children's lines.

#### Scenario: Own notification is dropped
- **WHEN** the listener receives a notification posted by its own package
- **THEN** the listener SHALL NOT store or send it.

### Requirement: Recognised transactions and settled money events are sent
The listener SHALL give the send outcome to:
- a recognised transaction;
- text with a Rupiah amount that the tracker would review rather than ignore.

Promotional or market wording SHALL NOT override a recognised transaction.

#### Scenario: myBCA spending is sent
- **WHEN** myBCA posts "Financial Diary" with "You spent IDR 25,000.00 at Groceries."
- **THEN** the listener SHALL send it.

#### Scenario: Jago card payment is sent
- **WHEN** Bank Jago posts "You've paid Rp103.440 using your debit card"
- **THEN** the listener SHALL send it.

#### Scenario: Jago pocket move is sent
- **WHEN** Bank Jago posts "You've moved Rp350.000 from My Emergency Fund Pocket to your Subscriptions Pocket."
- **THEN** the listener SHALL send it.

#### Scenario: ShopeePay top-up is sent
- **WHEN** ShopeePay posts "Pengisian saldo sebesar Rp150.000 telah ditambahkan ke ShopeePay-mu."
- **THEN** the listener SHALL send it.

#### Scenario: Transaction mentioning cashback is sent
- **WHEN** a recognised payment confirmation also says it earned cashback
- **THEN** the listener SHALL send it.

#### Scenario: Several amounts are sent for review
- **WHEN** a settled transfer notification shows both a fee and a total
- **THEN** the listener SHALL send it, so the tracker can flag it for review.

### Requirement: Noise is kept on the phone and never sent automatically
When text is not a recognised transaction, the listener SHALL keep it on the phone and never send it automatically if the text:
- is promotional;
- is market information;
- shows only headline figures;
- has no amount;
- reports a failure or pending state;
- has one amount but neither settlement nor direction wording.

#### Scenario: Gojek promotion is kept on the phone
- **WHEN** GoPay posts "Diskon 50% GoFood hari ini, klaim vouchernya!"
- **THEN** the listener SHALL store it on the phone and SHALL NOT send it.

#### Scenario: Stockbit market news is kept on the phone
- **WHEN** Stockbit posts "IHSG ditutup menguat, asing catat Net Sell Rp1,2 T"
- **THEN** the listener SHALL store it on the phone and SHALL NOT send it.

#### Scenario: ShopeePay offer is kept on the phone
- **WHEN** ShopeePay posts "GAJIAN! Cashback saldo s.d. Rp100.000"
- **THEN** the listener SHALL store it on the phone and SHALL NOT send it.

#### Scenario: Jago advertisement is kept on the phone
- **WHEN** Bank Jago posts "Get up to 30K BONUS when you invite friends"
- **THEN** the listener SHALL store it on the phone and SHALL NOT send it.

#### Scenario: Amount-less text is kept on the phone
- **WHEN** an enabled app posts "Transfer kamu sudah sampai" without an amount
- **THEN** the listener SHALL store it on the phone and SHALL NOT send it.

### Requirement: The owner can send a kept notification
The inbox SHALL list kept notifications under "Disimpan di ponsel", each with a "Kirim ke Tracker" action. That action SHALL change the row to the send outcome with an owner reason, and submit it with the next sync. Kept rows SHALL be deleted 7 days after they were posted.

#### Scenario: Owner sends a kept notification
- **WHEN** the owner taps "Kirim ke Tracker" on a kept row
- **THEN** the row SHALL leave the "Disimpan di ponsel" list and be submitted to the tracker.

#### Scenario: Kept rows expire
- **WHEN** a kept row was posted more than 7 days ago and has not been sent
- **THEN** the listener SHALL delete it.

### Requirement: Captures are stored durably
The listener SHALL store each capture through a single ordered capture queue. A failed write SHALL be retried, and a write that still fails SHALL be counted and shown in Settings. The captured text SHALL include inbox-style lines and messaging-style messages.

#### Scenario: Storage failure is visible
- **WHEN** a capture cannot be written after its retries
- **THEN** Settings SHALL show an increased failed-capture count and the time of the last failure.

#### Scenario: Inbox lines are captured
- **WHEN** a notification carries its transaction text only as inbox-style lines
- **THEN** the stored row SHALL contain those lines and be classified on them.

### Requirement: A re-post updates its row and repeated payments stay separate
When a notification arrives with the same key, identical content and the same posting time set by the app, the listener SHALL update the existing row and SHALL NOT create a new one. Identical text with a different posting time SHALL create a separate row. The payload hash formula SHALL stay unchanged.

#### Scenario: Re-post of the same notification
- **WHEN** an app posts the same notification key with identical text and the same posting time again
- **THEN** the listener SHALL update the existing row's re-post count and SHALL NOT create or send another row.

#### Scenario: Two identical payments stay separate
- **WHEN** myBCA posts "You spent IDR 25,000.00 at Groceries." twice under the same key, with different posting times
- **THEN** the listener SHALL store and send two rows with different payload hashes.

### Requirement: Cross-app duplicates are sent once
When two apps of the same institution post the same normalised text within 2 minutes, the listener SHALL keep the first as is. It SHALL store the later one with the drop outcome and reason `duplicate_source`. Each earlier row SHALL absorb at most one duplicate.

#### Scenario: Gojek and GoPay report the same payment
- **WHEN** the Gojek app and the GoPay app post "Kamu baru aja bayar Rp45.000 ke Contoh pake GoPay" 20 seconds apart
- **THEN** the listener SHALL send one row and store the other as dropped with `duplicate_source`.

#### Scenario: Outside the window both are kept
- **WHEN** the same text arrives from the two apps 5 minutes apart
- **THEN** the listener SHALL keep both rows with their own outcome.

#### Scenario: Two payments reported by both apps
- **WHEN** both apps each report two identical payments within the window
- **THEN** the listener SHALL send exactly two rows.

### Requirement: One rejected row does not block a batch
When the tracker rejects a whole batch with HTTP 400, 404 or 422, the listener SHALL resubmit each row of that batch on its own. Only rows that are rejected on their own SHALL be marked `invalid_request`. A network, server or authorisation failure during the per-row retry SHALL stop it and leave the remaining rows pending.

#### Scenario: One bad row in a batch
- **WHEN** a batch of three rows is rejected with 422 and only the second row is rejected on its own
- **THEN** the first and third rows SHALL be accepted, and only the second SHALL be marked `invalid_request`.

#### Scenario: Server becomes unavailable during isolation
- **WHEN** the per-row retry gets HTTP 503 for a row
- **THEN** that row and the rows after it SHALL stay pending for a later retry.

### Requirement: Every registered app can be toggled
Settings SHALL list every registered package once, with its registry label, and a toggle. The default for each package SHALL be unchanged: BCA mobile and Shopee are off, and the others are on.

#### Scenario: Gojek super-app can be disabled
- **WHEN** the owner turns off `com.gojek.app` in Settings
- **THEN** the listener SHALL ignore notifications from `com.gojek.app` and keep capturing from `com.gojek.gopay`.

## MODIFIED Requirements

### Requirement: Safety footers do not hide transactions
The listener SHALL classify a notification from an enabled app that mentions PIN, OTP or verification codes only as a warning, without a code next to the phrase, like any other notification. The warning phrase alone SHALL NOT make it dropped or kept on the phone.

#### Scenario: Transaction with a safety footer is kept
- **WHEN** a notification reads "Pembayaran Rp45.000 ke Kedai Contoh berhasil. Jangan berikan PIN/OTP kepada siapa pun"
- **THEN** the listener SHALL store and forward it.

#### Scenario: Amount near a phrase is not a code
- **WHEN** a notification reads "Transfer Rp1500000 berhasil. Jangan bagikan kode OTP"
- **THEN** the listener SHALL store and forward it.
