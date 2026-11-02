# notification-capture Specification

## Purpose
Define which notifications from enabled financial apps the listener stores locally and forwards to the tracker, so that real transactions are never lost on the device.

## Requirements

### Requirement: One-time codes never leave the device
The listener SHALL discard a notification before storing or sending it when its title contains a one-time-code phrase, or when its text shows a 4–8 digit code within 30 characters of such a phrase. A Rupiah amount SHALL NOT count as a code. The phrases are: OTP, one-time password, verification code, security code, kode verifikasi/OTP/akses/keamanan/rahasia, and PIN when a code sits next to it.

#### Scenario: Code in the body is dropped
- **WHEN** a notification reads "Kode OTP 123456 untuk transfer Rp50.000"
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: Code before the phrase is dropped
- **WHEN** a notification reads "998877 adalah kode verifikasi Anda. Jangan berikan kepada siapa pun"
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: One-time-code title is dropped
- **WHEN** a notification's title is "Your verification code"
- **THEN** the listener SHALL NOT store or send it.

#### Scenario: PIN with a code is dropped
- **WHEN** a notification reads "PIN sementara Anda 4821. Segera ganti"
- **THEN** the listener SHALL NOT store or send it.

### Requirement: Safety footers do not hide transactions
The listener SHALL classify a notification from an enabled app that mentions PIN, OTP or verification codes only as a warning, without a code next to the phrase, like any other notification. The warning phrase alone SHALL NOT make it dropped or kept on the phone.

#### Scenario: Transaction with a safety footer is kept
- **WHEN** a notification reads "Pembayaran Rp45.000 ke Kedai Contoh berhasil. Jangan berikan PIN/OTP kepada siapa pun"
- **THEN** the listener SHALL store and forward it.

#### Scenario: Amount near a phrase is not a code
- **WHEN** a notification reads "Transfer Rp1500000 berhasil. Jangan bagikan kode OTP"
- **THEN** the listener SHALL store and forward it.

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
The Aktivitas feed SHALL list kept notifications under "Di ponsel", and each kept notification's detail page SHALL offer "Kirim ke Tracker". That action SHALL change the row to the send outcome with an owner reason, and submit it with the next sync. Kept rows SHALL be deleted 7 days after they were posted.

#### Scenario: Owner sends a kept notification
- **WHEN** the owner taps "Kirim ke Tracker" on a kept row
- **THEN** the row SHALL leave the "Di ponsel" list and be submitted to the tracker.

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

### Requirement: The auto-sync switch governs every automatic send
While "Auto-Sync Notifikasi" is off, the companion SHALL NOT send captures on its own: no periodic sync, no send after a capture, no send on listener reconnect, no automatic resend of a queued typed capture. Turning it off SHALL cancel that work; turning it on SHALL schedule the periodic sync and send what is waiting, typed captures included. App start SHALL align work with the switch. Owner actions, including the first send of a typed capture, SHALL still send while it is off.

#### Scenario: Switch turned off
- **WHEN** the owner turns auto-sync off while the periodic sync is scheduled, a send is queued and a typed capture waits for its resend
- **THEN** all three are cancelled and no capture or typed capture is sent until the owner asks

#### Scenario: Listener reconnects while off
- **WHEN** the listener reconnects, a new capture is stored, or a typed capture's first send fails offline while auto-sync is off
- **THEN** no send or resend starts and the rows and the typed capture stay pending

#### Scenario: Switch turned on
- **WHEN** the owner turns auto-sync on
- **THEN** the periodic sync is scheduled, a send of pending rows is queued, and queued typed captures are resent once the network allows

#### Scenario: Upgrade with leftover periodic work
- **WHEN** the app starts with auto-sync off and a periodic sync left scheduled by an older build
- **THEN** that periodic sync is cancelled

#### Scenario: Owner syncs by hand while off
- **WHEN** the owner taps "Sinkronkan sekarang", sends a kept row, or taps "Kirim" on a queued typed capture while auto-sync is off
- **THEN** the send is queued, and "Sinkronkan sekarang" also queues the resend of typed captures

#### Scenario: No account connected
- **WHEN** auto-sync is on but no server address and API key are set
- **THEN** a capture, a typed capture or a reconnect starts no send

### Requirement: Sending an unsent notification offers only what the send carries
The detail page's form for a notification that has not been sent SHALL offer the type, amount and note, and SHALL NOT offer account or category pickers, because the ingest request carries no account or category. It SHALL say that the tracker picks the account and category when it records the notification. Sending SHALL store the type, amount and note as the row's labels and SHALL join a kept row to the send pipeline.

#### Scenario: Unsent row opened
- **WHEN** the owner opens the form of an unsent notification
- **THEN** the form shows no "Rekening" or "Kategori" picker and explains that the tracker picks them

#### Scenario: Unsent row sent
- **WHEN** the owner taps "Kirim Notifikasi ke Tracker" with amount 45000, type expense and note "Kedai Contoh"
- **THEN** exactly those three values are stored and the row is queued for sending

#### Scenario: Recorded or reviewable row opened
- **WHEN** the owner opens the form of a recorded ordinary transaction or of a row that needs review
- **THEN** the account and category pickers are offered, because the update or manual record carries them

#### Scenario: Already sent row
- **WHEN** the send is requested for a row that has been sent meanwhile
- **THEN** nothing is relabelled and the owner is told the notification was already sent

### Requirement: Owner edits keep the captured labels they do not change
When the owner sends an unsent notification from Rincian, the companion SHALL store the type, amount and note, SHALL keep the captured counterparty, and SHALL set the direction from the type only for expense ("out") and income ("in"); any other type keeps its captured direction. A "Perbaiki label" correction SHALL change only the tracker's category, pillar or merchant, never the row's stored labels.

#### Scenario: Pocket move sent unchanged
- **WHEN** an unsent Jago pocket move labelled transfer, direction internal and counterparty "Contoh Pocket → Kantong Utama" is sent from the detail page
- **THEN** the row keeps the counterparty and the internal direction, so it is submitted as a transfer

#### Scenario: Type changed to income
- **WHEN** an unsent expense with counterparty "Kedai Contoh" is sent as income
- **THEN** the direction becomes "in" and the counterparty stays "Kedai Contoh"

#### Scenario: Ledger edit accepted
- **WHEN** the tracker accepts a "Perbaiki label" correction of a recorded expense
- **THEN** the row's amount label and counterparty are unchanged and its result is read again from the tracker

### Requirement: The self-test notification is never captured
The listener SHALL decide, before reading any text for capture, whether a posted notification is this app's self-test (its own package, the self-test tag and a token), an app that is not an enabled registered financial app, or a capture. The self-test notification SHALL never be classified, stored in Room, synced or sent to the tracker as a notification; it only answers the waiting self-test.

#### Scenario: Self-test while a bank notification arrives
- **WHEN** the listener receives the self-test notification and a GoPay notification
- **THEN** only the GoPay notification is stored, and the self-test leaves no row of any outcome
