## ADDED Requirements

### Requirement: Catat answers time questions
Every capture SHALL be sent with `question_types` `amount`, `kind`, `account`, `category` and `time`. A `time` question ("Jam berapa?" or "Hari apa?") SHALL offer the tracker's moments as chips, sent as `option_id`, and, when text is allowed, the owner's own words, sent as `text`. The tracker's `time_label` SHALL appear among what is known and among the recorded facts.

#### Scenario: Ambiguous time
- **WHEN** the tracker asks "Jam berapa?" with "Hari ini 14.00" and "Kemarin 14.00"
- **THEN** both chips and a text field are shown, and tapping "Kemarin 14.00" sends `option_id` `t2`

#### Scenario: Owner's own words
- **WHEN** the owner writes "kemarin jam 2 siang" and saves
- **THEN** it is sent as `text` with a reply id

#### Scenario: Answer outside the time question
- **WHEN** an answer names an option the question does not offer
- **THEN** it is refused on the phone and nothing is sent

#### Scenario: Recorded with its time
- **WHEN** the capture is recorded with `time_label` "Kemarin 14.00"
- **THEN** the recorded card shows "Kemarin 14.00"

## MODIFIED Requirements

### Requirement: Catat opens the capture sheet
The hub SHALL show one large "Catat transaksi" button below the status card. Tapping it SHALL open the capture sheet over the hub, with the input, "Proses dengan AI" and a close button. While the sheet is open the hub behind it SHALL be hidden from TalkBack, and back or the close button SHALL close it.

#### Scenario: Opening and closing
- **WHEN** the owner taps "Catat transaksi" on the hub and then "Tutup catat cepat"
- **THEN** the sheet "Catat cepat" opens with an empty input and then closes, leaving the hub

#### Scenario: Accessible button
- **WHEN** TalkBack focuses the Catat button
- **THEN** it reads "Catat transaksi", a button, and its target is at least 60 dp high

### Requirement: Follow-up questions are answered one at a time
A capture that needs detail SHALL show its text, what is already known (including the tracker's time label), the missing part, the tracker's question, and controls for its type: a typed amount, chips and the owner's own words for time, or chips for kind, account or category with an "other" list from the tracker's accounts or categories when allowed. Each answer SHALL be stored with a reply id before it is sent; a resend SHALL reuse that reply id.

#### Scenario: Account question
- **WHEN** the tracker asks "Dari rekening mana?" with Tunai and Dompet Contoh and allows another account
- **THEN** both chips and "Rekening lain" are shown, and "Rekening lain" lists only liquid accounts without pockets

#### Scenario: Answer lost in the network
- **WHEN** an answer is sent and the connection drops, and the owner taps "Kirim ulang jawaban"
- **THEN** the same answer is sent with the same reply id

#### Scenario: Answer outside the question
- **WHEN** the answer is not one of the offered options, or is text for a chip-only question
- **THEN** it is refused on the phone and nothing is sent

#### Scenario: Invalid or stale answer
- **WHEN** the tracker answers 422, or 409 because the question changed
- **THEN** the stored answer is cleared, the sheet says so, and after a 409 the capture's current question is loaded

### Requirement: Captures appear in Aktivitas
Riwayat (formerly Aktivitas) SHALL list captures that are not undone or deleted among the notifications by time, with the title, amount ("Rp —" when unknown), status, "Diketik di ponsel", account and time. Semua SHALL list every such capture, Perlu cek those not yet recorded, and Di ponsel those the tracker has not accepted. A capture with a question SHALL open its sheet; any other SHALL open its Rincian over Riwayat.

#### Scenario: Recorded move
- **WHEN** a typed "tarik tunai 200rb" is recorded as a move from Bank Contoh to Tunai
- **THEN** the feed shows it as "Pindah saldo" with "Diketik di ponsel · Bank Contoh → Tunai"

#### Scenario: Queued capture under Di ponsel
- **WHEN** a capture waits in the queue and Di ponsel is selected
- **THEN** it is listed with "Rp —" and "Di antrean", and the feed says one typed capture waits to be sent

### Requirement: Hapus dari tracker only for a capture's own transaction
"Hapus dari tracker" SHALL be offered only for an expense or income a capture recorded under the current pairing, and SHALL ask for confirmation before `DELETE /api/transactions/{id}`. The result SHALL be reported truthfully. Notification rows SHALL keep only the local "Hapus dari aplikasi".

#### Scenario: Deleted
- **WHEN** the owner confirms "Hapus dari tracker" and the tracker answers 200
- **THEN** the capture is "Dihapus dari tracker" and leaves Riwayat

#### Scenario: Already gone
- **WHEN** the tracker answers 404
- **THEN** the owner is told the transaction is no longer in the tracker and the capture is marked deleted

#### Scenario: Offline
- **WHEN** the request cannot reach the tracker
- **THEN** the owner is told nothing was deleted and the capture stays recorded

#### Scenario: Notification row
- **WHEN** the owner opens a recorded notification
- **THEN** only "Hapus dari aplikasi" is offered

### Requirement: Captured money never enters the tracker's summary
Recorded captures SHALL never appear in a money figure on the phone's hub, and the phone SHALL compute no total from them; each capture keeps only its own amount in Riwayat and Rincian. The phone SHALL request no tracker summary after a capture changes.

#### Scenario: Recorded capture and the summary
- **WHEN** a capture of Rp10.000 is recorded
- **THEN** the hub shows no amount for it and no summary is requested
