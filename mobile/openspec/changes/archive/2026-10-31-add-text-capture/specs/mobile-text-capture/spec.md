## Purpose
Let the owner record a transaction on the phone by typing one short sentence, answer only what the tracker cannot work out, and undo, edit or delete it, without losing or duplicating a capture when the network, the server or the app fails.

## ADDED Requirements

### Requirement: Catat opens the capture sheet
The bottom bar SHALL show a centred, raised "Catat" button between Aktivitas and Tanya on the four top-level screens. Tapping it SHALL open the capture sheet over the current tab, with the input, "Proses dengan AI" and a close button. While the sheet is open the tab behind it SHALL be hidden from TalkBack, and back or the close button SHALL close it.

#### Scenario: Opening and closing
- **WHEN** the owner taps "Catat" on Beranda and then "Tutup catat cepat"
- **THEN** the sheet "Catat cepat" opens with an empty input and then closes, leaving Beranda selected

#### Scenario: Accessible button
- **WHEN** TalkBack focuses the Catat button
- **THEN** it reads "Catat transaksi", a button, with whether the sheet is open, and its target is at least 48 dp

### Requirement: Suggestions come only from the owner's history
When the sheet opens on a new capture it SHALL ask `GET /api/capture/suggestions` with the device time and offset, and list the suggestions under "Dari riwayatmu · yang biasa kamu catat sekitar jam ini". Tapping a suggestion SHALL put its text in the input and focus the input; it SHALL NOT send anything. Without suggestions the section SHALL be hidden; static examples SHALL never be shown.

#### Scenario: Suggestions present
- **WHEN** the tracker returns "parkir contoh 10rb" for Parkir Contoh, Rp10.000, Tunai, "biasanya jam segini"
- **THEN** the sheet lists "Parkir Contoh · Rp10.000" with "Tunai · Parkir · biasanya jam segini"

#### Scenario: Suggestion fills the input only
- **WHEN** the owner taps that suggestion
- **THEN** the input holds "parkir contoh 10rb" and has focus, and no capture is stored or sent

#### Scenario: No history
- **WHEN** the tracker returns no suggestions
- **THEN** neither the heading nor any suggestion is shown

#### Scenario: Suggestions unavailable
- **WHEN** the request fails or no tracker is connected
- **THEN** the sheet says "Saran dari riwayat belum bisa dimuat." and shows no suggestion

### Requirement: A capture is stored before it is sent
"Proses dengan AI" SHALL normalise the text as the tracker does (NFKC, whitespace collapsed, 1–200 characters, no control characters) and refuse invalid text in place. A valid capture SHALL be written to the phone's database with a new client capture id and the device time before any request, then sent at once.

#### Scenario: Empty input
- **WHEN** the owner taps "Proses dengan AI" with an empty input
- **THEN** the sheet says to write the transaction first and nothing is stored

#### Scenario: Stored before the request
- **WHEN** the owner submits "bayar parkir contoh 10rb" and the request then fails
- **THEN** the capture is already stored with its text, client capture id and device time

### Requirement: The sheet follows the capture's state
While the capture is being sent the sheet SHALL show the typed text, "AI memproses…" with the tracker's steps and a skeleton whose shimmer is removed when animations are off. It SHALL then show the screen of the capture's state: recorded, a question, queued, server error, refused, undone or deleted.

#### Scenario: Processing
- **WHEN** a capture is being sent
- **THEN** the sheet shows the quoted text, "AI memproses…", "Membaca kalimat", "Mencari nominal" and "Mencocokkan rekening & kategori"

#### Scenario: Server error
- **WHEN** the tracker answers 503 or 429
- **THEN** the sheet shows "AI belum bisa memproses", says the text is still stored, and offers "Coba lagi" and "Tutup"

#### Scenario: Refused text
- **WHEN** the tracker answers 422 `invalid_capture_date`
- **THEN** the capture is "Gagal dicatat", the sheet gives the reason and says nothing was recorded, and offers "Tulis ulang" and "Coba lagi"

### Requirement: Recorded capture with a 10-second undo
A recorded capture SHALL show "Tercatat di tracker", the amount, the description, category, Kakeibo pillar and account (or "Pindah saldo" with both accounts), "Ubah" for an expense or income, and "Batalkan" with the seconds left as text and a draining bar. The bar SHALL be removed when animations are off. The window SHALL be 10 seconds on the phone's clock from the tracker's deadline.

#### Scenario: Countdown
- **WHEN** 2.8 seconds of the window have passed
- **THEN** the button reads "Batalkan (8)" and TalkBack reads "Batalkan, sisa 8 detik"

#### Scenario: Animations off
- **WHEN** Android's animator duration scale is 0
- **THEN** the countdown text remains and the draining bar is not shown

#### Scenario: After the window
- **WHEN** the window has ended
- **THEN** "Batalkan" is replaced by "Tersimpan", the sheet says to delete through Rincian, and offers "Buka rincian"

### Requirement: Undo is sent only inside the window
"Batalkan" SHALL send `POST /api/capture/{id}/undo` only while the phone's window is open, and the capture SHALL become undone when the tracker confirms. A 409 SHALL close the window and say why, and a network failure SHALL leave the capture recorded.

#### Scenario: Undo at 9.9 seconds
- **WHEN** the owner taps "Batalkan" 9.9 seconds after the capture was recorded
- **THEN** the undo is sent and the sheet shows "Dibatalkan"

#### Scenario: Undo at 10.1 seconds
- **WHEN** an undo is asked 10.1 seconds after recording
- **THEN** no request is sent and the capture stays recorded

#### Scenario: Undo too late for the server
- **WHEN** the tracker answers 409 `undo_expired`, or `capture_transaction_changed` after an edit
- **THEN** the window closes, the transaction stays recorded, and the owner is told to delete it through Rincian

#### Scenario: Undo offline
- **WHEN** the undo request cannot reach the tracker
- **THEN** the capture stays recorded and the sheet says it was not undone

### Requirement: Follow-up questions are answered one at a time
A capture that needs detail SHALL show its text, what is already known, the missing part, the tracker's question, and controls for its type: a typed amount, or chips for kind, account or category with an "other" list from the tracker's accounts or categories when allowed. Each answer SHALL be stored with a reply id before it is sent; a resend SHALL reuse that reply id.

#### Scenario: Account question
- **WHEN** the tracker asks "Dari rekening mana?" with Tunai and Dompet Contoh and allows another account
- **THEN** both chips and "Rekening lain" are shown, and "Rekening lain" lists only liquid accounts without pockets

#### Scenario: Answer lost in the network
- **WHEN** an answer is sent and the connection drops, and the owner taps "Kirim ulang jawaban"
- **THEN** the same answer is sent with the same reply id

#### Scenario: Answer outside the question
- **WHEN** the answer is not one of the offered options, or is text for a chip question
- **THEN** it is refused on the phone and nothing is sent

#### Scenario: Invalid or stale answer
- **WHEN** the tracker answers 422, or 409 because the question changed
- **THEN** the stored answer is cleared, the sheet says so, and after a 409 the capture's current question is loaded

### Requirement: Queued captures are sent once the network returns
A capture that could not reach the tracker SHALL stay queued with its reason, and the sheet SHALL show "Disimpan di antrean". With auto-sync on, the phone SHALL resend queued captures when the network returns; with it off, they SHALL wait for the owner's "Kirim". Without a connected tracker nothing SHALL be sent.

#### Scenario: Offline with auto-sync on
- **WHEN** a capture is submitted offline with auto-sync on
- **THEN** the sheet shows "Disimpan di antrean", says it is sent automatically when online, and a resend waits for the network

#### Scenario: Offline with auto-sync off
- **WHEN** a capture is submitted offline with auto-sync off
- **THEN** it waits on the phone and the sheet says to send it with "Kirim"

#### Scenario: No tracker connected
- **WHEN** a capture is submitted before any tracker is paired
- **THEN** it stays queued, no request is made, and the sheet says to connect the tracker

### Requirement: A capture is never sent twice nor lost
Every send of a capture SHALL carry its stored client capture id. Only one send of a capture SHALL run at a time, and a capture the tracker accepted SHALL never be sent again. A send left unfinished by a killed process SHALL be resumed at the next start; a send claimed for longer than the send timeout SHALL be taken over.

#### Scenario: Resend after a lost response
- **WHEN** the first send is lost in the network and the resend succeeds
- **THEN** both requests carry the same client capture id, one capture row exists, and no further send follows

#### Scenario: Duplicate send
- **WHEN** two sends of the same capture start together
- **THEN** one request is made

#### Scenario: Process death during a send
- **WHEN** the app is killed while a capture is being sent and starts again
- **THEN** the capture is queued again, resent with the same client capture id, and recorded once

### Requirement: Captures appear in Aktivitas
Aktivitas SHALL list captures that are not undone or deleted among the notifications by time, with the title, amount ("Rp —" when unknown), status, "Diketik di ponsel", account and time. Semua SHALL list every such capture, Perlu cek those not yet recorded, and Di ponsel those the tracker has not accepted. A capture with a question SHALL open its sheet; any other SHALL open its Rincian.

#### Scenario: Recorded move
- **WHEN** a typed "tarik tunai 200rb" is recorded as a move from Bank Contoh to Tunai
- **THEN** the feed shows it as "Pindah saldo" with "Diketik di ponsel · Bank Contoh → Tunai"

#### Scenario: Queued capture under Di ponsel
- **WHEN** a capture waits in the queue and Di ponsel is selected
- **THEN** it is listed with "Rp —" and "Di antrean", and the feed says one typed capture waits to be sent

### Requirement: Rincian of a capture
A capture's Rincian SHALL show its status, amount, title, time with "Diketik di ponsel", the state and its action (answer, send, retry), what the tracker recorded with "Ubah transaksi" for an expense or income, and the typed text. A capture the tracker never accepted SHALL offer "Hapus dari aplikasi" behind a confirmation.

#### Scenario: Recorded expense
- **WHEN** the owner opens a recorded typed expense
- **THEN** the page shows the recorded facts with "Ubah transaksi", the typed text, and "Hapus dari tracker"

#### Scenario: Recorded move
- **WHEN** the owner opens a recorded typed move
- **THEN** no edit or delete from the phone is offered and the page says moves are changed and deleted on Tracker Web

### Requirement: Hapus dari tracker only for a capture's own transaction
"Hapus dari tracker" SHALL be offered only for an expense or income a capture recorded under the current pairing, and SHALL ask for confirmation before `DELETE /api/transactions/{id}`. The result SHALL be reported truthfully. Notification rows SHALL keep only the local "Hapus dari aplikasi".

#### Scenario: Deleted
- **WHEN** the owner confirms "Hapus dari tracker" and the tracker answers 200
- **THEN** the capture is "Dihapus dari tracker", leaves Aktivitas, and the tracker's summary is fetched again

#### Scenario: Already gone
- **WHEN** the tracker answers 404
- **THEN** the owner is told the transaction is no longer in the tracker and the capture is marked deleted

#### Scenario: Offline
- **WHEN** the request cannot reach the tracker
- **THEN** the owner is told nothing was deleted and the capture stays recorded

#### Scenario: Notification row
- **WHEN** the owner opens a recorded notification
- **THEN** only "Hapus dari aplikasi" is offered

### Requirement: Editing a capture uses the existing edit form
"Ubah" in the sheet and "Ubah transaksi" in Rincian SHALL open the existing transaction form for the capture's expense or income and send `PATCH /api/transactions/{id}`. A successful edit SHALL end the undo window and reload the capture from the tracker.

#### Scenario: Edited during the window
- **WHEN** the owner changes the amount to Rp30.000 while "Batalkan" is shown
- **THEN** the transaction is patched, the sheet shows Rp30.000, and "Batalkan" is no longer offered

### Requirement: Captured money never enters the tracker's summary
Recorded captures SHALL count only in figures that come from this phone. The tracker's own cash-flow summary SHALL be fetched again after a capture is recorded, undone, edited or deleted, and SHALL never be added to or from local captures.

#### Scenario: Recorded capture and the summary
- **WHEN** a capture of Rp10.000 is recorded
- **THEN** "Keluar tercatat hari ini" includes Rp10.000 and the tracker summary is requested again rather than increased on the phone
