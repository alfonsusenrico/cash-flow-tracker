# text-capture Specification

## Purpose
Record transactions the owner types as one short Indonesian sentence on the phone, asking only for details that cannot be worked out.

## Requirements

### Requirement: Deterministic Money Facts
The backend SHALL take the amount and kind of a typed capture only from deterministic parsing of the text. The model SHALL NOT set an amount. Exactly one money expression SHALL be required for an amount. The date and time SHALL come from deterministic parsing, the checked model fallback or the owner's answer, as the time requirements below state.

#### Scenario: Indonesian shorthand amounts
- **WHEN** the text is "bayar parkir ptc 10rb", "kopi 25k", "servis motor 1,5jt" or "beli bensin Rp20.000"
- **THEN** the parsed amounts SHALL be 10.000, 25.000, 1.500.000 and 20.000.

#### Scenario: Missing or several amounts are asked
- **WHEN** the text is "bayar parkir" or "parkir 10rb dan kopi 25rb"
- **THEN** no transaction SHALL be recorded
- **AND** the response SHALL be `needs_detail` asking for the amount, or asking to send the items separately.

#### Scenario: Yesterday
- **WHEN** the text is "kemarin makan bakso 20rb" sent on 29 Oct
- **THEN** the transaction date SHALL be 28 Oct in Asia/Jakarta.

### Requirement: Confident Captures Are Recorded
`POST /api/capture/text` SHALL record the transaction immediately when the amount, kind, account and classification are certain. Classification is certain from owner merchant memory, or from a model confidence at or above the configured threshold (default 0.8). The recorded row SHALL have `source = capture`, an informative description, and `review_status = reviewed`.

#### Scenario: Known merchant from memory
- **GIVEN** the owner earlier recorded "parkir ptc" as Parkir / Transportasi / Kebutuhan, paid with Tunai
- **WHEN** the owner types "bayar parkir ptc 10rb"
- **THEN** a Rp 10.000 expense on Tunai SHALL be recorded with that classification, `classified_by = rule`, and no model call.

#### Scenario: New merchant with a confident model
- **GIVEN** no memory for "es teh manis" and an explicit "tunai" in the text
- **WHEN** the model returns a category and pillar with confidence 0.9
- **THEN** the expense SHALL be recorded with `classified_by = ai`.

### Requirement: Targeted Follow-up Questions
When a capture is not certain, the response SHALL be `needs_detail` with exactly one question for the first missing field, in this order: amount, time, kind, account, then category with pillar. `POST /api/capture/{id}/answer` SHALL validate the answer and either record or ask the next question. Answers SHALL be idempotent per reply id, and a stale question SHALL return 409 `stale_capture_question`.

#### Scenario: Unknown account is asked
- **GIVEN** no account mention, no memory and no clear history for "beli pulsa 50rb"
- **WHEN** the owner sends it
- **THEN** the question SHALL be "Dari rekening mana?" with up to 6 liquid accounts, most recently used first
- **AND** answering with GoPay SHALL record the expense on GoPay with `classified_by = owner` for the parts the owner answered.

#### Scenario: Provider unavailable
- **WHEN** the model cannot be reached
- **THEN** the capture SHALL ask for the category with up to 5 ranked category chips instead of guessing.

#### Scenario: Time is asked after the amount
- **GIVEN** a client that answers time questions
- **WHEN** the owner sends "parkir ptc jam 2" at 10:00
- **THEN** the first question SHALL be the amount, and after "10rb" the next question SHALL be the time.

### Requirement: Own-Account Transfers From Text
Text describing a transfer between the owner's accounts SHALL be recorded as one bilateral movement with equal legs, never as spending or income. Examples are "tarik tunai", "setor tunai", "pindah … ke …" and "isi saldo … ke …". "tarik tunai" SHALL use the Cash account as target.

#### Scenario: Cash withdrawal
- **GIVEN** the owner usually withdraws from BCA Harian
- **WHEN** the owner types "tarik tunai 200rb"
- **THEN** a Rp 200.000 movement BCA Harian → Tunai SHALL be recorded, and neither leg SHALL count as spending.

### Requirement: Undo Window
A recorded capture SHALL be undoable through `POST /api/capture/{id}/undo` for 10 seconds after recording, plus 5 seconds of network grace, provided the transaction has not changed. Undo SHALL remove the created rows and restore balances. After the window the response SHALL be 409 `undo_expired`.

#### Scenario: Undo within the window
- **WHEN** the owner presses "Batalkan" 4 seconds after a capture was recorded
- **THEN** the transaction SHALL be deleted, the capture SHALL be `undone`, and balances SHALL be as before.

#### Scenario: Undo too late or after an edit
- **WHEN** undo arrives 20 seconds later, or after the owner edited the transaction
- **THEN** the response SHALL be 409 and nothing SHALL change.

### Requirement: Captures Teach Memory
A recorded capture SHALL append one classification feedback row of kind `capture`, with the local weekday and hour, amount, account and key. It SHALL also upsert merchant memory: as `owner` when the owner answered a question or named the merchant, otherwise as `ai`.

#### Scenario: Second capture needs no questions
- **GIVEN** the owner answered "Dari rekening mana?" with Tunai for "parkir ptc"
- **WHEN** "parkir ptc 10rb" is typed again
- **THEN** it SHALL be recorded without a question.

### Requirement: Capture Input Boundaries
The capture endpoints SHALL accept only the owner's own captures and SHALL bound the text to 1–200 characters. They SHALL be idempotent on the client capture id. Logs SHALL hold ids, status and reason codes only, never the text or amounts.

#### Scenario: Repeated submission
- **WHEN** the phone resends the same capture id after a timeout
- **THEN** the response SHALL describe the same capture, and no second transaction SHALL exist.

#### Scenario: Invalid or foreign requests
- **WHEN** the text is empty or longer than 200 characters, or the capture belongs to another user
- **THEN** the response SHALL be 422 or 404, and nothing SHALL change.

### Requirement: History-Based Capture Suggestions
`GET /api/capture/suggestions` SHALL return at most 6 suggestions built only from the owner's own captures and reviewed manual or capture transactions of the last 90 days. They SHALL be ranked by time-of-day and weekday fit, frequency and recency, with no model call. Each suggestion SHALL carry a prefill `text`, `label`, `amount`, category, account and `reason`.

#### Scenario: Morning parking habit
- **GIVEN** the owner recorded "parkir ptc 10rb" at about 08:00 on five weekdays in the last month
- **WHEN** suggestions are requested on a weekday at 08:30
- **THEN** the first suggestion SHALL be "parkir ptc 10rb" with amount 10.000 and reason "biasanya jam segini".

#### Scenario: Bank-notified spending is not suggested
- **GIVEN** frequent café payments recorded only from bank notifications
- **WHEN** suggestions are requested
- **THEN** none of those payments SHALL appear.

#### Scenario: No history
- **WHEN** a new owner requests suggestions
- **THEN** the list SHALL be empty.

#### Scenario: Invalid or foreign requests
- **WHEN** `device_time` has no offset or the request is unauthenticated
- **THEN** the response SHALL be 422 or 401, and no other owner's history SHALL ever be used.

### Requirement: Capture Time Reading
The backend SHALL read common Indonesian and English time words of a capture in the configured time zone: clock times, parts of the day, offsets such as "2 jam lalu", relative days, weekdays and explicit dates, alone or combined. A 12-hour time SHALL resolve to the most recent plausible past reading, and a reading SHALL never be after the device time. Text without time words SHALL keep the device time.

#### Scenario: The owner's afternoon parking
- **WHEN** "bayar parkir di ptc tadi jam 2 10rb" is sent at 15:00
- **THEN** the transaction SHALL be recorded at 14:00 the same day, not 02:00.

#### Scenario: Most recent past reading after midnight
- **WHEN** "parkir jam 2" is sent at 01:00
- **THEN** the reading SHALL be 14:00 the previous day, because 02:00 today is still ahead.

#### Scenario: A night owl
- **WHEN** "parkir jam 2" is sent at 03:00
- **THEN** the reading SHALL be 02:00 today.

#### Scenario: Parts of the day and named days
- **WHEN** "jam 7 malam" is sent at 21:00, "kemarin sore" at 09:00, "kemarin jam 2" at 09:00 or "sabtu kemarin" on a Monday at 15:00
- **THEN** the readings SHALL be 19:00 today, 16:00 yesterday, 14:00 yesterday and the previous Saturday at 15:00.

#### Scenario: Night after midnight
- **WHEN** "jam 11 malam" is sent at 00:30, or "kemarin jam 1 malam" at 09:00
- **THEN** the readings SHALL be 23:00 yesterday and 01:00 today.

#### Scenario: A stated date with a time
- **WHEN** "3/10 jam 14.30 makan 20rb" is sent on 2 Nov
- **THEN** the transaction SHALL be recorded on 3 Oct at 14:30 and the amount SHALL be 20.000.

#### Scenario: Words that are not times
- **WHEN** the text is "makan siang 30rb", "beli jam tangan 150rb" or "parkir 2 jam 10rb"
- **THEN** the device time SHALL be kept, the amount SHALL be read as before, and "makan siang" SHALL stay in the description.

### Requirement: Ambiguous Times Are Asked
When the reading cannot choose without guessing, the capture SHALL ask a `time` question instead of recording: two plausible readings of a named day, a reading more than 12 hours old when another exists, "minggu lalu" or "bulan lalu" without a day, conflicting time words, or a stated time after the device time. Impossible or future explicit dates and two different named days SHALL stay 422 `invalid_capture_date`.

#### Scenario: Ambiguous morning
- **WHEN** "parkir jam 2" is sent at 10:00
- **THEN** the question SHALL be "Jam berapa?" with the chips "Hari ini 02.00" and "Kemarin 14.00", and nothing SHALL be recorded.

#### Scenario: Named day with two plausible hours
- **WHEN** "kemarin jam 8 makan 20rb" is sent at 09:00
- **THEN** the question SHALL offer "Kemarin 20.00" and "Kemarin 08.00".

#### Scenario: Last week
- **WHEN** "minggu lalu bensin 50rb" is sent
- **THEN** the question SHALL be "Hari apa?" with the days of the previous calendar week as chips.

### Requirement: Time Question Answers
A `time` question SHALL offer up to 7 chips and a free-text answer. A chip SHALL record its moment. A typed answer SHALL be read by the same rules together with the open phrase, then alone. An answer that does not resolve to one moment, names a later day or lies after the device time SHALL be 422 `invalid_capture_answer`, and nothing SHALL change.

#### Scenario: Chip answer
- **GIVEN** the question for "parkir ptc jam 2 10rb" sent at 10:00
- **WHEN** the owner picks "Kemarin 14.00"
- **THEN** the expense SHALL be recorded at 14:00 yesterday with time basis `owner`.

#### Scenario: Typed answer with the open phrase
- **GIVEN** the question for "kemarin jam 8 parkir ptc 10rb"
- **WHEN** the owner types "malam"
- **THEN** the expense SHALL be recorded at 20:00 yesterday.

#### Scenario: Unusable answers
- **WHEN** the answer is "entahlah", "besok jam 8", an account id or an unknown chip
- **THEN** the response SHALL be 422 and the question SHALL stay open.

### Requirement: Model Time Fallback
Only for a time phrase the parser flags but cannot place, such as "abis maghrib", the model SHALL propose a local datetime, the exact span and a confidence, even when memory classifies the capture. The backend SHALL use it only when the span is the owner's words containing the phrase, it is not after the device time, it is on the parsed or stated day when there is one and otherwise within 31 days, and the confidence reaches the capture threshold.

#### Scenario: Accepted model time
- **GIVEN** memory for "parkir ptc"
- **WHEN** "parkir ptc abis maghrib 10rb" is sent at 20:00 and the model returns "abis maghrib", 18:15 today and confidence 0.9
- **THEN** the expense SHALL be recorded at 18:15 with time basis `model`, evidence "abis maghrib", amount 10.000 and `classified_by = rule`.

#### Scenario: Rejected model time is asked
- **WHEN** the model's time is after the device time, older than 31 days without a stated date, on another day than the parsed one, or its span is not in the text
- **THEN** the capture SHALL ask the `time` question and record nothing until it is answered.

#### Scenario: Low confidence is only a suggestion
- **WHEN** the model returns 18:15 with confidence 0.5
- **THEN** the `time` question SHALL offer "Hari ini 18.15" as its first chip.

#### Scenario: Model unavailable
- **WHEN** the model cannot be reached for "parkir ptc abis maghrib 10rb"
- **THEN** the capture SHALL ask the `time` question.

### Requirement: Time Questions Need a Declaring Client
`POST /api/capture/text` SHALL accept an optional `question_types` listing the question types the client answers. Without it the client SHALL be treated as answering only amount, kind, account and category, and a time the reading cannot decide SHALL be recorded at the device time, or on the named day at the device's time of day. Unknown types SHALL be 422 `invalid_question_types`.

#### Scenario: Older phone
- **WHEN** a client without `question_types` sends "parkir ptc jam 2 10rb" at 10:00
- **THEN** the expense SHALL be recorded at 10:00 with time basis `device_time`, and no `time` question SHALL be asked.

#### Scenario: Unknown question type
- **WHEN** `question_types` holds "photo"
- **THEN** the response SHALL be 422 and nothing SHALL be stored.

### Requirement: Capture Time Basis
A recorded capture SHALL store its time basis (`device_time`, `parsed`, `model` or `owner`) and, for `parsed` and `model`, the owner's words it was read from, on every row it creates. The capture summary SHALL show the moment, the basis, the evidence and a label such as "Hari ini 14.00", with no date while the time is asked. Rows from other sources SHALL keep a NULL basis and their own dates.

#### Scenario: Summary shows the reading
- **WHEN** "bayar parkir di ptc tadi jam 2 10rb" is recorded
- **THEN** the summary SHALL show "Hari ini 14.00", basis `parsed` and evidence "tadi jam 2", and the transaction SHALL carry the same basis and evidence.

#### Scenario: Both movement legs
- **WHEN** "tarik tunai 200rb dari bca harian kemarin jam 2" is recorded
- **THEN** both legs SHALL be at 14:00 yesterday with basis `parsed`.

#### Scenario: Owner edits the date
- **WHEN** the owner changes the date of a parsed capture row
- **THEN** its basis SHALL become `owner` and its evidence SHALL be cleared, while an edit of only the notes SHALL leave both unchanged.

#### Scenario: Notifications keep their post time
- **WHEN** a notification "You've paid Rp125.000 to Parkir Kemarin Jam 2" is recorded
- **THEN** the transaction date SHALL be the notification's post time and its time basis SHALL be NULL.

### Requirement: Capture Parse Preview
`POST /api/capture/parse` SHALL return the deterministic reading of a typed capture: amount, problems, kind, description, time with its phrase and options, an account hint resolved to one of the owner's accounts, and a category hint from the owner's merchant memory only. It SHALL write nothing, SHALL call no model, SHALL be owner-scoped, and SHALL apply the capture text bounds.

#### Scenario: Preview of a short expense
- **GIVEN** device time 2026-11-02 10:00 in Asia/Jakarta and an owner whose memory maps "parkir" to Transportasi (need)
- **WHEN** the owner previews "parkir 5rb jam 2"
- **THEN** the preview SHALL have amount 5.000, description "Parkir", time 14:00 with phrase "jam 2", and category hint Transportasi with pillar need

#### Scenario: Missing amount
- **WHEN** the owner previews "bayar parkir"
- **THEN** the amount SHALL be `null` and `problems` SHALL contain `missing_amount`

#### Scenario: Preview writes nothing and asks no model
- **WHEN** the owner previews any text
- **THEN** no capture, transaction, merchant memory, feedback or notification row SHALL be created or changed, and the model provider SHALL NOT be called

#### Scenario: Another owner's memory and accounts
- **GIVEN** only another owner has the account "Dompet Digital" and a memory for "parkir"
- **WHEN** this owner previews "parkir 5rb pakai dompet digital"
- **THEN** the account hint and the category hint SHALL be `null`

#### Scenario: Bounds and rate limit
- **WHEN** the text is empty or longer than 200 characters, or the owner sends more than 120 previews in 60 seconds
- **THEN** the response SHALL be 422, or 429 `capture_rate_limited`, and the `/capture/text` budget SHALL be unaffected

#### Scenario: Preview logs
- **WHEN** a preview is answered
- **THEN** the logs SHALL hold the user id, the outcome and problem codes only, never the text or the amount

### Requirement: Web Catat Dialog
The web "+ Catat" action, the phone tab and the N key SHALL open one capture dialog on "Tulis transaksi", which previews the typed text through the parse endpoint, refuses text without exactly one amount, records through `POST /api/capture/text` declaring the question types it answers, asks follow-up questions inline and offers "Urungkan" within the undo window. "Isi manual" SHALL turn the same dialog into the manual form.

#### Scenario: Typing shows the reading
- **WHEN** the owner types "parkir 5rb jam 2"
- **THEN** within a second the dialog SHALL show the "Terbaca" box with −Rp 5.000 in the expense colour, Jenis Pengeluaran, the account or "ditentukan saat dicatat", and Tanggal 14.00, without recording anything

#### Scenario: Text without an amount is refused
- **WHEN** the owner types "bayar parkir"
- **THEN** "Catat" SHALL be disabled and the dialog SHALL say "Jumlah belum ada. Tulis jumlahnya, misalnya 5rb." ("No amount yet. Write the amount, for example 5k.")

#### Scenario: Recording with a follow-up question
- **GIVEN** the owner has no memory for "pulsa" and no account was named
- **WHEN** the owner records "beli pulsa 50rb"
- **THEN** the dialog SHALL ask "Dari rekening mana?" with account choices in the same dialog, and answering SHALL record the expense and close the dialog with a toast offering "Urungkan"

#### Scenario: Undo right after recording
- **WHEN** the owner presses "Urungkan" in the toast within 10 seconds
- **THEN** the capture SHALL be undone through `/capture/{id}/undo`, the row SHALL be gone, and balances SHALL be as before

#### Scenario: Retry after a network failure
- **WHEN** recording fails with a network error and the owner presses "Catat" again
- **THEN** the dialog SHALL resend the same `client_capture_id`, and at most one transaction SHALL exist

#### Scenario: Manual form fallback
- **WHEN** the owner chooses "Isi manual" after typing "parkir 5rb"
- **THEN** the same dialog SHALL show "Catat transaksi" with the amount 5.000 filled in, no second dialog SHALL open, and "Tulis kalimat" SHALL return to the sentence with "parkir 5rb" still typed

#### Scenario: Preview unavailable
- **WHEN** the parse endpoint fails or answers 429
- **THEN** the dialog SHALL say "Belum bisa membaca sekarang. Teks tetap bisa dicatat." ("Cannot read it right now. You can still record the text.") and "Catat" SHALL stay available

#### Scenario: Examples
- **WHEN** the owner chooses an example under "Contoh"
- **THEN** the field SHALL hold that sentence with focus in it, and the preview SHALL show an amount for it

#### Scenario: English interface
- **WHEN** an English account opens Catat
- **THEN** the dialog SHALL read "Write a transaction", its examples SHALL be English sentences the parser reads, and no untranslated notice or `lang="id"` region SHALL appear
