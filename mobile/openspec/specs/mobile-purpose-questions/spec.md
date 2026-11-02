# mobile-purpose-questions Specification

## Purpose
Define how the companion receives, shows, answers and delivers the backend's "Apa ini?" purpose questions, so the owner can explain a merchant-less payment within seconds and no answer is lost or applied twice.

## Requirements

### Requirement: Purpose questions are validated before use
The companion SHALL accept a purpose question only on a complete recorded expense or income result, when the question's type is `purpose`, its transaction, direction, amount and IDR currency match the result, its question ID is a UUID, its prompt is present, and its suggestions have distinct short IDs and non-empty labels. It SHALL store the open question ID with the row.

#### Scenario: Backend question is stored
- **WHEN** a recorded expense result carries a well-formed purpose question with two suggestions
- **THEN** the row stores the question ID, and the stored result keeps the question and its suggestions

#### Scenario: Malformed question is ignored with a reason
- **WHEN** a recorded result carries a purpose question whose amount differs from the result, or whose question ID is not a UUID
- **THEN** the row stores no question ID, the reason code `purpose_question_mismatch` or `purpose_question_invalid`, and a result without the question
- **AND** the record alerts as an ordinary recorded transaction

#### Scenario: Unknown question type is ignored
- **WHEN** a result carries a question of another type
- **THEN** the row stores the reason code `purpose_question_unsupported` and no question ID

#### Scenario: Result without a question
- **WHEN** a recorded result carries no question, for example from a backend older than the purpose contract
- **THEN** the row stores neither a question ID nor a reason code, and behaves as in version 1.7.0

### Requirement: One actionable notification per question
For each valid open question that is prompted at once (an urgent question, or a purpose question while the evening reminder is off), the companion SHALL show one notification in the record's alert slot instead of the recorded-transaction alert. Its title SHALL be "Apa ini?" with the amount and the record's account. It SHALL offer at most three actions: suggestion buttons with labels shortened to 24 characters, and a "Tulis" free-text reply whenever the question allows text.

#### Scenario: Prompt with suggestions and text reply
- **WHEN** a valid question with five suggestions that allows text is delivered while the evening reminder is off
- **THEN** one notification titled "Apa ini? Rp187.500 · BCA Harian" shows the first two suggestions and "Tulis"
- **AND** the text says that three more suggestions are in the app

#### Scenario: Suggestion-only question
- **WHEN** a valid question that does not allow text has four suggestions and is prompted
- **THEN** the notification shows the first three suggestions and no "Tulis" action

#### Scenario: Long suggestion label
- **WHEN** a suggestion label is longer than 24 characters
- **THEN** the action label is shortened, at a word boundary in the second half of the label when there is one, and ends with "…"

#### Scenario: Notification permission denied
- **WHEN** the app lacks the Android 13+ notification permission
- **THEN** no notification is posted, the alert stays pending, and the row appears under "Perlu dijawab"

#### Scenario: Question answered or closed
- **WHEN** a refreshed result for the row no longer carries the question
- **THEN** a prompt that is still shown is replaced in the same slot by the recorded-transaction alert, without a new sound

#### Scenario: Refresh while an answer is pending
- **WHEN** the same open question is refreshed after the owner has answered it
- **THEN** the notification keeps showing the answer's state and does not offer the actions again

### Requirement: Answers are durable before delivery
The companion SHALL store each answer, a suggestion ID or a normalised text of 1 to the question's limit (at most 200) characters without control characters, with a new client reply ID before any network call. It SHALL refuse an answer to a question that is no longer the row's open question, or to a suggestion not in the question.

#### Scenario: Suggestion answer stored offline
- **WHEN** the owner taps a suggestion while offline
- **THEN** the answer is stored with a reply ID in state `queued`, delivery is scheduled, and no request is made yet

#### Scenario: Invalid text answer
- **WHEN** the owner submits blank text, text over the limit, or text with control characters
- **THEN** nothing is stored and the owner sees the text rule

#### Scenario: Stale question
- **WHEN** the owner answers a question ID that is not the row's open question
- **THEN** nothing is stored and the owner is told the question has changed

### Requirement: Delivery maps every server outcome
The companion SHALL post a stored answer to `POST /api/ingest/notifications/{event_id}/answer-purpose` with `question_id`, `reply_id`, and exactly one of `suggestion_id` and `text`, through the background work queue with network constraints. Every request SHALL carry `X-Companion-Version` with the app version.

#### Scenario: Accepted answer
- **WHEN** the server answers 200 with the event's result without a question
- **THEN** the answer is `accepted`, its text is cleared, the row's question ID is cleared, and the stored result has the new description

#### Scenario: Lost acknowledgement replayed
- **WHEN** the connection drops after the request is sent, and a later attempt succeeds
- **THEN** every attempt carried the same reply ID, and the answer is accepted once

#### Scenario: Server unavailable
- **WHEN** the server answers 503
- **THEN** the answer stays `queued`, its attempt count increases, and the next attempt is scheduled after the backoff delay

#### Scenario: Question closed on the server
- **WHEN** the server answers 409 with `stale_question` or `purpose_answer_conflict`
- **THEN** the answer is `closed` with that code, the owner sees "Pertanyaan sudah ditutup di server; jawaban ini tidak dipakai.", and the result refresh is scheduled

#### Scenario: Answer rejected
- **WHEN** the server answers 422 `unknown_suggestion` or 404
- **THEN** the answer is `failed` with that code or `event_not_found`, the owner sees why, and the row needs an answer again

#### Scenario: Version identifies the companion
- **WHEN** any answer request is sent by version 1.8.0
- **THEN** its `X-Companion-Version` header is `1.8.0`

### Requirement: The inbox shows open questions accessibly
The Aktivitas feed SHALL offer a "Perlu dijawab" filter, with the count in its label, that lists every open question: recorded rows whose purpose question has no answer other than a failed one, and rows awaiting a sender, pocket or account confirmation without a pending reply. Each such row SHALL carry a text badge naming its question. Tanya SHALL show every suggestion as an option, plus "Tulis sendiri" for a labelled text field when text is allowed.

#### Scenario: Answering from the sheet
- **WHEN** the owner opens a purpose question in Tanya and taps the option "makan kafe senja"
- **THEN** the suggestion ID is submitted, and TalkBack reads "Jawab: makan kafe senja, kategori Makanan & Minuman"

#### Scenario: Text answer validation in the sheet
- **WHEN** the owner opens "Tulis sendiri" in Tanya and submits an empty text field
- **THEN** the field shows the text rule and nothing is submitted

#### Scenario: Pending answer
- **WHEN** the row's answer is `queued`
- **THEN** Tanya shows "Jawaban tersimpan; mengirim ke server." and no options or text field

#### Scenario: Account disconnected
- **WHEN** the row belongs to a different pairing than the current settings
- **THEN** the options and the text field are disabled, and Tanya says to reconnect the account in Settings

### Requirement: Rejected purpose questions leave a reason code
The companion SHALL drop any purpose question it does not accept from the stored result and record a reason code on the row, without logging the question text.

#### Scenario: Duplicate suggestion IDs
- **WHEN** a recorded result carries a purpose question whose suggestions share an ID
- **THEN** the stored result has no question, the row has the reason code `purpose_question_invalid`, and the log carries only the code

### Requirement: Tapping a purpose question opens its row
Tapping a purpose question notification SHALL open the row's question in Tanya, below the original notification.

#### Scenario: Notification body tapped
- **WHEN** the owner taps the body of an "Apa ini?" notification
- **THEN** the app opens Tanya at that row's question

### Requirement: Purpose answers from the notification need authentication
Purpose question actions SHALL require device authentication where Android supports it.

#### Scenario: Answer from a locked device
- **WHEN** the owner taps a suggestion action on a locked device running Android 12 or later
- **THEN** Android asks for authentication before the answer is stored

### Requirement: Purpose questions stay in the app when notifications are off
The companion SHALL post nothing when the Android 13+ notification permission or the question channel is off, and SHALL keep the question available in the app.

#### Scenario: Question channel turned off
- **WHEN** the owner has turned off the "Apa ini?" notification channel
- **THEN** no notification is posted and the row appears under "Perlu dijawab"

### Requirement: A pending purpose answer is not replaced
Repeating the same answer SHALL reuse the stored reply, and a different answer SHALL be refused while one is pending. A new reply ID SHALL be issued only after the server rejected the previous answer.

#### Scenario: Same answer twice
- **WHEN** the owner taps the same suggestion twice before delivery
- **THEN** one reply with one reply ID is stored

#### Scenario: Different answer while pending
- **WHEN** the owner gives a different answer while the first is pending
- **THEN** the second answer is refused

#### Scenario: Answer after a rejection
- **WHEN** the server rejected the previous answer and the owner answers again
- **THEN** the new answer gets a new reply ID

### Requirement: Accepted and closed purpose answers are settled
When the server accepts an answer, the companion SHALL persist the returned result, mark the answer accepted, and clear the stored text. When the server answers 409, the answer SHALL become `closed` with the server's code, and the row's result SHALL be refreshed.

#### Scenario: Accepted text answer
- **WHEN** the server accepts a text answer
- **THEN** the answer is `accepted` and its stored text is empty

### Requirement: Rejected purpose answers fail with a code
When the server answers 404, 400 or 422, the answer SHALL become `failed` with the server's code or a default code.

#### Scenario: Bad request without a code
- **WHEN** the server answers 400 without an error code
- **THEN** the answer is `failed` with a default code and the row needs an answer again

### Requirement: Purpose answers wait for credentials
When the server answers 401 or 403, the answer SHALL wait for credentials instead of retrying.

#### Scenario: Credentials rejected
- **WHEN** the server answers 401 to an answer
- **THEN** the answer waits for credentials and no retry is scheduled

### Requirement: Purpose answers retry transient failures
On network errors, 429 and 5xx, the companion SHALL retry the answer with bounded exponential backoff and the same reply ID.

#### Scenario: Rate limited
- **WHEN** the server answers 429
- **THEN** the answer stays `queued` and the next attempt, after the backoff delay, carries the same reply ID

### Requirement: Purpose answer controls are accessible
Every purpose answer control SHALL have a target of at least 48 dp and a TalkBack description that names the answer and its category. The meaning SHALL be in text, not colour alone.

#### Scenario: Large text
- **WHEN** Tanya shows a purpose question at font scale 2
- **THEN** every option is at least 48 dp high, and its description reads "Jawab: <answer>", followed by ", kategori <category>" when the suggestion has a category

### Requirement: The sheet shows the answer's state
While an answer is pending, Tanya SHALL hide the controls and show its state. A failed answer SHALL show its reason and the controls again. A closed answer SHALL show the closed note in Tanya and on the detail page: for a 409, that the server closed the question and the answer is not used; for `question_closed`, that the question is finished on the server and the answer is not sent again.

#### Scenario: Failed answer
- **WHEN** the row's answer failed with `unknown_suggestion`
- **THEN** Tanya shows "Saran ini tidak berlaku lagi. Pilih saran lain atau tulis jawaban." and the controls again

#### Scenario: Closed answer
- **WHEN** the row's answer is `closed`
- **THEN** Tanya and the detail page show "Pertanyaan sudah ditutup di server; jawaban ini tidak dipakai."

#### Scenario: Answer closed by a refreshed result
- **WHEN** the row's answer is `closed` with `question_closed`
- **THEN** Tanya and the detail page show "Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi."

### Requirement: Open purpose questions are re-checked until they close
While a recorded row has a valid open purpose question, the companion SHALL keep one result check scheduled: 2 minutes, 10 minutes, 30 minutes, 2 hours and 6 hours after successive results that show the same question open, then every 12 hours. A different question SHALL start the schedule over. No check SHALL be scheduled later than 7 days after capture.

#### Scenario: Question recorded
- **WHEN** a recorded expense result with a valid purpose question is stored
- **THEN** the next result check is scheduled 2 minutes later

#### Scenario: Question still open
- **WHEN** eight successive checks find the same question open
- **THEN** they are scheduled 2 minutes, 10 minutes, 30 minutes, 2 hours, 6 hours, 12 hours, 12 hours and 12 hours apart, and the row stays under "Perlu dijawab"

#### Scenario: Backend asks a new question
- **WHEN** a check returns a different question ID for the row
- **THEN** the row stores the new question and its next check is 2 minutes later

#### Scenario: Seven days after capture
- **WHEN** the next step of a still-open question would fall more than 7 days after the row was captured
- **THEN** no further check is scheduled, and the row keeps its question under "Perlu dijawab"

#### Scenario: Record without a question
- **WHEN** a recorded result carries no purpose question
- **THEN** no follow-up check is scheduled

### Requirement: A question closed elsewhere settles the row
When a refreshed result for a row no longer carries the stored open question, the companion SHALL clear the row's question ID and stored question, stop its checks, remove it from "Perlu dijawab", and update its notification exactly as after an answer given on the phone. A question closed on the web, by a ledger edit, a bulk review, an accepted history suggestion or movement pairing SHALL be handled the same way.

#### Scenario: Answered on the web
- **WHEN** a follow-up check returns the record without a question after the owner answered it on the review page
- **THEN** the row leaves "Perlu dijawab", keeps status `recorded` with the returned description, and has no next check
- **AND** a prompt that is still shown is replaced in the same slot by the recorded-transaction alert, without a new sound

#### Scenario: Prompt already dismissed
- **WHEN** the owner had dismissed the "Apa ini?" notification and a check then finds the question closed
- **THEN** no notification reappears

### Requirement: A reviewed transaction has no open question
The companion SHALL treat a recorded result whose `review_status` is `reviewed` as carrying no open purpose question, even if the result still contains one, and SHALL store that result without the question and without a reason code.

#### Scenario: Reviewed result with a stale question
- **WHEN** a check returns a recorded result with `review_status` `reviewed` and the previous purpose question
- **THEN** the row stores no question ID, no reason code and a result without the question, and leaves "Perlu dijawab"

### Requirement: An unsent answer to a closed question is never sent
When a stored result shows that the question of a purpose answer that is `queued`, `waiting_auth` or `waiting_pairing` is no longer open, the companion SHALL settle that answer as `closed` with the code `question_closed` and SHALL NOT post it. An answer whose own delivery returned the result SHALL be settled by that delivery instead.

#### Scenario: Answer waiting offline while the web closes the question
- **WHEN** the owner answered on the phone offline, and a check then finds the question closed on the server
- **THEN** the answer is `closed` with `question_closed`, its delivery makes no request, and the detail page says "Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi."

#### Scenario: Question still open while an answer waits
- **WHEN** a check finds the same question still open while the owner's answer is `queued`
- **THEN** the answer stays `queued` and is delivered as usual

#### Scenario: Answer accepted by the server
- **WHEN** the server accepts an answer and returns the result without the question
- **THEN** that answer becomes `accepted`, not `closed`

### Requirement: Refused purpose answers from the notification stay answerable

When the companion refuses or fails to store an answer sent from the "Apa ini?" prompt, it SHALL show the row's open question again in the same slot, silently, with its actions and a statement that the answer was not saved, unless an answer other than a failed one is already stored or the row asks no question.

#### Scenario: Blank or missing text from the prompt
- **WHEN** the owner sends "Tulis" from the prompt with blank text or the reply text does not arrive
- **THEN** nothing is stored and the prompt still offers its suggestions and "Tulis" and says the answer was not saved

#### Scenario: Prompt answered on a locked device
- **WHEN** a prompt action reaches the companion while the device is locked
- **THEN** nothing is stored and the prompt stays answerable and says to unlock and answer again

#### Scenario: Prompt action for a changed question
- **WHEN** an action for an earlier question of the row arrives
- **THEN** nothing is stored and the row's open question is offered with its actions

### Requirement: Purpose answer attempts are traced privately

The companion SHALL record each step of every purpose answer attempt from the notification or the app with the same private, bounded reason-code record that sender answers use. Tanya SHALL state that an answer from the notification was not saved while the newest settled attempt for the shown purpose question is a refused notification answer.

#### Scenario: Refused prompt answer is traced
- **WHEN** a prompt answer is refused because the device is locked
- **THEN** the record holds a received step and a locked step for that question with the answer kind `purpose`, without the answer text

#### Scenario: Tanya shows an unsaved prompt answer
- **WHEN** the owner opens Tanya on a purpose question whose newest settled attempt is a refused notification answer
- **THEN** the card says the answer from the notification was not saved and keeps every option available

### Requirement: Light questions wait for the evening reminder
Purpose questions ("Apa ini?", including habit guesses) SHALL be light; sender, source-pocket and account questions SHALL be urgent and still interrupt at once. While the evening reminder is on, a recorded transaction with an open purpose question SHALL alert as the recorded transaction, and the question SHALL wait in the app. At the reminder time one summary SHALL list how many light questions wait. Every reply path and the answer-attempt record SHALL stay unchanged.

#### Scenario: Purpose question in the afternoon
- **WHEN** a recorded expense arrives with an open purpose question and the reminder is on
- **THEN** the recorded alert is shown, no "Apa ini?" prompt is posted, and the question waits under "Perlu dijawab", on the hub and in the question sheet as "Nanti malam · 20.00"

#### Scenario: Urgent question
- **WHEN** a sender question arrives while the reminder is on
- **THEN** its notification with inline reply is posted at once

#### Scenario: Evening summary
- **WHEN** it is 20.00 and three purpose questions wait
- **THEN** one notification says "Ada 3 pertanyaan ringan hari ini" and tapping it opens the question sheet

#### Scenario: Nothing waits
- **WHEN** it is 20.00 and no purpose question waits
- **THEN** no summary is posted and the next evening is planned

### Requirement: The evening reminder can be changed or turned off
The evening reminder SHALL be on by default at 20.00. Pengaturan SHALL let the owner turn it off or change its time; the next reminder SHALL be planned for that time today if it is still ahead, else tomorrow. With the reminder off, purpose questions SHALL be prompted at once as before.

#### Scenario: Reminder off
- **WHEN** the owner turns the evening reminder off
- **THEN** the planned reminder is cancelled and new purpose questions are prompted at once

#### Scenario: New time
- **WHEN** the owner sets 21.30 at 14.05
- **THEN** the next reminder is planned for 21.30 today
