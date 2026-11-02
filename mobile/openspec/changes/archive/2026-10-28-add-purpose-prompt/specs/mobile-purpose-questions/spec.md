## ADDED Requirements

### Requirement: Purpose questions are validated before use
The companion SHALL accept a purpose question only on a complete recorded expense or income result, when the question's type is `purpose`, its transaction, direction, amount and IDR currency match the result, its question ID is a UUID, its prompt is present, and its suggestions have distinct short IDs and non-empty labels. It SHALL store the open question ID with the row. It SHALL drop any other question from the stored result and record a reason code on the row, without logging the question text.

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
For each valid open question, the companion SHALL show one notification in the record's alert slot instead of the recorded-transaction alert. Its title SHALL be "Apa ini?" with the amount and the record's account. It SHALL offer at most three actions: suggestion buttons with labels shortened to 24 characters, and a "Tulis" free-text reply whenever the question allows text. Tapping it SHALL open the row's detail sheet. Actions SHALL require device authentication where Android supports it. The companion SHALL post nothing when Android 13+ notification permission or the channel is off, and it SHALL keep the question available in the app.

#### Scenario: Prompt with suggestions and text reply
- **WHEN** a valid question with five suggestions that allows text is delivered
- **THEN** one notification titled "Apa ini? Rp187.500 · BCA Harian" shows the first two suggestions and "Tulis"
- **AND** the text says that three more suggestions are in the app

#### Scenario: Suggestion-only question
- **WHEN** a valid question that does not allow text has four suggestions
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
The companion SHALL store each answer, a suggestion ID or a normalised text of 1 to the question's limit (at most 200) characters without control characters, with a new client reply ID before any network call. It SHALL refuse an answer to a question that is no longer the row's open question, or to a suggestion not in the question. Repeating the same answer SHALL reuse the stored reply, and a different answer SHALL be refused while one is pending. A new reply ID SHALL be issued only after the server rejected the previous answer.

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

- **Success:** the companion persists the returned result, marks the answer accepted, and clears the stored text.
- **409:** the answer becomes `closed` with the server's code, and the row's result is refreshed.
- **404, 400 or 422:** the answer becomes `failed` with the server's code or a default code.
- **401 or 403:** the answer waits for credentials.
- **Network errors, 429 and 5xx:** the companion retries with bounded exponential backoff and the same reply ID.

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
The inbox SHALL offer a "Perlu dijawab" filter, with the count in its label, that lists recorded rows with an open question and no answer other than a failed one. Each such row SHALL carry a text badge. The detail sheet SHALL show the question first: every suggestion as a chip, plus a labelled text field when text is allowed.

- Every control SHALL have a target of at least 48 dp and a TalkBack description that names the answer and its category.
- The meaning SHALL be in text, not colour alone.
- While an answer is pending, the controls SHALL be hidden and its state shown.
- A failed answer SHALL show its reason and the controls again.
- A closed answer SHALL show the closed note.

#### Scenario: Answering from the sheet
- **WHEN** the owner opens a row that needs an answer and taps the chip "makan kafe senja"
- **THEN** the suggestion ID is submitted, and TalkBack reads "Jawab: makan kafe senja, kategori Makanan & Minuman"

#### Scenario: Text answer validation in the sheet
- **WHEN** the owner submits an empty text field
- **THEN** the field shows the text rule and nothing is submitted

#### Scenario: Pending answer
- **WHEN** the row's answer is `queued`
- **THEN** the sheet shows "Jawaban tersimpan; mengirim ke server." and no chips or text field

#### Scenario: Account disconnected
- **WHEN** the row belongs to a different pairing than the current settings
- **THEN** the chips and the text field are disabled, and the sheet says to reconnect the account in Settings
