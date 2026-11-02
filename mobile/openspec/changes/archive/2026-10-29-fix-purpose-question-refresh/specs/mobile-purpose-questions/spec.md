## ADDED Requirements

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
- **THEN** the answer is `closed` with `question_closed`, its delivery makes no request, and the sheet says "Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi."

#### Scenario: Question still open while an answer waits
- **WHEN** a check finds the same question still open while the owner's answer is `queued`
- **THEN** the answer stays `queued` and is delivered as usual

#### Scenario: Answer accepted by the server
- **WHEN** the server accepts an answer and returns the result without the question
- **THEN** that answer becomes `accepted`, not `closed`

## MODIFIED Requirements

### Requirement: The sheet shows the answer's state
While an answer is pending, the sheet SHALL hide the controls and show its state. A failed answer SHALL show its reason and the controls again. A closed answer SHALL show the closed note: for a 409, that the server closed the question and the answer is not used; for `question_closed`, that the question is finished on the server and the answer is not sent again.

#### Scenario: Failed answer
- **WHEN** the row's answer failed with `unknown_suggestion`
- **THEN** the sheet shows "Saran ini tidak berlaku lagi. Pilih saran lain atau tulis jawaban." and the controls again

#### Scenario: Closed answer
- **WHEN** the row's answer is `closed`
- **THEN** the sheet shows "Pertanyaan sudah ditutup di server; jawaban ini tidak dipakai."

#### Scenario: Answer closed by a refreshed result
- **WHEN** the row's answer is `closed` with `question_closed`
- **THEN** the sheet shows "Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi."
