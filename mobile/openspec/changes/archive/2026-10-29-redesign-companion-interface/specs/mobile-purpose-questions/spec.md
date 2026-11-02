## MODIFIED Requirements

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

### Requirement: Tapping a purpose question opens its row
Tapping a purpose question notification SHALL open the row's question in Tanya, below the original notification.

#### Scenario: Notification body tapped
- **WHEN** the owner taps the body of an "Apa ini?" notification
- **THEN** the app opens Tanya at that row's question

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
