## ADDED Requirements

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

## MODIFIED Requirements

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
