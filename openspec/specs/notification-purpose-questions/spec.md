# notification-purpose-questions Specification

## Purpose
Ask the owner "Apa ini?" for every recorded payment or income whose notification names no merchant or
sender, offer ranked one-tap suggestions, accept a suggestion or short text from the companion or the web,
and turn the answer into the row's classification and a labelled example.

## Requirements

### Requirement: Purpose Question for Merchant-Less Notifications
When a notification is recorded as a single unpaired expense or income with a signature key, the system SHALL attach an open purpose question (new `question_id`, transaction id, Indonesian prompt naming amount and account, up to five suggestions, free text allowed) and flag the row `needs_review` with the reason `purpose_question`. Named merchants, self or owned-account transfers, movements and memory hits SHALL NOT get one.

#### Scenario: myBCA category-only payment
- **WHEN** "You spent IDR 187,500.00 at Groceries." is recorded
- **THEN** the event SHALL have an open purpose question whose prompt starts with "Apa ini?" and names Rp187.500.

#### Scenario: Masked incoming transfer after the sender flow
- **GIVEN** the owner named the masked sender earlier
- **WHEN** a transfer from that mask is recorded as income
- **THEN** a purpose question SHALL be attached.

#### Scenario: Named merchant
- **WHEN** "You've paid Rp125.000 to Kedai Awan" is recorded
- **THEN** no purpose question SHALL be attached.

#### Scenario: Leg paired as movement
- **WHEN** a merchant-less leg is paired automatically with its counterpart
- **THEN** no purpose question SHALL remain open on either event.

### Requirement: Ranked Purpose Suggestions
Suggestions SHALL rank the owner's feedback of the same direction with an active category by signature, account, amount band, local hour and weekday, as scored in the design, summing identical classifications. Then the current classification SHALL follow when it has a category, and incoming transfers SHALL include a "Patungan & Ganti Uang" suggestion when the owner has that category. At most five suggestions SHALL be offered.

#### Scenario: Time and amount ranking
- **GIVEN** owner answers "makan kafe senja" three times on Wednesday evenings around Rp200.000 and "beli roti bakar" twice on Monday mornings around Rp30.000, all for the same signature
- **WHEN** a Rp187.500 payment with that signature is recorded on a Wednesday at 19:00
- **THEN** "makan kafe senja" SHALL be the first suggestion.

#### Scenario: Repayment suggestion for incoming transfer
- **WHEN** a masked incoming transfer gets a question and the owner has "Patungan & Ganti Uang"
- **THEN** one suggestion SHALL use that category.

#### Scenario: No history
- **WHEN** the owner has no feedback yet
- **THEN** the suggestions SHALL contain only the current classification and, for incoming transfers, the repayment suggestion.

### Requirement: Answer a Purpose Question
`POST /api/ingest/notifications/{event_id}/answer-purpose` SHALL take `question_id`, `reply_id` and one of `suggestion_id` or `text`. A suggestion's values, or the validated output of a text normaliser, SHALL be applied with `classified_by = owner` and `review_status = reviewed`, closing the question and writing feedback and memory. If normalising fails, the text SHALL become the notes and the row SHALL stay `needs_review`. It SHALL be owner-scoped and idempotent on `reply_id`.

#### Scenario: Answer with a suggestion
- **WHEN** the owner answers with the suggestion "makan kafe senja" in "Makanan & Minuman"
- **THEN** the row SHALL have those notes and that category, `review_status = reviewed`, `classified_by = owner`, and the question SHALL be closed.

#### Scenario: Answer with text
- **WHEN** the owner answers "sewa lapangan arena futsal" and the normaliser returns merchant "Arena Futsal", description "Sewa lapangan Arena Futsal", category "Olahraga" and pillar culture
- **THEN** the row SHALL get those values and be reviewed.

#### Scenario: Normaliser fails
- **WHEN** the owner answers with text and the provider fails
- **THEN** the notes SHALL be the owner's text, the category SHALL be unchanged and the row SHALL be `needs_review`.

#### Scenario: Replay
- **WHEN** the same `reply_id` is sent again with the same answer
- **THEN** the response SHALL be the current result and nothing SHALL change.

#### Scenario: Conflicting replay
- **WHEN** the same `reply_id` is sent with a different answer
- **THEN** the response SHALL be 409 `purpose_answer_conflict`.

#### Scenario: Stale question
- **WHEN** the `question_id` is not the event's open purpose question
- **THEN** the response SHALL be 409 `stale_question`.

#### Scenario: Other owner's event
- **WHEN** another owner sends an answer for the event
- **THEN** the response SHALL be 404.

#### Scenario: Invalid body
- **WHEN** both or neither of `suggestion_id` and `text` are sent, or the suggestion id is unknown
- **THEN** the response SHALL be 422.

### Requirement: Owner Edit Closes the Question
When the owner corrects a row's classification through the ledger while its purpose question is open, the question SHALL close and the correction SHALL be recorded as feedback.

#### Scenario: Ledger edit before answering
- **WHEN** the owner changes the notes of a row with an open purpose question
- **THEN** the event SHALL have no open question.

### Requirement: Companion Version Gating
A notification result SHALL include `review_status` and an open purpose `question` only when the event's companion version is 1.8.0 or later. Older companions SHALL see the recorded result without these fields.

#### Scenario: Companion 1.8.0
- **WHEN** a 1.8.0 companion reads the result of an event with an open question
- **THEN** the result SHALL include `review_status = needs_review` and the question.

#### Scenario: Companion 1.7.0
- **WHEN** a 1.7.0 companion reads the same result
- **THEN** the result SHALL have neither field.

### Requirement: Open Questions for the Web
`GET /api/ingest/purpose-questions` SHALL list the owner's open purpose questions, newest first, each with its event id, question and a summary of the transaction, excluding questions whose transaction was deleted or became a movement leg. It SHALL not depend on the companion version.

#### Scenario: Listing
- **GIVEN** two open questions and one answered question
- **WHEN** the owner lists purpose questions
- **THEN** exactly the two open questions SHALL be returned, and none of another owner's.
