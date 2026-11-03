# Spec Delta

## MODIFIED Requirements

### Requirement: Answer a Purpose Question
`POST /api/ingest/notifications/{event_id}/answer-purpose` SHALL take `question_id`, `reply_id` and one of `suggestion_id` or `text`. A suggestion's values, or the validated output of a text normaliser, SHALL be applied with `classified_by = owner` and `review_status = reviewed`, closing the question and writing feedback and memory. If normalising fails, the text SHALL become the notes and the row SHALL stay `needs_review`. It SHALL be owner-scoped and idempotent on `reply_id`, and an applied answer SHALL return an undo token.

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
- **THEN** the response SHALL be the current result, `undo_token` SHALL be `null`, and nothing SHALL change.

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

#### Scenario: Undo token
- **WHEN** an answer is applied, also one that leaves the row `needs_review`
- **THEN** the response SHALL carry its existing fields plus a non-null `undo_token` and `expires_at`

#### Scenario: Replay of an undone answer
- **GIVEN** an answer that the owner undid
- **WHEN** the same `reply_id` is sent again with the same answer
- **THEN** the response SHALL be 409 `purpose_answer_undone`, nothing SHALL be reapplied, and the reopened question SHALL accept an answer with a new `reply_id`
