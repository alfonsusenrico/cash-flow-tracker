## ADDED Requirements

### Requirement: Transfer Answers Ask for the Other Side
An answer whose category is an internal-movement category SHALL try to link the answered row as a movement at once, by the pairing rules. Without a unique counterpart the row SHALL stay `needs_review` with the reason `awaiting_counterpart`, so Tinjau keeps asking for its other side. The question SHALL be closed either way.

#### Scenario: Answer names a transfer without a counterpart
- **GIVEN** a Jago income of Rp 500.000 on Kantong Utama with an open purpose question and no unlinked expense of Rp 500.000 within the pairing window
- **WHEN** the owner answers with the suggestion in "Internal Movement"
- **THEN** the question SHALL be closed, the row SHALL be in "Internal Movement" with `classified_by = owner`, `review_status = needs_review` and the reason `awaiting_counterpart`, and Tinjau SHALL list it under `awaiting`

#### Scenario: Answer names a transfer with one counterpart
- **GIVEN** an unlinked BCA expense of Rp 500.000 30 seconds earlier that is the row's only compatible counterpart
- **WHEN** the owner answers the Jago income's question in "Internal Movement"
- **THEN** both rows SHALL be linked as one movement, and the answer's undo SHALL also undo the link
