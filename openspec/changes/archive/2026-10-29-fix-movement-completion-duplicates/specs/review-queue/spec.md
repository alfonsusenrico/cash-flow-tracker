## MODIFIED Requirements

### Requirement: Review Queue
`GET /api/review/queue` SHALL return the owner's open purpose questions, groups of `auto` and `needs_review` expense/income rows, unlinked "Internal Movement" rows that the pairing flagged `awaiting_counterpart`, and ambiguous movement legs with their candidate counterparts. Every row SHALL appear in at most one section.

#### Scenario: Sections
- **GIVEN** a merchant-less payment with an open question, two automatic payments at "Kedai Awan" and two GoPay sends competing for one BCA credit
- **WHEN** the queue is requested
- **THEN** the question SHALL be listed under `questions` and not in any group
- **AND** one merchant group "Kedai Awan" SHALL hold both payments with their total
- **AND** the credit and both sends SHALL be listed under `ambiguous`, each with the others as candidates.

#### Scenario: Ambiguous leg without candidates
- **GIVEN** the credit of the previous scenario was kept separate
- **WHEN** the queue is requested
- **THEN** both sends SHALL be listed under `awaiting` and `ambiguous` SHALL be empty.

#### Scenario: Resolved reasons are hidden
- **WHEN** a linked movement leg stores the reason `awaiting_counterpart`, or a row stores `purpose_question` without an open question
- **THEN** the queue SHALL not show that reason.

#### Scenario: Older one-sided rows are not awaiting
- **GIVEN** an unlinked "Internal Movement" row from before the pairing existed, with review status `auto` and no stored `awaiting_counterpart` reason
- **WHEN** the queue is requested
- **THEN** the row SHALL NOT be listed under `awaiting`
- **AND** it SHALL be listed in a group, where accepting marks it reviewed.

### Requirement: Awaiting Transfer Completion on Tinjau
The `/review` page SHALL let the owner complete an awaiting transfer with an account or pocket picker through `POST /api/movements/complete`. When the server returns `counterpart_candidates`, the page SHALL list the existing rows and offer "Hubungkan", which links the chosen row through `POST /api/movements/merge`, and "Tetap buat baru", which resends with `create_new: true`.

#### Scenario: Pocket as target
- **WHEN** the owner picks a pocket for an awaiting transfer and submits
- **THEN** the completion request SHALL carry that pocket's id as `target_account_id`.

#### Scenario: Existing leg offered before creating
- **GIVEN** the chosen account already holds a matching Rp 50.000 income
- **WHEN** the owner submits the completion and the server answers 409 `counterpart_candidates`
- **THEN** the card SHALL list that income under "Sudah ada di rekening ini"
- **AND** choosing it and pressing "Hubungkan" SHALL send a merge of the two rows
- **AND** no completion with `create_new` SHALL be sent unless the owner presses "Tetap buat baru".
