## MODIFIED Requirements

### Requirement: Review Summary
`GET /api/review/summary` SHALL return, for the authenticated owner, `attention` (expense and income rows with `review_status = needs_review` or an open purpose question) and `unreviewed` (`auto` rows without an open question). Rows that are a leg of a linked movement SHALL NOT be counted unless they carry an open purpose question. The monthly report's waiting counts SHALL use the same rule. The navigation SHALL show a "Tinjau" entry linking to `/review` with the `attention` count as a badge whose number is also announced in words.

#### Scenario: Badge count
- **GIVEN** one open purpose question, two rows awaiting a counterpart, one ambiguous credit and two automatic merchant rows
- **WHEN** the summary is requested
- **THEN** `attention` SHALL be 4 and `unreviewed` 2.

#### Scenario: Completed movement is not counted
- **GIVEN** an automatic merchant row and both legs of a linked movement recorded with review status `auto`, and a linked leg with review status `needs_review`
- **WHEN** the summary and the monthly report are requested
- **THEN** `attention` SHALL be 0 and `unreviewed` 1 in both, and the stored review statuses of the legs SHALL be unchanged.

### Requirement: Review Queue
`GET /api/review/queue` SHALL return the owner's open purpose questions, groups of `auto` and `needs_review` expense/income rows that are not a leg of a linked movement, unlinked "Internal Movement" rows that the pairing flagged `awaiting_counterpart`, and ambiguous movement legs with their candidate counterparts. Every row SHALL appear in at most one section. `counts` SHALL include `attention` and `unreviewed` equal to the summary's values.

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

#### Scenario: Linked legs are not listed
- **GIVEN** both legs of a linked movement with review status `auto`
- **WHEN** the queue is requested
- **THEN** neither leg SHALL appear in any section and `counts.unreviewed` SHALL not include them.

### Requirement: Tinjau Page
The `/review` page SHALL show, in Indonesian: open "Apa ini?" questions with suggestion chips and a text answer sent to the existing answer endpoint; groups with at least one `needs_review` row under "Perlu jawaban" and groups of only `auto` rows under "Dicatat otomatis", which SHALL be collapsed by default and say that checking it is optional; group actions "Terima semua", "Ubah kategori semua" (category and Kakeibo pillar) and "Ganti nama" (merchant and notes); each row's link to the ledger edit dialog; awaiting transfers; and ambiguous legs. The page header SHALL show the `attention` count as the number that needs the owner and the `unreviewed` count separately as optional.

#### Scenario: Keyboard group accept
- **WHEN** the owner tabs to "Terima semua" of a group and presses Enter
- **THEN** one bulk request SHALL accept every row of the group and a status message SHALL name the group and the count.

#### Scenario: Answer by text
- **WHEN** the owner types an answer in "Jawab sendiri" and presses Enter
- **THEN** the answer SHALL be posted with the question id and a fresh reply id, and the question SHALL leave the list.

#### Scenario: Session answer passes the origin check
- **GIVEN** a browser session cookie
- **WHEN** the answer endpoint is called without an Origin header or from a foreign origin
- **THEN** the response SHALL be 403
- **AND** with the app's origin it SHALL apply the answer.

#### Scenario: Complete an awaiting transfer
- **WHEN** the owner chooses "BCA Harian" for a GoPay send and submits
- **THEN** the page SHALL call the completion endpoint with `target_account_id` of BCA Harian.

#### Scenario: Optional automatic rows
- **GIVEN** one group with a `needs_review` row and one group of only `auto` rows
- **WHEN** the page loads
- **THEN** the first group SHALL be under "Perlu jawaban", the second SHALL be inside the collapsed "Dicatat otomatis" section marked optional
- **AND** the header SHALL show the summary's `attention` and `unreviewed` numbers as two separate counts.

### Requirement: Review Group Keys
Review groups SHALL be keyed by type and merchant, else the phase 3 text signature, else notes, else category. Linked movement legs SHALL NOT form a group.

#### Scenario: Merchant-less rows with one signature
- **GIVEN** two automatic merchant-less expenses with the same text signature
- **WHEN** the queue is requested
- **THEN** both SHALL be in one group.

### Requirement: Awaiting Transfer Completion on Tinjau
The `/review` page SHALL let the owner complete an awaiting transfer with an account or pocket picker through `POST /api/movements/complete`. When the server returns `counterpart_candidates`, the page SHALL list the existing rows and offer "Hubungkan", which links the chosen row through `POST /api/movements/merge`, and "Tetap buat baru", which resends with `create_new: true`. For an outgoing leg from a liquid account the card SHALL also offer "Ini top up investasi", listing the owner's amount-tracked mutual-fund products as "Induk · Produk" and converting the leg through `POST /api/investment-topups/from-transaction`; unit-tracked mutual funds SHALL be named with a link to the accounts page, where they can switch to amount tracking. When the queue carries a `topup_suggestion` for the leg, the card SHALL lead with it.

#### Scenario: Pocket as target
- **WHEN** the owner picks a pocket for an awaiting transfer and submits
- **THEN** the completion request SHALL carry that pocket's id as `target_account_id`.

#### Scenario: Existing leg offered before creating
- **GIVEN** the chosen account already holds a matching Rp 50.000 income
- **WHEN** the owner submits the completion and the server answers 409 `counterpart_candidates`
- **THEN** the card SHALL list that income under "Sudah ada di rekening ini"
- **AND** choosing it and pressing "Hubungkan" SHALL send a merge of the two rows
- **AND** no completion with `create_new` SHALL be sent unless the owner presses "Tetap buat baru".

#### Scenario: Settle a debit as a top-up
- **GIVEN** an awaiting Rp 112.590 debit from "RDN Contoh" and an amount-tracked product "Reksa Dana A" under "Bibit Contoh"
- **WHEN** the owner chooses "Ini top up investasi", picks "Bibit Contoh · Reksa Dana A" and submits
- **THEN** the page SHALL post the conversion with the debit's id and the product's id
- **AND** after success the debit SHALL leave the awaiting section.

#### Scenario: Unit-tracked fund is explained
- **GIVEN** the owner's only mutual fund records units
- **WHEN** the owner opens "Ini top up investasi"
- **THEN** the card SHALL name that fund, say it must switch to amount tracking first and link to the accounts page, and SHALL send no conversion.

#### Scenario: Suggestion actions
- **GIVEN** a `topup_suggestion` for "Reksa Dana A"
- **WHEN** the owner presses "Ya, top up"
- **THEN** the conversion SHALL carry `suggested_target_account_id` equal to the product
- **AND** "Pilih produk lain" SHALL send the chosen product with the suggested one as `suggested_target_account_id`
- **AND** "Bukan top up" SHALL post the rejection and then show the ordinary account picker.
