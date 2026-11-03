# review-queue Specification

## Purpose
Let the owner confirm or correct every automatically recorded or doubtful row from one web page:
answer purpose questions, accept, recategorise or rename whole merchant groups, and finish one-sided
or ambiguous transfers, with every correction teaching merchant memory.

## Requirements

### Requirement: Review Summary
`GET /api/review/summary` SHALL return, for the authenticated owner, `attention` (expense and income rows with `review_status = needs_review` or an open purpose question) and `unreviewed` (`auto` rows without an open question). The navigation SHALL show a "Tinjau" entry linking to `/review` with the `attention` count as a badge whose number is also announced in words.

#### Scenario: Badge count
- **GIVEN** one open purpose question, two rows awaiting a counterpart, one ambiguous credit and two automatic merchant rows
- **WHEN** the summary is requested
- **THEN** `attention` SHALL be 4 and `unreviewed` 2.

### Requirement: Finished Movements Are Not Counted
Rows that are a leg of a linked movement SHALL NOT be counted in the summary, the queue counts or the monthly report's waiting counts unless they carry an open purpose question. Their stored review status SHALL NOT be changed.

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

### Requirement: Bulk Review Actions
`POST /api/review/bulk` SHALL apply one action (`accept`, `recategorise`, `rename`, `keep_separate`) to 1–200 distinct transaction ids of the owner in one database transaction. Any id that is not the owner's SHALL reject the whole request with 404 `transactions_not_found` and change nothing.

#### Scenario: Recategorise a merchant group
- **GIVEN** two automatic "Kedai Awan" payments in another category
- **WHEN** the owner recategorises both into "Jajan & Nongkrong" with pillar want
- **THEN** both rows SHALL be reviewed, classified by the owner, with that category and pillar
- **AND** exactly two correction feedback rows SHALL be written, one per row
- **AND** owner memory for `merchant:kedai awan` SHALL hold the new category
- **AND** repeating the request SHALL write no further feedback.

#### Scenario: Accept
- **WHEN** the owner accepts the two payments
- **THEN** both SHALL be reviewed, still classified by the rule, and no feedback SHALL be written.

#### Scenario: Wrong category kind
- **WHEN** the owner recategorises expense rows into an income category
- **THEN** the response SHALL be 422 `category_type_mismatch` and no row SHALL change.

#### Scenario: Foreign row in the batch
- **WHEN** the batch contains another user's transaction id
- **THEN** the response SHALL be 404 `transactions_not_found` naming that id and the owner's rows SHALL be unchanged.

#### Scenario: Movement legs in the batch
- **WHEN** a batch with a manual payment and both legs of a movement is recategorised
- **THEN** the response SHALL be 409 `movement_leg_not_editable` listing the legs and nothing SHALL change
- **AND** accepting the same batch SHALL mark all three reviewed and keep the movement linked.

#### Scenario: Invalid bodies
- **WHEN** the body has no ids, more than 200 ids, repeated ids, an unknown action, a field the action does not take, `recategorise` without a category or `rename` without a merchant or notes
- **THEN** the response SHALL be 422.

### Requirement: Tinjau Page
The `/review` page SHALL show, in Indonesian, a triage inbox: a list of folders and items, and one selected item with its answers. Items are open "Apa ini?" questions with suggestion choices and a text answer sent to the existing answer endpoint; groups with "Terima", "Ubah kategori" (category and Kakeibo pillar) and "Ganti nama" (merchant and notes); awaiting transfers; ambiguous legs; and history suggestions. Each row SHALL link to the ledger edit dialog.

#### Scenario: Keyboard group accept
- **WHEN** the owner moves focus to a group item's "Terima" and presses Enter
- **THEN** one bulk request SHALL accept every row of the group, and a status message SHALL name the group and the count and offer "Urungkan"

#### Scenario: Answer by text
- **WHEN** the owner types an answer in "Jawab sendiri" and presses Enter
- **THEN** the answer SHALL be posted with the question id and a fresh reply id, and the question SHALL leave the list

#### Scenario: Session answer passes the origin check
- **GIVEN** a browser session cookie
- **WHEN** the answer endpoint is called without an Origin header or from a foreign origin
- **THEN** the response SHALL be 403
- **AND** with the app's origin it SHALL apply the answer.

#### Scenario: Complete an awaiting transfer
- **WHEN** the owner chooses "BCA Harian" for a GoPay send and submits
- **THEN** the page SHALL call the completion endpoint with `target_account_id` of BCA Harian.

### Requirement: Tinjau Sections by Need
Tinjau SHALL put groups with at least one `needs_review` row in the "Perlu jawaban" folder, and groups of only `auto` rows in "Dicatat otomatis", which is folded by default and labelled optional. The header SHALL show the `attention` count as the number that needs the owner and the `unreviewed` count separately as optional.

#### Scenario: Optional automatic rows
- **GIVEN** one group with a `needs_review` row and one group of only `auto` rows
- **WHEN** the page loads
- **THEN** the first group SHALL be in "Perlu jawaban", the second SHALL be inside the folded "Dicatat otomatis" section marked "opsional · boleh dilewati"
- **AND** the header SHALL show the summary's `attention` and `unreviewed` numbers as two separate counts.

#### Scenario: Automatic rows inside a mixed group
- **GIVEN** a "Perlu jawaban" group that also holds two `auto` rows
- **WHEN** the page loads
- **THEN** the "Dicatat otomatis" fold SHALL say that 2 more automatic rows are in "Perlu jawaban" groups

### Requirement: Review Group Keys
Review groups SHALL be keyed by type and merchant, else the phase 3 text signature, else notes, else category. Linked movement legs SHALL NOT form a group.

#### Scenario: Merchant-less rows with one signature
- **GIVEN** two automatic merchant-less expenses with the same text signature
- **WHEN** the queue is requested
- **THEN** both SHALL be in one group.

### Requirement: Review Group Summary
Each review group SHALL report its count, total, first and last date, counts per review status and its open review reasons.

#### Scenario: Mixed review statuses
- **GIVEN** a group with one `auto` and one `needs_review` row of Rp 20.000 each on two days
- **WHEN** the queue is requested
- **THEN** the group SHALL report count 2, total Rp 40.000, both dates and one row per status.

### Requirement: Review Queue Row Limit
The review queue SHALL read at most 1 000 rows and `truncated` SHALL say when more exist.

#### Scenario: More rows than the limit
- **GIVEN** 1 001 rows awaiting review
- **WHEN** the queue is requested
- **THEN** `truncated` SHALL be true.

### Requirement: Bulk Accept Keeps the Classifier
The bulk `accept` action SHALL mark rows reviewed without changing their classifier.

#### Scenario: Accepting an AI row
- **GIVEN** an AI-classified row with `review_status = auto`
- **WHEN** the owner accepts it in bulk
- **THEN** it SHALL be reviewed and still classified by `ai`.

### Requirement: Bulk Recategorise and Rename
The bulk `recategorise` and `rename` actions SHALL reject movement legs with 409 `movement_leg_not_editable`, SHALL use the same owner-edit rule as editing one row, and SHALL write exactly one owner `correction` feedback row and owner merchant memory for each row whose classification changed.

#### Scenario: Rename two rows
- **WHEN** the owner renames two automatic rows to the merchant "Kedai Awan"
- **THEN** both rows SHALL be owner-classified and reviewed with one correction feedback row each.

### Requirement: Keep Movement Legs Separate
The bulk `keep_separate` action SHALL mark unlinked legs reviewed and never pair them automatically again, and SHALL reject linked legs with 409 `already_linked`.

#### Scenario: Linked leg
- **WHEN** the owner keeps a linked movement leg separate
- **THEN** the response SHALL be 409 `already_linked` and nothing SHALL change.

#### Scenario: Unlinked leg stays unpaired
- **GIVEN** an unlinked leg kept separate by the owner
- **WHEN** the movement repair pass later finds a matching counterpart
- **THEN** the leg SHALL stay unlinked and reviewed.

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

### Requirement: Top-up From an Awaiting Debit
For an awaiting outgoing leg from a liquid account the card SHALL offer "Ini top up investasi", listing the owner's amount-tracked mutual-fund products as "Induk · Produk" and converting the leg through `POST /api/investment-topups/from-transaction`. Unit-tracked mutual funds SHALL be named with a link to the accounts page. A `topup_suggestion` SHALL lead the card.

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

### Requirement: Ambiguous Leg Resolution on Tinjau
The `/review` page SHALL let the owner link an ambiguous leg to a candidate through `POST /api/movements/merge` or keep it separate.

#### Scenario: Link to a candidate
- **WHEN** the owner links an ambiguous send to a listed credit
- **THEN** the page SHALL call `POST /api/movements/merge` with the expense and income transaction ids.

#### Scenario: Keep separate
- **WHEN** the owner keeps an ambiguous leg separate
- **THEN** the page SHALL send the bulk `keep_separate` action for that leg.

### Requirement: Accessible Tinjau Actions
Every Tinjau action, including undo, SHALL be operable by keyboard, SHALL report success or failure in text, and SHALL never claim success for a failed request.

#### Scenario: Failed bulk action
- **WHEN** a group action fails
- **THEN** the page SHALL report the failure in text and SHALL NOT show a success message.

#### Scenario: Failed undo
- **WHEN** an undo is answered with 409, 410 or 404
- **THEN** the page SHALL say in text why it could not be undone, link to the row in Transaksi, and SHALL NOT show the item as restored

### Requirement: Batched Group Actions on Tinjau
Groups larger than 200 rows SHALL be sent in batches, and a partial failure SHALL state how many rows were saved. Undoing a batched action SHALL undo its batches newest first and SHALL state how many rows were restored.

#### Scenario: Partial failure
- **GIVEN** a group of 250 rows
- **WHEN** the owner accepts the group and the second batch fails
- **THEN** the page SHALL state that 200 rows were saved.

#### Scenario: Batched undo
- **GIVEN** a group of 250 rows accepted in two batches
- **WHEN** the owner presses "Urungkan" and the older batch's undo is refused
- **THEN** the page SHALL state that 50 of 250 rows were restored

### Requirement: Triage Folders and Counts
Tinjau SHALL offer the folder chips "Semua", "Belum dikenali", "Perlu jawaban" and "Pindah saldo belum lengkap" (awaiting and ambiguous legs), each with its row count. "Semua" SHALL be the default and SHALL list the three in that order. Without truncation, the three counts SHALL add up to the summary's `attention`. The header, "Perlu ditinjau", SHALL show no amount.

#### Scenario: Counts match the badge
- **GIVEN** one open question, two needs-review rows in one group, one awaiting send and one ambiguous credit
- **WHEN** Tinjau loads
- **THEN** the chips SHALL read Belum dikenali 1, Perlu jawaban 2 and Pindah saldo belum lengkap 2, the header and the sidebar badge SHALL both read 5, and "Semua" SHALL list four items

#### Scenario: Folder filter
- **WHEN** the owner chooses the "Pindah saldo belum lengkap" chip
- **THEN** only the awaiting and ambiguous legs SHALL be listed and the chip SHALL be marked pressed

### Requirement: One Item at a Time
Tinjau SHALL show the selected item large, with kind, title, account, local time, provenance, amount, reasons and numbered choices. When the main column is wider than 720 px, the list and the item SHALL sit side by side. When it is 720 px or narrower, one SHALL show at a time.

#### Scenario: Two panes on a wide screen
- **WHEN** Tinjau opens at 1280 px
- **THEN** the list and the first item SHALL both be visible

#### Scenario: One pane at 768 px
- **WHEN** Tinjau opens at 768 px
- **THEN** only the list SHALL be visible until the owner opens an item

#### Scenario: Hidden balances
- **GIVEN** "Sembunyikan saldo" is on
- **WHEN** an item is shown
- **THEN** its amount SHALL be masked as elsewhere in the app

### Requirement: Auto-Advance After an Answer
After a successful action, Tinjau SHALL select the item that followed the answered one in the list as it was before the action, else the one before it, and SHALL move focus to that item's heading. When the folder is empty, it SHALL move to "Semua". When nothing is left there, it SHALL show "Semua beres".

#### Scenario: Next item
- **GIVEN** three items in "Perlu jawaban" with the second selected
- **WHEN** the owner accepts it
- **THEN** the third item SHALL be selected and focused, and the status message SHALL name it

#### Scenario: Answered elsewhere
- **GIVEN** the selected item is answered in another tab
- **WHEN** Tinjau refetches
- **THEN** it SHALL select the next item by the same rule and say that the item was resolved elsewhere

### Requirement: Semua Beres
When no item needs the owner, Tinjau SHALL show "Semua sudah ditinjau" with this cycle's Keluar, the safe-to-spend amount (or the remaining amount when no daily figure is available) and the unclassified amount from the cycle summary, a link back to Beranda and, when automatic rows exist, "Cek yang otomatis". A failed summary SHALL omit the figures rather than show zero.

#### Scenario: Nothing waiting
- **GIVEN** no question, group, awaiting or ambiguous leg, and three automatic rows
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua sudah ditinjau", the three cycle figures and "Cek yang otomatis"

#### Scenario: Summary unavailable
- **GIVEN** nothing is waiting and the cycle summary request fails
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua sudah ditinjau" without figures and SHALL NOT show Rp 0

### Requirement: Phone List and Item
At 720 px main width or narrower, opening an item SHALL show it in place of the list with a "Daftar" button, and SHALL add a browser history entry. "Daftar", Esc and the browser's back SHALL return to the list with focus on that item's row.

#### Scenario: Back to the list
- **GIVEN** Tinjau at 390 px
- **WHEN** the owner opens an item and then uses the browser's back
- **THEN** the list SHALL show with focus on the item's row, and the app SHALL stay on `/review`

### Requirement: Tinjau Deep Links
`/review?item=<transaction id>` SHALL select the item that contains that transaction. `folder=apa|jawab|pindah|saran|otomatis`, or else the legacy hashes `#review-questions`, `#review-groups`, `#review-awaiting` and `#review-ambiguous`, SHALL choose its folder. An id that is no longer waiting SHALL open the folder at its first item and say so.

#### Scenario: Link from Kerjakan dulu
- **WHEN** the owner follows Beranda's "Pasangkan" link `/review?item=<send id>#review-awaiting`
- **THEN** Tinjau SHALL open "Pindah saldo belum lengkap" with that send selected and its heading focused

#### Scenario: Resolved meanwhile
- **WHEN** the owner opens `/review?item=<id>#review-questions` for a question already answered
- **THEN** "Belum dikenali" SHALL open at its first item, or "Semua sudah ditinjau" if empty, with the message that the item was already resolved

### Requirement: Triage Keys
While focus is inside the triage region, J and K SHALL move to the next and previous item of the folder. E SHALL press the item's highlighted choice. T SHALL open "Ini top up investasi" on an eligible outgoing leg. 1–9 SHALL press the nth numbered choice of an open picker, else of the item. Z SHALL undo. Keys SHALL be ignored in fields, outside the region and with the single-key switch off.

#### Scenario: Walk and accept
- **GIVEN** focus on the first row of "Perlu jawaban"
- **WHEN** the owner presses J, then E
- **THEN** the second item SHALL be selected and accepted, and the third item SHALL be selected

#### Scenario: Outside the region
- **WHEN** focus is on the top bar's search button and the owner presses J
- **THEN** nothing SHALL happen in Tinjau

#### Scenario: Typing an answer
- **WHEN** the owner types "1 kopi" into "Jawab sendiri"
- **THEN** no choice SHALL be pressed

#### Scenario: Switched off
- **GIVEN** "Pintasan satu tombol" is off
- **WHEN** the owner presses E with focus in an item
- **THEN** nothing SHALL be accepted

#### Scenario: Help lists the page keys
- **WHEN** the owner presses ? on Tinjau
- **THEN** the help dialog SHALL list J/K, Enter, E, T, 1–9 and Z under "Di Tinjau"

### Requirement: Urungkan on Tinjau
Each successful action that returns an undo token SHALL show a status message with "Urungkan". "Urungkan" and Z SHALL undo the tab's newest undoable action through `POST /api/review/undo` until its `expires_at`, also after the message has closed. After an undo, the restored item SHALL be selected and focused. Tokens SHALL be kept only in memory.

#### Scenario: Undo by Z after the message closed
- **GIVEN** the owner accepted a group 2 minutes ago and the message has closed
- **WHEN** the owner presses Z inside Tinjau
- **THEN** the accept SHALL be undone, the group SHALL be selected again in its folder and its heading focused

#### Scenario: Nothing to undo
- **WHEN** the owner presses Z with no undoable action in this tab
- **THEN** the page SHALL say that there is nothing to undo and send no request

#### Scenario: Response without a token
- **WHEN** an action's response carries no `undo_token`
- **THEN** its message SHALL show no "Urungkan" and Z SHALL not target it

#### Scenario: Tokens are not stored
- **WHEN** the owner acts and reloads the page
- **THEN** no token SHALL be found in browser storage or the URL, and Z SHALL have nothing to undo

### Requirement: Awaiting Leg as an Expense
An awaiting outgoing leg SHALL offer "Bukan pindah saldo, ini pengeluaran". It asks for an expense category and optional pillar and sends the bulk `recategorise` action for that leg, so the leg becomes a reviewed, owner-classified expense that is no longer awaiting.

#### Scenario: Leg recorded as spending
- **GIVEN** an awaiting Rp 30.000 send from "GoPay Contoh"
- **WHEN** the owner chooses "Bukan pindah saldo, ini pengeluaran", picks "Makanan & Minuman" and saves
- **THEN** one bulk `recategorise` request SHALL carry the leg's id and that category, and the leg SHALL leave "Pindah saldo belum lengkap"
- **AND** the movement repair pass SHALL NOT pair it afterwards
