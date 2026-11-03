# Spec Delta

## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: Triage Folders and Counts
Tinjau SHALL offer the folder chips "Semua", "Apa ini?", "Perlu jawaban" and "Pindah saldo belum lengkap" (awaiting and ambiguous legs), each with its row count. "Semua" SHALL be the default and SHALL list the three in that order. Without truncation, the three counts SHALL add up to the summary's `attention`. The header SHALL show no amount.

#### Scenario: Counts match the badge
- **GIVEN** one open question, two needs-review rows in one group, one awaiting send and one ambiguous credit
- **WHEN** Tinjau loads
- **THEN** the chips SHALL read Apa ini? 1, Perlu jawaban 2 and Pindah saldo belum lengkap 2, the header and the sidebar badge SHALL both read 5, and "Semua" SHALL list four items

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
When no item needs the owner, Tinjau SHALL show "Semua beres" with this cycle's Keluar, the safe-to-spend amount (or the remaining amount when no daily figure is available) and the unclassified amount from the cycle summary, a link back to Beranda and, when automatic rows exist, "Cek yang otomatis". A failed summary SHALL omit the figures rather than show zero.

#### Scenario: Nothing waiting
- **GIVEN** no question, group, awaiting or ambiguous leg, and three automatic rows
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua beres", the three cycle figures and "Cek yang otomatis"

#### Scenario: Summary unavailable
- **GIVEN** nothing is waiting and the cycle summary request fails
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua beres" without figures and SHALL NOT show Rp 0

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
- **THEN** "Apa ini?" SHALL open at its first item, or "Semua beres" if empty, with the message that the item was already resolved

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
