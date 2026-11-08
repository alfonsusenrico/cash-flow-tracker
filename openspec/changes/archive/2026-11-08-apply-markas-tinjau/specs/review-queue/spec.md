## MODIFIED Requirements

### Requirement: Tinjau Page
The `/review` page SHALL show, in the interface language, a triage inbox: a folder rail, an item list and one selected item with its answers. Items are open "Apa ini?" questions with suggestion choices and a text answer sent to the existing answer endpoint; groups with "Terima", "Ubah kategori" (category and Kakeibo pillar) and "Ganti nama" (merchant and notes); awaiting transfers; ambiguous legs; and history suggestions. Each transaction row SHALL link to the ledger edit dialog.

#### Scenario: Keyboard group accept
- **WHEN** the owner moves focus to a group item's "Terima" and presses Enter
- **THEN** one bulk request SHALL accept every row of the group, and a status message SHALL name the group and the count and offer "Urungkan"

#### Scenario: Answer by text
- **WHEN** the owner opens "Jawab sendiri", types an answer and presses Enter
- **THEN** the answer SHALL be posted with the question id and a fresh reply id, the dialog SHALL close, and the question SHALL leave the list

#### Scenario: Session answer passes the origin check
- **GIVEN** a browser session cookie
- **WHEN** the answer endpoint is called without an Origin header or from a foreign origin
- **THEN** the response SHALL be 403
- **AND** with the app's origin it SHALL apply the answer.

#### Scenario: Complete an awaiting transfer
- **WHEN** the owner chooses "BCA Harian" for a GoPay send and submits
- **THEN** the page SHALL call the completion endpoint with `target_account_id` of BCA Harian.

### Requirement: Tinjau Sections by Need
Tinjau SHALL put groups with at least one `needs_review` row in the "Perlu jawaban" folder, and groups of only `auto` rows in "Dicatat otomatis", an optional folder below the rail's rule that "Semua" does not list. The hero SHALL show the `attention` count as the number that needs the owner, and its line SHALL show the `unreviewed` count separately as optional.

#### Scenario: Optional automatic rows
- **GIVEN** one group with a `needs_review` row and one group of only `auto` rows
- **WHEN** the page loads
- **THEN** the first group SHALL be in "Perlu jawaban" and listed in "Semua", and the second SHALL be listed only when the owner chooses "Dicatat otomatis" below the rule, whose caption SHALL say that checking is optional
- **AND** the hero SHALL show the summary's `attention` and its line the `unreviewed` number, as two separate counts.

#### Scenario: Automatic rows inside a mixed group
- **GIVEN** a "Perlu jawaban" group that also holds two `auto` rows
- **WHEN** the owner chooses "Dicatat otomatis"
- **THEN** its caption SHALL say that 2 more automatic rows are in "Perlu jawaban" groups

### Requirement: Triage Folders and Counts
Tinjau SHALL show a folder rail under a "Folder" label: "Semua", "Belum dikenali", "Perlu jawaban" and "Pindah saldo belum lengkap" (awaiting and ambiguous legs), each with a right-aligned row count, then a rule and the optional folders. "Semua" SHALL be the default and SHALL list the needed three in that order. A folder without items SHALL be hidden unless it is "Semua" or current. Without truncation, the three counts SHALL add up to `attention`. The hero SHALL show no amount.

#### Scenario: Counts match the badge
- **GIVEN** one open question, two needs-review rows in one group, one awaiting send and one ambiguous credit
- **WHEN** Tinjau loads
- **THEN** the folders SHALL read Belum dikenali 1, Perlu jawaban 2 and Pindah saldo belum lengkap 2, the hero and the sidebar badge SHALL both read 5, and "Semua" SHALL list four items

#### Scenario: Folder filter
- **WHEN** the owner chooses "Pindah saldo belum lengkap"
- **THEN** only the awaiting and ambiguous legs SHALL be listed, and the folder SHALL be marked current with the straight selection bar beside its fill

#### Scenario: Empty folders hidden
- **GIVEN** no open question and no optional item
- **WHEN** Tinjau loads with items in "Perlu jawaban"
- **THEN** the rail SHALL show "Semua" and "Perlu jawaban" with their counts, and SHALL NOT show "Belum dikenali", the rule or any optional folder

#### Scenario: Optional folder chosen
- **GIVEN** three pending history suggestions and nothing that needs the owner
- **WHEN** the owner chooses "Saran" below the rule
- **THEN** its count SHALL read 3, it SHALL be marked current, and its items SHALL be listed with the caption that they are not counted as needing an answer

#### Scenario: Phone folders
- **WHEN** Tinjau opens at 390 px
- **THEN** the folders SHALL be one line of 44 px chips that scrolls sideways inside its box, the current chip SHALL be marked by its fill and edge without a coloured bar, and the page SHALL NOT scroll horizontally at 390 or 320 px

### Requirement: One Item at a Time
Tinjau SHALL show the selected item with its kind label, its position "N dari M", the question as its heading, one meta line (kind detail, amount, local time, account), the reasons and the numbered answers. From 1200 px the rail, list and item SHALL be three panes on one top edge. From 768 to 1199 px the item SHALL follow the rail and list at full width. Below 768 px an item SHALL open in a sheet.

#### Scenario: Two panes on a wide screen
- **WHEN** Tinjau opens at 1280 px
- **THEN** the list and the first item SHALL both be visible side by side, with the folder rail beside the list

#### Scenario: One pane at 768 px
- **WHEN** Tinjau opens at 768 px, and again at 767 px
- **THEN** at 768 px the first item SHALL be visible below the rail and the list, and at 767 px only the folder chips and the list SHALL be visible until the owner opens an item

#### Scenario: Three panes on a wide screen
- **WHEN** Tinjau opens at 1440 px with items waiting
- **THEN** the rail, the list and the first item SHALL be visible side by side at 232, 336 and the remaining width with 24 px gaps, and all three SHALL start at the same top edge

#### Scenario: Item below the list at 1024 px
- **WHEN** Tinjau opens at 1024 px
- **THEN** the rail and the list SHALL sit side by side, and the first item SHALL be visible below them at full width

#### Scenario: Hidden balances
- **GIVEN** "Sembunyikan saldo" is on
- **WHEN** an item is shown
- **THEN** every amount in its meta line, answers and rows SHALL be masked as elsewhere in the app, and a question that names an amount SHALL be replaced by one without it

### Requirement: Semua Beres
When no item needs the owner, Tinjau SHALL show "Semua sudah ditinjau" with one line that nothing needs an answer and, when automatic rows exist, "Cek yang dicatat otomatis", which opens the first item of "Dicatat otomatis". When optional items exist, the rail SHALL stay so they can be opened. Tinjau SHALL show no cycle figure in this state.

#### Scenario: Nothing waiting
- **GIVEN** no question, group, awaiting or ambiguous leg, and three automatic rows
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua sudah ditinjau" and "Cek yang dicatat otomatis", the hero SHALL read 0, and no Pengeluaran, Aman dipakai or Belum diklasifikasi figure SHALL appear

#### Scenario: Summary unavailable
- **GIVEN** nothing is waiting and the cycle summary request would fail
- **WHEN** Tinjau loads
- **THEN** it SHALL show "Semua sudah ditinjau" without figures, SHALL NOT show Rp 0, and SHALL NOT request the cycle summary

#### Scenario: Nothing at all
- **GIVEN** no item in any folder
- **WHEN** Tinjau loads
- **THEN** it SHALL show only the title block and "Semua sudah ditinjau" with its line, without the hero, the rail or a button

### Requirement: Phone List and Item
Below 768 px, opening an item SHALL show it in a full-height sheet titled by its kind and SHALL add a browser history entry. The sheet's close button, Esc and the browser's back SHALL return to the list with focus on that item's row. The keys of the item SHALL work inside the sheet.

#### Scenario: Back to the list
- **GIVEN** Tinjau at 390 px
- **WHEN** the owner opens an item and then uses the browser's back
- **THEN** the sheet SHALL close, the list SHALL show with focus on the item's row, and the app SHALL stay on `/review`

#### Scenario: Answer inside the sheet
- **GIVEN** Tinjau at 390 px with a group item open in the sheet
- **WHEN** the owner presses 1
- **THEN** the group SHALL be accepted, and the sheet SHALL show the next item with its heading focused

### Requirement: Seen-Twice Section
`GET /api/review/queue` SHALL return `seen_twice`: duplicate observations from the last 30 days that the owner has not acknowledged and that no account link skipped, each with its app, amount, post time and the leg it duplicates. These items SHALL count in `unreviewed` and not in `attention`, and Tinjau SHALL list them in the optional "Terlihat dua kali" folder. Acknowledging SHALL remove an item; recording it separately SHALL create its own leg with an undo token.

#### Scenario: Seen twice
- **GIVEN** Jago's "You've received Rp500.000 from GoPay." recorded as a duplicate observation of a GoPay credit
- **WHEN** the queue is requested
- **THEN** `seen_twice` SHALL list it with the GoPay leg as the kept leg, `counts.unreviewed` SHALL include it and `counts.attention` SHALL NOT
- **AND** "Benar, satu transaksi" SHALL acknowledge it and remove it from the queue, and "Catat terpisah" SHALL record the event as its own leg with an undo token

#### Scenario: Held report without a match
- **GIVEN** an event held with `possible_duplicate_observation` by the earlier prompt `notification-interpretation-11`
- **WHEN** migration V46 runs
- **THEN** the event SHALL be queued again and decided by the deterministic path, and `seen_twice` SHALL never list a held report

#### Scenario: The pair on Tinjau
- **WHEN** the owner opens a "Terlihat dua kali" item
- **THEN** it SHALL show the recorded leg as a whole-row link to that transaction and this report as a plain row, each with its time and amount

### Requirement: Skipped by a Linked Account
`GET /api/review/queue` SHALL return `skipped`: the owner's events skipped by an account link in the last 30 days, newest first, each with its app, direction, amount, post time, the kept leg and the link. They SHALL count only in `counts.skipped`, never in `unreviewed` or `attention`. Tinjau SHALL list them in the optional "Dilewati otomatis" folder with "Catat terpisah".

#### Scenario: A skipped Jago report
- **GIVEN** the link "Gopay juga dilaporkan Bank Jago" and a Jago report skipped as the second report of a GoPay credit
- **WHEN** the owner opens Tinjau
- **THEN** "Dilewati otomatis" SHALL list it with the GoPay leg and the link, `counts.skipped` SHALL be 1 and `counts.unreviewed` SHALL NOT include it

#### Scenario: Recording a skipped report separately
- **WHEN** the owner chooses "Catat terpisah" on that item
- **THEN** the event SHALL be recorded as its own leg with an undo token, the link SHALL stay active, and "Urungkan" SHALL make it a skipped report of the same link again

#### Scenario: Another owner's skipped report
- **WHEN** another owner requests `POST /api/review/seen-twice/{event_id}/record-separately` for that event
- **THEN** the response SHALL be 404 and nothing SHALL change

### Requirement: Question Row Direction
A "Belum dikenali" row in Tinjau's list SHALL show its amount with "+" and the income colour for an income and with "−" for an expense. In "Semua" its meta line SHALL read "Belum dikenali" with its date and time. The item's meta line SHALL say "Uang masuk" for an income and "Pembayaran tanpa nama toko" for an expense. Without a merchant or notes, its title SHALL be "Uang masuk" for an income and "Pembayaran tanpa nama toko" for an expense.

#### Scenario: Income question
- **GIVEN** an open question on an income of Rp 10.000 into Jago · Kantong Utama with notes "Uang masuk dari GoPay"
- **WHEN** Tinjau lists it and the owner opens it
- **THEN** the row SHALL show "+Rp 10.000" in the income colour and "Belum dikenali" with its time, the item's meta line SHALL say "Uang masuk", and neither SHALL show "−" or "Pembayaran tanpa nama toko"

#### Scenario: Expense question
- **WHEN** Tinjau lists and opens an open question on an expense of Rp 187.500
- **THEN** the row SHALL show "−Rp 187.500", and the item's meta line SHALL say "Pembayaran tanpa nama toko"

### Requirement: Triage Keys
While focus is inside the triage region or its sheet, J and K SHALL move to the next and previous item of the folder. E SHALL press the item's suggested answer. T SHALL open "Ini top up investasi" on an eligible outgoing leg. 1–9 SHALL press the nth numbered answer of an open picker, else of the item. Z SHALL undo. Keys SHALL be ignored in fields, outside the region and with the single-key switch off. Only numbered answers SHALL show a key cap.

#### Scenario: Walk and accept
- **GIVEN** focus on the first row of "Perlu jawaban"
- **WHEN** the owner presses J, then E
- **THEN** the second item SHALL be selected and accepted, and the third item SHALL be selected

#### Scenario: Outside the region
- **WHEN** focus is on the top bar's search button and the owner presses J
- **THEN** nothing SHALL happen in Tinjau

#### Scenario: Typing an answer
- **WHEN** the owner types "1 kopi" into "Jawab sendiri"
- **THEN** no answer SHALL be pressed

#### Scenario: Switched off
- **GIVEN** "Pintasan satu tombol" is off
- **WHEN** the owner presses E with focus in an item
- **THEN** nothing SHALL be accepted

#### Scenario: Help lists the page keys
- **WHEN** the owner presses ? on Tinjau, or chooses "Bantuan & pintasan" in its "…" menu
- **THEN** the help dialog SHALL list J/K, Enter, E, T, 1–9 and Z under "Di Tinjau"

#### Scenario: No hint strip
- **WHEN** Tinjau shows an item with three numbered answers
- **THEN** the key caps 1, 2 and 3 SHALL be shown on the answers, and no other key hint SHALL be visible on the page

## ADDED Requirements

### Requirement: Tinjau Hero
Unless nothing waits in any folder, Tinjau SHALL show one hero "Perlu ditinjau" with the `attention` count in the page's one 44 px figure and one muted line with the `unreviewed` count as optional. When the queue is truncated, one more muted line SHALL say that only the latest rows are shown.

#### Scenario: Hero equals the badge
- **GIVEN** `attention` 6 and `unreviewed` 2
- **WHEN** Tinjau loads at 1440 px
- **THEN** the hero SHALL read 6, the sidebar badge SHALL read 6, the line SHALL read "2 dicatat otomatis, opsional", and no other figure on the page SHALL be 33 px or larger

#### Scenario: Truncated queue
- **GIVEN** a queue response with `truncated: true`
- **WHEN** Tinjau loads
- **THEN** the hero SHALL be followed by the line that only the latest 1.000 transactions are shown

### Requirement: Tinjau Page Actions
Tinjau's title block SHALL show one "…" menu button and no other action. Its menu SHALL offer "Cari saran dari riwayat", which opens the history suggestion dialog, and "Bantuan & pintasan", which opens the shell's help dialog. Up and down arrows SHALL move in the menu, Esc SHALL close it, and focus SHALL return to the button.

#### Scenario: Open the suggestion dialog by keyboard
- **WHEN** the owner focuses "…", presses Enter, moves down to "Cari saran dari riwayat" and presses Enter
- **THEN** a centered 520 px dialog "Cari saran dari riwayat" SHALL open with focus inside it, and closing it SHALL return focus to "…"

#### Scenario: Nothing in the pane
- **WHEN** Tinjau shows its panes
- **THEN** no "Cari saran dari riwayat" control SHALL appear in the rail, the list or the item

### Requirement: Category and Name Dialog
"Ubah kategori", "Ganti nama" and "Bukan pindah saldo, ini pengeluaran" SHALL open one centered 560 px dialog, a bottom sheet below 768 px. In category mode it SHALL ask for a category and a Kakeibo pillar ("Ikuti kategori" by default); in name mode for a store name and notes. Saving SHALL send exactly one request, the same as today, and the dialog SHALL close only when it succeeds.

#### Scenario: Recategorise a group
- **GIVEN** a "Perlu jawaban" group of three rows
- **WHEN** the owner chooses "Ubah kategori", picks "Makanan & Minuman", leaves the pillar on "Ikuti kategori" and saves
- **THEN** one bulk `recategorise` request SHALL carry the three ids and that category and no `kakeibo_type`, and the dialog SHALL close

#### Scenario: Rename without input
- **WHEN** the owner opens "Ganti nama", clears both fields and saves
- **THEN** no request SHALL be sent, the dialog SHALL stay open, and a message SHALL ask for a store name or notes

#### Scenario: Failed save
- **WHEN** the save request fails
- **THEN** the dialog SHALL stay open with the failure in text, and no success message SHALL appear

#### Scenario: Focus return
- **WHEN** the owner closes the dialog with Esc
- **THEN** no request SHALL be sent and focus SHALL return to the answer that opened it

### Requirement: Text Answer Dialog
"Jawab sendiri" on a "Belum dikenali" item SHALL be a quiet button under the numbered answers that opens a centered 520 px dialog with one text field of the question's length limit and "Simpan jawaban". An empty answer SHALL not be sent.

#### Scenario: Empty answer
- **WHEN** the owner presses "Simpan jawaban" with an empty field
- **THEN** no request SHALL be sent, the field SHALL be marked invalid and an example answer SHALL be shown under it

### Requirement: Tinjau Loading and Failure
While the queue loads, Tinjau SHALL show static skeleton rows marked busy without any amount or count. When the queue request fails, it SHALL show one error box with "Coba lagi" that refetches, and SHALL NOT show 0 or an empty state.

#### Scenario: Loading
- **WHEN** the queue request is pending
- **THEN** the region SHALL have `aria-busy="true"`, no hero, no count and no amount SHALL be shown, and nothing SHALL pulse

#### Scenario: Failure and retry
- **GIVEN** the queue request fails
- **WHEN** the owner presses "Coba lagi" and the second request succeeds
- **THEN** the error box SHALL be announced as an alert, and after the retry the hero and panes SHALL show

### Requirement: Tinjau in Both Languages
Tinjau SHALL render its own copy in the interface language, English or Indonesian, and SHALL NOT show the untranslated-page notice. Server text (prompts, suggestion labels and reasons, error messages) SHALL be shown as the server sends it. User data and notification text SHALL be shown as stored, without a language mark. Every count, amount and date SHALL be identical in both languages.

#### Scenario: English interface
- **GIVEN** an English account with one awaiting send of Rp 300.000 from BCA Utama
- **WHEN** it opens Tinjau
- **THEN** the title SHALL read "Review", the folder "Transfer not complete", the question "Money out of BCA Utama. Where did it go?", the amount "Rp 300.000", and no "This page is in Indonesian for now." line SHALL appear

#### Scenario: Indonesian interface
- **GIVEN** an Indonesian account with the same data
- **WHEN** it opens Tinjau
- **THEN** the title SHALL read "Tinjau", the folder "Pindah saldo belum lengkap", and every count, amount and date SHALL equal the English page's

#### Scenario: Spending term
- **WHEN** either language names spending on Tinjau
- **THEN** it SHALL say "Spending" in English and "Pengeluaran" in Indonesian, never "Expense", "Keluar" or "Belanja" as a label
