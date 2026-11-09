# money-flow-map Specification

## Purpose
Show where a period's money came from and where it went, with every account balancing and every link traceable to its transactions.

## Requirements

### Requirement: Flow Endpoint
`GET /api/dashboard/flow` SHALL take `timeframe` (`cycle` or `month`), `cycle_offset` and an optional `through` local date inside the window. `through` SHALL default to today for the current period and to the window end otherwise. The response SHALL hold nodes, links and a summary for the rows up to the end of `through`, with non-negative integer link amounts. A `through` outside the window SHALL answer 422 `through_outside_window`. The response SHALL be owner-scoped.

#### Scenario: Replay day
- **WHEN** the flow is requested with `through` set to the third day of the cycle
- **THEN** only rows up to the end of that local day SHALL be in the links, and "Masih ada" SHALL be each account's balance at that moment

#### Scenario: Outside the window
- **WHEN** `through` is a day after the window end
- **THEN** the response SHALL be 422 with `through_outside_window`

#### Scenario: Another owner
- **WHEN** owner A requests the flow
- **THEN** no account, link or amount of owner B SHALL be included

### Requirement: Flow Nodes
The flow nodes SHALL be:

- the income sources and "Saldo sebelum periode";
- liquid top-level accounts with their non-goal pockets folded in;
- goal-linked saving accounts and top-level investment accounts with their products folded in;
- the six Kakeibo buckets;
- "Tidak dihitung belanja", "Menunggu pasangan" and "Masih ada".

#### Scenario: Pocket folded into its account
- **GIVEN** "Bank Utama" with a non-goal pocket "Belanja Harian" and a goal pocket "Dana Darurat"
- **WHEN** the flow is requested
- **THEN** "Bank Utama" SHALL be one account node that includes "Belanja Harian", and "Dana Darurat" SHALL be a saving node

#### Scenario: Excluded category
- **WHEN** an expense in a category excluded from spending is not a movement leg
- **THEN** it SHALL be linked from its account to "Tidak dihitung belanja"

### Requirement: Every Account Node Balances
Every transaction row of an account node in the window SHALL belong to exactly one link, and movements between a parent and its own folded pockets SHALL be internal to the node. For every account node, the links in SHALL add up to the links out. That means the opening ledger balance at the window start plus inflows equals outflows plus "Masih ada", the ledger balance at the end of `through`.

#### Scenario: Conservation
- **GIVEN** an owner with income, spending in all six buckets, an excluded category, a reimbursement, a transfer between two accounts, a move into a pocket of the same account, a move into a goal pocket, a top-up, a sale back to cash and an awaiting leg
- **WHEN** the flow is requested for every day of the window
- **THEN** every account node SHALL balance on every day, and its "Masih ada" on the last day SHALL equal its ledger balance

### Requirement: Negative Balances in the Flow
A negative opening balance SHALL appear as a link from the account to "Saldo minus sebelum periode", and a negative closing balance as a link from "Saldo minus di akhir" to the account. Both SHALL carry the absolute amount, so every link stays non-negative and the node still balances.

#### Scenario: Negative balance
- **GIVEN** an account whose balance is Rp −150.000 at the window start
- **WHEN** the flow is requested
- **THEN** a link of Rp 150.000 SHALL leave that account to "Saldo minus sebelum periode", and the node SHALL balance

### Requirement: Waiting Rows in the Flow
A movement leg waiting for its counterpart or ambiguous between candidates SHALL be linked to "Menunggu pasangan", never to spending or saving. A row waiting for a purpose or category answer SHALL stay in the link of its bucket, and that link SHALL report its waiting count and amount.

#### Scenario: Awaiting leg
- **GIVEN** a Rp 112.590 debit from "RDN Contoh" awaiting its counterpart
- **WHEN** the flow is requested
- **THEN** a link from "RDN Contoh" to "Menunggu pasangan" SHALL carry Rp 112.590, and no pillar or saving link SHALL include it

#### Scenario: Question inside a bucket
- **GIVEN** a Rp 45.000 expense with an open purpose question and the effective pillar `want`
- **WHEN** the flow is requested
- **THEN** the account's link to Keinginan SHALL include Rp 45.000 and report one waiting row of Rp 45.000

### Requirement: Flow Summary
The flow summary SHALL give gross spending, net saving, net investment, the amounts waiting in each direction, "Masih ada", and Masuk. When `through` is the window end, the summary's Belanja and Keluar SHALL equal the analytics figures of the window.

#### Scenario: Full window
- **WHEN** the flow is requested with `through` at the window end
- **THEN** the summary's Belanja and Keluar SHALL equal the analytics `total_outflow` and Kakeibo total of the same window

### Requirement: Flow Rows
`GET /api/dashboard/flow/rows` SHALL return the transactions behind one link or one node for the same window and `through`, 50 per page, with the total count and amount. The total amount of a link's rows SHALL equal the link's amount. Opening and remaining links SHALL return no rows. An unknown key, or a key naming another owner's account, SHALL answer 404 `flow_link_not_found`, and a malformed key SHALL answer 422.

#### Scenario: Rows equal the link
- **WHEN** the rows of the link from "Bank Utama" to Kebutuhan are requested page by page
- **THEN** their amounts SHALL add up to that link's amount

#### Scenario: Another owner's key
- **WHEN** owner A requests rows for a key naming owner B's account
- **THEN** the response SHALL be 404 with `flow_link_not_found`

### Requirement: Peta Aliran Map and Keyboard
Peta Aliran SHALL draw the flow as a four-column Sankey (Dari, Rekening, Lalu ke, Dipakai atau disimpan) with bands coloured by kind, transfers, investments and waiting links also patterned, a legend, and every label inside the frame. Its nodes SHALL be focusable buttons named with their amounts in and out. The arrow keys SHALL move between nodes, Enter SHALL open the node's details, and Esc SHALL close them with focus restored.

#### Scenario: Keyboard node walk
- **WHEN** the owner tabs into the map, presses the Right arrow and then Enter
- **THEN** focus SHALL move to a node in the next column, and its details SHALL open with its links

#### Scenario: Link rows in the drawer
- **WHEN** the owner opens the link from "Bank Utama" to Kebutuhan
- **THEN** the details SHALL list its rows 50 at a time, with their total equal to the link's amount

#### Scenario: Labels inside the frame
- **WHEN** Peta Aliran is shown at 1440, 1200 and 1024 px with an account named with 40 characters
- **THEN** every node label SHALL lie inside the map's frame, a long name SHALL end with "…", and the node's accessible name SHALL hold the full name

#### Scenario: Bands and legend
- **WHEN** the owner highlights "Pengeluaran"
- **THEN** spending bands SHALL be drawn at 0.30 opacity and the others at 0.06, and the legend SHALL name Masuk, Pindah saldo, Pengeluaran, Tabungan & investasi and Menunggu pasangan with their colour and pattern

### Requirement: Peta Aliran Table
A "Tabel" view SHALL list every link, including the opening and remaining links, exactly once with Dari → Ke, Jenis, Jumlah and Transaksi. Below 768 CSS pixels the table SHALL be the default view, any map SHALL scroll inside its box, and the Transaksi column MAY be hidden.

#### Scenario: Table complete
- **WHEN** the owner switches to "Tabel"
- **THEN** the table SHALL list every link of the response, including the opening and "Masih ada" links, exactly once

#### Scenario: Phone
- **WHEN** Peta Aliran is shown at 390 and 320 CSS pixels
- **THEN** the table SHALL be shown and reachable, and no content SHALL overflow the page horizontally

### Requirement: Peta Aliran Details Open Centered
The node and link details of Peta Aliran SHALL open as a centered dialog from 768 px and as a bottom sheet below 768 px. The map SHALL keep its full width while one is open.

#### Scenario: Link detail on a wide window
- **WHEN** the owner opens the link from "Bank Utama" to Kebutuhan at 1440 px
- **THEN** its rows SHALL be listed in a centered dialog over the dimmed page, the map behind it SHALL keep its width, and Esc SHALL close the dialog and return focus to the link

#### Scenario: Node detail on a phone
- **WHEN** the owner opens a node at 390 px
- **THEN** its detail SHALL be a bottom sheet with focus inside it

### Requirement: Peta Aliran Figures
Peta Aliran SHALL show Masuk as its hero and four supporting figures from the flow summary: Pengeluaran (`summary.spending`, the Pengeluaran basis), Ditabung & top up, Menunggu pasangan and Masih ada. A "Sorot aliran" control (Semua, Pengeluaran, Pindah saldo, Tabungan & investasi, Menunggu pasangan) SHALL dim the other bands on the map and filter the table.

#### Scenario: Figures from the summary
- **GIVEN** a window with a Rp 300.000 expense in the Tabungan pillar
- **WHEN** Peta Aliran is shown for the full window
- **THEN** its Pengeluaran SHALL equal Ringkasan's hero and the comparison's `totals.spending` of the window, not the Belanja-basis `summary.belanja`, and Masih ada SHALL equal the summary's `remaining`

#### Scenario: Highlight in the table
- **WHEN** the owner highlights "Pindah saldo" in the table view
- **THEN** only transfer links SHALL be listed

### Requirement: Peta Aliran Balances
The map SHALL draw income links and SHALL NOT draw the opening, negative-balance or "Masih ada" links, so spending sets the scale. The table SHALL list them, and "Masih ada" SHALL be a supporting figure.

#### Scenario: Balances off the map
- **WHEN** Peta Aliran opens
- **THEN** income bands SHALL be drawn, no opening or "Masih ada" band SHALL be drawn, no fold control SHALL be shown, and the table SHALL list the opening and "Masih ada" links
