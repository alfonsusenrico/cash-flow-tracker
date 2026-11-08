## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: Peta Aliran Figures
Peta Aliran SHALL show Masuk as its hero and four supporting figures from the flow summary: Pengeluaran, Ditabung & top up, Menunggu pasangan and Masih ada. A "Sorot aliran" control (Semua, Pengeluaran, Pindah saldo, Tabungan & investasi, Menunggu pasangan) SHALL dim the other bands on the map and filter the table.

#### Scenario: Figures from the summary
- **WHEN** Peta Aliran is shown for a full window
- **THEN** its Pengeluaran SHALL equal Ringkasan's hero and the analytics `total_outflow` of the window, and Masih ada SHALL equal the summary's `remaining`

#### Scenario: Highlight in the table
- **WHEN** the owner highlights "Pindah saldo" in the table view
- **THEN** only transfer links SHALL be listed

### Requirement: Peta Aliran Balances
The map SHALL draw income links and SHALL NOT draw the opening, negative-balance or "Masih ada" links, so spending sets the scale. The table SHALL list them, and "Masih ada" SHALL be a supporting figure.

#### Scenario: Balances off the map
- **WHEN** Peta Aliran opens
- **THEN** income bands SHALL be drawn, no opening or "Masih ada" band SHALL be drawn, no fold control SHALL be shown, and the table SHALL list the opening and "Masih ada" links

## REMOVED Requirements

### Requirement: Peta Aliran Replay
**Reason**: The prototype and the audit inventory remove "Putar ulang"; it duplicates stepping through Kalender and adds a slider and timed requests to a reading page.
**Migration**: The web requests the flow without `through` and shows the period to today or its end. `GET /api/dashboard/flow` keeps `through` for API consumers ("Flow Endpoint" is unchanged).

### Requirement: Peta Aliran Fold
**Reason**: The fold control is replaced by a fixed rule: income is drawn, balances are not.
**Migration**: "Peta Aliran Balances" replaces it. The stored choice `analysis.flow.folded` is no longer read.
