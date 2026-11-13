## MODIFIED Requirements

### Requirement: Beranda Hero
Beranda SHALL open with one hero, "Aman dipakai sampai gajian", as the page's only 44 px figure, followed by one line, a split bar of Pengeluaran, Ditabung & investasi, Terjadwal and Aman dipakai with the percentages in its accessible name, and Masuk, Pengeluaran, Ditabung & investasi and Terjadwal as supporting figures with one caption each, followed by "Tagihan kredit" when the owner has an active credit account. No other Beranda figure SHALL be 33 px or larger. The hero figure SHALL NOT be a control; each supporting figure SHALL be one link to its detail, and "Tagihan kredit" one button that opens its dialog.

#### Scenario: Hero on a desktop
- **WHEN** the owner opens Beranda at 1440 × 900
- **THEN** the hero SHALL start at `y = 192`, the supporting figures SHALL follow in one row in that order, and each figure SHALL appear once on the page

#### Scenario: Figures add up to Masuk
- **WHEN** the hero shows masuk Rp 9.800.000
- **THEN** Pengeluaran, Ditabung & investasi, Terjadwal and the hero figure SHALL add up to Rp 9.800.000, or the hero line SHALL say by how much the period is over

#### Scenario: Screen reader
- **WHEN** a screen reader reaches the split bar
- **THEN** it SHALL read one image named with each segment and its percentage of masuk, and every supporting figure SHALL be read as text with its label and caption

#### Scenario: Savings decrease
- **WHEN** the period's net fresh saving is −Rp 500.000
- **THEN** Ditabung & investasi SHALL show Rp 500.000 with the caption "Tabungan berkurang", and its segment SHALL take no width in the bar

#### Scenario: Terjadwal caption
- **WHEN** the scheduled amount includes goal set-asides until 23 November
- **THEN** the Terjadwal caption SHALL read "termasuk setoran · 23 Nov", and without set-asides "tagihan sampai 23 Nov"
- **AND** every supporting-figure caption SHALL stay on one line from 1024 to 1440 px, also with a ten-digit amount, the figures wrapping to a second row where five do not fit

#### Scenario: No figure leaves its block
- **WHEN** Beranda is shown at any width from 320 px, including 1024 to 1199 px with the full sidebar
- **THEN** no hero, supporting or panel figure SHALL extend past its block or the page frame, and the page SHALL NOT scroll horizontally

### Requirement: Recent Activity on Beranda
Under "Aktivitas terbaru" Beranda SHALL list up to eight transactions from today and yesterday that do not wait for an answer, newest first, under one heading per day. Each row SHALL show the time, the title (the note, else the merchant, else the category) or a movement's note or route, the account or route, the pillar, status or saving direction, and the amount with its sign and type colour, expenses in red, plus a link to all transactions. A row SHALL open the shared transaction dialog.

#### Scenario: Waiting row in the list
- **WHEN** a row from today has an open question
- **THEN** it SHALL NOT be listed under "Aktivitas terbaru", and it SHALL be reachable from "Perlu tindakan" or Tinjau

#### Scenario: Day headings
- **WHEN** today and yesterday both have rows
- **THEN** they SHALL be grouped under "KAMIS, 5 NOVEMBER · HARI INI" and "RABU, 4 NOVEMBER · KEMARIN", and each row SHALL show its time, such as "18.23", and not its date

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once, titled by its note with "source → target" as its meta, or by "source → target" when it has no note, with a sky amount, and SHALL show its saving direction ("Ditabung", "Diambil dari tabungan", "Investasi", "Bayar utang") or "—" instead of a pillar, never "Tidak dihitung"
- **AND** a pocket SHALL be named with its parent, for example "Jago · Main Pocket → BCA · ATM"

#### Scenario: Expense and income rows
- **WHEN** an expense of Rp 30.000 and an income of Rp 9.518.400 are listed
- **THEN** the expense SHALL read "−Rp 30.000" in the expense red with its pillar, and the income SHALL read "+Rp 9.518.400" in green with "Uang masuk"

#### Scenario: Row opens the transaction drawer
- **WHEN** the owner selects "Kedai Awan Rp 30.000" in "Aktivitas terbaru"
- **THEN** the transaction dialog SHALL open for that row with its provenance, "Ubah", "Tandai sudah dicek" where allowed and "Hapus"
- **AND** closing it SHALL return focus to the row, and a delete confirmed there SHALL refresh Beranda's figures

#### Scenario: Row deleted elsewhere
- **GIVEN** the row was deleted in another tab
- **WHEN** the owner selects it on Beranda
- **THEN** the dialog SHALL say "Transaksi ini sudah tidak ada" and Beranda's list SHALL refresh

#### Scenario: Phone row
- **WHEN** Beranda is shown at 390 px
- **THEN** each row SHALL show two lines, the title and "18.23 · Kebutuhan", with the amount at the right and no horizontal scrolling

#### Scenario: Expense in Perlu tindakan
- **WHEN** "Perlu tindakan" lists an open question about an expense of Rp 187.500
- **THEN** its amount SHALL read "−Rp 187.500" in the expense red through the same shared amount as "Aktivitas terbaru"

## ADDED Requirements

### Requirement: Supporting Figures Open Their Detail
Masuk, Pengeluaran and Ditabung & investasi SHALL link to Transaksi with Beranda's period (`period=cycle` in a salary cycle, else `period=month`) and `type` `income`, `expense` or `saving`, and Terjadwal SHALL link to Target & Tagihan at its bills. Each link's accessible name SHALL name the figure and where it goes.

#### Scenario: Opening Pengeluaran
- **WHEN** the owner selects Pengeluaran Rp 5.395.841 in a salary cycle
- **THEN** Transaksi SHALL open at `/ledger?period=cycle&type=expense` and its Pengeluaran SHALL read Rp 5.395.841

#### Scenario: Opening Ditabung & investasi
- **WHEN** the owner selects Ditabung & investasi
- **THEN** Transaksi SHALL open with `type=saving` for the same period and its hero SHALL show the same Ditabung & investasi

#### Scenario: Opening Terjadwal
- **WHEN** the owner selects Terjadwal
- **THEN** Target & Tagihan SHALL open at its bills section

#### Scenario: Keyboard
- **WHEN** the owner tabs to a supporting figure and presses Enter
- **THEN** it SHALL show a visible focus ring and open its target

### Requirement: Credit Bills Figure
When the owner has an active credit account, Beranda SHALL show "Tagihan kredit" ("Credit bills"): the sum of what the credit accounts owe, with the caption "jatuh tempo {date} · {left}" for the nearest unpaid bill across accounts. It SHALL take no part of the split bar and SHALL change no other figure, because credit purchases already count in Pengeluaran.

#### Scenario: Two credit accounts
- **GIVEN** Spaylater owes Rp 3.046.014 with a bill due 11 December and Kredivo owes Rp 2.200.000 with a bill due 20 November of Rp 400.000
- **WHEN** the owner opens Beranda on 13 November
- **THEN** "Tagihan kredit" SHALL read Rp 5.246.014 with "jatuh tempo 20 Nov · Rp 400.000", and Aman dipakai SHALL be unchanged

#### Scenario: No credit account
- **WHEN** the owner has no active credit account
- **THEN** Beranda SHALL show no "Tagihan kredit"

#### Scenario: Nothing owed
- **WHEN** every credit account owes Rp 0
- **THEN** the figure SHALL read Rp 0 with "tidak ada tagihan"

#### Scenario: Hidden balances
- **WHEN** "Sembunyikan saldo" is on
- **THEN** the figure and its caption amount SHALL be hidden like every other amount
