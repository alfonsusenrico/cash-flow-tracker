## MODIFIED Requirements

### Requirement: Recent Activity on Beranda
Under "Aktivitas terbaru" Beranda SHALL list up to eight transactions from today and yesterday that do not wait for an answer, newest first, under one heading per day. Each row SHALL show the time, the merchant or a movement's note or route, the account or route, the pillar, status or saving direction, and the amount with its sign and type colour, expenses in red, plus a link to all transactions. A row SHALL open the shared transaction dialog.

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
