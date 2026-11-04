## ADDED Requirements

### Requirement: Ledger Table Columns
The desktop ledger table SHALL show the columns Transaksi, Jam, Jenis, Pilar, Kategori and Jumlah. Pilar SHALL show the pillar text with its dot. Kategori SHALL show the row's category name as the owner wrote it. The table SHALL NOT show a provenance column, and the transaction detail SHALL keep showing it.

#### Scenario: Expense shows its category
- **GIVEN** an expense "Indomaret" in the category "Belanja Harian", whose pillar is Kebutuhan
- **WHEN** the owner opens Transaksi on a desktop
- **THEN** its row SHALL show "Kebutuhan" under Pilar and "Belanja Harian" under Kategori, and no "Pengguna · Diketik di ponsel" text SHALL appear in the row

#### Scenario: Movement has no category
- **WHEN** the table shows a movement "Jago · Makan-Jalan → Gopay"
- **THEN** Pilar SHALL read "Tidak dihitung" and Kategori SHALL read "—", never "Internal Movement"

#### Scenario: Provenance stays in the detail
- **WHEN** the owner opens that expense's detail
- **THEN** the detail SHALL still show "Dicatat" and "Diklasifikasikan"

#### Scenario: Row without a category
- **WHEN** the table shows an expense or income without a category, or a waiting row without one
- **THEN** Kategori SHALL read "Tanpa kategori" for the expense or income and "—" for the waiting row, and an investment top-up SHALL read "—"

### Requirement: Ledger Day Heading Colours
Each day heading SHALL colour its amounts by kind: Pengeluaran in the expense red, Masuk in the income green and Pindah saldo in the transfer blue, using the theme tokens that meet contrast on every surface. The labels SHALL stay in the neutral text colour, so the kind is never carried by colour alone.

#### Scenario: Day with all three kinds
- **WHEN** a day heading shows Pengeluaran Rp 35.000, Masuk Rp 100.000 and Pindah saldo Rp 21.701
- **THEN** Rp 35.000 SHALL be red, Rp 100.000 green and Rp 21.701 blue, each after its label
