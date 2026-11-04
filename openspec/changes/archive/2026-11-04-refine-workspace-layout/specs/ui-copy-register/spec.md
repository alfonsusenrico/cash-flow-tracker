## ADDED Requirements

### Requirement: Spending Terms
Every page SHALL name spending figures with the same terms: "Pengeluaran" for spending without savings, "Ditabung & investasi" for the saving bucket, "Masuk" for income, and "Aman dipakai sampai gajian" for the remaining amount of the period. "Keluar" and "Belanja" SHALL NOT be figure labels. Direction words in row text, such as "Uang keluar", SHALL stay allowed.

#### Scenario: Same figure, same name
- **WHEN** Beranda, Transaksi, Analisis and Laporan show the period's spending without savings
- **THEN** each SHALL label it "Pengeluaran", and no figure on these pages SHALL be labelled "Keluar" or "Belanja"

#### Scenario: Copy guard
- **WHEN** a change labels a figure "Keluar" or "Belanja" in a component the copy guard covers
- **THEN** `copyRegister.test.ts` SHALL fail and name the file

#### Scenario: Row text keeps direction words
- **WHEN** Tinjau shows "Uang keluar dari RDN Contoh. Masuknya ke mana?"
- **THEN** the copy guard SHALL accept it, because it describes a row and is not a figure label
