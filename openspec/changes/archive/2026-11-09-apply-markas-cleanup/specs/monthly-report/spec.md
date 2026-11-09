## MODIFIED Requirements

### Requirement: Four Kakeibo questions in a fixed order
The page SHALL answer four numbered questions in this order: 1 "Berapa uang masuk?" (Masuk and its sources by category), 2 "Berapa yang ditabung?" (Ditabung & investasi: saving-classified expenses, net savings movements and spending paid from savings, separate from the surplus), 3 "Berapa pengeluarannya?" (Pengeluaran and its breakdowns), 4 "Apa yang mau diperbaiki bulan depan?" (facts and the note). Every figure SHALL come from the existing reporting services, and the parts listed under question 2 SHALL add up to its figure.

#### Scenario: Savings separate from surplus
- **GIVEN** a month with Rp 5.000.000 inflow, Rp 3.000.000 outflow, a Rp 500.000 movement into a goal-linked pocket and a Rp 100.000 saving-classified expense
- **WHEN** the report is requested
- **THEN** fresh savings SHALL be Rp 600.000 with both components listed under question 2, the API surplus SHALL be Rp 2.000.000, and the page's Selisih SHALL be Masuk minus Pengeluaran, Rp 2.100.000, without calling it money set aside

#### Scenario: Spending paid from savings
- **GIVEN** the same month plus a Rp 150.000 food expense paid from a Tabungan account
- **WHEN** the report is requested
- **THEN** `savings` SHALL carry `saving_expenses` 100000, `net_saving_transfers` 500000, `paid_from_savings` 150000 and `fresh_saving` 450000, Pengeluaran SHALL still count the Rp 150.000 in its pillar, and "Rincian tabungan" SHALL list "Pengeluaran berpilar Tabungan" Rp 100.000, "Pindah bersih ke rekening tabungan atau investasi" Rp 500.000 and "Pengeluaran dari tabungan" −Rp 150.000 noted "tetap dihitung sebagai pengeluaran", adding up to the figure Rp 450.000

#### Scenario: Nothing paid from savings
- **WHEN** no expense of the period was paid from a savings account
- **THEN** `paid_from_savings` SHALL be 0 and "Rincian tabungan" SHALL list only its two rows

#### Scenario: No reflection note
- **WHEN** neither the period nor the previous period has a reflection note
- **THEN** question 4 SHALL show its facts and a "Tulis catatan untuk {period}" button, and no empty form and no line about last period's note

#### Scenario: Writing the first note
- **GIVEN** the October report without a note
- **WHEN** the owner selects "Tulis catatan untuk Oktober 2026", types "Kurangi jajan kopi" and saves
- **THEN** the note SHALL be stored with `PUT /api/reports/monthly/reflection`, the form SHALL close, the note SHALL be shown read-only with "Ubah", and focus SHALL move to "Ubah"

#### Scenario: Numbered questions
- **WHEN** the October report is shown at 1440 and at 390 px
- **THEN** the four questions SHALL be numbered 1 to 4 in that order, each heading SHALL be a level-2 heading, and each question SHALL be 48 px below the previous one with a rule between them (32 px on the phone)
