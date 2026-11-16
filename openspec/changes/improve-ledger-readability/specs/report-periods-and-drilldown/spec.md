## ADDED Requirements

### Requirement: Salary Cycle Labels
Every label that names a salary cycle (Transaksi, Analisis, Beranda, Laporan) SHALL read "{start} – {end}" in the interface language. A finished cycle SHALL end on its last day. The running cycle SHALL end on its estimated last day: the day before the expected next salary, but never before today. The expected next salary SHALL be the regular-receipts estimate when known, otherwise the payday setting's date one month after the payday nearest the cycle's start.

#### Scenario: Running cycle from the payday setting
- **GIVEN** a cycle that began on 30 Okt, two confirmed receipts, and the payday setting 29
- **WHEN** the owner opens Transaksi on 16 Nov
- **THEN** the period SHALL read "30 Okt – 28 Nov", and "Oct 30 – Nov 28" in English

#### Scenario: Salary is late
- **GIVEN** the same cycle and no new receipt
- **WHEN** the owner opens Transaksi on 30 Nov
- **THEN** the period SHALL read "30 Okt – 30 Nov"

#### Scenario: Salary arrives early
- **WHEN** a salary receipt of 27 Nov is confirmed
- **THEN** the finished cycle SHALL read "30 Okt – 26 Nov" and the running one "27 Nov – 28 Des"

#### Scenario: Regular receipts decide
- **GIVEN** receipts on 25 Agu, 25 Sep and 24 Okt and the payday setting 29
- **WHEN** the running cycle is labelled on 2 Nov
- **THEN** it SHALL read "24 Okt – 23 Nov"

### Requirement: Cycle Label Is Not a Window
The running cycle's estimated last day SHALL be returned as `label_end_date` beside the period's start and end and used only for its label. Report windows, figures, ledger filters and cycle boundaries SHALL keep running to now and SHALL close only on a confirmed salary receipt.

#### Scenario: Figures still run to now
- **GIVEN** the running cycle labelled "30 Okt – 28 Nov"
- **WHEN** the owner opens Analisis for it on 16 Nov
- **THEN** the figures SHALL cover 30 Okt to now, and the ledger link SHALL carry no date after now

#### Scenario: Estimate past due
- **GIVEN** the estimated last day 28 Nov has passed without a receipt
- **WHEN** the owner opens Laporan on 1 Des
- **THEN** the running cycle SHALL still be open and read "30 Okt – 1 Des"
