## ADDED Requirements

### Requirement: Salary Cycle Labels
Every label that names a salary cycle (Transaksi, Analisis, Beranda, Laporan) SHALL read "{start} – {end}" in the interface language, without a "Siklus" prefix. Cycles SHALL read payday to payday: a finished cycle SHALL end on the day its next salary arrived, and the running cycle on the expected next salary day, but never before today.

#### Scenario: Running cycle from the payday setting
- **GIVEN** a cycle that began on 30 Okt, two confirmed receipts, and the payday setting 29
- **WHEN** the owner opens Transaksi on 16 Nov
- **THEN** the period SHALL read "30 Okt – 29 Nov", and "30 Oct – 29 Nov" in English

#### Scenario: Salary is late
- **GIVEN** the same cycle and no new receipt
- **WHEN** the owner opens Transaksi on 30 Nov
- **THEN** the period SHALL read "30 Okt – 30 Nov"

#### Scenario: Salary arrives early
- **WHEN** a salary receipt of 27 Nov is confirmed
- **THEN** the finished cycle SHALL read "30 Okt – 27 Nov" and the running one "27 Nov – 29 Des"

### Requirement: Expected Salary Day for Labels
The expected next salary day that ends a running cycle's label SHALL be the regular-receipts estimate when it is known, otherwise the payday setting's day one month after the payday nearest the cycle's start, clamped to that month's length.

#### Scenario: Regular receipts decide
- **GIVEN** receipts on 25 Agu, 25 Sep and 24 Okt and the payday setting 29
- **WHEN** the running cycle is labelled on 2 Nov
- **THEN** it SHALL read "24 Okt – 24 Nov"

### Requirement: Cycle Label Is Not a Window
The running cycle's estimated last day SHALL be returned as `label_end_date` beside the period's start and end and used only for its label. Report windows, figures, ledger filters and cycle boundaries SHALL keep running to now and SHALL close only on a confirmed salary receipt.

#### Scenario: Figures still run to now
- **GIVEN** the running cycle labelled "30 Okt – 29 Nov"
- **WHEN** the owner opens Analisis for it on 16 Nov
- **THEN** the figures SHALL cover 30 Okt to now, and the ledger link SHALL carry no date after now

#### Scenario: Estimate past due
- **GIVEN** the expected salary day 29 Nov has passed without a receipt
- **WHEN** the owner opens Laporan on 1 Des
- **THEN** the running cycle SHALL still be open and read "30 Okt – 1 Des"
