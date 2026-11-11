## MODIFIED Requirements

### Requirement: Credit Purchase Rows
A credit purchase row SHALL carry the meta "dibayar nanti · {provider} · jatuh tempo {date}" ("pay later · {provider} · due {date}"). With instalments it SHALL add "cicilan {k}/{n}" ("installment {k} of {n}"), and once settled it SHALL read "{provider} · lunas {date}" ("paid {date}"). Its amount SHALL be in the credit colour until it is fully settled and in the expense red afterwards, and it SHALL count as Pengeluaran on its day.

#### Scenario: Credit purchase meta
- **GIVEN** a Rp 120.000 SPayLater dinner on 5 Nov whose next due date is 11 Dec
- **WHEN** Transaksi lists it
- **THEN** its meta SHALL read "dibayar nanti · SPayLater · jatuh tempo 11 Des" and its amount "−Rp 120.000" in the credit colour (`--credit-fg`)

#### Scenario: Instalments and settlement
- **WHEN** a purchase in 3 instalments has 1 settled, and another purchase was settled on 4 Dec
- **THEN** the first SHALL read "dibayar nanti · SPayLater · cicilan 2/3 · jatuh tempo …" in the credit colour, and the second "SPayLater · lunas 4 Des" in the expense red
