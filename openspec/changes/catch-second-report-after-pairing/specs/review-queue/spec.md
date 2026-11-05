## ADDED Requirements

### Requirement: Question Row Direction
A "Belum dikenali" row in Tinjau's list SHALL show its amount with "+" and the income colour for an income and with "−" for an expense. Its caption SHALL read "Belum dikenali · Uang masuk" for an income and "Belum dikenali · Pembayaran tanpa nama toko" for an expense. Without a merchant or notes, its title SHALL be "Uang masuk" for an income and "Pembayaran tanpa nama toko" for an expense.

#### Scenario: Income question
- **GIVEN** an open question on an income of Rp 10.000 into Jago · Kantong Utama with notes "Uang masuk dari GoPay"
- **WHEN** Tinjau lists it
- **THEN** the row SHALL show "+Rp 10.000" in the income colour and "Belum dikenali · Uang masuk", and SHALL NOT show "−" or "Pembayaran tanpa nama toko"

#### Scenario: Expense question
- **WHEN** Tinjau lists an open question on an expense of Rp 187.500
- **THEN** the row SHALL show "−Rp 187.500" and "Belum dikenali · Pembayaran tanpa nama toko"
