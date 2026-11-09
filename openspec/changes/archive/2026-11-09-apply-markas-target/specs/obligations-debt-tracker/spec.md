## MODIFIED Requirements

### Requirement: Payoff Progress & Estimated Timeline
The system SHALL compute payoff progress from the total and remaining debt and SHALL NOT display an estimated payoff duration that ignores interest or changes to payments. Under "Utang & kredit" each active debt SHALL be one whole-row button with its name, what is left, a 4 px bar of the paid share, its due date and "sisa · N% terbayar" in words.

#### Scenario: Viewing debt payoff status
- **WHEN** a user views an obligation with 6,000,000 IDR remaining out of 12,000,000 IDR
- **THEN** the card SHALL show 50% paid and SHALL NOT claim an estimated payoff time

#### Scenario: Debt row in the panel
- **GIVEN** "Cicilan Motor" with Rp 11.050.000 left of a Rp 20.500.000 total, `payoff_percentage` 46 and due date 1 December
- **WHEN** the owner opens `/goals`
- **THEN** its row SHALL read "Cicilan Motor", the amount left, "1 Des" and "sisa · 46% terbayar", the bar SHALL fill 46 % as decoration, and no payoff time SHALL be shown

## ADDED Requirements

### Requirement: Paid-Off Debts Stay Reachable
Target & Tagihan SHALL read obligations with `include_archived=true` and list archived or fully paid debts under "Sudah lunas · N", collapsed by default. Each SHALL open "Ubah utang", which SHALL say that the debt was archived automatically when paid and becomes active again if its payment is reversed. Active debts and every other consumer of the active list SHALL be unchanged.

#### Scenario: One paid-off debt
- **GIVEN** an archived debt "Pinjaman Laptop" with `remaining_amount` 0
- **WHEN** the owner opens `/goals` and activates "Sudah lunas · 1"
- **THEN** the toggle SHALL report `aria-expanded="true"`, "Pinjaman Laptop" SHALL be listed with "lunas" and its total in the muted text, and the active debt list and count SHALL not include it

#### Scenario: Reversal reactivates
- **WHEN** the payment that paid off "Pinjaman Laptop" is deleted and the page refreshes
- **THEN** the debt SHALL be listed with the active debts again and "Sudah lunas" SHALL not list it, as the server's lifecycle decides
