## MODIFIED Requirements

### Requirement: Notifications Include Due Plans
The bell SHALL list the ready monthly report, every active expense plan and every unpaid credit bill that is overdue, due today or due within two days, plus one item for recent automatic plan links. Each item SHALL show its title, detail and date. An overdue or due-today plan, or the automatic-link item, SHALL open "Rencana jatuh tempo"; a credit bill SHALL open Catat's Perpindahan into its account; other items SHALL open their page.

#### Scenario: Due plan from the bell on Transaksi
- **GIVEN** a plan "Tagihan SPayLater" due today
- **WHEN** the owner opens Transaksi
- **THEN** no due-plan banner SHALL be shown, the bell SHALL list the plan, and activating it SHALL open "Rencana jatuh tempo" where the plan can be linked, skipped or recorded with the same confirmation as before

#### Scenario: No duplicate plan item
- **WHEN** a plan is both due today in the pending list and within the two-day window
- **THEN** the bell SHALL list it once

#### Scenario: Seen state unavailable
- **WHEN** browser storage throws
- **THEN** the bell SHALL list every item as unread and keep working without an error

#### Scenario: Due credit bill
- **GIVEN** a BCA credit card bill due tomorrow with Rp 1.250.000 left and no plan for it
- **WHEN** the owner opens the bell
- **THEN** it SHALL list "Tagihan BCA Kartu Kredit Rp 1.250.000 besok" with "Kredit · jatuh tempo {day}", and activating it SHALL open Perpindahan with the card as "Ke" and Rp 1.250.000, recording nothing until the owner saves
