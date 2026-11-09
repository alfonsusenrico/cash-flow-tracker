## MODIFIED Requirements

### Requirement: Kerjakan Dulu
Under "Perlu tindakan" Beranda SHALL list the first three of what needs the owner, ranked: open questions, plans and credit bills past their due date, those due today, awaiting transfers, ambiguous pairs, groups that need an answer, automatically linked plans to check, and plans and credit bills due within two days. Each row SHALL name the item, its kind and time and its amount where it has one, and the whole row SHALL be its one action.

#### Scenario: Items and order
- **GIVEN** one open question, two awaiting transfer legs, one group that needs an answer and a plan due tomorrow
- **WHEN** the owner opens Beranda
- **THEN** the header SHALL show "Buka Tinjau · 4", and the rows SHALL be the question, then the awaiting transfers, then the group
- **AND** each review row SHALL open Tinjau at the matching item, and the plan due tomorrow SHALL be reached through the bell and Target & Tagihan

#### Scenario: Due plans come before checks and pairing
- **GIVEN** one open question, an awaiting transfer leg, four automatically linked plans, a plan past its due date and a plan due today
- **WHEN** the owner opens Beranda
- **THEN** the rows SHALL be the question, the overdue plan and the plan due today, and "N rencana ditautkan otomatis" and the pairing row SHALL be reached through "Buka Tinjau"

#### Scenario: One-column Beranda
- **WHEN** Beranda is shown at any width, including one column below 1200 px
- **THEN** "Perlu tindakan" SHALL follow the hero directly and come before "Aktivitas terbaru" in the visual, reading and focus order

#### Scenario: Recording a due plan
- **WHEN** the owner selects a plan due today in "Perlu tindakan"
- **THEN** "Rencana jatuh tempo" SHALL open with the same link, skip and record actions as before the redesign, same-amount captures SHALL be offered for linking first, and nothing SHALL be posted without the owner's explicit confirmation

#### Scenario: Nothing to do
- **WHEN** nothing waits and no plan is due
- **THEN** the section SHALL say "Tidak ada yang perlu dijawab." in one line, and its header link SHALL read "Buka Tinjau" without a count

#### Scenario: Credit bill ranked with plans
- **GIVEN** an open question, a SPayLater bill due today with Rp 434.000 left and a plan due today
- **WHEN** the owner opens Beranda
- **THEN** the rows SHALL be the question, then the plan and the bill due today ordered by due date and then name, the bill reading "Tagihan SPayLater", "Jatuh tempo hari ini · Kredit" and Rp 434.000
