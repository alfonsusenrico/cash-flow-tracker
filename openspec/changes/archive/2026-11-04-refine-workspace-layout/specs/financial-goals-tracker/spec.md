## ADDED Requirements

### Requirement: Target & Tagihan Layout
On wide screens Target & Tagihan SHALL open with three equal cards, Ketahanan Dana (months, monthly essential cost, days covered, emergency balance and target), Utang (remaining, paid, minimum payment and due date) and Tagihan 30 hari (count, total and the next three), followed by "Target tabungan" at full width. No card SHALL sit beside an empty column, and goal and debt details SHALL open in a centered dialog.

#### Scenario: Balanced desktop layout
- **WHEN** the owner opens `/goals` at 1440 px with one debt, two goals and no bill in the next 30 days
- **THEN** the three cards SHALL share the first row at equal width, "Tagihan 30 hari" SHALL say that no bill is due, and the goals SHALL follow at full width

#### Scenario: Unknown essential cost
- **WHEN** the essential monthly cost is unknown
- **THEN** Ketahanan Dana SHALL say why it cannot be computed and SHALL show no number of months
