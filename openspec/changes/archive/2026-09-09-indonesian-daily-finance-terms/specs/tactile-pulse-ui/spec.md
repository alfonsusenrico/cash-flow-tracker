## MODIFIED Requirements

### Requirement: Daily Spending Pulse Glance
The Pulse screen SHALL prominently display:
1. `Batas Belanja Hari Ini`: The dynamic daily spending allowance.
2. `Pengeluaran Hari Ini`: The total amount spent today.
3. `Laju Pengeluaran`: A visual indicator showing whether spending is on-track relative to cycle days elapsed.

#### Scenario: Over-budget visual feedback
- **WHEN** spending today exceeds the daily allowance
- **THEN** the pulse tile displays a clear, calm status indicator reflecting the pace adjustment for remaining cycle days without punitive scoring or guilt dialogs
