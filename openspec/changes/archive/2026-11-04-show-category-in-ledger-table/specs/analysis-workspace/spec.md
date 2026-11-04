## MODIFIED Requirements

### Requirement: Ringkasan Daily Spending
Ringkasan SHALL show one spending bar per local day of the period up to today, with the highest day labelled with its date and amount and today marked as still running, and SHALL offer the same values as a table. Pointing at a bar, or tapping it on a touch screen, SHALL show that day's date and amount above the bar until the pointer leaves the chart, the bar is tapped again or Esc is pressed.

#### Scenario: Daily bars
- **WHEN** the current cycle is shown
- **THEN** the highest day SHALL be labelled with its date and amount, today SHALL be marked as still running, later days SHALL be absent, and the same values SHALL be available as a table

#### Scenario: Hovering a bar
- **GIVEN** 1 November with Rp 40.000 of spending
- **WHEN** the owner points at its bar
- **THEN** "1 Nov" and "Rp 40.000" SHALL appear above the bar, inside the card, and SHALL disappear when the pointer leaves the chart or Esc is pressed

#### Scenario: Tapping a bar
- **WHEN** the owner taps today's bar on a phone
- **THEN** its date with "hari ini" and its amount SHALL appear, and a second tap SHALL hide them
