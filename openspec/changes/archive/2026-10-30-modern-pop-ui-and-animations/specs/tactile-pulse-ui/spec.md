## ADDED Requirements

### Requirement: Animated Headline Figures
Headline money figures on the Beranda balance card, the Beranda cash-flow matrix and the Rekening net-worth summary SHALL move from their previous value to a new value over about 380 ms with an ease-out curve. When the viewer requests reduced motion, the final value SHALL be shown at once.

#### Scenario: Balance changes
- **GIVEN** the Beranda balance card shows Rp 100.000
- **WHEN** the balance changes to Rp 150.000
- **THEN** the figure SHALL count from Rp 100.000 to Rp 150.000 within about 380 ms and end on the exact value

#### Scenario: Reduced motion
- **GIVEN** the operating system requests reduced motion
- **WHEN** a headline figure changes
- **THEN** the new value SHALL be shown immediately without intermediate values

### Requirement: Quick Capture Save Confirmation
After Quick Capture saves a transaction or movement, the submit button SHALL show a brief success state (`✓ Tersimpan!`, or `✓ Berhasil dipindahkan` for a movement) and the modal SHALL close about 450 ms later. If the record is saved but its receipt upload fails, the modal SHALL stay open with an error message instead.

#### Scenario: Transaction saved
- **WHEN** the owner submits an expense and the save succeeds
- **THEN** the submit button SHALL show `✓ Tersimpan!` and the modal SHALL close shortly afterwards

#### Scenario: Receipt upload fails after the save
- **WHEN** the transaction is saved but its receipt upload fails
- **THEN** the modal SHALL stay open and say that the transaction was saved but the receipt was not uploaded
