# Spec Delta

## MODIFIED Requirements

### Requirement: Explanation Content
An explanation SHALL show a "Rincian hitungan" eyebrow, the figure's name, a one-sentence rule, the lines with label, optional note and signed amount, the total equal to the figure, any caveat about waiting rows or unavailable inputs, and a link to the matching transactions or page. It SHALL show only the lines the server returned, worded in the neutral register (ui-copy-register).

#### Scenario: Lines and total
- **WHEN** the owner opens the "Aman dipakai hari ini" explanation
- **THEN** it SHALL list masuk, keluar, tagihan, setoran target, the remaining amount, the division by the days left and the rounding, ending with the figure itself

#### Scenario: Figure without lines
- **WHEN** a figure is unavailable
- **THEN** the explanation SHALL state the reason and what would make the figure available, and SHALL show no total
