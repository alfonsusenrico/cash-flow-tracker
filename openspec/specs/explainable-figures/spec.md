# explainable-figures Specification

## Purpose
Any money figure the web app marks as explainable can show how it was computed ("Kok segini?"): the lines that make it up, the rule behind it and a link to the transactions, so the owner never has to trust a number blindly.

## Requirements

### Requirement: Explainable Figure Trigger
An explainable figure SHALL be a button that keeps the figure as its visible text, is marked by a dotted underline, and has an accessible name that combines the figure's label and amount with "kok segini?". It SHALL be reachable by keyboard and have a target of at least 24 by 24 CSS pixels.

#### Scenario: Keyboard user
- **WHEN** a keyboard user tabs to the "Keluar siklus ini" figure and presses Enter or Space
- **THEN** its explanation SHALL open and focus SHALL move into it

#### Scenario: Screen reader name
- **WHEN** a screen reader reaches the figure Rp 214.000 labelled "Aman dipakai hari ini"
- **THEN** it SHALL announce a button named "Aman dipakai hari ini Rp 214.000: kok segini?"

### Requirement: Explanation Content
An explanation SHALL show a "Rincian hitungan" eyebrow, the figure's name, a one-sentence rule, the lines with label, optional note and signed amount, the total equal to the figure, any caveat about waiting rows or unavailable inputs, and a link to the matching transactions or page. It SHALL show only the lines the server returned, worded in the neutral register (ui-copy-register).

#### Scenario: Lines and total
- **WHEN** the owner opens the "Aman dipakai hari ini" explanation
- **THEN** it SHALL list masuk, keluar, tagihan, setoran target, the remaining amount, the division by the days left and the rounding, ending with the figure itself

#### Scenario: Figure without lines
- **WHEN** a figure is unavailable
- **THEN** the explanation SHALL state the reason and what would make the figure available, and SHALL show no total

### Requirement: Explanation Presentation
At widths above 760 px the explanation SHALL be a non-modal dialog anchored to its trigger and kept inside the viewport. At 760 px and below it SHALL be a bottom sheet. Esc, the close button or activating outside SHALL close it and return focus to the trigger. Only one explanation SHALL be open at a time.

#### Scenario: Desktop popover
- **WHEN** the owner opens an explanation at 1440 px near the right edge of the window
- **THEN** the popover SHALL stay fully visible, SHALL not cover its trigger, and SHALL close on Esc with focus back on the trigger

#### Scenario: Phone sheet
- **WHEN** the owner opens an explanation at 390 px
- **THEN** it SHALL open as a bottom sheet with a visible close button, the page behind SHALL not scroll, and closing SHALL return focus to the trigger

#### Scenario: Zoom and reflow
- **WHEN** the page is zoomed to 400% at 1280 px
- **THEN** the explanation SHALL reflow as a sheet without horizontal scrolling and without clipping its lines

### Requirement: Explanation Drill-Down
The link in an explanation SHALL open the page that lists the rows behind the figure with the matching filter, period and pillar applied, and the filter SHALL be visible there.

#### Scenario: From a pillar line to Transaksi
- **WHEN** the owner follows the "Keinginan" line of the "Keluar" explanation
- **THEN** Transaksi SHALL open on the same period with the `kakeibo=want` filter visible, and its summary SHALL equal that line
