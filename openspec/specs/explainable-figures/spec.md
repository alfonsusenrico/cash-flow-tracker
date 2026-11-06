# explainable-figures Specification

## Purpose
Any money figure the web app marks as explainable can show how it was computed ("Kok segini?"): the lines that make it up, the rule behind it and a link to the transactions, so the owner never has to trust a number blindly.

## Requirements

### Requirement: Explainable Figure Trigger
On a page redesigned to Markas+ an explanation SHALL be opened by one quiet "Rincian" button beside the figure's label, named with that label and marked as opening a dialog. On a page not yet redesigned an explainable figure SHALL stay a button that keeps the figure as its visible text, is marked by a dotted underline, and is named with its label, amount and "lihat rincian". Every trigger SHALL be reachable by keyboard and at least 24 by 24 CSS pixels.

#### Scenario: Keyboard user
- **WHEN** a keyboard user tabs to "Rincian" on Beranda and presses Enter or Space
- **THEN** its explanation SHALL open and focus SHALL move into it

#### Scenario: Screen reader name
- **WHEN** a screen reader reaches Beranda's "Rincian", and the dotted figure Rp 214.000 labelled "Pengeluaran periode ini" on a page not yet redesigned
- **THEN** it SHALL announce a button named "Rincian aman dipakai sampai gajian" that opens a dialog, and a button named "Pengeluaran periode ini Rp 214.000: lihat rincian"

#### Scenario: No dotted figures on a redesigned page
- **WHEN** Beranda is shown
- **THEN** no figure on it SHALL be a dotted button

### Requirement: Explanation Content
An explanation SHALL show the figure's name with "Rincian hitungan", a one-sentence rule, the lines with label, optional note and signed amount, the total equal to the figure, any caveat about waiting rows or unavailable inputs, and a link to the matching transactions or page. In a Rincian dialog, a line that has server lines of its own SHALL open them in place. It SHALL show only the lines the server returned, worded in the neutral register (ui-copy-register).

#### Scenario: Lines and total
- **WHEN** the owner opens Beranda's "Rincian"
- **THEN** it SHALL list Masuk, Pengeluaran, Ditabung & investasi and Terjadwal with the end of the window, end with the total "Aman dipakai" equal to the hero figure, note how much waits for an answer, and link to the period's transactions

#### Scenario: A line with its own lines
- **WHEN** the owner opens the Pengeluaran line inside that Rincian
- **THEN** the five spending buckets SHALL appear under it with their amounts and ledger links, adding up to the line

#### Scenario: Figure without lines
- **WHEN** a figure is unavailable
- **THEN** the explanation SHALL state the reason and what would make the figure available, and SHALL show no total

### Requirement: Explanation Presentation
A "Rincian" SHALL open a modal dialog 480 px wide, centered from 768 px and a bottom sheet below 768 px. A dotted figure on a page not yet redesigned SHALL open a non-modal dialog anchored to it and kept inside the viewport from 768 px, and a bottom sheet below. Esc, the close button or activating outside SHALL close either and return focus to the trigger. Only one explanation SHALL be open at a time.

#### Scenario: Rincian dialog on a desktop
- **WHEN** the owner opens Beranda's "Rincian" at 1440 px
- **THEN** a centered 480 px dialog SHALL open above a scrim with focus inside and trapped, and Esc SHALL close it with focus back on "Rincian"

#### Scenario: Desktop popover
- **WHEN** the owner opens a dotted figure's explanation at 1440 px near the right edge of the window on a page not yet redesigned
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
