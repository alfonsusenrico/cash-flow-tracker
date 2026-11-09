## ADDED Requirements

### Requirement: Rincian Dialog Presentation
A "Rincian" SHALL open a modal dialog 480 px wide, centered from 768 px and a bottom sheet below 768 px. Esc, the close button or activating outside SHALL close it and return focus to the trigger. Only one explanation SHALL be open at a time, and no explanation SHALL open as a popover anchored to a figure.

#### Scenario: Rincian dialog on a desktop
- **WHEN** the owner opens Beranda's "Rincian" at 1440 px
- **THEN** a centered 480 px dialog SHALL open above a scrim with focus inside and trapped, and Esc SHALL close it with focus back on "Rincian"

#### Scenario: Phone sheet
- **WHEN** the owner opens an explanation at 390 px
- **THEN** it SHALL open as a bottom sheet with a visible close button, the page behind SHALL not scroll, and closing SHALL return focus to the trigger

#### Scenario: Zoom and reflow
- **WHEN** the page is zoomed to 400% at 1280 px
- **THEN** the explanation SHALL reflow as a sheet without horizontal scrolling and without clipping its lines

## MODIFIED Requirements

### Requirement: Explainable Figure Trigger
An explanation SHALL be opened by one quiet "Rincian" button beside the figure's label, named with that label and marked as opening a dialog. No figure SHALL be a dotted button. Every trigger SHALL be reachable by keyboard and at least 24 by 24 CSS pixels.

#### Scenario: Keyboard user
- **WHEN** a keyboard user tabs to "Rincian" on Beranda and presses Enter or Space
- **THEN** its explanation SHALL open and focus SHALL move into it

#### Scenario: Screen reader name
- **WHEN** a screen reader reaches Beranda's "Rincian"
- **THEN** it SHALL announce a button named "Rincian aman dipakai sampai gajian" that opens a dialog

#### Scenario: No dotted figures on a redesigned page
- **WHEN** any page is shown, every page now being redesigned
- **THEN** no figure on it SHALL be a dotted button

## REMOVED Requirements

### Requirement: Explanation Presentation
**Reason**: It specified a popover for dotted figures on pages not yet redesigned; that path was removed with the last such page (apply-markas-catat) and every page is redesigned.
**Migration**: "Rincian Dialog Presentation" keeps the dialog, sheet, focus and reflow scenarios unchanged.
