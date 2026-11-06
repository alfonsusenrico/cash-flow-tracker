# theme-presets-switcher Specification

## Purpose
Specify theme preset selection and persistence together with consistent shared button styling.

## Requirements

### Requirement: Modern Button Styling System
The application SHALL provide a consistent button hierarchy built from the workspace tokens.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders an action button
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a consistent button hierarchy:
  1. **Primary action**: the coral accent fill with its contrast ink, such as "+ Catat" in the top bar, "Simpan" and "Simpan Transaksi". It turns to the accent hover tone under the pointer and to the pressed tone, 1 px lower, while pressed. Its label, icon and shortcut hint SHALL measure at least 4.5:1 (WCAG 2.2 AA) against the fill at rest, under the pointer and pressed, in light and dark themes. No other solid fill (ink, green, sky or lavender) SHALL be used for a button.
  2. **Secondary button**: transparent with a 1 px strong-line border and the text colour, the surface fill under the pointer and the raised fill while pressed.
  3. **Quiet button**: no border, muted text, the surface fill and text colour under the pointer. Its small variant is 28 px high, used for actions such as "Rincian" and "Kelola". Small actions inside rows, such as "Jawab", "Pasangkan" and "Pilih", SHALL be quiet buttons, not coloured pills.
  4. **Link button**: coral text ink without a box, underlined 3 px below the text under the pointer.
  5. **Geometry**: buttons 36 px high (44 px at phone width) with 16 px side padding, an 8 px icon gap, 14 px medium text and the 6 px control radius; icon buttons 36 by 36 px (44 by 44 px at phone width). There are no preset-dependent radii.
  6. **Destructive and debt action buttons**: solid rose fill from `--danger-solid` (`--danger-solid-hover` on hover) with `--danger-contrast` text, measuring at least 4.5:1 (WCAG 2.2 AA) in light and dark themes; a quiet destructive action uses the expense text ink. A confirmation button SHALL keep this styling regardless of the styling of the control that opened it.
  7. Every button SHALL have a target of at least 24 by 24 CSS pixels, a disabled state at 45 % opacity, and keyboard focus shown as the 2 px neutral focus outline of `design-tokens` "Neutral Focus Indicators", never a coloured ring.

#### Scenario: Tactile button press feedback
- **WHEN** a user clicks or taps any primary or secondary action button
- **THEN** the button changes its fill within 140 ms, the primary button moves 1 px down while pressed, and with reduced motion requested the fill changes without a transition and without movement

#### Scenario: Primary button text contrast
- **WHEN** a primary action button such as "+ Catat" or "Simpan profil" is shown at rest, under the pointer or pressed, in the light or the dark theme
- **THEN** its label, icon and shortcut badge measure at least 4.5:1 against the button's coral background

#### Scenario: Destructive button text contrast
- **WHEN** a destructive or debt action such as "Ya, lanjutkan", "Ya, hapus", "Cabut perangkat" or "+ Tagihan Baru" is shown at rest or under the pointer, in the light or the dark theme
- **THEN** its label measures at least 4.5:1 against the button's rose background

#### Scenario: Keyboard focus on a button
- **WHEN** a keyboard user tabs to a primary, secondary, quiet or icon button
- **THEN** the button shows a 2 px outline in the focus token 2 px outside its edge, and no coloured ring

#### Scenario: Small row actions are quiet
- **WHEN** a Tinjau row on a page not yet redesigned shows a small action such as "Pilih" or "Ubah"
- **THEN** the action is a small quiet button with muted text and no semantic fill

#### Scenario: Whole-row items
- **WHEN** Beranda's "Perlu tindakan" lists an item
- **THEN** the whole row SHALL be its one action, and the row SHALL show no separate action button or pill

### Requirement: Light, Dark and System Theme
The web app SHALL offer "Terang", "Gelap" and "Ikuti sistem" themes, default to "Ikuti sistem", persist the choice in the browser, and apply it before the first paint so no page flashes the other theme. With "Ikuti sistem" the theme SHALL follow operating-system changes while the page is open.

#### Scenario: First visit
- **WHEN** a visitor with a dark operating-system theme opens any page for the first time
- **THEN** the page SHALL render dark from the first paint

#### Scenario: Persisted choice
- **WHEN** the owner chooses "Terang" and reloads `/ledger`
- **THEN** the page SHALL render light from the first paint, also when the operating system is dark

#### Scenario: Earlier stored value
- **WHEN** the browser still holds `theme = dark` from before the redesign
- **THEN** the app SHALL treat it as the "Gelap" choice

#### Scenario: Storage unavailable
- **WHEN** browser storage throws or is empty
- **THEN** the app SHALL follow the operating system and keep working without an error

### Requirement: Tampilan Settings Section
Pengaturan SHALL have a Tampilan section at `/settings/tampilan` with the theme as a labelled radio group ("Ikuti sistem", "Terang", "Gelap"), the "Pintasan satu tombol" switch and "Sembunyikan saldo". Each control SHALL use the same stored setting as its shell counterpart (the top bar's theme menu, the single-key shortcut setting and the top bar's "Sembunyikan saldo" button), apply immediately and stay in sync with it without a reload.

#### Scenario: Choosing a theme in Tampilan
- **WHEN** the owner selects "Gelap" in Tampilan
- **THEN** the page SHALL switch to the dark theme at once, the top bar's theme menu SHALL show "Gelap" checked, and a reload SHALL render dark from the first paint

#### Scenario: Turning off single-key shortcuts
- **WHEN** the owner turns off "Pintasan satu tombol" in Tampilan
- **THEN** single-key shortcuts SHALL stop on every page, ? SHALL no longer open "Bantuan & pintasan", and ⌘K/Ctrl+K and Esc SHALL keep working

#### Scenario: Hiding balances
- **WHEN** the owner turns on "Sembunyikan saldo" in Tampilan
- **THEN** every balance and amount the app masks today SHALL be masked on every page, and the top bar's "Sembunyikan saldo" button SHALL show the pressed state

#### Scenario: Storage unavailable
- **WHEN** browser storage throws while the owner changes a Tampilan control
- **THEN** the change SHALL apply for the open page, no error SHALL be shown, and the next visit SHALL start from the defaults
