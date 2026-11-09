# design-tokens Specification

## Purpose
Keep every web colour, surface and type choice in one set of semantic tokens, so both themes stay consistent and no legacy alias or palette remap returns.

## Requirements

### Requirement: Workspace Tokens Only
`frontend/tokens.css` SHALL declare only the workspace token set in `:root` and `.dark`, and SHALL NOT declare a compatibility alias. `tailwind.config.js` SHALL map only workspace tokens and SHALL NOT remap a Tailwind palette colour (`emerald`, `rose`, `amber`, `sky`, `violet` or any other) to a token. Every declared token and every Tailwind colour key SHALL be used by a stylesheet or component under `frontend/src`.

#### Scenario: A legacy name is declared again
- **WHEN** `tokens.css` or `tailwind.config.js` declares `--bg`, `--border`, `--primary` or another name from the removed alias list, or remaps a palette colour
- **THEN** `tokens.test.ts` SHALL fail

#### Scenario: An unused token
- **WHEN** `tokens.css` declares `--nav-bg`, or `tailwind.config.js` maps a colour key such as `surface-3`, and nothing under `frontend/src` uses it
- **THEN** `tokens.test.ts` SHALL fail and name the token or key

#### Scenario: Pages keep their look after the sweep
- **WHEN** the unused tokens, Tailwind keys and global classes are removed
- **THEN** captures of every route and dialog at 1440 and 390 px in both themes and both languages SHALL be pixel-identical to the same captures before the removal

### Requirement: No Legacy Styling in Components
No source file under `frontend/src` SHALL reference a legacy token name, use a Tailwind default palette colour utility, or use the legacy utility classes `btn-primary`, `btn-secondary`, `cockpit-card`, `card-squircle`, `pressable` or `donut-ring`.

#### Scenario: A legacy name is used again
- **WHEN** a change adds `var(--text-secondary)`, `bg-surface-raised`, `text-emerald-600` or `className="btn-primary"` to any file under `frontend/src`
- **THEN** `tokens.test.ts` SHALL fail and name the file and the legacy name

#### Scenario: Every used token exists
- **WHEN** the test reads every `var(--…)` in `frontend/src` and every token named in `tailwind.config.js`
- **THEN** each SHALL be declared in `tokens.css` for both themes, or be a documented runtime variable set by a component

#### Scenario: Pages keep their look after the removal
- **WHEN** the alias removal is complete
- **THEN** screenshots of every route at 1280 and 390 px in both themes SHALL show no colour change against the same routes before the removal, except where this change restyles a page on purpose

### Requirement: Token Contrast Coverage
Every text and icon pair that the workspace tokens define (text, muted and the semantic inks on the window, the surfaces and their soft fills, and each solid fill with its contrast ink) SHALL be measured by `tokens.test.ts` in both themes: at least 4.5:1 for text and at least 3:1 for meaningful icons and chart marks. No component SHALL lower a text token's contrast with opacity.

#### Scenario: A muted caption faded with opacity
- **WHEN** a component renders muted caption text with an `opacity-*` utility or an inline opacity below 1
- **THEN** the copy test or the browser axe check SHALL report it, and the caption SHALL use the muted token at full opacity instead

#### Scenario: Contrast measured in both themes
- **WHEN** `tokens.test.ts` runs
- **THEN** it SHALL report every defined pair in light and dark at or above its threshold, and fail on the first pair below it

### Requirement: Markas Palette Values
The workspace tokens SHALL carry the option-6 "Markas+" colour values in both themes, set on the existing token names: warm paper surfaces in light, soft charcoal in dark, one coral accent, the semantic text inks and the mark colours. A token without a prototype counterpart SHALL keep its value until the change that restyles its page, and SHALL be listed in this change's verification record.

#### Scenario: Light theme values
- **WHEN** a page renders in the light theme
- **THEN** the canvas SHALL be `#F8F7F4`, cards and panels `#F1EFEB`, the raised fill `#E6E3DD`, lines `#E1DDD6`, control edges `#847F78`, text `#1C1E22`, muted text `#5B5E65`, the primary fill `#FF334B` with its `#0A0D14` ink, and coral text `#B8142D`

#### Scenario: Dark theme values
- **WHEN** a page renders in the dark theme
- **THEN** the canvas SHALL be `#151517`, cards and panels `#1C1D20`, the raised fill `#26272B`, lines `#2F3034`, control edges `#6E7076`, text `#E8E7E4`, muted text `#A3A3A7`, the primary fill `#FF4757` with its `#0A0D14` ink, and coral text `#FF6B78`

#### Scenario: No tinted semantic fills
- **WHEN** a component paints a semantic colour at reduced opacity behind text, such as `bg-out-solid/10`
- **THEN** `tokens.test.ts` SHALL fail and name the file, and badges SHALL instead use the raised neutral fill with the semantic text ink

#### Scenario: Contrast still enforced
- **WHEN** `tokens.test.ts` runs after the values change
- **THEN** every text pair SHALL measure at least 4.5:1 and every mark at least 3:1 in both themes, and the test SHALL fail on the first pair below its threshold

### Requirement: Type Families and Scale
The web app SHALL render text only in Inter and Manrope, only at 12, 14, 16, 20, 24, 32 and 44 px, and only at weights 400, 500 and 600. Manrope SHALL be used only for page, dialog and section titles, Laporan questions, the brand and a page's hero figure. Amounts, counts, times, column dates, tables, fields, badges and chart labels SHALL use tabular lining figures; prose SHALL use proportional lining figures.

#### Scenario: A stray size or weight is added
- **WHEN** a change adds `font-size: 13.5px`, `text-[15px]`, `text-lg`, `font-weight: 700` or `font-bold` to a stylesheet or a component under `frontend/src`
- **THEN** `typeScale.test.ts` SHALL fail and name the file and the value

#### Scenario: Every page measured
- **WHEN** every route is captured at 1440 and 390 px in both themes and the computed styles of visible text are read
- **THEN** the families SHALL be only Inter and Manrope, the sizes only the seven listed and the weights only 400, 500 and 600, with Manrope only on titles and the brand

#### Scenario: Figures align, prose stays natural
- **WHEN** a ledger column shows amounts under each other and a subtitle reads "hari ke-14"
- **THEN** the amounts SHALL use tabular figures and line up digit for digit, and the subtitle SHALL use proportional figures so the hyphen keeps its normal width

### Requirement: Geometry, Borders and Elevation
Corner radii SHALL be 6 px for controls, 12 px for panels, tiles, popovers and toasts, and 16 px for dialogs and sheets; rows and bars SHALL be square and only dots circular. Every border SHALL be 1 px in a neutral line token, and no border, outline or ring SHALL use the accent or a semantic colour except selection bars, group rails and keyboard focus. Only dialogs, sheets, popovers, menus and toasts SHALL cast a shadow.

#### Scenario: A thick or coloured border is added
- **WHEN** a change gives a control, card, chip, dialog or field a 2 px border, a coral or semantic border colour, a coloured ring or a radius other than 0, 6, 12, 16 or a circle
- **THEN** `geometry.test.ts` SHALL fail and name the file

#### Scenario: Cards are flat
- **WHEN** a card or panel renders in the light or the dark theme
- **THEN** its computed `box-shadow` SHALL be `none`, while an open dialog, menu or toast SHALL carry the layer shadow token of that theme

#### Scenario: Field error stays neutral
- **WHEN** the owner submits Catat with an empty amount
- **THEN** the amount field's edge SHALL stay the neutral field-focus colour, the error SHALL be named in 12 px expense text under the field, and the field SHALL carry `aria-invalid` and `aria-describedby`

### Requirement: Neutral Focus Indicators
Keyboard focus on buttons, links, rows, navigation items, tabs, segments and swatches SHALL be a 2 px outline in the focus token with a 2 px offset, inset 2 px on rows and navigation items, at least 3:1 against every surface. A focused text field SHALL show no outline but a 1 px edge in the field-focus token, at least 3:1 against its resting edge, and a 3 px soft neutral ring.

#### Scenario: Keyboard focus on a button
- **WHEN** a keyboard user tabs to "+ Catat"
- **THEN** it SHALL show a 2 px `#45484F` outline 2 px outside it in light and a `#BDBEC2` one in dark, with no coloured ring

#### Scenario: Clicking into the sentence field
- **WHEN** the owner clicks into "Tulis transaksi" in Catat in the light theme
- **THEN** the field SHALL show a 1 px `#2B2D32` edge and a 3 px `rgba(28, 30, 34, 0.10)` ring, no outline and no coral

#### Scenario: Pointer click on a button
- **WHEN** the owner clicks a button with the pointer
- **THEN** no focus outline SHALL appear

### Requirement: State and Layer Motion
Hover, press, selection, switch and field-focus changes SHALL take 140 ms, and dialogs, popovers, toasts and sheets SHALL enter within 200 ms, all on the curve `cubic-bezier(0.2, 0, 0, 1)` without bounce. No figure SHALL count between values. When reduced motion is requested, no transition or animation SHALL run.

#### Scenario: A figure changes
- **GIVEN** Beranda shows "Pengeluaran" Rp 100.000
- **WHEN** a new transaction makes it Rp 150.000
- **THEN** the figure SHALL show Rp 150.000 at once, without intermediate values

#### Scenario: Reduced motion
- **GIVEN** the operating system requests reduced motion
- **WHEN** the owner opens a dialog or hovers a row
- **THEN** the dialog SHALL appear and the row SHALL change colour without any transition

### Requirement: Straight Selection Marks
A selected or current item marked at its left SHALL use the shared straight bar: on navigation and rail items a 2 by 20 px bar 8 px left of the item's fill, on square rows a 2 px bar along the full height at the left edge, with no radius. Group rails SHALL be a shared 2 px straight line beside the group. Neither SHALL be drawn as a border or an inset shadow on a rounded element.

#### Scenario: A curved mark is added again
- **WHEN** a change styles `[aria-pressed="true"]`, `[aria-current]`, `[aria-selected]` or `[data-selected]` with `border-left` or a left inset `box-shadow`, or uses a Tailwind `border-l-` utility in a component
- **THEN** `selectionMarks.test.ts` SHALL fail and name the file

#### Scenario: Chosen pocket in the goal account picker
- **WHEN** the owner chooses a child pocket in the goal account picker
- **THEN** the pocket SHALL be marked by the straight bar at its left edge, which does not follow any corner

### Requirement: Interface Icons
Interface icons SHALL be drawn on a 24 px grid with a 1.5 px stroke, round caps and joins and `currentColor`, rendered at 16 px in controls, rows and navigation and at 20 px in the phone header and tab bar. The shell's icons SHALL use the prototype's drawings. Icons SHALL be hidden from assistive technology unless they are the only label, which their control then names.

#### Scenario: Shell icons measured
- **WHEN** the sidebar, top bar, phone header and tab bar are captured at 1440 and 390 px
- **THEN** every icon `svg` SHALL have a 1.5 px stroke, the nav and top-bar icons SHALL be 16 px and the phone header and tab bar icons 20 px

#### Scenario: Icon-only control
- **WHEN** a screen reader reaches the bell or the "…" button
- **THEN** it SHALL announce the control's name, for example "Notifikasi, 2 baru" or "Tindakan lain", and not the icon
