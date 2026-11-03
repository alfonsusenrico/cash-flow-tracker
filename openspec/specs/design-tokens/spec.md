# design-tokens Specification

## Purpose
Keep every web colour, surface and type choice in one set of semantic tokens, so both themes stay consistent and no legacy alias or palette remap returns.

## Requirements

### Requirement: Workspace Tokens Only
`frontend/tokens.css` SHALL declare only the workspace token set in `:root` and `.dark`, and SHALL NOT declare a compatibility alias. `tailwind.config.js` SHALL map only workspace tokens and SHALL NOT remap a Tailwind palette colour (`emerald`, `rose`, `amber`, `sky`, `violet` or any other) to a token.

#### Scenario: A legacy name is declared again
- **WHEN** `tokens.css` or `tailwind.config.js` declares `--bg`, `--border`, `--primary` or another name from the removed alias list, or remaps a palette colour
- **THEN** `tokens.test.ts` SHALL fail

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
