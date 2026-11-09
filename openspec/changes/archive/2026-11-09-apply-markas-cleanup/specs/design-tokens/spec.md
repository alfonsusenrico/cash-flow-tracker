## MODIFIED Requirements

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
