# Proposal

## Why

The Accounts page remains hard to scan when an account has many pockets: cramped card actions and inconsistent pocket presentation obscure balances. The full-width row revision clarified actions, but a twelve-pocket account makes the page unnecessarily long. Account cards need a consistent size and a bounded pocket list.

## What Changes

- Present top-level accounts as equal-height cards in persisted order, using three columns on wide desktops, two on medium desktops, and one on narrow screens. Keep pocket rows inside a bounded, independently scrollable card region while the account summary and actions remain visible; cards without pockets use the same height.
- Make account and pocket actions clearly named and appropriately sized. Keep the most relevant action visible and place secondary actions in a discoverable options disclosure without dropping transfer, investment, valuation, reconciliation, edit, archive, create-pocket, or reorder workflows.
- Provide non-drag ordering controls for accounts and sibling pockets alongside existing drag-and-drop support, with order persisted through the existing endpoint.
- Preserve account color identity, balances, investment information, Bahasa Indonesia labels, light/dark themes, and existing financial behavior.
- Move compact account actions to the card's upper-right, make pocket expansion a small, clearly named chevron control near the pocket count, and compose investment rows so identity, valuation, gain, and trading actions stay legible in the fixed-height cards.
- Use the same add-pocket `+` primary action for top-level liquid accounts whether or not they already contain pockets. Keep eligible source-specific transfers in `Opsi`; investment positions retain their trade controls.
- Remove the redundant desktop top-bar Settings shortcut. Keep Settings in the desktop sidebar and make the mobile drawer reachable from a top-bar menu control, with Settings available inside the drawer.
- Replace outdated specifications requiring full-width groups and page-only pocket scrolling. No API or database contract changes are intended.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `clean-core-ledger`: Present the persisted account order in equal-height cards while retaining color identity and the existing reorder API.
- `pocket-reordering`: Keep accessible pocket rows, drag, and explicit ordering controls inside a bounded scroll region.
- `account-reconciliation`: Keep reconciliation available for every eligible account after account-card actions move into the new group/row action layout.

## Impact

- Frontend: `/accounts` desktop and mobile hierarchy, shared action presentation, reorder interactions, responsive top-bar/sidebar Settings access, and focused UI tests.
- Backend/API: existing account, reconciliation, transfer, investment, and `POST /api/accounts/reorder` contracts remain unchanged; no migration or dependency is planned.
- Verification: browser checks with a many-pocket investment account at desktop and mobile widths, keyboard/touch access to every action, regression tests, lint, type-check, build, and OpenSpec validation.
