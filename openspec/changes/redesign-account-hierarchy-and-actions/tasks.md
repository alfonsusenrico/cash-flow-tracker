# Tasks

## 1. Establish the Action and Layout Baseline

- [x] 1.1 Inventory visible account and pocket actions for liquid, parent, and investment items in both desktop and mobile markup; record a before/after action matrix and verify it covers transfer, trade, valuation, reconciliation, create pocket, edit, archive, and reorder.
- [x] 1.2 Reproduce the reported layout at 2048px with an account containing twelve pockets and capture a baseline screenshot; verify which containers, controls, and responsive rules cause the clipping, uneven cards, or undersized targets.

## 2. Rebuild the Account Hierarchy

- [x] 2.1 Replace nested pocket mini-cards with compact child rows and preserve account/pocket order, balances, investment metadata, and expansion; verified against the twelve-pocket fixture before the equal-height card revision.
- [x] 2.4 Arrange top-level accounts in equal-height responsive cards, including accounts without pockets, with a bounded scrollable pocket region and persistent summary/actions; verify first/last pocket reachability, card alignment, no horizontal clipping, and ordering in a twelve-pocket browser fixture.
- [x] 2.5 Show three equal-height account cards per row at wide-desktop widths while retaining two columns at medium desktop and one on narrow screens; verify card alignment, readable actions, reachable pocket rows, and no horizontal overflow at 1536px and 2048px.
- [x] 2.6 Place compact primary/options controls at each account card's upper-right, use a clearly named chevron beside the pocket count for collapse, and recompose dense investment pocket rows; verified five dense positions with long names, units, cost, value, gain/loss, and separated trade controls at 1536px, 2048px, 390px, and 320px in the local browser.
- [ ] 2.2 Align the mobile hierarchy and responsive spacing with the same group/row structure and preserve empty and loading states; verify no horizontal overflow or hidden content at 320px and 390px widths and at 200% zoom.
- [x] 2.3 Preserve configured account colors, high-contrast labels, dark/light surfaces, and tabular amounts; verify legibility for long names and large currency amounts in both themes.

## 3. Restore and Clarify Every Action

- [x] 3.1 Implement labelled, adequately sized contextual primary controls and an accessible `Opsi` disclosure for secondary operations, using existing action handlers; verify every action in the matrix from task 1.1 is reachable on both desktop and mobile.
- [x] 3.2 Preserve confirmation and error behavior for archive, valuation, transfer, trade, and `Sesuaikan Saldo`; verify each action targets the selected account or pocket and unsupported actions are absent.
- [x] 3.3 Add move-up/move-down controls for top-level accounts and sibling pockets alongside drag ordering; verify boundary states, sibling-only moves, and persisted order after refresh through `POST /api/accounts/reorder`.
- [x] 3.4 Remove the duplicate desktop header Settings shortcut and connect the mobile top-bar menu to the existing drawer Settings entry; frontend tests and the 390px browser check verified the menu, Settings dialog, and drawer close while the desktop sidebar entry remained.
- [x] 3.5 Make `+` the primary add-pocket action for top-level liquid accounts with or without pockets, moving an eligible source-specific transfer into `Opsi`; focused tests verified the correct pocket parent, transfer source, and unchanged investment-position controls, and the local built page showed `+` on empty and populated cards.

## 4. Regression and Browser Verification

- [x] 4.1 Extend focused Accounts-page tests for many-pocket rendering, action availability, disclosure keyboard behavior, and reorder payloads; verify the focused frontend test suite passes.
- [x] 4.2 Run frontend lint, type-check, and production build, plus `git diff --check`; verify all pass on the final working tree.
- [ ] 4.3 Inspect the local built page at 2048px, 1280px, 390px, and 320px, including light/dark themes, 200% zoom, pointer/touch and keyboard-only use; verify readable values, at least 44×44px action targets, focus visibility, no clipped controls or horizontal overflow, and representative actions/reordering in the browser.
- [x] 4.4 Run `openspec validate redesign-account-hierarchy-and-actions --strict`, review the final changed-file scope and action matrix against the approved specs, and update `PROJECT_STATE.md` with the exact verification and remaining limitations.
