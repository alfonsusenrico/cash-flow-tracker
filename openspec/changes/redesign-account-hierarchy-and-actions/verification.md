# Accounts hierarchy verification

## Root-cause and baseline

- The owner's 2026-09-24 23:48 screenshot (2048px, Jago with twelve pockets) is the original failure fixture. The local frontend image then in use was `sha256:f5c8babbc9a15fa6d31023a3f170ab84237cea197658a4bf9d4613224cb99eba`, per `PROJECT_STATE.md`.
- Before this change, `/accounts` used `columns-1 md:columns-2 2xl:columns-3` around top-level account cards. Each pocket was another bordered card with a separate strip of very small action buttons. The uneven column flow and repeated nested borders were present in the screenshot. Some controls relied on tiny icons and `title` text; an investment position's desktop actions were especially crowded.
- The earlier natural-height CSS correction removed an inner scrollbar but retained the same three-column/card-within-card structure. A subsequent full-width row revision clarified actions but made the twelve-pocket account too tall. The owner requested equal-height cards with pockets scrolling inside each card. The discriminating check is now a two-column desktop layout with equal card heights, twelve reachable border-separated pocket rows, and labelled actions without clipping.

## Account action inventory

| Item | Before desktop | Before mobile | Required after change |
| --- | --- | --- | --- |
| Liquid standalone account | transfer, reconcile, add pocket, edit, archive, drag reorder | transfer, reconcile, add pocket, edit, archive | same operations plus move up/down; transfer primary, others under labelled `Opsi` |
| Parent with pockets | add pocket, edit, archive, drag reorder; investment parent also bought the first child | add pocket, edit, archive | add pocket, edit, archive, drag and move up/down; child trade remains on its named row, not an ambiguous first-child shortcut |
| Liquid child pocket | transfer, reconcile, edit, archive, drag reorder | transfer, reconcile, edit, archive | same operations plus move up/down; transfer primary |
| Investment standalone position | buy, sell, valuation, reconcile, add pocket, edit, archive, drag reorder | buy, sell, reconcile, add pocket, edit, archive | buy, sell, valuation, reconcile, add pocket, edit, archive, drag and move up/down |
| Investment child position | buy, sell, valuation, reconcile, edit, archive, drag reorder | buy, sell, reconcile, edit, archive | buy, sell, valuation, reconcile, edit, archive, drag and move up/down |

The desktop-only first-child buy shortcut and mobile-only missing valuation access were inconsistent presentations, not separate financial operations. The redesign targets the selected position explicitly and makes supported actions available at both widths.

## Implementation and checks — 2026-09-25

- Replaced the old account-card variants with one shared account-group/child-row renderer. The group keeps the configured color and balance; position rows keep symbol, units, market and buy price, and Capital Gain. After owner feedback, top-level cards became equal-height two-column cards where space permits, with a bounded pocket-list scroll area; mobile remains one column.
- Liquid accounts/pockets expose `Pindah Saldo`; investment positions expose `Beli` and `Jual`. `Opsi` holds relevant valuation, reconciliation, editing, archiving, and move actions. A parent platform without an instrument now emphasizes adding a pocket rather than trading an unspecified position. The archive action still requires confirmation. Reorder failure and archive failure show explicit alerts.
- Corrected the header count to distinguish top-level accounts from child pockets. Account tag foreground selection now uses relative luminance so bright configured colors receive black text.
- The final frontend suite passed 83 tests across 27 files; lint, type-check, host and Docker production builds passed. The local frontend container was recreated from image `sha256:17de7c20dac1ec27efa2e920ac479ecf1ea5f44ff1fd462794b2d8a5c88ec8fa`; the API was not restarted. Other services were not recreated.
- A headless local Chrome pass used synthetic intercepted API responses and a twelve-pocket fixture against the rebuilt local frontend. At 2048, 1280, 640, 390, and 320 CSS-pixel widths the Jago and pocket-free Cash cards both measured 448px high, the twelve-pocket area scrolled, the last pocket was reachable, no document-level horizontal overflow appeared, and no visible group control measured below 44×44 CSS pixels. At 1280px and 2048px the cards used two columns; narrow widths used one. The 640px pass approximates 200% reflow from a 1280px viewport, not actual toolbar zoom. Dark and light 390px screenshots were reviewed. At 390px the `Opsi` panel stayed within the viewport, Escape closed it and restored focus, an investment buy action opened its dialog, and a move action posted to the existing reorder route. No synthetic action altered the local database.
- The bounded pocket viewport initially clipped a pocket's `Opsi` disclosure. The disclosure now renders in a viewport-positioned layer; a repeat 390px dark/light browser check found the pocket menu outside the scroller, inside the viewport, and Escape returned focus. The full frontend suite passed again after that correction. The synthetic API interceptor prevented financial mutations.
- A separate 200% root-text-resize check kept the document width at 390px and the twelve account rows present, but exposed a pre-existing app-wide header overlap: the title is compressed behind the fixed-size top-bar controls. The affected rule is around `frontend/src/components/layout/TopBar.tsx:51` (`sm:flex-nowrap` with fixed control widths). This change did not modify the shared top bar; do not claim the entire page passes text-resize or WCAG checks. Physical touch, VoiceOver/TalkBack, and actual toolbar zoom were not run.

## Three-column desktop revision — 2026-09-25

- The owner approved three account cards per row on wide desktops, with two on medium desktops and one on narrow screens. The desktop grid now uses `2xl:grid-cols-3` over `xl:grid-cols-2`; alerts and empty states span the full grid row. Card height and bounded pocket scrolling are unchanged.
- Frontend verification on the revised worktree: 83/83 Vitest tests, lint, type-check, host production build, and Docker frontend build passed. Only the local frontend container was recreated.
- A repeated local headless Chrome pass with synthetic intercepted account data found three cards in the first row at 1536px and 2048px, two at 1280px, and one at 640px, 390px, and 320px. Cash (no pockets) and Jago (twelve pockets) remained 448px high at every checked width. The last pocket was reachable by scrolling, no document horizontal overflow appeared, and no visible Jago control measured below 44×44 CSS pixels. The 1536px screenshot was visually reviewed for readable balances and uncramped actions. Dark/light 390px menu and form-help checks still passed; no local database writes occurred.
- Actual browser toolbar zoom, physical touch/virtual keyboard, and screen-reader checks remain unverified. The pre-existing shared TopBar overlap under 200% root-text resize remains outside this grid revision.
