# Proposal

## Why

The change archived on 2026-10-31 (`finish-small-follow-ups`) left three open decisions for the owner. The phone's top categories leave out uncategorised spending, so the listed amounts can fall short of the summary's own outflow. 82 class uses of `shadow-xs` render no shadow, because Tailwind 3.4.17 has no such utility. The violet `--color-transfer` token is used by no component, while the ledger paints transfers sky blue.

## Approval

On 2026-10-31 the owner approved implementing the three items ("do the three items too"). Recorded 2026-10-31.

## Expected Outcome

- `GET /api/dashboard/cash-flow-summary` ranks uncategorised spending as "Tanpa kategori" (`category_id: null`, `kakeibo_type: null`) among its top three. When three or fewer spending groups exist, the listed amounts add up to `outflow`; they never exceed it.
- Cards, chips and buttons that use `shadow-xs` show a subtle 1px raise in the light and the dark theme. No heavier shadow appears, and surfaces that already had a shadow keep it.
- The design tokens no longer carry a violet transfer colour. Transfers stay sky blue.

## What Changes

- `top_categories` keeps the breakdown's "Tanpa kategori" row, with `category_id` and `kakeibo_type` null. This is additive: `category_id` becomes nullable, and the other fields keep their names and types.
- `frontend/tokens.css` gains `--shadow-xs` (light and dark), and `tailwind.config.js` maps `boxShadow.xs` to it.
- `--color-transfer`, `--color-transfer-hover` and `--color-transfer-soft` are removed from both themes, along with the Tailwind `transfer` colour and the unused `chartColors.transfer`, `transferLight` and `accent` aliases.
- `tokens.test.ts` checks the `shadow-xs` mapping and keeps `--shadow-xs` lighter than `--shadow-sm` in both themes.

## Non-goals

- Android changes. The companion is updated in parallel to accept `category_id: null` with `kakeibo_type: null`. design.md D2 records what the installed 2.3.3 does.
- `shadow-2xs`. It has 33 class uses and also renders nothing. It is recorded under "Open decisions for owner", not changed.
- Restyling, new figures or changed totals. `outflow`, the category breakdown and the web reports are unchanged.
- Production access or release. Verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised. Opening and merging the pull request stay with the owner, and release uses the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `financial-reporting-consistency`: the top categories include uncategorised spending, and their sum equals `outflow` when no entry is cut.
- `tactile-pulse-ui`: the surface elevation clause names `--shadow-xs` as the plain card raise.

Standards: api-consumers (additive change, with the companion's 2.3.3 behaviour recorded), money invariants (no figure changes, and the list reuses the report breakdown), multi-user (owner-scoped). For the changed surfaces, WCAG 2.2 AA contrast is not affected because no text or colour changes. No project capability is added.

## Impact

- Backend: `services/report_figures.py` (`top_categories`). No migration.
- API: in `GET /api/dashboard/cash-flow-summary`, a `top_categories` entry may have `category_id: null` and `kakeibo_type: null`.
- Web: `tokens.css`, `tailwind.config.js`, `components/charts/theme.ts`, `app/tokens.test.ts`.
- Specs: `financial-reporting-consistency`, `tactile-pulse-ui`.
- Android: none in this repository. Phones on 2.3.3 show the new row without a pillar (design.md D2).
