# Design

## Context

Facts from the code at origin/main 15dbf61:

- `top_categories` (`services/report_figures.py`) is built from `category_breakdown`, the breakdown every report uses. The breakdown already holds uncategorised spending as a "Tanpa kategori" row (`id` None, `is_uncategorized` true, `kakeibo_type` "need"). `top_categories` dropped that row, because the contract typed `category_id` as a UUID (archived `finish-small-follow-ups`, design D2).
- `outflow` is `max(0, gross − applied)`. A reimbursement is applied only to the category it names, and never more than that category's spending (`load_spending_offsets`). Uncategorised spending has no category, so no reimbursement lowers it. With every row of the breakdown listed, the rows therefore add up to `outflow` exactly.
- `tailwind.config.js` extends `boxShadow` with `sm` and `md` only. Tailwind 3.4.17 has no `xs` (or `2xs`) default, so the 82 `shadow-xs` and 33 `shadow-2xs` class uses produce no CSS rule. `.card-squircle` and `.cockpit-card` set `box-shadow: var(--shadow-sm)` in unlayered CSS after `@tailwind utilities`, so on those surfaces they win over any `shadow-*` utility.
- `--color-transfer`, `-hover` and `-soft` (violet `#7C3AED` / `#8B5CF6`) feed only the Tailwind `transfer` colour and `chartColors.transfer`, `transferLight` and `accent`. No component or stylesheet uses any of them. Ledger transfers use Tailwind's `sky-500` directly.

## Goals / Non-Goals

Goals: list uncategorised spending on the phone with an exact sum, give `shadow-xs` its intended subtle raise, and drop the unused violet transfer token.

Non-goals: Android work, `shadow-2xs`, other unused chart aliases (`cyan`, `incomeLight`, `expenseLight`, `grid`, `text` predate this change), new figures, production.

## Decisions

1. **Uncategorised spending is a ranked entry with null identity.** `top_categories` keeps every breakdown row with `spent > 0` and maps "Tanpa kategori" to `{"category_id": null, "name": "Tanpa kategori", "kakeibo_type": null, "outflow": <net>}`. It ranks by outflow like any category and is cut by the limit of 3 like any category. `kakeibo_type` is null because there is no category to classify. The breakdown's `need` default for the bucket stays in the web reports, which this change does not touch. The sort key is `(-outflow, name, id or "")`. The empty string gives the uncategorised entry a total order even against a category that happens to be named "Tanpa kategori". Rejected alternatives:
   - A separate `uncategorized_outflow` field: the phone would have to rank two lists, and a figure the phone ranks itself is a figure it computes.
   - A synthetic UUID for the bucket: it would look like a category the owner could open.
   - Classifying the bucket as `need`: the phone would show a pillar for spending nobody classified.
2. **Android 2.3.3 shows the entry.** This was checked read-only against `financial-tracker-mobile-listener` main 4e0d3e6 (`versionName` 2.3.3, `versionCode` 25). `TopCategory.categoryId` is a nullable `String?` that `shownTopCategories()` never validates. It rejects the list only when there are more than 3 entries, a null entry, a blank name, a missing or negative outflow, an unknown non-null pillar, or entries out of order. A null `kakeibo_type` is allowed, and `pillarLabel(null)` returns null, so `CategoryRow` shows the name, amount and bar with no pillar line. TalkBack reads "Tanpa kategori, Rp…". The tile is therefore not hidden. A probe on a `git archive` copy of 4e0d3e6 in the scratch folder confirmed this. It used Robolectric with Gson parsing, `isComplete()`, `shownTopCategories()`, the stored-JSON round trip, and the rendered `HomeScreen` tile. The existing `TrackerOptionalFieldsTest` (9) and `TopCategoriesUiTest` (36) also passed on that copy. No phone was used.
3. **`--shadow-xs` restores the token the classes were written for.** History:
   - `e99f1cb` and `82d5c0d` (2026-09-17) defined `--shadow-xs` with `boxShadow.xs`. Light was `0 1px 2px 0`, at first `rgba(0,0,0,0.03)` and later 0.04. Dark was `rgba(0,0,0,0.3)`.
   - `a9b8867` (2026-09-18, "restoring production UI design") removed the token and the mapping but kept the classes.
   - `0d839cc` (the same day) moved the shadow ink to `rgba(8, 10, 14, …)`.

   The new token keeps the last defined geometry and opacity in the current ink: `0 1px 2px 0 rgba(8, 10, 14, 0.04)` light and `0 1px 2px 0 rgba(0, 0, 0, 0.3)` dark. Both stay below `--shadow-sm` (2px/8px at 0.06 light, 4px/14px at 0.7 dark). The 1px `--border` still does the separating, as `AGENTS.md` and `tactile-pulse-ui` item 2 require. Rejected alternatives:
   - Dropping the 82 classes: this would discard the design intent the history shows and touch 24 files.
   - Mapping `xs` to `--shadow-sm`: this would turn every chip into a 8px-blur card, heavier than intended.
   - Tailwind 4's `rgb(0 0 0 / 0.05)`: this would add a third ink to a token family that uses one per theme.
4. **Surfaces that already have a shadow keep it.** `.card-squircle` and `.cockpit-card` keep `--shadow-sm`, because their unlayered rule comes after the utilities. No element mixes `shadow-xs` with another shadow size at the same variant. The browser measurement before and after confirms that only elements whose `box-shadow` was `none` changed.
5. **The violet transfer token is removed, not repointed.** No component uses it, and the ledger's sky blue is Tailwind's `sky-500` palette, not a token. Pointing `--color-transfer` at sky would create a second source for the same colour with no consumer. Removing the token takes with it the Tailwind `transfer` colour and the three chart aliases that only referred to it. `tokens.test.ts` needs no pair change, because it never checked `--color-transfer`. Its "defines every custom property used" test still guards against a dangling `var()`.
6. **Tests pin the contract and the tokens.**
   - `test_cash_flow_top_categories.py` gains ranked-entry, exact-sum (1, 2 and 3 groups, `today` and `month`), never-exceeds (cut by the limit, then after a reimbursement), reimbursement and tenant-isolation tests (both owners' views).
   - The unit test now expects the uncategorised row and a total order.
   - `tokens.test.ts` checks `boxShadow.xs → var(--shadow-xs)`, and that `--shadow-xs` in each theme has a 1px offset and at most 2px blur, and is lighter than `--shadow-sm`.

## API

```
GET /api/dashboard/cash-flow-summary?period=today|month
  … existing fields unchanged …
  "top_categories": [
    {"category_id": "<uuid>|null", "name": "<string>", "kakeibo_type": "need|want|culture|unexpected|saving|null", "outflow": <int>}
  ]
```

- `category_id` is null only for "Tanpa kategori", whose `kakeibo_type` is then null as well.
- There are at most 3 entries, and the list is `[]` without spending.
- When the window has at most 3 entries with spending, `sum(outflow) == outflow`. Otherwise the sum is below `outflow` by the entries that were cut.
- Failure behaviour is unchanged: 401 without a valid session or Bearer key, and 422 for another `period`.

## Risks / Trade-offs

- A client that parses `category_id` as a non-null UUID would reject the new entry. The only known client is the companion, and D2 shows 2.3.3 accepts it. The parallel 2.3.4 change makes the acceptance explicit.
- `shadow-xs` now paints on 122 rendered elements across the 7 pages, 4 widths and themes measured. The change is visual only. Rollback removes one token line per theme and one config line.

## Migration Plan

There is no schema or data change. The rollout order does not matter: the backend can deploy before or after phones move to 2.3.4, because 2.3.3 already shows the entry. Rollback is a revert of the commit. Phones then stop receiving the uncategorised entry, and the web loses the 1px raise again.

## Open decisions for owner

- **`shadow-2xs` renders nothing.** It has 33 class uses, in buttons, chips, icon tiles, the phone list containers and the search field. The archived design counted one, which was a miscount. These elements stay flat. If wanted, define `2xs` as an even lighter hairline, for example `0 1px 0 0 rgba(8, 10, 14, 0.04)`. Tailwind 4's `2xs` is the same shape. Alternatively, rename the classes to `shadow-xs`. Either way is a visual change that was not part of the approved three items.
- **Unused chart aliases.** `chartColors.cyan`, `incomeLight`, `expenseLight`, `grid` and `text` are unused but predate this change, so they were left alone.
