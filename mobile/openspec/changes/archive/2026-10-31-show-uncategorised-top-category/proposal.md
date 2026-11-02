# Proposal

## Approval
On 2026-10-31 the owner approved the coordinator's follow-ups, "do the three items too", which include showing the tracker's uncategorised spending in "Kategori teratas". It builds on the companion redesign plan the owner approved with defaults on 2026-10-29 (research/notes/2026-10-29-companion-redesign-plan.md) and on the archived `show-habit-label-and-top-categories`; coordinator-reviewed. It ships as 2.3.4, code 26.

## Why
The tracker is extending the `top_categories` of its cash-flow summary so that spending without a category can appear as one entry with `category_id: null` and the name "Tanpa kategori". The phone's contract for the tile does not say what a null id means, does not name an entry whose name is blank, and accepts any number of null-id entries, so a broken list could show several "Tanpa kategori" rows.

## Expected Outcome
- Beranda's "Kategori teratas" shows the uncategorised entry like any other: the tracker's name, or "Tanpa kategori" when the name is blank, its amount and its bar, with no Kakeibo pillar. TalkBack reads it as "Tanpa kategori, Rp…".
- A list with more than one null-id entry hides the tile, as every other contract break does, and the summary's totals stay.
- Every other rule of the tile is unchanged.

## What Changes
- `top_categories` validation accepts one entry with a null `category_id` as the uncategorised spending. Its name may be blank or missing and is then shown as "Tanpa kategori"; any pillar the tracker sends with it is not shown. An unknown pillar still breaks the contract.
- More than one null-id entry breaks the contract and hides the tile.
- The cached summary keeps the null id across a restart through the existing JSON cache.
- README describes the uncategorised entry.
- Version 2.3.4, code 26.

## Non-goals
- Any backend change; the null-id entry is the tracker's parallel additive contract.
- Computing an uncategorised total on the phone, or linking the entry to the phone's categories.
- A Room or preference change.
- Installing on the owner's TECNO, or any production access.

## Capabilities
### New Capabilities
None.
### Modified Capabilities
- `companion-interface`: "Top categories come from the tracker or are not shown" now covers the tracker's single uncategorised entry and treats two of them as a contract break.

## Impact
- Code: `data/network/model/ApiModels.kt` (`LedgerCashFlowSummary.shownTopCategories`, `TopCategory`), `app/build.gradle.kts` version, `README.md`. `ui/home/TopCategoriesTile.kt` is unchanged; it shows what validation returns.
- Tests: `TrackerOptionalFieldsTest`, `TopCategoriesUiTest`, `LedgerSummaryRefreshTest`.
- API: reads one more shape of an existing optional field. No database, dependency or permission change.
