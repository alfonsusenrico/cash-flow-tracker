# Design

## Context
The worktree starts at mobile origin/main 4e0d3e6 (2.3.3, code 25, Room v9). The tile, its validation (`LedgerCashFlowSummary.shownTopCategories`) and its cache come from the archived `show-habit-label-and-top-categories`.

The coordinator fixed the contract change on 2026-10-31: `top_categories` may hold `{"category_id": null, "name": "Tanpa kategori", "kakeibo_type": null, "outflow": <int>}`; everything else stays (at most three, sorted by outflow descending, the same period and rules as `outflow`, an empty list when nothing was spent, absent on older servers). The backend change is made in parallel. At cash-flow-tracker origin/main 15dbf61, read with `git show`, `report_figures.top_categories` still leaves the uncategorised bucket out, and `category_breakdown` labels that bucket `"kakeibo_type": "need"` internally. The phone is therefore tested against MockWebServer and fixtures only.

On 4e0d3e6 the validation never looked at `category_id`. A null-id entry with the name "Tanpa kategori" and no pillar was already shown, and read by TalkBack as "Tanpa kategori, Rp…". A null-id entry with a blank name hid the tile, a null-id entry with a pillar showed that pillar, and two null-id entries were both shown.

## Goals / Non-Goals
Goals: name the null-id entry's meaning in the contract, give it a name and no pillar, and reject a list with two of them. Non-goals: any change to the tile's layout, the cache format or the other validation rules.

## Decisions
1. **A null id means "without a category".** `TopCategory.isUncategorised` is `categoryId == null`. Rejected: recognising the entry by its name "Tanpa kategori", which a real category could share.
2. **Validation normalises the entry it returns.** `shownTopCategories()` returns the uncategorised entry with its name set to "Tanpa kategori" when the tracker's name is blank or missing, and with no pillar. The tile then shows and speaks what validation returns, unchanged, so TalkBack reads "Tanpa kategori, Rp150.000". The stored JSON keeps the tracker's raw entry, so the cache round trip is exact. Rejected: special-casing the entry in the tile, which would put contract knowledge in two places.
3. **No pillar for spending without a category.** A pillar the tracker sends with a null id is dropped rather than shown, because the contract says null and the tracker's breakdown labels the bucket "need" only internally. An unknown pillar on that entry still breaks the contract, as on every entry. Rejected: rejecting a known pillar on a null-id entry, which would hide the whole tile for a harmless difference.
4. **Two null ids break the contract.** More than one null-id entry returns null and hides the tile; the summary's totals stay. Rejected: merging them on the phone, which would compute a total the tracker did not send.
5. **Every other rule stays.** At most three entries, no null entry, a non-blank name on entries with an id, a non-negative outflow, a known or null pillar, sorted by outflow descending.
6. **Spec delta.** "Top categories come from the tracker or are not shown" is modified with all six scenario names kept, its statement allowing one entry without a category (kept under the strict 500-character limit, so the naming rule sits in the scenario), the scenario "Spending without a category" added, and "Categories that break the contract" extended with two null ids and limited to blank names on entries with an id.

## Data, API and failure behaviour
- No new request. `GET /api/dashboard/cash-flow-summary?period=today` is read as before.
- Gson leaves a JSON `null` or missing `category_id` as a Kotlin null; the summary cache is written by Gson, which omits nulls, so the restored entry has a null id again.
- Older trackers, and trackers that leave the bucket out, look exactly as on 2.3.3.
- Nothing new is logged.

## Migration and rollback
No schema or preference change. Installing 2.3.4 over 2.3.3 keeps every row and setting. A rollback to 2.3.3 (same Room 9, installed as a downgrade) shows a correctly named uncategorised entry as before, but would show a pillar sent with it, hide it when its name is blank, and accept two of them.

## Risks / Trade-offs
- [The backend sends `kakeibo_type: "need"` with the null id, as its internal breakdown does] → the phone drops it (decision 3), so the entry still shows without a pillar.
- [The backend sends the bucket under a name other than "Tanpa kategori"] → the phone shows the tracker's name, as the contract says.

## Open decisions for owner
1. **Pillar sent with a null id.** Dropped, not a contract break (decision 3). The stricter alternative hides the tile.
2. **Missing name on a null id.** Treated like a blank name and shown as "Tanpa kategori". The instruction named only a blank name.
