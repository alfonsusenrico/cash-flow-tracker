# Proposal

## Why

`add-monthly-report` made spending without a Kakeibo pillar visible as "Belum diklasifikasi", and it left five open decisions (its design.md, "Open decisions for owner"). Three code paths still invented `need` or derived a pillar the user never chose. Pillar edits in Settings and Analisis taught habit learning nothing. Seven of the eight starter expense categories had no pillar, so every new user started with unclassified spending. The phone-web bottom navigation failed WCAG contrast in the light theme.

## Approval

On 2026-11-02 the owner approved the five recommended defaults with "defaults ok":
1. Essential cost keeps unclassified spending, labelled.
2. Category pillar changes teach the AI.
3. No more silent `need`.
4. Starter categories come with sensible default pillars for new users.
5. The phone-web bottom navigation reaches at least 4.5:1 contrast in both themes.

This follows the Phase 0 approval of the same day ("ok I approve all you recommend"): the product is public and multi-user, the AI learns each user, and amounts are never guessed. Recorded 2026-11-02.

## Expected Outcome

- A user who changes a category's pillar in Settings or Analisis teaches habit learning exactly as "Lengkapi klasifikasi" does: one `classification_feedback` correction per following expense whose effective pillar changed.
- A category created through the API without a pillar stays "Belum diklasifikasi". A typed capture recorded in a category without a pillar stays unclassified unless the user or the model chose a pillar.
- A new user's starter categories already carry pillars: daily meals, groceries, transport, bills and health are `need`; snacks, shopping and entertainment are `want`. Existing users' categories are unchanged; they use "Lengkapi klasifikasi".
- The inactive labels of the phone-web bottom navigation measure at least 4.5:1 in both themes.
- The essential monthly cost still counts unclassified spending, labelled as such. This is confirmed as decided, with no change.

## What Changes

- `PATCH /api/categories/{id}`: a change of the effective pillar of a reportable expense category writes feedback through the owner-edit path, under the same owner lock and with the same row selection as `POST /api/classification/categories/{id}`. The response gains an additive `corrections` count.
- **BREAKING (meaning, not shape)**: `POST /api/categories` without `kakeibo_type` stores `null` instead of `need`/`want` derived from `is_primary`.
- **BREAKING (meaning, not shape)**: typed capture (`/api/capture/*`) stores no transaction pillar for a category without one, and category options for such a category carry `kakeibo: null`. A pillar the owner answers, or the model proposes, is kept.
- New users' starter expense categories are seeded with default pillars (design.md D4). No migration: existing users' data is untouched.
- Web: the shared `BottomNav` uses new dock tokens (`--dock-surface`, `--dock-text`, `--dock-muted`) instead of the page's `--muted` and `--text`, which were painted on an always-dark dock.

## Non-goals

- Any change to the essential cost (default 1 is confirmed only).
- Changing existing users' categories or transactions.
- Android code. `mobile/` already accepts a null pillar (design.md D3).
- The `is_primary` derivation on `PATCH /api/categories/{id}` and the movement-fee category fallback. They are recorded as open decisions.
- Other contrast findings outside the dock (design.md, Follow-ups).
- Production access or release. All verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `kakeibo-classification-completeness`: category edits teach habit learning; API category creation and typed capture leave a missing pillar unclassified.
- `clean-core-ledger`: `is_primary` no longer derives a pillar when a category is created.
- `custom-category-management`: starter category pillars for new users.
- `kazz-ui-and-kakeibo`: the floating navigation dock meets text contrast in both themes.

Standards:
- money: no amount or formula changes; the essential cost is unchanged;
- multi-user: the category edit stays owner-scoped and writes feedback only for the owner's rows;
- api-consumers: additive `corrections`; a null pillar was already part of the contract (`kakeibo-classification-completeness`), and the phone handles it;
- llm: the model may still propose a pillar; only the silent fallback is removed;
- database: no migration.

No new project capability is added.

## Impact

- Backend: `routers/categories.py`, `services/classification_gaps.py` (shared row selection), `services/capture_resolution.py`, `services/capture_normalization.py`, `db/init_db.py`; new `tests/test_kakeibo_defaults.py`.
- Web: `components/layout/BottomNav.tsx`, `tokens.css`, `app/globals.css`, `app/tokens.test.ts`.
- Specs: the four modified capabilities above.
- Android: none.
