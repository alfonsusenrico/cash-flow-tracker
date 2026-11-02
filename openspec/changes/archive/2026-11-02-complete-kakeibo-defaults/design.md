# Design

## Context

`add-monthly-report` (archived 2026-11-02) introduced "Belum diklasifikasi" and listed five open decisions plus a contrast follow-up. The owner approved the recommended defaults on 2026-11-02 ("defaults ok"). This change applies them on the same branch, after main was merged in (its migration became `V41__monthly_reflections.sql`).

## Decisions

### D1. Essential cost keeps unclassified spending, labelled (confirmed, no change)

`emergency-cost-estimation` already defines per-period essential spending as `need` plus unclassified, reports `unclassified_amount` per period, and keeps the `need`-budget fallback for categories without a pillar. `test_essential_cost_still_counts_unclassified_spending_and_reports_it` covers it. The open decision is closed as decided; no code or spec changes.

Rejected: excluding unclassified spending. A missing pillar would shrink the cost and raise Ketahanan Dana.

### D2. Category pillar edits write feedback through the Lengkapi klasifikasi path

`PATCH /api/categories/{id}` now does what `POST /api/classification/categories/{id}` does whenever a pillar can change, that is when `kakeibo_type`, `is_primary` or `kind` is sent:

1. It takes the owner advisory lock (`notification:<user>`) before locking the category row, in the same order as the quick fix, so the two cannot deadlock.
2. If the final kind is `expense` and the category is reportable, it snapshots the category's followers before the update. Followers are the expenses with no own pillar and no movement, selected by the shared `category_followers` helper.
3. After the update, it calls `record_owner_corrections(..., close_questions=False)`. Only rows whose effective classification changed get one `correction` row and an owner memory upsert. A name, icon or budget edit therefore writes nothing.

The response gains `corrections`, an additive field. The web ignores it.

Non-reportable categories ("Investasi", "Internal Movement", categories excluded from budget) write no feedback. The quick fix refuses them, so "exactly as Lengkapi klasifikasi" means the same set of rows.

Rejected alternatives:
- Routing the web form to `POST /api/classification/categories/{id}`: other API clients that use the PATCH would still teach nothing.
- A new feedback kind: the habit learner reads `correction`.

### D3. No silent `need`

- `POST /api/categories`: the pillar is the one sent, or `null`. `is_primary` no longer derives `need`/`want` on creation.
- Typed capture: `default_pillar` returns the category's own pillar or `null`. `validate_capture_normalization` keeps the model's proposed pillar, then the category's, and otherwise `null`. Category options for a category without a pillar carry `kakeibo: null`. An answer with an explicit pillar is stored as given.
- Kept on purpose: a pillar the model proposes for a category without one. That is an explicit classification under the existing confidence gate, the same as notification processing, and it is not a silent default. It is recorded as open decision 1.

Android compatibility, checked in `mobile/` on main:
- `CaptureOption.kakeibo`, `CaptureTransaction.kakeibo` and `TopCategory.kakeiboType` are nullable `String?`.
- `kakeiboLabel(null)` and `pillarLabel(null)` return `null`, and the callers use `listOfNotNull` or `?.let`, so the pillar line is simply omitted.
- `CategoryItemResponse` does not read the pillar.
- `TrackerOptionalFieldsTest` and `LedgerSummaryRefreshTest` already parse `"kakeibo_type": null`.

No mobile change is needed, and none was made.

### D4. Starter pillars for new users

`DEFAULT_CATEGORIES` in `backend/app/db/init_db.py`. `seed_user_defaults` runs only at registration and only for a user without categories.

| Starter category | Kind | Pillar | Reason |
| --- | --- | --- | --- |
| Makanan & Minuman | expense | need | regular daily meals |
| Jajan & Nongkrong | expense | want | unchanged; snacks and cafés |
| Belanja Harian | expense | need | groceries and daily essentials |
| Transportasi | expense | need | routine transport |
| Tagihan & Utilitas | expense | need | bills |
| Belanja | expense | want | non-essential shopping |
| Hiburan | expense | want | entertainment |
| Kesehatan | expense | need | medicine and routine care; a sudden cost can be marked `unexpected` per row |
| Investasi | expense | none | never counted as spending |
| Internal Movement | expense, income | none | movement legs |
| Gaji, Pendapatan Lain, Patungan & Ganti Uang | income | none | pillars apply to expenses only |

The mapping follows the pillar definitions already used by the classification prompts (`capture_normalization.md`, `notification_interpretation.md`).

There is no migration. Existing users, the owner included, keep their categories and use "Lengkapi klasifikasi". Rollback is a code revert; users registered in the meantime keep their seeded pillars, which they can edit.

### D5. Bottom navigation contrast

`.glass-dock` (unlayered, after `@tailwind utilities`) overrode the nav's `bg-[var(--surface)]/95`. The dock was therefore dark in both themes, while inactive labels used the page's `--muted`:
- light theme: `#4E5D70`, about 2.2 to 2.4:1;
- hover: `--text`, dark ink on dark in the light theme.

New tokens in `tokens.css` (`:root`, inherited by `.dark`):
- `--dock-surface: rgba(23, 26, 33, 0.94)`, the existing fill;
- `--dock-muted: #8B9AAF`, the dark theme's existing muted ink, so the dock looks the same in both themes;
- `--dock-text: #F8FAFC`.

`.glass-dock` uses `--dock-surface`. `BottomNav` uses `--dock-muted` with `hover:--dock-text` and drops the dead `bg-[var(--surface)]/95` and `border-[var(--border)]/60` utilities. `tokens.test.ts` checks both inks against the fill composited over each theme's surface, with a minimum of 4.5:1. It also checks that `BottomNav` uses no page inks.

Measured in headless Brave at 390 px: 5.17:1 over white and 5.25:1 or 6.16:1 over the page in light or dark. The active pill is unchanged at 8.83:1.

Rejected: a light dock in the light theme. It changes the approved floating dark capsule look and the lime pill pairing.

## Risks

- API clients that relied on `POST /api/categories` deriving a pillar now get an unclassified category. The web always sends the chosen pillar or `null`, and the phone never creates categories.
- More feedback rows: one per following row per pillar change, the same volume as the quick fix.

## Open decisions for owner

1. **Model-proposed pillar for a category without one (typed capture).** It is kept as a model classification (D3). Should a capture in such a category always stay unclassified, even when the model is confident?
2. **`is_primary` on `PATCH /api/categories/{id}`.** It still derives `need`/`want` when `is_primary` is sent without a pillar. No web or phone client sends it. Remove it as well?
3. **Movement fee fallback.** `movement_pairing` gives a fee row `need` when the owner cleared the pillar of the "Biaya Transaksi" category. Should that row stay unclassified instead?

## Follow-ups

- Contrast findings outside the dock, seen by axe at 390 px in the light theme: `text-emerald-500` income amounts on Beranda (3 nodes) and the ledger (7), and the `bg-emerald-500` button on Analisis (2, both themes). These predate this change.
- The phone reminder for the monthly report (from `add-monthly-report`).
