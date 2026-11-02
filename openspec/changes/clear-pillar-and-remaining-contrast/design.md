# Design

## Context

`finish-classification-and-green-contrast` (archived 2026-11-02) left one open decision ("Clearing a pillar through `PATCH`") and four contrast follow-ups (its design.md, "Open decisions for owner" and "Follow-ups"). The owner answered the decision with "clear the group" on 2026-11-02. The earlier "do the colour tweaks too" covers the contrast follow-ups. This change applies both on `fix/clear-pillar-and-contrast`, branched from origin/main e05cde9.

The contrast fixes follow the token approach of `fix-light-theme-text-contrast` and `finish-classification-and-green-contrast`:
- text inks of the same hue, mapped in Tailwind's `textColor` so fills stay as they are;
- contrast inks for text on fills;
- pairs in `tokens.test.ts`.

## Decisions

### D1. An explicit null clears a category pillar

`update_category` tells the two cases apart with Pydantic's `model_fields_set`, as it already does for `monthly_budget`:
- `"kakeibo_type" in payload.model_fields_set` with `None` writes `kakeibo_type = NULL`;
- a request without the field leaves the column alone.

`pillar_may_change` now depends on whether the field was sent, not on its value. A clear therefore takes the same path as any pillar change:
1. the owner lock;
2. a snapshot of the category's following expenses;
3. the update;
4. `record_owner_corrections(..., close_questions=False)`.

Each expense without its own pillar whose effective pillar changed gets one `correction` row (`after_kakeibo_type` null), and the response reports the count as `corrections`. Rows with their own pillar keep it. Purpose questions stay open, and review state is unchanged. A clear of a category that has no pillar changes nothing and reports 0.

For an income category, a null pillar is accepted (it is already null) and a non-null pillar still answers 422.

Clients checked, so nothing sends null by accident:
- **Web category form** (`app/insights/page.tsx`): it sends `kakeibo_type: null` in two cases only:
  - the owner chose "Belum diklasifikasi";
  - the category is income.

  The form opens with the category's stored pillar. Expense rows come from `category_variance` (`/dashboard/analytics`), whose `kakeibo_type` is `c.kakeibo_type` from `category_breakdown`. Income rows come from `GET /api/categories`. An untouched form therefore re-sends the stored pillar. The pillar values are validated by `PILLAR_PATTERN` on write, so `pillarOrUnclassified` cannot turn a stored pillar into "".
- **Other web writers**: the archive restore sends only `is_archived`, and `BudgetPanel` sends only `monthly_budget`. `ClassificationDialog` uses `/classification/...`, not this endpoint. `QuickCaptureModal`, the ledger edit and `ReviewGroupCard` send pillars to transaction endpoints, which this change does not touch.
- **Phone** (`mobile/`, read only): `IngestionApiService` only calls `GET api/categories`. No Kotlin source writes a category.
- **Backend tests**: none relied on null being ignored. The full suite passes.

Rejected:
- A separate "clear" field or endpoint. The owner chose the explicit null, and it needs no new contract.
- Treating `""` as clear. The pattern already rejects it, and null is the API's existing "no pillar" value.

### D2. One assignment per column

The update built its `SET` clause from a list. For a request with `kind: income`, the income reset appended `monthly_budget = NULL, kakeibo_type = NULL` after any sent `monthly_budget = %s`. The web form sends `monthly_budget: null` for every income category, so editing an income category from Analisis failed:
- error: `multiple assignments to same column "monthly_budget"`;
- status: HTTP 500;
- found by `test_income_category_form_with_null_pillar_and_budget_saves` on the previous code.

D1 would have added the same failure for `kakeibo_type`. The update now collects one value per column in a dict. The income reset overwrites the entries, and the `SET` clause is built from the dict's keys. These keys are code literals, never request input.

### D3. The form always offers "Belum diklasifikasi"

The select used to render "Belum diklasifikasi" only while the value was already "". It is now always the first option for an expense category. Choosing it sets "", and the existing payload rule sends `null`. The help text for "" already explains the result. New categories still default to `need`, as before.

Rejected: a separate "Hapus pilar" button. It adds a control for what the select already expresses.

### D4. Amber text ink

`text-amber-600` (`#D97706`) measures 3.19:1 on white. Like emerald and coral, amber text gets a text ink of the same hue:

| Token | Light | Dark |
| --- | --- | --- |
| `--warning-text` | `#8F4F04` (HSL 32°, the hue of `#D97706`) | `var(--color-warning)` `#FBBF24` |

`#8F4F04` in light measures:
- 6.39:1 on `--surface`;
- 5.38:1 on `--bg`;
- 4.75:1 on `--surface-raised`;
- 5.74:1 on `--color-warning-soft`;
- 5.92:1 on a 10% amber tint over white, and 4.50:1 on a 10% tint over the raised surface.

`#FBBF24` in dark measures 11.31:1, 12.14:1 and 10.00:1 on the three surfaces.

`tailwind.config.js` maps `text-amber-400` to `text-amber-700` to `--warning-text` in `theme.extend.textColor`, so every amber fill (`bg-amber-500`, tints, borders) is unchanged. In dark, `--warning-text` equals Tailwind's `amber-400`, so the existing `dark:text-amber-400` overrides render exactly as before. Amber text without a dark override moves to `#FBBF24`, which passes.

The mapping also lifts amber text the axe run did not reach. That is about 18 uses in 10 files, for example the "Waspada" badges on Analisis, the burn-rate badge and the reconciliation flag. That was the reason for mapping emerald globally (D4 of the previous change): a later `text-amber-500` cannot regress.

Rejected:
- `#B45309` (`amber-700`): 3.73:1 on the raised surface and 4.23:1 on `--bg`.
- `#92400E` (`amber-800`): it passes, but its hue is 23°, which reads as brown rather than amber.
- Fixing only the "Pindah Saldo" class.

### D5. Text on the strong emerald fill

`bg-emerald-600` and `bg-emerald-700` paint `--color-income-hover`: `#047857` in light and `#34D399` in dark. White on it measures 5.48:1 in light and 1.92:1 in dark. `--income-contrast` (`#0A0D14`) fails in light at 3.54:1. The new contrast ink is therefore theme-dependent:

| Token | Light | Dark |
| --- | --- | --- |
| `--income-hover-contrast` | `#FFFFFF` (5.48:1) | `#0A0D14` (10.11:1) |

Tailwind exposes it as `text-income-hover-contrast`. It is applied to the three `bg-emerald-600` buttons on Target & Tagihan:
- "Target Baru" and "Buat Target Pertama Anda", the two axe found;
- "Simpan Progres". It already used a dark ink in dark through a hard-coded `dark:text-[#062A1E]`, now replaced by the token.

Their hover, `bg-emerald-700`, resolves to the same fill.

Rejected:
- Moving these buttons to `bg-income` with `text-income-contrast`. That changes the fill.
- A `dark:text-[#...]` per button. It is a hard-coded colour outside the tokens.

### D6. Decorative icon tiles in `HeroWalletCard`

The three quick-action tiles ("Pengeluaran", "Pemasukan", "Pindah Saldo") repeat the label of the button they sit in. They are now `aria-hidden="true"`, and each button is named by its text alone.

The emerald tile's icon also moves from white to `text-income-contrast`: 5.16:1 on `#059669` in light and 7.66:1 on `#10B981` in dark, against 2.54:1 for white in dark. It reads as an income cue even though it is decorative.

The coral tile's white icon measures 3.60:1 in light and 3.34:1 in dark. The amber tile's measures 2.15:1 in both themes. Both are decorative and stay as they are (Follow-ups).

### D7. Donut name and summary

Recharts 2.14 paints each pie sector as `<path role="img">` without a name, which axe reports as `svg-img-alt`. `CategoryDonutChart`:
- gives every `Cell` an `aria-label` ("Makanan & Minuman: Rp 42.000 (6.6%)"), which Recharts passes to the sector path;
- wraps the chart in `role="figure"` named "Diagram donat <title>". On Beranda that is "Diagram donat Pengeluaran per Kategori".
- links the figure with `aria-describedby` to a visually hidden summary: the total, the number of categories and each category with its amount and share.

Amounts go through `bal()`, so hidden balances stay masked in the summary.

Rejected:
- `aria-hidden` on the SVG alone. The sectors would keep an unnamed `role="img"` in the DOM, and the chart would have no text equivalent.
- Recharts' `accessibilityLayer`. It adds keyboard and `application` semantics that this display chart does not need.

### D8. Tests

Backend, in `tests/test_kakeibo_defaults.py`, all on the previous code first:
- `test_category_edit_with_an_explicit_null_pillar_clears_it_and_writes_feedback` (clear, feedback, own pillar kept, repeat clear writes nothing);
- `test_category_form_choosing_unclassified_clears_the_pillar` (the web form's payload shape);
- `test_category_edit_without_the_pillar_field_keeps_the_stored_pillar` (keep);
- `test_clearing_another_owners_pillar_is_not_found_and_changes_nothing` (owner isolation);
- `test_income_category_form_with_null_pillar_and_budget_saves` (D2).

Web:
- `tokens.test.ts`:
  - `--warning-text` and `--income-hover-contrast` pairs in both themes;
  - the amber mapping with the amber fill kept;
  - the pinned `--color-income-hover` and `--color-warning` fills;
  - source checks for the Target & Tagihan buttons, the `HeroWalletCard` tiles and the donut labels.
- `app/insights/page.test.tsx`: choosing "Belum diklasifikasi" sends `kakeibo_type: null`.
- `components/dashboard/CategoryDonutChart.test.tsx`: the figure's name and description, and axe in jsdom.

## Risks

- An API client that sends `kakeibo_type: null` meaning "unchanged" now clears the pillar. No known client does (D1).
- `text-amber-500` and `bg-amber-500` now paint different colours in light. The Tailwind comment and the mapping test state this.
- Light amber text is visibly deeper than before. It keeps the 32° hue, and every amber fill is unchanged.

## Migration

None. Backend and web ship with the normal release. Rollback is a revert. Pillars that owners clear in the meantime stay cleared, and their feedback rows stay as learned history.

## Open decisions for owner

1. **Transfer colour in the spec.** `tactile-pulse-ui` lists "sky blue for internal transfers ("Pindah Saldo")", but the web paints the "Pindah Saldo" actions on Beranda amber: the desktop text button and the `HeroWalletCard` capsule. The phone-width home view already uses sky blue for its "Pindah Saldo" action. This change keeps each colour as it is, because the brief says no colour changes meaning, and only fixes the amber contrast. Should transfers use one colour, and if so, amber or sky blue, in a later change?

## Follow-ups

- `HeroWalletCard`: the amber tile's white icon (2.15:1) and the coral tile's (3.34:1 in dark). Both are decorative and hidden from assistive technology now. A dark ink would lift the amber one if the owner wants the icons to read on their own.
- The "Simpan Progres" dialog on Target & Tagihan was not opened in the browser. The source check and the token pair cover it.
