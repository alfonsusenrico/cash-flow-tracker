# Design

## Context

`complete-kakeibo-defaults` (archived 2026-11-02) closed five of the monthly report's open decisions and left three more, plus a contrast follow-up (its design.md, "Open decisions for owner" and "Follow-ups"). The owner approved the recommended defaults on 2026-11-02 ("defaults ok"). This change applies them on `fix/classification-and-green-contrast`, branched from origin/main 3b3d7ef.

The earlier `fix-light-theme-text-contrast` change (archived 2026-10-31) fixed lime and coral text with text tokens of the same hue, a Tailwind `textColor` mapping that leaves fills alone, and `tokens.test.ts` pairs. D4 and D5 apply the same approach to emerald.

## Decisions

### D1. A confident model pillar on a typed capture is kept (confirmed, no code change)

`classification_for` in `services/capture_resolution.py` accepts the model's category and pillar only when `category_confidence` meets `capture_confidence_threshold`. `validate_capture_normalization` then keeps the model's pillar, else the category's, else none. That is an explicit model classification under the same gate as notification processing, recorded with `classified_by = 'ai'`, so it is not a silent default.

The open decision is closed as decided:
- The `kakeibo-classification-completeness` requirement "Forms keep a row unclassified until the owner chooses" now states it, with the scenario "Typed capture with a confident model pillar".
- `test_typed_capture_keeps_a_confident_model_pillar_for_a_category_without_one` covers it with a mocked provider.

### D2. `is_primary` never derives a pillar

`PATCH /api/categories/{id}` no longer writes `need`/`want` when `is_primary` is sent without `kakeibo_type`. An update without a pillar leaves the stored pillar unchanged. `is_primary` is still stored and returned for compatibility.

`pillar_may_change` now depends only on `kakeibo_type` and `kind`. An `is_primary`-only edit therefore takes no owner lock, snapshots no rows and writes no feedback. Its `corrections` is 0.

Clients checked, so nothing relies on the old behaviour:
- Web (`frontend/src`): the category form in `app/insights/page.tsx` sends `name`, `icon`, `color`, `kind`, `monthly_budget` and `kakeibo_type`. The archive restore sends `is_archived`, and `BudgetPanel` sends `monthly_budget`. None sends `is_primary`. `is_primary` appears only as an optional read field (`CategoryDonutChart`) and in a test fixture.
- Phone (`mobile/`, read only): `IngestionApiService` only reads `GET api/categories`. No Kotlin source mentions `is_primary`.
- Backend tests: none relied on the derivation. The `is_primary` tests create categories or mock reads.

Existing categories whose pillar was once derived this way keep it, because there is no migration.

### D3. Movement fee without a pillar stays unclassified

`link_pair` stores the fee row with the fee category's pillar as before, but without the `or "need"` fallback. A category the pairing creates (`ensure_fee_category`) still starts as `need`, as the spec's "Fee category on first use" requires. Its `ON CONFLICT` branch does not touch the pillar.

The case arises when the owner already has a "Biaya Admin & Transfer" expense category without a pillar. Since `complete-kakeibo-defaults`, `POST /api/categories` without a pillar creates exactly that. The fee row then has no pillar. It follows the category, so the reports count it as "Belum diklasifikasi", and "Lengkapi klasifikasi" lists it. When the owner later gives the category a pillar, the fee follows.

The owner's wording was the "Biaya Transaksi" category. The category the code uses is `FEE_CATEGORY_NAME = "Biaya Admin & Transfer"`, and no "Biaya Transaksi" category exists in code or specs.

### D4. Emerald text ink

`--color-income` (`#059669` light) measures 3.77:1 on white, 3.17:1 on the paper and 2.80:1 on the raised surface. The new text token keeps the hue (162° against 161°):

| Token | Light | Dark |
| --- | --- | --- |
| `--income-text` | `#056548` | `var(--color-income)` `#10B981` |

Measured `#056548` in light: 7.09:1 on `--surface`, 5.97:1 on `--bg`, 5.27:1 on `--surface-raised` and 6.73:1 on `--color-income-soft`. It also measures 6.31:1 on a 10% emerald tint over white and 4.53:1 on a 15% tint over the raised surface. In dark, `#10B981` measures 7.44:1, 7.99:1 and 6.58:1.

`tailwind.config.js` maps `text-emerald-400` to `text-emerald-700` and `text-income` to `--income-text` in `theme.extend.textColor`. `bg-`, `border-`, `ring-` and `shadow-` utilities still read `colors`, so every emerald fill, tint and chart is unchanged.

In dark, `text-emerald-600/700` without a `dark:` override move from `#34D399` to `#10B981`, which still passes. `text-emerald-300`, `-200` and `-800` are standard Tailwind values and stay.

Rejected:
- `#047857` (the existing hover token): 4.08:1 on the raised surface.
- Replacing about 100 classes by hand: a later `text-emerald-500` would regress, as D4 of the light-theme change argued for coral.

### D5. Text on emerald fills

White on the emerald fill measures 3.77:1 in light and 2.54:1 in dark. `--income-contrast: #0A0D14` is a dark ink in both themes, the same choice as `--primary-contrast` on coral. It measures 5.16:1 on `#059669` and 7.66:1 on `#10B981`. Tailwind exposes it as `text-income-contrast` through `colors.income.contrast`.

It is applied to every white-on-emerald text on the pages the owner named:
- Analisis: "Tambah Kategori" and "Tambah Kategori Pemasukan" (the two axe found). Their hover, `bg-emerald-400`, resolves to the same fill.
- Transaksi:
  - "Catat Transaksi" (1280 px). Its hover moves from `bg-income-hover` to `bg-income/90`: dark ink on `#047857` would be 3.54:1, and on the 90% tint it is 5.78:1 or better in light and 6.30:1 or better in dark. Its "N" key hint moves from `bg-black/20 text-white/90` (3.42:1 in dark) to `bg-white/30` with the same ink (7.80:1 or better).
  - The active type chip.
  - "Terapkan" in the filter drawer.

Rejected:
- White text on a darker green fill. It changes the fill, and a fill dark enough for white in dark mode (`#047857` or deeper) no longer reads as the dark theme's emerald.
- A lighter ink: none passes on both fills.

### D6. Tests

Backend, in `tests/test_kakeibo_defaults.py`:
- `test_category_edit_with_is_primary_keeps_the_stored_pillar[True/False]`;
- `test_movement_fee_in_a_fee_category_without_a_pillar_stays_unclassified`;
- `test_typed_capture_keeps_a_confident_model_pillar_for_a_category_without_one` (D1; it passes before and after, because the behaviour is unchanged).

Web, in `tokens.test.ts`:
- `--income-text` on `--surface`, `--bg`, `--surface-raised` and `--color-income-soft`, and `--income-contrast` on `--color-income`, both at 4.5:1 or better in both themes;
- the Tailwind mapping test: emerald text reads the ink, and the emerald fills keep `--color-income`;
- the signature fills pinned: `#059669` light and `#10B981` dark;
- a source check that no emerald-filled class string on Analisis or Transaksi pairs `text-white` with the fill.

## Risks

- `text-emerald-500` and `bg-emerald-500` now paint different colours. The Tailwind comment and the mapping test state this.
- Light emerald text is visibly deeper than the emerald fills. It keeps the same hue, and the plus sign and labels also carry the income meaning.
- API clients that sent `is_primary` to set a pillar now keep the old pillar. No known client does (D2).

## Migration

None. Backend and web ship with the normal release. Rollback is a revert. Data written in the meantime keeps its stored pillars: fees recorded without a pillar stay unclassified until the owner classifies them.

## Open decisions for owner

1. **Clearing a pillar through `PATCH`.** `kakeibo_type: null` is treated as "not sent", so the category form's "Belum diklasifikasi" choice cannot clear an existing pillar. This change keeps that, as the conservative reading of "no pillar sent leaves it unchanged". Should an explicit `null` clear the pillar, with feedback written as for any change?

## Follow-ups

Contrast findings outside this change's scope:
- `/` at 1280 px in light: the amber "Pindah Saldo" button (`text-amber-600`, 3.18:1 on white). It was found by axe before and after this change.
- Target & Tagihan: white text on `bg-emerald-600`, which is `#34D399` in dark (1.92:1), in two buttons. A third already uses a dark ink in dark. That page was not part of this check.
- `HeroWalletCard`: the white arrow icon on an emerald tile (2.54:1 in dark). It is decorative, inside the labelled "Pemasukan" button.
- `/` at 1280 px: axe `svg-img-alt` on the recharts donut sectors (5 nodes in both themes, before and after).
