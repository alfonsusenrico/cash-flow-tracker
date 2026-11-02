# Proposal

## Why

`complete-kakeibo-defaults` (archived 2026-11-02) left three open decisions and one contrast follow-up:

- A typed capture keeps a pillar the model proposes for a category without one. This was kept but not confirmed.
- `PATCH /api/categories/{id}` still derives `need` or `want` from `is_primary` when no pillar is sent. This is the last silent `need` in the category API.
- A movement fee falls back to `need` when the owner's "Biaya Admin & Transfer" category has no pillar.
- axe found green contrast failures:
  - emerald income amounts on Beranda and Transaksi in the light theme;
  - the white-on-emerald button on Analisis in both themes.

## Approval

On 2026-11-02 the owner approved the recommended defaults with "defaults ok":

1. Model-proposed pillars in typed captures stay. A confident AI pillar on a Catat entry is kept. No code change: this is documented as decided.
2. Remove the last silent "need". `PATCH /api/categories/{id}` without a pillar leaves the stored pillar unchanged and never derives one from `is_primary`.
3. Movement fees. If the fee category has no pillar, a movement fee stays unclassified (null) instead of falling back to `need`.
4. Green contrast in light and dark. Green text uses a text-specific income token, the green fills stay, and text on the green button reaches at least 4.5:1. The colours keep their meaning.

This follows the Phase 0 approval of the same day ("ok I approve all you recommend"): the product is public and multi-user, the AI learns each user, and amounts are never guessed. Recorded 2026-11-02.

## Expected Outcome

- Saving a category with only `is_primary` changed keeps its pillar, including no pillar, and writes no habit-learning feedback.
- A fee paired into a "Biaya Admin & Transfer" category without a pillar is reported as "Belum diklasifikasi", not as need. A fee category created by the pairing itself still starts as `need`.
- Income amounts, "Uang Masuk" badges and other emerald text measure at least 4.5:1 on their surfaces in both themes, at 390 and 1280 px.
- The emerald buttons on Analisis ("Tambah Kategori", "Tambah Kategori Pemasukan") and Transaksi ("Catat Transaksi", the active type chip, "Terapkan") carry dark ink that measures at least 4.5:1 in both themes.
- The emerald fills, the income meaning and the expense colours are unchanged.

## What Changes

- **BREAKING (meaning, not shape)**: `PATCH /api/categories/{id}` with `is_primary` and no `kakeibo_type` no longer writes `need` or `want`. The stored pillar stays as it is. No web or phone client sends `is_primary` (design.md D2).
- Movement pairing records a fee with the fee category's pillar, or with no pillar when the category has none. The `need` fallback is removed.
- Typed capture is unchanged. The model-proposed pillar is recorded as decided in design.md and the spec.
- Web: new tokens `--income-text` (`#056548` light, the existing `#10B981` dark) and `--income-contrast` (`#0A0D14`). `text-emerald-400` to `text-emerald-700` and `text-income` read `--income-text`. The emerald buttons on Analisis and Transaksi use `text-income-contrast`. Fills keep `--color-income`.

## Non-goals

- Existing data. Categories whose pillar was once derived from `is_primary`, and existing fee rows, keep their stored pillars. There is no migration.
- Clearing a category's pillar through `PATCH` with an explicit `kakeibo_type: null`. It is ignored today, as before (design.md, Open decisions).
- Other colour families and pages: the amber "Pindah Saldo" button on Beranda, white text on `bg-emerald-600` in Target & Tagihan, and the icon tile in `HeroWalletCard` (design.md, Follow-ups).
- Android code. `mobile/` does not send `is_primary` and already accepts a null pillar.
- Production access or release. All verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `clean-core-ledger`: `is_primary` never derives a pillar, on creation or on update.
- `kakeibo-classification-completeness`: a confident model pillar on a typed capture is kept, now stated as decided.
- `movement-pairing`: a fee in a fee category without a pillar stays unclassified.
- `tactile-pulse-ui`: emerald text uses a text ink, and text on emerald fills meets 4.5:1 in both themes.

Standards:
- money: no amount, balance or formula changes;
- multi-user: the category edit stays owner-scoped;
- api-consumers: response shapes are unchanged, and a null pillar is already part of the contract;
- llm: unchanged; the model's confident pillar is kept under the existing gate;
- database: no migration.

No new project capability is added.

## Impact

- Backend: `routers/categories.py`, `services/movement_pairing.py`; four new tests (five cases) in `tests/test_kakeibo_defaults.py`.
- Web: `tokens.css`, `tailwind.config.js`, `app/insights/page.tsx`, `app/ledger/page.tsx`, `app/tokens.test.ts`.
- Specs: the four modified capabilities above.
- Android: none.
