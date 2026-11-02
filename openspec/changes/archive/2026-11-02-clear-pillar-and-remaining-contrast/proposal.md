# Proposal

## Why

`finish-classification-and-green-contrast` (archived 2026-11-02) left one open decision and four contrast follow-ups:

- `PATCH /api/categories/{id}` treats `kakeibo_type: null` as "not sent". The web form's "Belum diklasifikasi" choice therefore cannot clear a pillar, and the form only offers that choice when the category has no pillar yet.
- axe and the probe found:
  - the amber "Pindah Saldo" text button on `/` at 1280 px in light (3.18:1);
  - white text on `bg-emerald-600` in two Target & Tagihan buttons in dark (1.92:1);
  - the white icon on the emerald tile in `HeroWalletCard` in dark (2.54:1);
  - `svg-img-alt` on the donut chart on `/` (5 sectors without a name).

## Approval

Recorded 2026-11-02:

1. **Clearing a pillar.** On the open decision about an explicit null pillar, the owner answered "clear the group". An explicit `kakeibo_type: null` clears the category's stored pillar, and leaving the field out keeps it. Clearing writes habit-learning feedback like any pillar change.
2. **Remaining contrast.** The owner's earlier "do the colour tweaks too" covers the remaining contrast fixes above.

Both follow the Phase 0 approval of the same day ("ok I approve all you recommend"): the product is public and multi-user, the AI learns each user, and amounts are never guessed.

## Expected Outcome

- In Analisis, the owner opens a category that has a pillar, chooses "Belum diklasifikasi" and saves. The category then has no pillar, its expenses without their own pillar count as "Belum diklasifikasi", and habit learning receives one `correction` row per expense whose effective pillar changed.
- Saving a category without the `kakeibo_type` field keeps its pillar, as before.
- Editing an income category in the web form saves. It failed before with HTTP 500 (design.md D2).
- The amber "Pindah Saldo" text button, and text on the strong emerald buttons of Target & Tagihan, measure at least 4.5:1 in both themes at 390 and 1280 px.
- The `HeroWalletCard` icon tiles are marked decorative, and the emerald tile's icon measures at least 3:1.
- The donut on `/` has an accessible name and a text summary, and axe reports no `svg-img-alt`.
- No colour changes meaning, and every fill stays as it is.

## What Changes

- **BREAKING (meaning, not shape)**: `PATCH /api/categories/{id}` with an explicit `kakeibo_type: null` clears the stored pillar. It was ignored before. The web category form is the only client that sends it, on purpose (design.md D1).
- `PATCH /api/categories/{id}` builds its `SET` clause from one entry per column. An income-kind save that also sends `monthly_budget` or `kakeibo_type` no longer assigns a column twice.
- Web: the category form always offers "Belum diklasifikasi" for an expense category.
- Web tokens:
  - `--warning-text` (`#8F4F04` light, `--color-warning` dark): `text-amber-400` to `text-amber-700` read it;
  - `--income-hover-contrast` (`#FFFFFF` light, `#0A0D14` dark): text on the strong emerald fill.
- Target & Tagihan: the three `bg-emerald-600` buttons use `text-income-hover-contrast`.
- `HeroWalletCard`: the icon tiles are marked decorative (`aria-hidden`), and the emerald tile's icon uses `text-income-contrast`.
- `CategoryDonutChart`: a named figure with a text summary, and a label on every sector.

## Non-goals

- Existing data: there is no migration. No stored pillar changes until an owner clears one.
- Transaction pillars. `PATCH /api/transactions/{id}` is unchanged.
- Other colour families, and amber fills. The `HeroWalletCard` amber and coral tiles keep white icons, now marked decorative (design.md, Follow-ups).
- The spec's "sky blue for internal transfers" against the amber "Pindah Saldo" in code. It is recorded for reconciliation, not changed (design.md, Open decisions).
- Android code. `mobile/` only reads `GET api/categories`.
- Production access or release. All verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `kakeibo-classification-completeness`: an explicit null clears a category pillar (added), and clearing teaches habit learning (modified).
- `tactile-pulse-ui`: amber text, text on the strong emerald fill, and decorative icon tiles.
- `finance-dashboard-cockpit`: the category donut has an accessible name and summary.

Standards:
- money: no amount, balance or formula changes. Clearing a pillar moves spending between Kakeibo buckets only.
- multi-user: the category edit stays owner-scoped (404 for another owner, no feedback written).
- api-consumers: response shapes are unchanged, and `kakeibo_type: null` was already a valid value.
- llm: feedback rows reach habit learning through the existing path, with no model call.
- database: no migration.

No new project capability is added.

## Impact

- Backend: `routers/categories.py`; five tests in `tests/test_kakeibo_defaults.py`.
- Web: `tokens.css`, `tailwind.config.js`, `app/insights/page.tsx`, `app/goals/page.tsx`, `components/dashboard/HeroWalletCard.tsx`, `components/dashboard/CategoryDonutChart.tsx`.
- Web tests: `app/tokens.test.ts`, `app/insights/page.test.tsx`, and the new `components/dashboard/CategoryDonutChart.test.tsx`.
- Specs: the three capabilities above.
- Android: none.
