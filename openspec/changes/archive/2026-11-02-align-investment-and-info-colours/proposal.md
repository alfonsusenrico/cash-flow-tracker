# Proposal

## Why

`tactile-pulse-ui` names lavender for investments and, since `unify-transfer-colour-and-guard-disk`, sky blue for transfers. The web does not follow either rule yet:

- **Investments use four colours.** The `HeroWalletCard` "Investasi" pill is amber, the phone-width home balance is sky, the Beranda "Invest" badge is blue (3.28:1 in the light theme, flagged by axe), the Rekening allocation share is indigo, and the top-up badges and notes are purple.
- **Sky does not only mean transfers.** The "Pilar Kakeibo" card, the Target & Tagihan progress note, the "Ditautkan otomatis" pill and the phone-width "Dipilih" selection label are sky, but none of them is a transfer.

`unify-transfer-colour-and-guard-disk` left both points as open decisions for the owner.

## Approval

Recorded 2026-11-02:

1. The owner answered the two open decisions of `unify-transfer-colour-and-guard-disk` (archived 2026-11-02), "Investment colour" and "Other sky uses", with "lavender and neutral as recommended".
2. This follows the Phase 0 approval of the same day ("ok I approve all you recommend"): the product is public and multi-user, optional features stay hidden until needed, and amounts are never guessed.

## Expected Outcome

- Every investment element on the web is lavender in the light and the dark theme: the Beranda wallet pill, the phone-width investment balance and investment wallet card, the "Invest" badge, the Rekening allocation share, the price-sync message, the instrument controls, and every top-up badge, row, amount and note on Transaksi and in the plan and top-up dialogs.
- Lavender text measures at least 4.5:1 on every surface, on the lavender soft fill and on 10 to 25% lavender tints, and text on the solid lavender fill measures at least 4.5:1, in both themes.
- Sky appears only on transfers. The "Pilar Kakeibo" card, the two information notes and the "Dipilih" label use the existing neutral tokens.
- Transfers stay sky, warnings amber, income emerald and expenses coral.
- axe reports no `color-contrast` violation on a changed element on `/`, `/ledger`, `/accounts` or `/insights` at 390 and 1280 px in either theme.

## What Changes

- Web tokens: `--color-investment` (lavender fill), `--color-investment-soft`, `--investment-text` (`#5B21B6` light, the fill in dark) and `--investment-contrast` (ink on the solid fill). Tailwind maps `text-violet-400` to `text-violet-700` to `--investment-text`, and adds `bg-investment`, `bg-investment-soft`, `text-investment` and `text-investment-contrast`. The violet fills keep their colour.
- Beranda, `HeroWalletCard`, `MobileHomeView`, Transaksi, `MobileLedgerFeed`, Rekening, `RecurringRulesModal`, `InvestmentTopupModal`, `AmountTrackingSwitchModal` and `InvestmentTradeModal`: investment elements move to lavender.
- `MetricMatrixGrid` ("Pilar Kakeibo"), Target & Tagihan (progress note), `PendingScheduledBanner` ("Ditautkan otomatis") and `MobileLedgerFeed` ("Dipilih"): sky moves to neutral tokens.
- `tokens.test.ts`: the new token pairs and tints, the violet mapping, the lavender inventory, the solid-fill ink, and a source check that every sky utility sits in a transfer branch.

## Non-goals

- Investment top-ups in the Beranda and phone-width "Aktivitas Terkini". `/api/dashboard` sends no `investment_topup_id`, so the web cannot tell a top-up from a transfer there. They stay sky (design.md, Open decisions).
- Colours that users choose for accounts and categories, including the API's default account colour `#3b82f6`.
- The Kakeibo pillar palette, where "Budaya" is sky and "Kebutuhan" is violet (design.md, Open decisions).
- Blue that is neither an investment nor a transfer: the "Rutin" icon, the "Akun Induk" badge, the "Bank" share, the funding-source icon, the bank pocket toggle and the "Dulu alokasi gaji" badge.
- Decorative indigo glow orbs and the indigo "Laporan" icon.
- The existing `color-contrast` findings on `/accounts` at 1280 px (muted text at 75% opacity), which this change does not touch.
- Backend, database, Android and release. All verification is local with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `tactile-pulse-ui`: lavender for every investment element with a text ink and a contrast ink, sky for transfers only, and neutral tokens for information that is neither.

Standards:
- money: no amount, balance or formula changes. Only colours change.
- multi-user: no change to owner scoping. No colour depends on the owner.
- personal-data: none touched.

No new project capability is added.

## Impact

- Web only: `tokens.css`, `tailwind.config.js`, `app/page.tsx`, `app/ledger/page.tsx`, `app/accounts/page.tsx`, `app/goals/page.tsx`, `components/dashboard/HeroWalletCard.tsx`, `MobileHomeView.tsx`, `MetricMatrixGrid.tsx`, `components/ledger/MobileLedgerFeed.tsx`, `components/recurring/RecurringRulesModal.tsx`, `PendingScheduledBanner.tsx`, `components/ui/InvestmentTopupModal.tsx`, `AmountTrackingSwitchModal.tsx` and `InvestmentTradeModal.tsx`. Tests: `app/tokens.test.ts`.
- No API, schema, migration, backend, Android or dependency change. Rollback is a revert.
