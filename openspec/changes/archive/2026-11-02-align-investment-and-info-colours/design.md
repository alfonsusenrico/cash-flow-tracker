# Design

## Context

The owner answered the two open decisions of `unify-transfer-colour-and-guard-disk` with "lavender and neutral as recommended" (2026-11-02). Work happens on `fix/investment-lavender-neutral-sky`, branched from origin/main fb40d0a.

The approach follows `fix-light-theme-text-contrast` and `unify-transfer-colour-and-guard-disk`:
- a fill token, plus a text ink of the same hue mapped in Tailwind's `textColor`, so the fills stay as they are;
- a contrast ink for text on the solid fill;
- pairs and tints checked in `tokens.test.ts`.

Live tokens: light `--surface` `#FFFFFF`, `--bg` `#E8ECEF`, `--surface-raised` `#D8DFE5`; dark `#0E1118`, `#050608`, `#181E2A`.

## Decisions

### D1. Lavender tokens

Lavender is Tailwind's violet family (hue 258°). The rest of the web already uses it for the investment controls on Rekening.

| Token | Light | Dark |
| --- | --- | --- |
| `--color-investment` | `#8B5CF6` (violet-500) | `#A78BFA` (violet-400) |
| `--color-investment-soft` | `#F5F3FF` (violet-50) | `rgba(167, 139, 250, 0.16)` |
| `--investment-text` | `#5B21B6` (violet-800) | `var(--color-investment)` |
| `--investment-contrast` | `#0A0D14` | `#0A0D14` |

Measured contrast:
- `#5B21B6` in light: 8.98:1 on `--surface`, 7.56:1 on `--bg`, 6.68:1 on `--surface-raised`, and 8.19:1 on the soft fill. On violet-500 tints of 10 to 25%, it measures at least 5.10:1 (25% over the raised surface).
- `#A78BFA` in dark: 6.94:1, 7.45:1 and 6.13:1 on the three surfaces, and 5.48:1 on the soft fill. On the same tints, it measures at least 4.56:1 (25% over the raised surface).
- `--investment-contrast` measures 4.59:1 on `#8B5CF6` and 7.14:1 on `#A78BFA`.

`tailwind.config.js`:
- maps `text-violet-400` to `text-violet-700` to `--investment-text` in `theme.extend.textColor`, so every violet fill, tint and border keeps Tailwind's violet;
- adds the colour `investment` (`bg-investment`, `bg-investment-soft`, `text-investment-contrast`), with `text-investment` reading the text ink, as `text-transfer` does.

Rejected:
- `#6D28D9` (violet-700) as the light ink: 4.27:1 on a 20% tint over the raised surface.
- White on the solid fill: 4.23:1 on `#8B5CF6` and 2.72:1 on `#A78BFA`.
- `#7C3AED` (violet-600) as the light fill: white passes (5.70:1), but the dark ink fails (3.41:1), and it is the Kakeibo "Kebutuhan" colour.
- Keeping purple for the top-up badges and notes. Purple (hue 272°) passed contrast, but it is a second lavender with its own dark-mode classes. One family keeps the meaning simple.

### D2. Inventory of investment elements

Found by searching for `invest`, `saham`, `stock`, `portfolio`, `reksa`, `topup` and `instrument`, and every amber, sky, blue, indigo, violet and purple utility in `frontend/src`:

| Place | Element | Before | After |
| --- | --- | --- | --- |
| `HeroWalletCard` | "Investasi" pill, dot and amount | amber tint, `bg-amber-500` dot, `text-amber-700` | `bg-violet-500/10`, `border-violet-500/25`, `text-investment`, `bg-investment` dot |
| `MobileHomeView` | "Investasi" balance under the total | `text-sky-500` | `text-investment` |
| `MobileHomeView` | investment wallet card | indigo gradient `#1E1B4B` to `#312E81`, `border-indigo-500/30` | violet gradient `#2E1065` to `#4C1D95`, `border-violet-500/30`. White text measures 10.95:1 or more, and the 70% label 6.10:1. |
| `MobileHomeView`, Beranda | colour of an investment account without its own | `#38bdf8`, `#3b82f6` | `var(--color-investment)` |
| Beranda | "Invest" badge | `bg-blue-500/10 text-blue-500` (3.28:1 light) | `bg-violet-500/10 text-investment` |
| Transaksi | "↔️ Top up Investasi" badge | purple | `bg-violet-500/10 text-investment border-violet-500/20` |
| Transaksi | top-up type badge on a single leg | sky or coral, labelled "Top up Investasi" | `bg-violet-500/10 text-investment` |
| Transaksi | top-up category dot, target product, leg amount | `--color-transfer`, `text-sky-500` | `--color-investment`, `text-investment` |
| Transaksi | "Debit ini untuk membeli reksadana?" note | purple | `border-violet-500/25 bg-investment-soft text-investment` |
| `MobileLedgerFeed` | top-up icon tile, amount, "Top up Investasi" status | purple tile, sky amount and status | lavender tile, `text-investment`, `bg-violet-500/10` |
| Rekening | "Investasi" allocation dot and bars | `bg-indigo-500` | `bg-investment` |
| Rekening | price-sync message | `bg-blue-500/10 text-blue-400` | `bg-violet-500/10 text-investment border-violet-500/20` |
| Rekening | multi-pocket platform note | `text-violet-300` (light 1.9:1) | `text-investment` |
| Rekening | selected "Instrumen Investasi" and instrument-type buttons | `bg-violet-600 text-white` | `bg-investment text-investment-contrast` |
| Rekening | investment icons, ticker symbols, spinner | `text-violet-400` | unchanged classes; the ink comes from D1 |
| `RecurringRulesModal` | "Top up Investasi" badge | purple | `bg-violet-500/10 text-investment` |
| `InvestmentTopupModal`, `AmountTrackingSwitchModal` | product notes | purple | `border-violet-500/25 bg-investment-soft text-investment` |
| `InvestmentTradeModal` | instrument badge | `text-violet-600 dark:text-violet-300` | `text-investment` |

Left alone:
- the decorative amber "card chip" on every phone-width wallet card, the same on all account types;
- colours users choose for accounts and categories (Non-goals);
- the investment row amounts in "Aktivitas Terkini" (Open decisions).

### D3. Sky only for transfers

Every remaining `*-sky-*` utility was classified:

| Place | Element | Kind | Result |
| --- | --- | --- | --- |
| Beranda | movement icon tile, amount, "Pindah Saldo" text button, detail amount | transfer | sky |
| `HeroWalletCard` | "Pindah Saldo" capsule | transfer | sky |
| Transaksi | "↔️ Pindah Saldo" badge, movement badge, "Tergabung", target account, movement amount | transfer | sky |
| `MobileLedgerFeed` | movement icon tile, amount, "Tergabung" or "Transfer" status | transfer | sky |
| `MobileHomeView` | "Transfer" action icon, movement icon tile and amount | transfer | sky |
| `MobileHomeView` | "Investasi" balance | investment | lavender (D2) |
| `MetricMatrixGrid` | "Pilar Kakeibo" card tint and icon tile | information | plain card, tile `bg-[var(--surface-raised)] text-[var(--text-secondary)]` |
| Target & Tagihan | progress dialog note "Ini hanya memperbarui angka progres target…" | information | `border-[var(--border)] bg-[var(--surface-raised)] text-[var(--text-secondary)]` (8.02:1 light, 6.51:1 dark) |
| `PendingScheduledBanner` | "Ditautkan otomatis" pill | information | `border-[var(--border)] bg-[var(--surface-raised)] text-[var(--text)]` (14.44:1 light, 15.96:1 dark) |
| `MobileLedgerFeed` | "Dipilih" selection label | selection state | `text-[var(--text)]`; the row already has the lime selection outline |

The "Pilar Kakeibo" card now matches the neutral cards. Its bar still shows the pillar colours.

### D4. Tests

`app/tokens.test.ts`, 10 new cases, all failing on the previous tokens, config and components:
- `--investment-text` on the three surfaces and the soft fill, and `--investment-contrast` on the fill, in both themes;
- `--investment-text` on 10, 15, 20 and 25% violet-500 tints over the card and the raised surface, in both themes;
- the violet text mapping, with the violet fill and the investment fills pinned;
- the inventory of D2: the wallet pill, the phone-width balance and card, the "Invest" badge, the account fallback colours, every `investment_topup_id` branch on Transaksi and `MobileLedgerFeed`, the Rekening share and price-sync message, and no `purple-` utility anywhere;
- every solid lavender fill with text uses `text-investment-contrast`, never white;
- every `*-sky-*` utility in `frontend/src` sits in a transfer branch (a movement, a "Pindah Saldo" action, a transfer target), named on its own line or in the eight lines before it, and the three information elements use the neutral tokens.

On the previous code, the sky check lists exactly the five non-transfer elements of D3.

## Risks

- `text-violet-500` and `bg-violet-500` now paint different colours in light, as with sky, emerald, amber and coral. The Tailwind comment and the mapping test state it.
- Light lavender text is visibly deeper (violet-800). It keeps the hue, and the violet fills are unchanged.
- The sky source check is textual. A future transfer element written far from its condition would fail the test and need a marker nearby. That is the intended direction: the failure names the file and line.
- The contrast ink on the light lavender fill measures 4.59:1, close to the minimum. The token test pins it.

## Migration

None. The change ships with the normal release. Rollback is a revert.

## Open decisions for owner

1. **Top-ups in "Aktivitas Terkini".** `/api/dashboard` sends `recent_transactions` without `investment_topup_id`, so Beranda and the phone-width home view show a top-up as a sky transfer labelled "Top up Investasi". Making it lavender needs one additive, read-only field in that response. This change stays web-only and leaves them sky. Should a later change add the field?
2. **Default account colour.** The API stores `#3b82f6` (blue) for an account created without a colour, investments included, so their tiles and bars on Beranda are blue. The colour is the user's choice. Should the default for an investment account be the lavender fill?
3. **Kakeibo pillars.** "Budaya" uses a sky colour (`--color-kakeibo-culture`) and "Kebutuhan" a violet one, close to transfer sky and investment lavender. They are chart categories that never share a chart with transfers or investments. Keep them, or give the pillars hues that avoid both?

## Follow-ups

- Blue that is neither an investment nor a transfer (Non-goals in proposal.md). Most of it passes contrast. The "Akun Induk" badge (`text-blue-400` on a blue tint) and the funding-source icon were not measured in the browser.
- `/accounts` at 1280 px: two `color-contrast` findings per theme on muted text at 75% opacity ("0 cicilan aktif" and the runway basis note). They are unchanged by this change.
- The "Rencana" icon on the phone-width home view is still amber, which reads as a warning.
