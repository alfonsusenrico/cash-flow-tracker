# Proposal

## Why

`align-investment-and-info-colours` (PR #56, merged) made transfers sky and investments lavender, and left three follow-ups:

- **Home feed top-ups are sky.** `/api/dashboard/overview` does not say whether a `recent_transactions` row belongs to an investment top-up. The Beranda "Aktivitas Terkini" (desktop and phone width) therefore shows a top-up as a sky "Pindah Saldo".
- **New investment accounts are blue.** The API gives an account created without a colour `#3b82f6`, and the web form starts every new account on blue `#2563EB`, investments included.
- **Kakeibo pillars reuse meanings.** "Budaya" is sky (transfer), "Kebutuhan" violet (investment), "Keinginan" coral (expense) and "Tabungan" emerald (income).

## Approval

Recorded 2026-11-02:

1. After PR #56 merged, the owner approved these three follow-ups: "merged, all three follow-ups as recommended".
2. This follows the Phase 0 approval of the same day ("ok I approve all you recommend"): the product is public and multi-user, optional features stay hidden until needed, and amounts are never guessed.

## Expected Outcome

- In "Aktivitas Terkini" on Beranda, desktop and phone width, an investment top-up row is lavender and is named "Top up Investasi", and so is its detail dialog. Transfers stay sky, income emerald and expenses coral.
- `/api/dashboard/overview` gives every `recent_transactions` row an `investment_topup_id`: the top-up's id on both legs of a top-up, `null` on every other row. The response takes the same number of database statements however many top-ups the feed holds.
- A new investment account, including an investment pocket and a stock position a broker notification opens, is lavender `#8B5CF6` unless the owner picks a colour. Other new accounts keep their current defaults. Stored colours do not change.
- Each Kakeibo pillar has its own hue, at least 25 degrees from the transfer sky, the investment lavender, the income emerald and the expense coral, and at least 40 degrees from the other pillars, in both themes. "Belum diklasifikasi" stays neutral. Every pillar colour measures at least 4.5:1 as text and 3:1 as a mark on its surfaces in both themes. Labels still name every pillar.

## What Changes

- Backend: the `recent_transactions` query of `GET /api/dashboard/overview` joins `investment_topups` and returns `investment_topup_id`, the field and meaning the ledger already uses. New accounts take their default colour from `app/services/account_colours.py`: `#8B5CF6` for `investment`, `#3b82f6` otherwise. The broker position insert sets the investment colour.
- Web: Beranda and `MobileHomeView` paint top-up rows and the detail dialog with the lavender tokens and name them. The Rekening form starts a new investment account on a new "Lavender" swatch and follows the account type until the owner picks a colour.
- Web tokens: new values for the five `--color-kakeibo-*` tokens and their soft fills in both themes. The unused `chartColors.cyan` alias of the "Kebutuhan" token is removed.
- Tests: backend tests for the field, the statement count and the defaults; web tests for the home rows, the dialog, the form and the tokens.

## Non-goals

- Rewriting colours users already have, or a migration. The `accounts.color` column default `#3b82f6` stays: every API path now supplies a colour.
- Category colours and the account tag palette other than the added "Lavender" swatch.
- A sixth pillar or any change to pillar classification, aggregation or the 50/30/20 groups.
- Deleting a movement leg, a top-up or a transfer, from the Beranda detail dialog. That button calls the generic transaction delete, which refuses movement legs, as before (design.md, Open decisions).
- The Android companion, which shows pillar names without colours.
- Production. All verification is local with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `investment-topups`: the home feed marks and paints top-ups as investments.
- `clean-core-ledger`: the default colour of a new account depends on its type.
- `tactile-pulse-ui`: distinct Kakeibo pillar hues with contrast rules.

Standards:
- money: no amount, balance, formula or ledger effect changes. The dashboard join is one-to-one on the top-up id and adds no rows.
- multi-user: the top-up join is scoped to the row owner (`topup.user_id = t.user_id`). No colour depends on the owner.
- api-consumers: `investment_topup_id` is an added, nullable field. `AccountCreate.color` now defaults to `null`, meaning "pick by type"; a client that sends a colour gets it unchanged.
- personal-data: none touched.

No new project capability is added.

## Impact

- Backend: `app/routers/dashboard.py`, `app/routers/accounts.py`, `app/services/notification_application.py`, new `app/services/account_colours.py`. Tests: new `tests/test_home_feed_colours.py`, `tests/test_notification_processing_database.py`.
- Web: `tokens.css`, `app/page.tsx`, `components/dashboard/MobileHomeView.tsx`, `app/accounts/page.tsx`, new `lib/accountColours.ts`, `components/charts/theme.ts`. Tests: `app/tokens.test.ts`, new `app/page.test.tsx`, `components/dashboard/MobileHomeView.test.tsx`, `app/accounts/page.test.tsx`.
- No schema, migration, dependency or Android change. Rollback is a revert.
