# Proposal

## Why

Monthly Bibit autodebits buy mutual funds by a fixed rupiah amount, while the existing Beli form requires units and NAB per unit. Generic transfers also reject investment positions, so the owner cannot record the actual RDN debit against its Bibit product without inventing trade information.

## Expected Outcome

The owner can record Rp112.590 from BCA RDN to a specific Bibit product using **Top up Investasi**, without units or unit prices. A monthly rule for each product waits for confirmation of the successful bank debit, then records one contribution and advances its schedule. Contributions increase invested capital, preserve existing gains, and appear as investment transfers rather than daily spending.

## What Changes

- Add amount-only contributions for active mutual-fund leaf accounts currently tracked by total amount, with a funding account, positive rupiah amount, transaction time, and optional notes.
- Add a visible Top up action, defaulting the funding account from the product or its parent, and a monthly-rule shortcut.
- Preserve bilateral ledger entries, ownership validation, funding-balance checks, atomicity, and retry safety. Support editing and deleting contributions through their own lifecycle so balances and valuations remain consistent.
- Keep capital separate from opening ledger balances. Update the current investment estimate by the contribution amount without claiming a fresh market quote; Update Nilai remains the way to enter Bibit's actual total value.
- Extend recurring rules with a manually confirmed investment-top-up type. The owner selected **confirm each monthly debit**; these rules cannot auto-post.
- Preserve existing Beli/Jual trades for unit-tracked positions. The first version does not convert existing unit-tracked products to amount tracking or invent units.

## Capabilities

### New Capabilities

- `investment-topups`: Amount-only mutual-fund contributions, valuation and capital integrity, reversible ledger lifecycle, and product-level entry.

### Modified Capabilities

- `automated-transactions`: Investment-top-up recurring rules and exactly-once manual confirmation through the contribution service.
- `investment-trade-recording`: Preserve exact unit-based trades and reject mixing them into a product already using amount contributions.

## Impact

- Backend: account valuation and responses, transactions/movements lifecycle guards, recurring rules, a contribution router/service, and router registration.
- Database: a new versioned migration after V22 for contribution metadata and explicit amount-mode cost basis; recurring constraints extended without rewriting existing rules or historical trades.
- Web: accounts/product actions and form, ledger inspection/edit/delete dispatch, recurring forms/types, and pending-confirmation feedback. Existing query caches must refresh after financial mutations.
- Verification: disposable-PostgreSQL integration tests, focused frontend tests, type-check/lint/build, responsive keyboard/browser smoke tests, and native OpenSpec validation.
- No new external provider or dependency. No bank/Bibit execution, Android changes, notification-parser work, live account mutation, deployment, or automatic market pricing is included. Recording an already logged debit must not be repeated; cross-channel matching of bank notifications to manual contributions needs a separate change.
