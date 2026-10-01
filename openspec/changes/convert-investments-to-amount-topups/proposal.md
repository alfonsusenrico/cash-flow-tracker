# Proposal

## Why

Amount-only top-ups (change `add-amount-based-investment-topups`) work only on mutual-fund products without units, so the owner's two Bibit money-market products, created with units, still show only Beli/Jual and Update Nilai. Automated RDN → Bibit debits are also already recorded from bank notifications as lone outgoing expenses, and there is no way to attach such a debit to a product: merging needs an incoming leg that Bibit never reports, and generic movements reject investment targets.

## What Changes

- Add an explicit, one-way conversion of a unit-tracked mutual-fund product to amount tracking. It carries over the current value (units × last price) and the invested cost (units × average buy price, else the opening balance), and clears units and per-unit average so the product becomes eligible for Top up and amount-based Update Nilai.
- Add conversion of an existing lone outgoing expense from a liquid account into an investment top-up for a chosen amount-tracked product. The existing debit becomes the top-up's outbound leg (keeping its notification link and idempotency key), an inbound leg is added to the product, and the product's value and cost increase by the amount. No second debit is created and the funding balance does not change.
- Deleting a top-up created this way restores the original lone expense (category, kakeibo, notes) instead of removing the bank debit; editing it uses the existing top-up correction lifecycle.
- Web: "Ubah ke pelacakan nominal" in the options menu of a unit-tracked mutual-fund product, with a confirmation showing the carried-over value and cost; "Jadikan Top up Investasi" in the transaction edit view of an eligible lone expense, with a product picker.
- Out of scope: automatic matching of debit notifications to products or recurring rules, conversion back to units, and conversion of stock or gold positions.

## Capabilities

### New Capabilities
- `investment-amount-conversion`: converting a unit-tracked mutual-fund product to amount tracking, and converting a recorded lone expense into an amount top-up with a reversible lifecycle.

### Modified Capabilities
None in `openspec/specs/`. This builds on the unarchived `investment-topups` capability from `add-amount-based-investment-topups` without changing its requirements.

## Impact

- Backend: Flyway `V24` (nullable `investment_topups.converted_from` JSONB) mirrored in a startup SQL file; `services/investment_topups.py` (conversion services, delete restores converted expenses); `routers/accounts.py` (`POST /api/accounts/{id}/amount-tracking`); `routers/investment_topups.py` (`POST /api/investment-topups/from-transaction`).
- Frontend: accounts product options menu and confirmation dialog; ledger transaction edit modal action and product picker; shared helper for eligibility.
- No change to the Android companion, notification processing, or recurring rules. Production data changes only when the owner uses the new actions.
