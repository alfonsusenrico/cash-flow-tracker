# Proposal

## Why

The Kakeibo 50/30/20 breakdown on the dashboard is displaying invalid "Tabungan" (Savings) amounts (e.g. Rp 310.000 in the current cycle) composed entirely of liquid cash transfers (ATM to GoPay, GoPay to Dana Darurat) and temporary card verification pre-auth holds. This happens because the `Internal Movement` category was seeded with `kakeibo_type = 'saving'`, causing liquid movements to inherit `saving` status and be improperly aggregated into the monthly savings metrics.

## What Changes

- **Database Cleanup Migration**:
  - Update category metadata in `categories` table: set `kakeibo_type = NULL` for `Internal Movement` categories.
  - Reset `kakeibo_type = NULL` for past internal movement transactions that moved funds between liquid cash/bank/e-wallet accounts or represented pre-auth holds.
- **Backend Movement Provisioning**:
  - Update [`backend/app/routers/movements.py`](file:///Users/enrico/enrico/project/enrico/cash-flow-tracker/backend/app/routers/movements.py) to provision `Internal Movement` categories with `kakeibo_type = NULL` instead of `'saving'`.
- **Backend Kakeibo Aggregation Logic**:
  - Refine [`backend/app/routers/dashboard.py`](file:///Users/enrico/enrico/project/enrico/cash-flow-tracker/backend/app/routers/dashboard.py) so `saving_transfers` only aggregates internal movements that target investment accounts or are explicitly linked to a savings goal (`goal_id IS NOT NULL`), strictly ignoring liquid transfers and pre-auth hold transactions.
- **Automated Tests**:
  - Add regression tests verifying that liquid internal movements and pre-auth transactions never count towards Kakeibo savings.

## Capabilities

### Modified Capabilities
- `kakeibo-reconciliation`: Enforce that category seeding and transaction ingestion for internal movements never default `kakeibo_type` to `saving`, ensuring only genuine transfers to investment accounts or goal funding count toward monthly savings.

## Impact

- **Affected Code**:
  - `backend/app/routers/movements.py`
  - `backend/app/routers/dashboard.py`
  - Database schema / data migration script (`backend/migrations/` or Alembic/Flyway)
  - `backend/tests/` (unit and regression tests)
- **API Changes**: No breaking changes to API schema; responses for `/api/pulse` and `/api/insights` will now return mathematically accurate Kakeibo allocation figures.
