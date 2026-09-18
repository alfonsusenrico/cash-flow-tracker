# Design

## Context

See `proposal.md` for motivation. In the current system, the `Internal Movement` category was seeded with `kakeibo_type = 'saving'`. Consequently, any internal movement recorded without an explicit `kakeibo_type` inherited `'saving'` from the category metadata. In addition, the `saving_transfers` query in `backend/app/routers/dashboard.py` aggregated all expense transactions with category `Internal Movement` where `kakeibo_type = 'saving'`. This caused liquid transfers between checking/e-wallet accounts and temporary card authorization holds to falsely inflate the "Tabungan" (Savings) Kakeibo allocation.

## Goals / Non-Goals

**Goals:**
- Ensure `Internal Movement` category has `kakeibo_type = NULL`.
- Ensure internal movements between liquid operational accounts (cash, bank checking, e-wallets) are saved with `kakeibo_type = NULL`.
- Ensure card authorization pre-auth hold transactions are saved with `kakeibo_type = NULL`.
- Clean up existing misclassified transactions and category metadata in the database.
- Safeguard the `dashboard.py` Kakeibo calculation query against SQL `NULL` notes evaluation issues and ensure only true transfers to investment accounts or goal funding count toward savings.

**Non-Goals:**
- Altering the 5-table data model or introducing new categorization tables.
- Modifying stock purchase execution tracking logic (trades remain `kakeibo_type = NULL`).

## Decisions

### Decision 1: Category `Internal Movement` Decoupling
- **Choice:** Set `kakeibo_type = NULL` for `Internal Movement` categories in both database seeding and auto-provisioning (`backend/app/routers/movements.py`).
- **Rationale:** An internal movement is neutral by default. It only represents an allocation to "Tabungan" if funds are transferred into a dedicated investment account (RDN, broker, mutual funds) or an explicit goal account.
- **Alternative considered:** Keeping `kakeibo_type = 'saving'` on the category and overriding it per-transaction. *Rejected:* Error-prone; any generic transaction endpoint or third-party ingestion would default back to `'saving'`.

### Decision 2: SQL Query Hardening in `dashboard.py`
- **Choice:** Update the `saving_transfers` query in `backend/app/routers/dashboard.py`:
  1. Only aggregate transactions where `t.kakeibo_type = 'saving'` OR `t.goal_id IS NOT NULL`.
  2. Use `COALESCE(t.notes, '') NOT LIKE ...` so transactions with `NULL` notes are not accidentally dropped by SQL ternary truth logic.
- **Rationale:** Defensive against NULL notes and ensures pure liquid moves without `'saving'` are completely excluded.

### Decision 3: Versioned Database Migration
- **Choice:** Provide a reproducible database migration script that:
  1. Updates `categories SET kakeibo_type = NULL WHERE name = 'Internal Movement'`.
  2. Updates `transactions SET kakeibo_type = NULL WHERE category_id IN (SELECT id FROM categories WHERE name = 'Internal Movement') AND (account_id IN (SELECT id FROM accounts WHERE type != 'investment') OR notes LIKE '%Pre-auth%' OR notes LIKE '%Payment%')`.
- **Rationale:** Ensures clean data on both local and production environments when deployed via pipeline.

## Risks / Trade-offs

- **[Risk] Existing analytics shift for users:** Users will see their "Tabungan" metric decrease to its true value.
  → *Mitigation:* This is the desired outcome; the previous value was false inflation from liquid transfers.
- **[Risk] SQL NULL comparison:** SQL `NULL NOT LIKE '%...'` yields `NULL` (false), which previously caused transactions with NULL notes to behave unexpectedly.
  → *Mitigation:* Wrapped with `COALESCE(t.notes, '')`.

## Migration Plan

1. Code changes made locally in `movements.py` and `dashboard.py`.
2. Database migration script added under migrations.
3. Tests executed locally to verify correct Kakeibo allocation calculation.
4. Git commit and push.
5. Deployed through standard deployment script (`deploy_remote_release.sh`).
