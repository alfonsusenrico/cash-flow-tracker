# Proposal: Consolidate Internal Movements in Ledger

## Why

In the current ledger implementation, every internal transfer between accounts or pockets is stored as two double-entry records (one outbound expense on the source account, and one inbound income on the target account). On the `/ledger` screen, displaying both halves as distinct rows (`PINDAH SALDO (KELUAR)` with `-Rp` and `PINDAH SALDO (MASUK)` with `+Rp`) creates severe visual noise, cognitive clutter, and doubles the table height for what is mentally a single user action with net-zero cash impact.

Consolidating paired internal movement records into a single unified row with a bilateral `↔️ PINDAH SALDO` badge provides immediate clarity, halves table clutter, and aligns with modern personal finance design standards.

## What Changes

- **Single-Row Consolidation:** In the `/ledger` table (especially under the default "Semua Rekening" view), automatically pair outbound and inbound internal movement records into a single consolidated row.
- **Bilateral Badge (`↔️ PINDAH SALDO`):** Replace duplicate `PINDAH SALDO (KELUAR)` and `PINDAH SALDO (MASUK)` status pills with a unified `↔️ PINDAH SALDO` pill styled in neutral slate/cyan.
- **Clear Route Columns:** Display the source account in the `Rekening` column and target account in the `Target / Tagihan` column (or route format `Sumber → Tujuan`).
- **Neutral Nominal Styling:** Render the transfer amount in a clean neutral color without deceptive `+` green or `-` red prefixes, emphasizing that net cash movement across the user's accounts is Rp 0.
- **Account-Filtered Context:** When filtering the ledger by a specific account (e.g. "Dana Darurat"), provide appropriate directional context (outflow from or inflow to the selected account).
- **Synchronized Edit/Delete Actions:** Deleting or editing a consolidated movement row in the slide-out sheet seamlessly updates or removes both underlying database records to safeguard double-entry balance integrity.

## Capabilities

### Modified Capabilities
- `transaction-ledger-management`: Add requirement for consolidating paired internal movement transactions into a single bilateral row in the ledger view while preserving synchronized balance updates.

## Impact

- **Frontend:**
  - `frontend/src/app/ledger/page.tsx`: Add paired transaction grouping logic, update table row rendering with `↔️` badge and source/target routing, and synchronize edit/delete modals.
  - `frontend/src/types/`: Add paired transaction metadata types if applicable.
- **Backend:**
  - `backend/app/routers/transactions.py`: Ensure transactions return necessary pairing hints (e.g. `idempotency_key`, linked transfer notes, or pair detection) if needed.
- **Database:** Zero schema modifications. Preserves strict 5-table architecture and double-entry mathematical balance.
