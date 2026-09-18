# Spec Delta: transaction-ledger-management

## ADDED Requirements

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL consolidate paired internal movement transactions into a single bilateral row in the ledger table:
1. In the default "Semua Rekening" ledger view, paired inbound and outbound records representing the same internal transfer SHALL be rendered as a single consolidated row.
2. The consolidated row SHALL display a neutral `↔️ PINDAH SALDO` status badge.
3. The row SHALL display the origin account in the `Rekening` column and destination account in the `Target / Tagihan` column (or format `Sumber → Tujuan`).
4. The transaction amount SHALL be rendered with neutral tabular typography without misleading `+` or `-` prefixes, accurately reflecting zero net cash flow change.
5. In account-filtered views, the movement SHALL display directional context relative to the selected account.
6. Editing or deleting a consolidated movement row SHALL atomically update or delete both underlying paired records to safeguard double-entry balance integrity.

#### Scenario: Viewing internal movements in all-accounts ledger
- **WHEN** the user views the `/ledger` screen with "Semua Rekening" selected
- **THEN** paired transfer records are merged into a single row displaying `↔️ PINDAH SALDO`, the source account, the destination account, and neutral amount formatting

#### Scenario: Deleting a consolidated internal movement
- **WHEN** the user opens the detail sheet for a consolidated transfer and confirms deletion
- **THEN** both the outbound and inbound paired records are removed and balances on both affected accounts are cleanly restored
