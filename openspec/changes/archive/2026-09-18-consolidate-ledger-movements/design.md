# Design: Consolidated Internal Movements in Ledger

## Context

The system persists internal liquidity transfers using double-entry accounting:
- Outbound record (`type = 'expense'`, `category = 'Internal Movement'`, `is_excluded_from_budget = TRUE`)
- Inbound record (`type = 'income'`, `category = 'Internal Movement'`, `is_excluded_from_budget = TRUE`)

Both records share identical amount, matching timestamps, and paired notes (e.g., `Pindah Saldo: Dana Darurat → Subscription`) or paired idempotency keys (`<key>:out` / `<key>:in`).

On `/ledger`, rendering both rows 1:1 doubles table rows unnecessarily. See `proposal.md` for background and motivation.

## Goals / Non-Goals

**Goals:**
- Present paired internal movements as a single consolidated row with a `↔️ PINDAH SALDO` badge in `/ledger`.
- Show origin account under `Rekening` and destination account under `Target / Tagihan`.
- Format amount neutrally without misleading `+` or `-` prefixes.
- Keep edit and delete actions synchronized across both database records.
- Preserve 100% database balance integrity without altering the 5-table core schema.

**Non-Goals:**
- Modifying the database schema or eliminating double-entry records.
- Altering the behavior of non-movement income or expense entries.

## Decisions

### Decision 1: View-Layer Pairing Algorithm
**Choice:** Pair transactions in the Ledger client-side data pipeline prior to rendering the table rows.
- **Pairing criteria:** Two records $T_{\text{out}}$ and $T_{\text{in}}$ form a movement pair if:
  1. Both belong to the `Internal Movement` category (`is_excluded_from_budget === true` or `category_name === 'Internal Movement'`).
  2. One has `type === 'expense'` (source) and the other `type === 'income'` (target).
  3. They have identical `amount`.
  4. They have close timestamps (within 60 seconds) and matching notes (or matching idempotency base keys).
- **Consolidated Row Structure:**
  - `id`: `T_out.id` (with reference to `partnerId: T_in.id`)
  - `type`: `transfer` (internal virtual type for rendering)
  - `account_name`: Source account name
  - `target_account_name`: Target account name (parsed from notes or target record account name)
  - `amount`: `T_out.amount`
  - `notes`: Stripped clean description
- **Alternative Considered:** Adding a `linked_transaction_id` column to the database. Rejected because it requires schema migrations, complicates ingestion, and adds unnecessary coupling (violates Ponytail radical simplicity).

### Decision 2: Visual Styling Compliant with Scandinavian Tactile Minimalist Standards
- **Badge:** Slate/cyan pastel pill with `↔️ PINDAH SALDO` (`bg-sky-500/10 text-sky-400 border border-sky-500/20`).
- **Nominal:** `tabular tracking-tight font-medium text-[var(--foreground)]` (e.g. `Rp 350.000` without colored plus/minus).
- **Columns:**
  - `Rekening`: Source pocket/account
  - `Target / Tagihan`: Destination pocket/account with subtle directional arrow indicator.

### Decision 3: Synchronized Action Handling
- When opening the slide-out sheet for a consolidated transfer, the sheet recognizes both `id` and `partnerId`.
- **Deletion:** Calling delete triggers removal of both `T_out` and `T_in`, safely restoring balances on both accounts.
- **Editing:** If amount or date is edited, updates are applied to both records.

## Risks / Trade-offs

- **[Pagination Split Risk]** If a pair crosses page boundaries:
  - *Mitigation:* `movements.py` and `ingest.py` write both records with the exact same timestamp (`date` and `created_at`), keeping them strictly adjacent in `ORDER BY date DESC, created_at DESC`. In the rare event an isolated half appears on a page boundary, it safely renders with single-account directional context.
