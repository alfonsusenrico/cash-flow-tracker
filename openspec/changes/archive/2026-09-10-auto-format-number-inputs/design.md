## Context

Currently, numeric financial inputs (targets, balances, bills, budgets) accept raw strings or numbers without live grouping. When users type large Indonesian Rupiah figures like 20 million (`20000000`), the lack of dot grouping makes it difficult to tell whether 6, 7, or 8 digits were typed.

## Goals / Non-Goals

**Goals:**
- Provide instant, real-time thousand separator formatting using dots (`.`) as the user types across all financial amount inputs.
- Ensure backspace, deletion, and copy-paste work naturally without breaking digits.
- Keep state handling intuitive and clean across all forms and modals (Goals, Obligations, Accounts, Dashboard Transfers, Ledger Edits, and Category Budgets).
- Guarantee zero data corruption: all mutations continue to parse clean integers.

**Non-Goals:**
- Non-currency date/day inputs (such as payday day `1-31`) are not thousands amounts and should remain standard number inputs.
- No heavy external input masking libraries; keep implementation lightweight, performant, and native.

## Decisions

### 1. Centralized Formatting Utilities in `frontend/src/lib/utils.ts`
Implement two pure functions in `frontend/src/lib/utils.ts`:
- `formatNumberWithDots(val: string | number | null | undefined): string`:
  - Strips all non-digit characters (`replace(/[^0-9]/g, "")`).
  - Removes redundant leading zeroes (e.g., `"05"` becomes `"5"`, while `"0"` stays `"0"`).
  - Groups digits by thousands with periods: `clean.replace(/\B(?=(\d{3})+(?!\d))/g, ".")`.
- `parseNumberFromDots(val: string | number | null | undefined): number`:
  - Strips all non-digit characters and parses to integer `parseInt(clean, 10) || 0`.

### 2. Standardized Form Field Updates
For each target modal:
- On `onChange`: format input value via `formatNumberWithDots(e.target.value)` and update state.
- On modal open / prefill: wrap existing entity numbers with `formatNumberWithDots(...)`.
- On submit / mutation: extract integer via `parseNumberFromDots(...)` or `replace(/[^0-9]/g, "")`.

### 3. QuickCapture Math Expression Tolerance
Ensure `QuickCaptureModal` sanitizes thousand dots (`val.replace(/\./g, "")`) so expressions like `20.000 + 10.000` evaluate safely to `30000`.

## Risks / Trade-offs

- **Cursor position during middle-edit**: When inserting digits in the middle of an already formatted number, reformatting may reposition the cursor to the end in simple inputs.
  - *Mitigation*: For standard fast financial capture, users almost always append digits or clear/backspace. For critical multi-edit fields, the reformatting runs synchronously on string length change.
