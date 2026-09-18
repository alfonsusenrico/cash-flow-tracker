# Tasks: Consolidate Internal Movements in Ledger

## 1. Data Pipeline & Pairing Logic

- [x] 1.1 Implement transaction pairing utility in `frontend/src/app/ledger/page.tsx` that consolidates matching outbound and inbound internal movement records into unified transfer items and verify pairing behavior.
- [x] 1.2 Parse and resolve source account and target account from paired records and transfer notes (`Sumber → Tujuan`) and verify account attribution.

## 2. Table & Badge UI Rendering

- [x] 2.1 Update ledger table row rendering to display `↔️ PINDAH SALDO` badge with neutral sky-blue styling for consolidated movements and verify badge styling.
- [x] 2.2 Populate `Target / Tagihan` column with destination account and render transfer amount in neutral tabular typography without colored `+` or `-` prefixes.

## 3. Synchronized Actions & Context Sheet Integration

- [x] 3.1 Update slide-out context drawer (sheet) to recognize consolidated movements and present bilateral transfer details.
- [x] 3.2 Implement synchronized delete and edit handling in the drawer to atomically operate on both paired database records and verify balance restoration.

## 4. Verification & Validation

- [x] 4.1 Run frontend type-check and linting (`npm run type-check && npm run lint`) and verify clean output.
- [x] 4.2 Validate OpenSpec change integrity with `openspec validate consolidate-ledger-movements`.
