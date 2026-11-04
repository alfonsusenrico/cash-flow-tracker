# Tasks

Evidence for every task goes into `verification.md`.

**Data:** synthetic only. Use the 2026-11-04 preview seed shaped like the owner's account, extended with:
- a Jago pocket and a BCA pocket with similar names;
- a linked movement whose inbound leg is the only leg on a filtered page;
- an unlinked income filed as Internal Movement.

**Backend tests:** run as `.github/workflows/checks.yml` does, on a disposable PostgreSQL 16 migrated by Flyway with `REQUIRE_TEST_DATABASE=1`.

**Local processes:** every one uses this change's own ports and is stopped by PID or port. Record `ps`, `lsof` and `docker ps` at the end.

## 1. Baseline

- [x] 1.1 Record the baseline on `main`:
  - the backend suite;
  - Vitest, type-check, lint and build;
  - first-load JS of `/`, `/ledger`, `/insights` and `/accounts`;
  - screenshots of Transaksi (all accounts and filtered to a pocket), Beranda, Rekening detail, the transaction detail and Peta Aliran's link detail at 1440 light, 1440 dark and 390 dark.

  Evidence: the baseline table and screenshot paths, including the reversed inbound title as the failing state.

Expected Result: the defects are reproduced and the starting point is measured.

## 2. Server: parent names

- [x] 2.1 Add `account_parent_name` and `transfer_target_account_parent_name` to the list and detail queries in `backend/app/routers/transactions.py` (D1). Evidence: a database test that asserts both fields for:
  - a pocket;
  - an account without a parent;
  - a linked leg;
  - an unlinked leg.
- [x] 2.2 Prove the change is additive. Evidence: the seeded ledger's list and detail responses equal the baseline after removing the two new fields.

Expected Result: rows name their parents with nothing else changed.

## 3. Client: one route rule

- [x] 3.1 Add `movementRoute` and use it in `rowTitle` (D2). Carry parent names through `consolidateLedgerMovements`. Evidence: Vitest over the five row shapes, with and without parents. The inbound case fails before the change.
- [x] 3.2 Use the route in:
  - the Transaksi subline and filtered-view direction (D3);
  - Beranda "Aktivitas terbaru";
  - Rekening detail's recent rows;
  - the transaction detail's title and its "Dari" and "Ke".

  Evidence: Vitest per surface, including "? → Jago · Main Pocket" with "Pasangan belum ditemukan", and "Masuk dari Jago · Dana Darurat" in a filtered list.

Expected Result: every movement reads in the direction the money moved, with its bank.

## 4. Client: centered dialogs

- [x] 4.1 Turn `Drawer` into `dialog` and `sheet` modes, and remove the drawer slot from `AppLayout` and `globals.css` (D4). Evidence: Vitest for dialog mode (focus inside, Esc, scrim click, close button, focus return, topmost-only keyboard with a stacked dialog) and sheet mode unchanged.
- [x] 4.2 Check every caller:
  - the transaction detail from Transaksi, Beranda, Kalender and Peta Aliran;
  - "Rencana jatuh tempo" on Beranda and Transaksi;
  - the Peta Aliran node and link details.

  Evidence: existing tests pass, plus a browser check of each that finds no side column, a centered dialog, and form fields at full width.
- [x] 4.3 Remove dead code: `DrawerSlotContext`, `ws-drawer-slot`, `data-drawer` and the overlay CSS. Evidence: `rg` finds no remaining references, and lint passes.

Expected Result: no right-side panel anywhere, and forms have room.

## 5. Verification

- [x] 5.1 Run the full suites as CI does: backend pytest; frontend `npm ci`, type-check, lint, Vitest and build. Evidence: counts with 0 failures, at the tested revision.
- [x] 5.2 Browser matrix:
  - Transaksi (all and filtered), Beranda, Rekening detail, Peta Aliran, Kalender and the dialogs;
  - at 1440, 1280, 1024, 768 and 390 px, light and dark;
  - the checks: axe serious or critical, overflow, console errors, targets under 24 px, keyboard (Tab order, Esc, focus return) and reduced motion.

  Evidence: the results table with 0 serious findings.
- [x] 5.3 Compare screenshot pairs against the baseline for each surface in 1.1. Evidence: a compare page, read by the agent, showing routes with parents and centered dialogs.
- [x] 5.4 Unchanged writes: edit, delete, type switch, movement edit and "Rencana jatuh tempo" actions from the dialog send the same requests as on `main`. Evidence: request bodies compared.
- [x] 5.5 Check JS per route against the baseline (limit +5 kB), then `openspec validate --all --strict`, `git diff --check` and the clean-up proof. Evidence: outputs.

Expected Result: the change is proven locally.

## 6. Acceptance

- [ ] 6.1 Present to the owner:
  - verification.md;
  - the compare page;
  - OD112–OD115 with their applied defaults.

  Record the acceptance with the owner's words and the date in proposal.md. Evidence: the quote.

## Workflow follow-up

- After acceptance:
  - archive the change on the branch with `openspec archive show-movement-routes-and-centered-dialogs -y`;
  - confirm `openspec validate --all --strict` exits 0;
  - push the branch.
- The owner opens and merges the pull request. The release runs only through `deploy.yml`.
- After the deploy, check production read-only, then delete the branch locally and on the remote.
