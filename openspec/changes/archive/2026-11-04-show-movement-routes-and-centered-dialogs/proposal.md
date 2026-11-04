# Show movement routes and open details in centered dialogs

## Why

After the 2026-11-04 refinement, the owner reported two problems.

**Movement routes.** "For internal movements, I like the previous where it's clear the movement is where to where between the source and target, now it's not shown."

The pre-redesign ledger (`6cc6704`, `frontend/src/app/ledger/page.tsx`) showed the source account in its own column and "→ target" or "← source" in the next. Today one helper, `rowTitle` in `frontend/src/components/beranda/activity.tsx`, writes every movement title, and it has three defects:

1. **Reversed direction.** A movement leg shown without its partner renders "`account_name` → `transfer_target_account_name`". The API's `transfer_target_account_name` is the partner's account. For an inbound leg the partner is the source, so the title reads backwards: "Main Pocket → Dana Darurat" for money that came from Dana Darurat. This happens wherever rows are not consolidated:
   - Rekening detail's recent rows (`AccountsView.tsx`);
   - an account-filtered Transaksi list;
   - a page whose partner leg sits on another page;
   - the transaction detail of an inbound leg.
2. **Unlinked legs lose their route.** A row filed as Internal Movement without a partner shows only its own account name, for example "Main Pocket". Nothing says that the other side is missing.
3. **Pockets lose their bank.** Pocket names are shown without their parent ("Main Pocket", "Tabungan"). The owner has pockets with the same or similar names at Jago and BCA. The ledger API returns no parent names.

**Side panels.** "For any menu or features that opens right side panel, I want to move back into opening centered middle modal. Adding another panel on the side not help as the input field, feature is a lot and compressed because of the limited width."

The shared `Drawer` (`frontend/src/components/workspace/overlays.tsx`) is a 392 px column above 1180 px and a 392 px overlay from 761 to 1180 px. Its forms (edit, debt allocation, type switch, movement edit) are squeezed into that width.

## Expected Outcome

The owner can see these results:

- **Every movement shows its route,** in the direction the money moved, with the bank or parent name: "Jago · Main Pocket → BCA · ATM". This holds on Transaksi, Beranda "Aktivitas terbaru", Rekening detail and the transaction detail, in every view, including account-filtered lists and a leg whose partner is not loaded.
- **A one-sided movement says which side is missing:** "? → Jago · Main Pocket" with "Pasangan belum ditemukan". When Tinjau lists the row, its detail links to the existing completion there.
- **Details open as centered dialogs.** Clicking a transaction, a Peta Aliran node or link, or "Rencana jatuh tempo" on a desktop or tablet opens a centered, dimmed dialog wide enough for its forms. No right-side column opens or narrows the page.
- **The phone keeps its bottom sheet** (OD112).

## Approval

**Plan approval, 2026-11-04.** After a summary of both plans (`show-movement-routes-and-centered-dialogs` and `recognise-one-arrival-seen-twice`), the owner said "approved, defaults ok". OD112–OD115 take their recommended defaults (design.md, open decisions).

**Acceptance, 2026-11-04.** After the summary of both changes with their verification, the owner said "accept, do as your recommend". OD112–OD115 keep their applied defaults.

## What Changes

- **API, transaction rows.** `GET /api/transactions`, the transaction detail and the rows Beranda and Rekening read SHALL carry `account_parent_name` and `transfer_target_account_parent_name`, read in the same query and with no extra round trip. This is additive, so existing fields keep their meaning.
- **Shared route formatting.** One function, `movementRoute(row)`, SHALL return `{ source, target }` labels for a movement row:
  - from `movement_role` and `transfer_target_*` for a single leg;
  - from the consolidated or inferred fields for a merged row;
  - with "Bank · Pocket" labels when a parent exists;
  - with `null` for an unknown side.
  `rowTitle`, the Transaksi subline, Beranda, Rekening detail and the transaction detail SHALL all use it.
- **One-sided movement marker.** A movement row with an unknown side SHALL show "?" for that side and the existing label "Pasangan belum ditemukan". The direction of an unlinked leg comes from its type (an income is the target, an expense the source). The missing side SHALL NOT be guessed from another row. The transaction detail SHALL use the same direction for its "Dari" and "Ke" fields, which today treat every unlinked leg as outbound.
- **Account-filtered views.** On Rekening detail and in an account-filtered Transaksi list, a movement row SHALL show the full route plus the direction relative to the viewed account ("Masuk dari …", "Keluar ke …"), as the pre-redesign ledger did with ← and →.
- **Centered detail dialog.** `Drawer` SHALL render as a centered modal dialog above 760 px, with a dimmed scrim, focus trapped, Esc and the close button closing it, and focus returned to the opener. At 760 px and below it SHALL stay a bottom sheet. The width is set in design D4.
- **Workspace window.** The right-hand drawer slot SHALL be removed from `AppLayout` and `globals.css`. The window holds the sidebar and the main column only.
- **Copy.** The new row texts "Masuk dari" and "Keluar ke" use neutral standard Indonesian. "Pasangan belum ditemukan" already exists.

## Non-goals

- No change to how movements are paired, linked, edited or deleted.
- No change to Change B (`recognise-one-arrival-seen-twice`), which owns the duplicate-arrival defect.
- No new detail content. Each dialog shows what its drawer shows today.
- The Android companion is out of scope.
- **Local verification only.** Production is touched only by the approved deploy after the owner merges, then checked read-only.

## Capabilities

**New capabilities:** none.

**Modified capabilities:**
- `transaction-ledger-management`: "Transaction Detail & Inline Editing" (dialog instead of side column) and "Consolidated Internal Movement Ledger Representation" (route labels, parent names, one-sided marker, filtered views).
- `executive-sidebar-layout`: "Fluid Full-Width Workbench Layout" (no right-hand drawer column).
- `beranda-cycle-summary`: "Recent Activity on Beranda" (the route with parent names, and the detail opens as a dialog).
- `money-flow-map`: ADDED "Peta Aliran Details Open Centered".

`ui-copy-register` needs no delta, because "Masuk dari" and "Keluar ke" are row direction text, which "Spending Terms" already allows.

No project capability is added. The existing capabilities apply:
- **money:** route direction must match the ledger;
- **api-consumers:** additive fields only;
- **personal-data:** names only for the signed-in owner.

## Impact

- **Backend:** `backend/app/routers/transactions.py` (the list and detail queries add parent joins), and the Beranda and Rekening row sources if they use another query. No migration.
- **Frontend:**
  - `components/beranda/activity.tsx` (`rowTitle`, new `movementRoute`);
  - `components/transactions/LedgerList.tsx` (subline);
  - `components/transactions/TransactionDrawer.tsx`;
  - `components/accounts/AccountsView.tsx` (recent rows);
  - `components/workspace/overlays.tsx` (`Drawer` becomes a dialog);
  - `components/layout/AppLayout.tsx` and `app/globals.css` (slot removal);
  - `app/ledger/page.tsx`, `components/beranda/BerandaView.tsx` and `components/analysis/flow/PetaAliranTab.tsx` (callers, unchanged API);
  - `lib/ledgerMovements.ts` and `types`.
- **Tests:** Vitest for `movementRoute` and each surface, `overlays` dialog behaviour, and a backend test for the parent fields. Browser matrix and screenshots.
