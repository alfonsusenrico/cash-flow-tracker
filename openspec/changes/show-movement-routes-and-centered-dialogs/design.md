# Design: show movement routes and open details in centered dialogs

## Context

- **One title helper.** `rowTitle` (`frontend/src/components/beranda/activity.tsx`) titles rows on Beranda, Transaksi, Rekening detail and the transaction detail. For a movement it returns `account_name → (target_account_name ?? transfer_target_account_name)`.
  - `target_account_name` exists only on rows that `consolidateLedgerMovements` merged. There it is correct, because the merged row is always the outbound leg.
  - `transfer_target_account_name` is the partner's account (`backend/app/routers/transactions.py`, `partner_account.name AS movement_target_account_name`). For an inbound leg that is the source, so the title is reversed.
  - Consolidation is off for account-filtered lists, and Rekening detail reads `/transactions?account_id=…`. In both, every inbound leg reads backwards.
- **The detail's "Dari" and "Ke"** (`TransactionDrawer.tsx`) already use `movement_role`. They are right for linked legs. For an unlinked leg with no role they treat the row as outbound, so an unlinked income shows "Dari: Main Pocket".
- **No parent names.** The ledger list and detail queries return `account_name` and the partner's name only. Several services already format "Parent · Pocket" (`movement_linkage.py`, `notification_mapping.py`, `topup_learning.py`), so the label form is established.
- **`Drawer`** (`frontend/src/components/workspace/overlays.tsx`) has three modes:
  - `side`, in AppLayout's `ws-drawer-slot` third column above 1180 px;
  - `overlay`, a fixed 392 px panel at the right from 761 to 1180 px;
  - `sheet`, at 760 px and below.
  Its callers are:
  - the transaction detail (`TransactionDrawer`, used by Transaksi, Beranda, Kalender and Peta Aliran rows);
  - "Rencana jatuh tempo" on Beranda and Transaksi;
  - the Peta Aliran node and link details.
  The goal and debt details (`GoalsView.tsx`) and the Rekening detail already use the centered `Modal`.
- **`Modal`** (`components/ui/Modal.tsx`) is centered from 640 px, with `wide` at 672 px. It has its own focus handling and stacks: the topmost dialog owns the keyboard.

## Goals

- One rule for a movement's route, used everywhere, correct for every leg shape: linked, consolidated, inferred, unlinked, and partner not loaded.
- Pockets named with their bank.
- Details open as centered dialogs on desktop and tablet, with room for their forms. The phone keeps its sheet.

## Non-goals

- Pairing, linking and editing rules don't change.
- No new detail content.
- Rekening detail's recent rows stay non-interactive. Making them open the transaction detail is a separate request.

## Decisions

### D1. Parent names come from the server

The ledger list and detail queries add `LEFT JOIN accounts sa_parent ON sa_parent.id = sa.parent_id` and the same join for `partner_account`. They return:
- `account_parent_name`;
- `transfer_target_account_parent_name`.

These fields are additive, and `null` when there is no parent or no partner.

Rejected: resolving parents in the client from `/accounts`. Every surface would need the accounts query loaded, and a row could render before it arrived, naming a pocket without its bank.

### D2. One route function

`movementRoute(row): { source: Label | null; target: Label | null }`, in `components/beranda/activity.tsx`. `Label` is `"Parent · Name"` or `"Name"`. The source of each side depends on the row:

| Row | Source | Target |
| --- | --- | --- |
| Consolidated or inferred (`is_consolidated_transfer`, `is_inferred_transfer`) | the row's own account | `target_account_*` |
| Linked leg, `movement_role = outbound` | own account | partner (`transfer_target_*`) |
| Linked leg, `movement_role = inbound` | partner | own account |
| Unlinked movement leg, `type = expense` | own account | `null` |
| Unlinked movement leg, `type = income` | `null` | own account |

The consolidated and inferred fields gain parent names in `consolidateLedgerMovements`, copied from the partner row's `account_parent_name`.

These consume it:
- `rowTitle`, as `source ?? "?"` → `target ?? "?"`;
- the Transaksi subline: `notes`, then "Pasangan belum ditemukan" when a side is `null`;
- the transaction detail's "Dari" and "Ke";
- Rekening detail's recent rows.

Rejected: patching `rowTitle` alone. The detail's "Dari" and "Ke" would keep their own, different rule.

### D3. Account-filtered direction

In an account-filtered Transaksi list and on Rekening detail, a movement row adds its direction relative to the viewed account:
- "Masuk dari <source>" when the viewed account is the target;
- "Keluar ke <target>" when it is the source.

The amount keeps the movement's neutral sky style. This brings back the pre-redesign ← and → cue (`6cc6704`, `app/ledger/page.tsx`, Target column) without a separate column.

### D4. `Drawer` becomes a centered dialog above 760 px

`Drawer` keeps its props and callers. Its modes become `dialog` (above 760 px) and `sheet` (760 px and below).
- **Dialog mode:**
  - a fixed, centered panel over the existing `ws-sheet-scrim`;
  - width `min(720px, 100vw - 48px)` and max-height `calc(100dvh - 48px)`, scrolling inside;
  - the existing header, close button, focus trap, Esc layer, scroll lock and focus return.
- **Sheet mode:** unchanged.
- **AppLayout:** `DrawerSlotContext`, the `ws-drawer-slot` element, `data-drawer` on the window, and the `.ws-drawer-slot` and `[data-mode="overlay"]` CSS are removed.

720 px gives the debt-allocation editor and the type switch the width of the old ledger forms. It's also wider than `Modal` wide (672 px), because the transaction form has two-column field rows (OD113).

Rejected: migrating every caller to `Modal`. Its breakpoint is 640 px, not the workspace's 760 px phone query, and its focus logic differs. Keeping one component keeps one behaviour.

Stacking: a dialog opened from a dialog (the movement edit dialog from the transaction detail) stays on top, with the topmost owning Esc and Tab, as `Modal` already does. `Drawer` adopts the same check.

### D5. Verification gates

- **Vitest:**
  - `movementRoute` over the five row shapes, plus parent and no-parent labels;
  - each surface: Beranda, Transaksi in all-accounts and filtered views, Rekening detail, and the transaction detail title with "Dari" and "Ke";
  - `Drawer` in dialog and sheet mode: focus, Esc, scrim click, focus return, stacking.
- **Backend:** a database test that the list and detail rows carry both parent fields, and that every pre-existing field is unchanged on a seeded ledger (snapshot compared before and after).
- **Browser:**
  - Transaksi, Beranda, Peta Aliran and "Rencana jatuh tempo" at 1440, 1280, 1024, 768 and 390 px, light and dark;
  - no side column;
  - the dialog centered;
  - the form fields at their full width;
  - 0 serious axe findings, 0 overflow, 0 console errors.
- **Screenshots:** the owner-shaped seed, including Jago and BCA pockets with similar names and an unlinked movement leg.

## Compatibility, migration and rollback

- No migration. The API change is additive.
- Rollback is a revert of the merge.

## Risks

- A centered dialog covers the list, so the owner can no longer read the list beside an open detail. That's accepted, because the owner asked for it.
- Long routes ("Stockbit · Bank Central Asia Tbk → Jago · Main Pocket") wrap on a phone. The title wraps to two lines and never truncates the amount.

## Open decisions (recommended defaults)

- **OD112: phone layout.** At 760 px and below, details stay bottom sheets. *Default: yes.* The alternative is a centered dialog on the phone too.
- **OD113: dialog width.** *Default: 720 px.* The alternative is `Modal` wide (672 px), to match the Rekening detail.
- **OD114: unknown side.** *Default:* the unknown side of an unlinked leg is written "?" with the label "Pasangan belum ditemukan". The alternative is omitting the arrow ("Main Pocket · pasangan belum ditemukan").
- **OD115: direction text in filtered views.** *Default:* add "Masuk dari" or "Keluar ke" in account-filtered views only. The alternative is adding it on every surface.
