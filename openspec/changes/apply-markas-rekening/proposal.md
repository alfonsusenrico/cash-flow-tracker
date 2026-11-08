## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Rekening & Kantong (`/accounts`) is the eighth page of the phased plan (`cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md`, phase 8). It is still the pre-redesign page inside the new shell.

The audit (`design-options/audit/AUDIT.md` §Rekening & Kantong) measured why it reads heavy:
- the same split is drawn twice, as a bar and as a donut, and the summary has five figures plus a coloured "Untung/Rugi" pill;
- 26 pills on the page, and every card carries a coloured 4 px top edge on an 18 px rounded corner;
- the drag hint sentence is always visible;
- the investment detail has six equal buttons in one row plus "Arsipkan";
- at 390 px the rows scroll sideways (564 px of content in a 358 px column).

Three owner requests bind this phase:
- **Tagihan off Rekening** (2026-11-05): "tagihan is there in the accounts, while the menu 'Target & Tagihan' should be the place for it, better remove it from the accounts". Utang shows credit accounts only, with one link to the obligations in Target & Tagihan (plan owner decision 2, default B).
- **Straight marks** (2026-11-05, headquarter reference): straight selection bars and thin group rails, never a curved left border.
- **Capital gain wherever it exists** (2026-11-05, kept from `organize-accounts-by-use`): on every product, kind, rekening and investment group, in rupiah and percent.

The accounts-by-use work on `main` stays: the Kegunaan roles, the two groupings, the saved draggable order ("I organize the display based on my usual usage"), the composition ring as a supporting view, and display 2 (Dompet) as the default.

`add-credit-spending-flow` hands this phase block H8 (its design D12): the credit tile, the credit detail with bills and settlement facts, and the bill day in the account form.

## Expected Outcome

- At 1440 px Rekening & Kantong reads top to bottom as in the prototype:
  - the title "Rekening & Kantong" with its subtitle, "Perbarui harga" and "Tambah rekening", and one "…" with "Atur urutan" and "Rencana";
  - the hero "Kekayaan bersih" in Manrope 44 with the page's one "Rincian", the line "dari aset Rp …", an 8 px split bar by use and its legend with percentages;
  - "Kartu · Grafik" at the left and "Per kegunaan · Per rekening" at the right;
  - groups with a 2 px straight rail in the use colour, an uppercase label, a muted count, the gain where it exists, and the total at the right;
  - flat tiles (min 112 px, radius 12, 1 px neutral edge) with a 24 px mark, the name, one meta line, the balance in Inter 20 and the gain "▲ +Rp 396.000 · +8,0%".
- **Utang** lists credit accounts only. When obligations are active, the group ends with one row "Tagihan di Target & Tagihan · 2 tagihan · −Rp 12.250.000" that links to `/goals`. Kekayaan bersih stays the server's figure, which still subtracts the obligations; its "Rincian" lists them.
- **Credit (H8):** a credit tile shows what is owed, "Sisa limit Rp 2.107.000" or "Melewati limit", and the next bill "jatuh tempo 11 Des · Rp 434.000". Its detail lists the bills per due date with amount, paid, left and status. Each bill opens its items with instalment, settled date, days to settle and whether it was paid on time. The account form edits "Tagihan terbit tiap tgl" and "Jatuh tempo tiap tgl", and says that past bills keep their due dates.
- **Investasi** shows one quiet line per kind (Reksa dana, Saham, Emas) with its count, gain and total, then the product tiles with their gains. "Modal belum diisi" replaces a partial sum.
- **Grafik** keeps the owner's composition ring of uses as the selector. Beside it, the legend lists each use with its amount and share of assets. The chosen use's contents follow as horizontal bars with gains: "Investasi per jenis" and "Per produk" for Investasi, and one bar per kantong otherwise.
- **Atur urutan** ("…") turns on reorder mode, the only place the drag hint appears. Tiles show a dashed edge and a grip. Cards move within their row and rows by their handle, by pointer, by Alt + arrows, or with move buttons on a phone. The order is saved per grouping as today.
- A tile opens a centered 560 px detail dialog (a bottom sheet on a phone). It shows the figure, a label and value list, the pockets or products, "Juga dilaporkan" and "Transaksi terakhir". Its footer has one primary action, one secondary action and the rest under "Lainnya". The add, edit, Beli, Jual, Top up, Perbarui nilai, Sesuaikan saldo, Cocokkan saldo and amount-tracking dialogs sit on the shared dialog in the SPEC look. Each sends exactly the request it sends today.
- On a phone: hero, legend, the two controls, Uang harian as a swipeable wallet (tiles 280 × ≥ 152), then the other groups one tile per row. "Perbarui harga" moves into "…". Nothing scrolls sideways at 390 or 320 px.
- An English account sees the page in plain English ("Accounts & Pockets", "Net worth", "Everyday money", "Limit left"). An Indonesian account sees neutral firm Indonesian. "This page is in Indonesian for now." no longer appears on `/accounts`.
- With "Sembunyikan saldo" every amount reads "Rp ••••••" and is announced as hidden.
- Every figure, total, gain and count equals today's value for the same data, in both languages. The intended presentation changes are listed in `verification.md`: the gain percent with one decimal, and the Utang group total being credit only.

## Approval

Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D24, summarized in "Decisions and their defaults"). No default changes money behaviour. Every figure is a server field or today's client sum of server balances (the use and kind totals, the gains from `cost_basis`), shown as today, and every action sends today's request. Seven items are flagged for review before implementation (task 1.1), because they go beyond the prototype or the phased plan:
- **D5 (owner-directed scope, deviation from the prototype):** Utang lists credit accounts only, and one "Tagihan" row links to Target & Tagihan. The prototype shows obligation tiles under Utang and an "Utang lain" row under Per rekening. The Utang group total becomes `credit_owed`; it was `total_liabilities` (credit plus obligations). Net worth is unchanged.
- **D5 (scope, earlier owner choice):** Per rekening shows each rekening's composition as words in its header ("Uang harian Rp … · Tabungan Rp …", as the prototype), and drops the stacked composition bar that `organize-accounts-by-use` added. Fallback: keep the bar under the header.
- **D11 (owner preference over the prototype and the dataviz rule):** Grafik keeps the composition ring of uses that the owner chose as a supporting view, in place of the prototype's per-use bars. The ring has one ring of uses only; the outer ring of kantong and products becomes the bars. Fallback: the prototype's bars, without a ring.
- **D12 (scope, interaction):** dragging and Alt + arrows work in "Atur urutan" mode only, as in the prototype. Outside the mode a tile only opens its detail. Today, dragging is always on at desktop width. Fallback: always-on dragging, with the hint only in the mode.
- **D8 (scope addition, no money change):** "Bayar tagihan" in a credit detail opens Catat's Perpindahan into that account with the next bill's `left_to_pay`. It does this through a new `openMovementCapture({ targetAccountId, amount })` in the workspace context, which phase 9 reuses. Nothing is posted without the owner's confirmation.
- **D6 (presentation of figures):** gains read "▲ +Rp 396.000 · +8,0%" with one decimal everywhere, including the detail, which shows two decimals today. The values are unchanged.
- **D3 (presentation of figures):** the five summary figures leave the page header (every figure once). Dana investasi, Investasi and its gain become group totals. Utang becomes the Utang group total plus the Tagihan row. "Saldo tersedia" becomes the "Dana likuid" line in the Rincian, under the glossary term `terms.availableCash`.

## Deviations from the prototype

Each is a decision in `design.md` and is captured for the owner's end review:
1. **Owner-directed:** Utang lists credit accounts only, plus one "Tagihan" row linking to Target & Tagihan, and Per rekening has no "Utang lain" row (D5).
2. **Owner-directed:** Grafik's per-use view is the composition ring with its legend, not horizontal bars (D11).
3. The credit tile follows H8: owed, "Sisa limit" or "Melewati limit", and the next bill. The prototype shows "terpakai dari limit" and "bayar tiap tgl 5" (D7).
4. The credit detail has no "Minimal bayar", because the API has no minimum payment. "Tanggal cetak" reads "Tagihan terbit tiap tgl" (the credit glossary), and the detail adds the bill list of H8 (D8).
5. A tile may carry one quiet note line: "Melewati limit", "Perlu dicocokkan" or "N pindah saldo belum lengkap". The prototype has none, but the app keeps these states (D6).
6. The institution mark uses the account's own display colour and initials, because the app has no institution field. Initials are white when they reach 4.5:1 and ink otherwise (D6).
7. The account form keeps the app's nine colours, the investment model fields and the default pocket. The prototype shows six colours and fewer fields (D9).
8. Beli and Jual keep the date and note fields and have no "Biaya" field, because the trade request has no fee (D10).
9. The "…" item "Rencana" opens the Rencana dialog in place instead of linking to Target & Tagihan (D2).
10. On a phone, reorder mode uses move buttons, because touch drag is out of scope since `organize-accounts-by-use` (D12).
11. The detail keeps "Juga dilaporkan" and "Buka {induk}", and an investment platform's detail keeps its group gain (D8).
12. **Owner rule:** expense amounts in "Transaksi terakhir" are red, through phase 5's shared `RowAmount` (D8).
13. Cocokkan saldo keeps the server's causes with their links and the "Periksa lagi" action (D10).

## What Changes

- **Title block:** "Perbarui harga" (secondary; inside "…" on a phone), "Tambah rekening" (secondary) and "…" with "Atur urutan" and "Rencana" (D2).
- **Hero:** shared `HeroFigure` with "Kekayaan bersih" (`net_worth`), the quiet "Rincian" opening the shared 480 px `ExplanationDialog` with the server's `lines`. The line reads "dari aset {total_assets}". Below it, the shared `SplitBar` of the four asset uses and a legend with percentages. On failure, the error box "Kekayaan bersih belum dapat dihitung" with "Coba lagi", and no Rp 0 (D3).
- **Controls:** SPEC segmented controls "Kartu · Grafik" (kept in the URL as `?tampilan=grafik`) and "Per kegunaan · Per rekening" (kept per browser as today, shown on Kartu only) (D4).
- **Groups and tiles:** `.grp` with a 2 px rail in `--rail`, `.grp-h` (label, count, gain, total), `.tiles` `repeat(auto-fill, minmax(240px, 1fr))` gap 16, groups 48 apart. Shared `Tile` with `InstitutionMark`, names, balance, `GainText` and one optional note line. Investasi gets per-kind lines; Per rekening headers get per-use subtotals in words (D5, D6).
- **Utang without Tagihan (owner request):** credit tiles only; one "Tagihan" link row when `total_liabilities − credit_owed > 0`; the group total is `−credit_owed`. `Bill`, `BILLS_ROW`, `billsOf`, `billsIn`, `BillCard` and `BillsRow` are removed (D5).
- **Credit (H8):** the credit tile and the credit detail, reading `GET /api/accounts` (`credit_owed`, `limit_left`, `over_limit`, `next_due`, `statement_day`, `payment_day`) and `GET /api/credit/accounts/{id}` (`bills[]` with `items[]`, `totals`). The account form gains `statement_day` (D7, D8, D9).
- **Detail dialog:** `AccountDetail` moves from `Modal` to `ShellDialog` at 560 px, as a standalone `AccountDetailDialog` that loads its own data, so Target & Tagihan (phase 9) can open it. The footer follows one primary, one secondary and the rest in "Lainnya" per account kind (D8).
- **Forms and money dialogs:** the add and edit account form and the add pocket form at 560, with Jenis as a segmented control; Beli and Jual at 520; Top up at 520 (`InvestmentTopupModal`, handed over by phase 5); Perbarui nilai, Sesuaikan saldo and the amount-tracking switch at 520; Cocokkan saldo at 560. All move onto `ShellDialog` with the SPEC fields and send unchanged requests (D9, D10).
- **Grafik:** the ring of uses (one ring, 200 px) with the legend as a toggle group with the straight selection bar. Below it, the chosen use's contents as `.hbar` rows (`200px 1fr 136px 136px`, min-h 48) with gains. Rows that lead to a product or kantong open its detail (D11).
- **Atur urutan:** the SPEC selection bar with the one-line hint and "Selesai". Dashed tiles with grips and row handles. Pointer drag, Alt + arrows and phone move buttons. Today's layout API and the "Urutan gagal disimpan" restore are unchanged (D12).
- **Phone:** viewport media queries replace the container measure `useWidthAtMost(rootRef, 767)`. Uang harian becomes a `.wallet` scroll-snap row, and the dialogs become sheets (D13).
- **States:** skeleton hero and tiles with `aria-busy`, the SPEC error box, and the empty state "Belum ada rekening" with "Tambah rekening" (D14).
- **Prices:** "Perbarui harga" reports through a toast "Harga diperbarui · N produk, per 14.32" or "Harga belum dapat diperbarui", replacing the inline notice (D15).
- **Language:**
  - an `accounts` namespace in `messages/en.json` and `id.json` (copy in design Appendix A);
  - new `terms` keys `debt`, `limitLeft`, `overLimit`, `leftToPay`, `everydayMoney` and `billStatus.*`, which phase 9 reuses;
  - `/accounts` added to `TRANSLATED_ROUTES`, and its entry removed from `untranslatedPages.ts`;
  - the copy register roots extended (D16).
- **Shared:** `ProgressBar` (`.prog`, 4 px and 8 px) and `GainText` in `components/workspace/figures.tsx`, `components/accounts/credit.ts` (`creditFacts`, the bill status labels) and `openMovementCapture` in the workspace context, each with a named phase 9 user (D18).
- **Held branch `fix/selection-bars-and-tinjau` (`64909ee`):** its Rekening, Grafik, Tagihan and Utang parts are taken here (table in D19). The branch and its worktree are deleted after this phase merges.
- **Removed** (each with its destination in D20):
  - `WalletSummary` with its five-figure `dl`, `MiniRing` and `RekeningComposition`;
  - the outer ring and the focus list of `WalletChart`;
  - the obligation cards and row;
  - the coloured card top edge (`--card-accent`, `.cardDebt::before`), the use tags and the `prov` logos on cards;
  - the always-visible drag hint and `ReorderPanel`;
  - the Kartu/Grafik `role=tablist`;
  - `GainChip`, replaced by `GainText`;
  - the six-button action row;
  - the Rekening-only rules of `wallet.module.css` and `money.module.css`;
  - `ExplainableFigure` on Rekening;
  - the `useWidthAtMost` call.
- **BREAKING (spec):**
  - `accounts-wallet-view` rewrites its three requirements and adds the hero, groups, Utang, Grafik, detail, states and language requirements.
  - `net-worth-analytics` replaces "Asset Composition on Rekening" (the Komposisi aset card) with the split bar by use, and changes the gain format.
  - `pocket-reordering` drops "Account Card Alignment and Compact Height".
  - `clean-core-ledger`, `account-reconciliation`, `credit-accounts` and `mobile-native-layout` change the requirements that describe this page.

## Non-goals

- No change to any API field, request, figure, total, gain rule, role rule, schema, data or the Android companion. `backend/`, `db/` and `mobile/` are untouched. Every field this phase reads exists once `add-credit-spending-flow` has merged.
- No change to what Kekayaan bersih counts: obligations stay subtracted, and parent and child balances are counted once (`buildHoldings`, `net_worth_breakdown`). No change to Ketahanan Dana or its deduplication.
- No new roles, no second role per account, no tags, no touch drag.
- No Target & Tagihan page body (phase 9): the obligations keep their current dialogs there, and the "Tagihan" row only links to them. The Rencana dialog keeps its current body until phase 9 rebuilds it.
- No Catat redesign (phase 11). "Bayar tagihan" opens today's Perpindahan form with two prefilled values.
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `accounts-wallet-view`:
  - the page layout, capital gain and saved layout requirements are rewritten for the Markas+ page;
  - added: "Rekening Hero", "Rekening Groups and Tiles", "Utang Without Obligations", "Rekening Grafik", "Atur Urutan Mode", "Account Detail Dialog", "Rekening Loading, Failure and Empty States" and "Rekening in Both Languages".
- `net-worth-analytics`: "Net Worth Endpoint and Display" (the hero's failure state), "Asset Composition on Rekening" (the split bar by use) and "Investment Gain in the Account List" (the gain format, per kind and per group).
- `pocket-reordering`:
  - "Sibling Pocket Drag-and-Drop" becomes the pocket order inside Atur urutan;
  - removed: "Account Card Alignment and Compact Height" (the two-column cards no longer exist).
- `clean-core-ledger`: "Account and Pocket Action Clarity" (one primary, one secondary, "Lainnya") and "Parent Balance Aggregation and Grouped Representation" (item 4, the tiles).
- `account-reconciliation`: "Reconciliation-Required Flag on the Accounts Page" (the tile note and the detail).
- `credit-accounts`:
  - "Credit Account Type": the tile scenario, over the `add-credit-spending-flow` version, which merges first;
  - added, H8 verbatim: "Credit Account Tile", "Credit Account Detail" and "Credit Days in the Account Form".
- `mobile-native-layout`: "Phone Rekening & Kantong Composition".

`account-roles`, `investment-trade-recording`, `investment-topups`, `investment-amount-conversion`, `interface-language` and `ui-copy-register` need no delta: their behaviour is unchanged, and adding `/accounts` to the translated routes follows the rule that the untranslated set only shrinks.

Standards:
- UI and accessibility (WCAG 2.2 AA):
  - landmarks and headings;
  - focus order, and focus return through every dialog and the phone sheet;
  - focus not obscured by the sticky top bar or the reorder bar;
  - target size 44 px on the phone;
  - reflow at 320 px;
  - non-colour cues: ▲/▼, the sign and a hidden "untung/rugi" on gains, words for over-limit and bill status;
  - keyboard reorder;
  - reduced motion;
  - 3.1.1 and 3.1.2 (language of the page and of parts).
- The copy register in both languages (`ui-copy-register`).
- `money`: figures identical before and after and in English and Indonesian; parent and child never double-counted; Utang and the Tagihan row add up to `total_liabilities`.
- `api-consumers`: no API change; the companion is unaffected.
- `personal-data`: no rendered text or user data in logs.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Rekening:**
  - `components/accounts/`:
    - `AccountsView.tsx` (split into the page, `RekeningHero`, `RekeningControls`, `RekeningGroups` and `ReorderBar`);
    - `WalletRails.tsx`, rebuilt as groups and tiles;
    - `WalletChart.tsx`, rebuilt as the ring and bars;
    - `WalletSummary.tsx`, `ReorderPanel.tsx` and `GainChip.tsx`, removed;
    - `AccountDetail.tsx`, which becomes `AccountDetailDialog.tsx`;
    - `AccountDialogs.tsx`, `BalanceCheckDialog.tsx`, `AccountLinks.tsx`, `RoleField.tsx`, `walletParts.tsx`, `wallet.ts` (the bills removed), `accountFacts.ts` and `useWalletLayout.ts`;
    - new `credit.ts`, `CreditBills.tsx` and `Tile.tsx`;
    - `wallet.module.css` and `money.module.css` (Rekening rules removed; the Target rules stay for phase 9).
  - `components/ui/InvestmentTradeModal.tsx`, `InvestmentTopupModal.tsx` and `AmountTrackingSwitchModal.tsx`, moved onto `ShellDialog`.
  - `app/accounts/page.test.tsx` and the tests beside the dialogs.
- **Shared:**
  - `components/workspace/figures.tsx` (`ProgressBar`, `GainText`) and `context.tsx` (`openMovementCapture`);
  - `components/layout/AppLayout.tsx`: the movement defaults gain an amount;
  - `app/globals.css`: `.tiles`, `.tile`, `.inst`, `.gain`, `.grp`, `.grp-h`, `.reorder`, `.parent-block`, `.hbar`, `.prog`, `.wallet` and `.ring-legend` from `styles.css`, with the phase 1 token names;
  - `app/selectionMarks.test.ts`: the WalletChart source assertion moves to the new legend;
  - `app/komponen/ComponentSheet.tsx`: the tile, the group and the bar samples.
- **Language:**
  - `messages/en.json` and `id.json` (`accounts`, the new `terms`);
  - `lib/i18n/translatedRoutes.ts` and `components/workspace/untranslatedPages.ts`;
  - `copyRegister.test.ts` roots;
  - `workspace.test.tsx`'s untranslated `/accounts` subtitle example, which moves to `/goals`.
- **Tooling (local only):** `scripts/verify/markas/`:
  - the Rekening scenarios in both scenario files;
  - `rekening-checklist.js` for `measure.js`;
  - the keyboard flows `rekening-keys.mjs`;
  - the parity seed extended with credit accounts and bills, platforms with gains and a missing cost basis, obligations, a reconciliation flag and an awaiting movement;
  - the stress and USD seed for Rekening.
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit; the saved layouts are read and written as today.
- **Dependencies:** `add-language-system` and `localize-server-text`, `apply-markas-transaksi` (phase 5, shared `RowAmount` and the `ShellDialog` dialog conventions) and `add-credit-spending-flow` (H8 fields) must be merged first. Phase 9 (`apply-markas-target`) builds on `ProgressBar`, `creditFacts`, the new `terms`, `AccountDetailDialog` and `openMovementCapture`.
