## 1. Approval, dependencies and baseline

- [ ] 1.1 Record the approval and the flagged review:
  - in `proposal.md` ("Approved 2026-11-06 under the owner's standing instruction …", already written) and in `PROJECT_STATE.md`, with the date;
  - the owner feedback of 2026-11-06 (already in `proposal.md` "Owner feedback"), in `PROJECT_STATE.md` with the date;
  - the flag decisions of 2026-11-06 (already in `design.md` "Flag decisions" and the spec delta): every default taken, with D27's labels shown only where Beranda counts (`dana_investasi` and saving-to-product "—", tabungan-to-credit "Diambil dari tabungan", "Bayar utang" informational) and `to_investment` removed;
  - the credit amendment of 2026-11-08 (H5 built here, D30) and the stacked base (D26 amended) in `proposal.md`, `design.md` and the spec delta.

  Evidence: both files name the dates and the decisions; `openspec validate apply-markas-transaksi --strict` passes after the edits.
- [ ] 1.2 Confirm the dependencies and update the branch (D26). `apply-markas-tinjau`, `add-language-system` and `localize-server-text` must be merged into `main`; stop and report if one is not. Then update `feat/markas-transaksi` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for the three merge commits;
  - `components/workspace/rail.tsx`, `ListRow`'s `rowKey`, `frontend/src/messages/en.json`, `lib/format.ts` and `lib/i18n/translatedRoutes.ts` exist;
  - `PATCH /movements/{id}` with equal effective accounts returns `{code, params, message}` in a backend test of `localize-server-text`, and the code is listed in `errors` of both web catalogues; otherwise stop and report the gap (D16);
  - an English Transaksi shows "This page is in Indonesian for now.";
  - every difference between phase 4's merged API and design D22 is written to `verification.md`;
  - `openspec validate --all --strict` passes on the updated branch, and the revision is recorded in `verification.md`.
- [ ] 1.3 Capture the "before" state on that `main` revision, tied to its hash:
  - Transaksi with the parity seed and the audit seed, in `en` and `id`, at 1440, 1280, 1200, 1199, 1100, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances;
  - every D24 scenario that exists before the change (the dialog, its delete confirmation, the edit, the movement edit, the filter popover, the merge selection, loading, failure and empty);
  - Beranda, Analisis › Kalender and Peta Aliran with the dialog open, the Analisis and Laporan headers, and the Rekening detail of one pocket (the pages whose shared parts change, D22);
  - `parity-before.json`: every count, amount and date on Transaksi, in each day heading and in the dialog of one row per kind, plus Beranda's "Ditabung & investasi" and its four parts from `GET /dashboard/cycle-summary` for the open cycle, the previous cycle and the current month;
  - the `GET /transactions` response time at the 95th percentile on the stress seed, for D27's query-cost check.

  Evidence: captures and the JSON in `research/evidence/apply-markas-transaksi/before/`; the Indonesian rendered-text snapshot `copy-before-id.json` for task 4.4.
- [ ] 1.4 Extend the seeds (D24), local and disposable only:
  - `seed_parity.py` gains a transfer with a linked fee row, a converted top-up, a reimbursement income with an offset, a habit-recorded row, a row with a receipt path, and a day with more than 50 rows;
  - one transfer per `saving_flow` case of D27 (harian → tabungan, tabungan → harian, harian → goal-linked pocket, RDN cash → product top-up, harian → dana_investasi, tabungan → product top-up, harian → credit, tabungan → credit, a Beli trade, harian → harian, each with and without a note), an expense paid from a tabungan pocket, a saving-pillar expense, and a reimbursement offsetting the saving pillar;
  - a stress and USD seed: the audit seed (17 pockets, long pocket routes), more than 1,000 rows, 60-character merchants and notes, Rp 1.234.567.890, and the USD display currency.

  Evidence:
  - `test_seed_parity.py` passes;
  - the seed report lists every addition and every skip with its reason;
  - `GET /api/transactions?include=daily_totals,type_counts` on the seeded database returns a row with `movement_fee`, one with `fee_movement`, one with `investment_topup_id`, one with `receipt_path`, and a day whose `count` exceeds the first page;
  - `git diff --stat origin/main -- db mobile` is empty (the seed scripts live in `scripts/verify/markas/`).
- [ ] 1.5 Add the Transaksi scenarios of D24 to `scenarios-app.json` and `scenarios-prototype.json` (`en` and `id` for the app), `transaksi-checklist.js` for `measure.js`, and `transaksi-keys.mjs`. Evidence: a dry run over the prototype at 1440, 1100, 1024 and 390 in both themes writes captures and a checklist JSON whose expected values equal D1 to D15 and the prototype's computed values; the run records the prototype's own overflow at 1024 and 1100 px that motivates deviation 1; `transaksi-keys.mjs --dry-run` lists the fifteen flows of D24.

Expected Result: the dependencies are on the branch, every row kind, saving direction and dialog can be seeded and captured, and the pre-change state is recorded in both languages.

## 2. Backend additions (read-only, flagged D27 and D28)

- [ ] 2.1 Move the savings predicates (`_SAVINGS_DESTINATION_SQL`, `_PRODUCT_SQL`, the top-up and legacy-saving rule) from `routers/dashboard.py` into `services/saving_flow.py`, and make `get_kakeibo_breakdown` read them (D27). Evidence:
  - the existing Kakeibo, cycle-summary, dashboard and `kakeibo-reconciliation` tests pass unchanged on migrated disposable PostgreSQL;
  - `GET /dashboard/cycle-summary` on the parity seed returns byte-identical JSON before and after the move, for the open cycle, the previous cycle and the current month;
  - `git diff origin/main -- backend/app/routers/dashboard.py` shows only the moved definitions and their imports.
- [ ] 2.2 Add `saving_flow` to the rows of `GET /transactions` and `GET /transactions/{id}` (D27). Evidence: pytest cases, one per value of the D27 table, including the three decided cases (into `dana_investasi` → `null`; tabungan → product top-up → `null`; tabungan → credit → `taken`), harian → credit → `debt_payment` and not in `type=saving`, both legs carrying the same value, a consolidated row, an account-filtered leg, a goal-linked harian pocket (`saved`), a Beli trade (`null`), an expense paid from savings with the saving pillar (`paid_from_savings`), another owner's accounts never read; spec scenarios of "Ledger Saving Flow", including "Move inside savings and investments" and "Aggregate unchanged" (`saved` + `invested` = `saving_into`, `taken` = `saving_out`); every existing field of the response unchanged (snapshot of the parity seed's first page without the new key).
- [ ] 2.3 Add `type=saving`, `type_counts.saving` and the summary's `saving_net` (D28). Evidence: pytest cases for the spec scenarios "Savings filter equals Beranda", "Savings net only without other filters", "Unknown type" and "Tab counts"; `saving_net` equal to `cycle_summary` `tabungan.total` for the open cycle, the previous cycle, the current month and all time on the parity seed, including the reimbursement that offsets the saving pillar; `logical_movements=true` keeps one row per movement for `type=saving`; the other `type` values and their counts unchanged.
- [ ] 2.4 Add `type=credit` and `type_counts.credit` (H5, D30): rows on a credit account and the cash legs of movements into or out of one; `logical_movements` keeps one row per movement for it. Evidence: pytest cases for the spec scenarios "Credit type filter" and "Tab counts" (a purchase, a fee, a refund and a payment listed; an ordinary expense not listed; another owner's rows never read); the other `type` values and their counts unchanged.
- [ ] 2.5 Check cost and safety. Evidence: the `GET /transactions` 95th-percentile time on the stress seed against task 1.3, with any rise over 50 ms reported; the full backend suite passes on migrated disposable PostgreSQL, with the count recorded in `verification.md`; `git diff --stat origin/main -- db mobile` is empty and there is no migration file.

Expected Result: the server reports each row's saving direction and the "Tabungan & investasi" list and total by the same rule as Beranda, with every existing figure unchanged.

## 3. Shared pieces

- [ ] 3.1 Extend `ListRow` with `selectable` (label, checkbox, `checked`, `disabled`, `onToggle`) and use phase 4's `rowKey` and `selected` (D9, D22). Evidence:
  - `primitives.test.tsx` cases: the selectable row is one `label` whose checkbox toggles from anywhere on the row; a disabled row has a disabled checkbox at 45 % opacity; `data-row` is set; `data-selected` gives `--surface-2` and the 2 px full-height bar;
  - the Beranda and Tinjau row tests pass unchanged, and `selectionMarks.test.ts` passes;
  - the `/komponen` sheet shows the full row at rest, hover, pressed, selected, focus and selectable in both themes, and the checklist values (min-h 56, padding 0 12, gap 16, columns 40 · 168 · 136 · ≥ 120) pass there.
- [ ] 3.2 Move the row helpers and one shared `RowAmount` and `RowStatus` to `components/transactions/rows.tsx` (D11, D22, D29). Remove `RowAvatar`, `AVATAR`, the old `RowAmount`, `ActivityAmount` and Perlu tindakan's own amount. Point Beranda (Aktivitas terbaru, Perlu tindakan), `AccountDetail.tsx`, Kalender and the dialog to the new module. Evidence:
  - `activity.test.ts` (moved to `rows.test.ts`) keeps every existing case except the ink expense and the movement title, and adds: the four amount variants with their colour classes (expense `c-expense`), the note-first transfer title with the route as meta and the route title without a note, the placeholder note treated as no note (read from the backend default), the top-up title "Top up {product}" and its fallback, each `saving_flow` label of D27 and "—", the category words of D11, and the waiting status with the warning flag;
  - the Beranda tests pass with the intended changes only (red expenses, transfer titles and saving directions), and the `beranda-1440-light` pixel compare differs only in those;
  - the Rekening detail capture shows red expenses and no other change against task 1.3.
- [ ] 3.3 Extend `HeroFigure` with `aside` and an optional `line` (`.hero.is-split`), and `FigureBlock` with `bar={false}` (D4, D22). Evidence: component tests for the split hero (flex gap 24 48, wrap, margin-bottom 32) and the bar-less block (no `sw-bar` element); the Beranda and Tinjau hero tests pass unchanged; the checklist passes on the `/komponen` sheet in both themes.
- [ ] 3.4 Add `pressed` to `RailItem` (D5). Evidence: a component test shows `aria-pressed="true"`, the `--s1` fill and the 2 × 20 px bar at `left: -8px; top: 8px` for a pressed item; the Tinjau rail tests pass unchanged; `selectionMarks.test.ts` covers the pressed item.
- [ ] 3.5 Restyle `PeriodStepper` to SPEC `.stepper` with the kept reset (D3). Evidence:
  - `PeriodStepper` tests: a labelled group; 36 × 36 icon buttons (44 at 390 px) with their `common.period.*` names; the label 14/500 with `min-width: 168px` and `aria-live`; next disabled at offset 0 and previous at −36; the reset after a step back;
  - spec scenarios "Cycle range" and "Stepper look";
  - the Analisis and Laporan header captures differ from task 1.3 only in the stepper, and their figures are unchanged.
- [ ] 3.6 Add the CSS of D22 from `styles.css` with the phase 1 token names (`.tx`, `.tx-rail`, `.tx-types`, `.toolbar`, `.search`, `.chips`, `.chip`, `.sel-bar`, `.stepper`, `.dl`, `.box`, `.form-grid` where missing), `.c-expense` and the 136 px pillar column. Evidence: the `/komponen` sheet shows each part in both themes, and the checklist values for each pass there; `selectionMarks.test.ts` passes.
- [ ] 3.7 Pin the expense ink (D29). Evidence: `tokens.test.ts` adds `--canvas` to the `--out-fg` pairs and passes at ≥ 4.5:1 on `--canvas`, `--window`, `--surface`, `--surface-2` and `--dialog` in both themes (measured 5.16–6.17:1 light, 5.42–6.62:1 dark); a test asserts that `.c-expense` and `RowAmount`'s expense variant resolve to `var(--out-fg)`; the spec scenario "Expense ink token test" passes; a planted lighter value fails the test.

Expected Result: every piece Transaksi needs exists once in its shared place, is measured on the component sheet, and leaves Beranda and Tinjau unchanged.

## 4. Language

- [ ] 4.1 Add the `transactions` namespace (with `flow.*` and the savings hero keys), `terms.net`, `terms.calendarMonth`, `terms.savingsAndInvestments` and the movement and merge error codes to `messages/en.json` and `id.json` (D20, Appendix A). Move the row keys from `home` to `transactions.row` with Indonesian unchanged, and remove `common.rowMarks.needsReview` and `automatic`. Write the full key list to `research/evidence/apply-markas-transaksi/copy-keys.md`. Evidence:
  - the catalogue parity test passes (same keys, same arguments, `one`/`other` in English);
  - `npm run type-check` fails on a planted missing key and passes without it;
  - the Beranda copy snapshot is unchanged after the row keys move.
- [ ] 4.2 Move every Transaksi string to keys, and every date, time, amount and range to `useFormat()`. This covers the page, `rows.tsx`, the dialogs, `lib/ledgerDays.ts`, `lib/movementFormSchema.ts`, `useTransactionUndo.ts` and `ExpenseTopupConversionModal.tsx`. Evidence:
  - `ledgerDays.test.ts`, `ledgerFilters.test.ts` and `ledgerMovements.test.ts` keep their data results;
  - `rg 'id-ID|Intl\.DateTimeFormat' frontend/src/app/ledger frontend/src/components/transactions frontend/src/lib/ledgerDays.ts` finds nothing (the `en-CA` day key of `jakartaDay` is the only exception, recorded);
  - formatter cases for `id-ID`, `en-ID` and `en-US` render a row's "12.37" / "12:37 PM", the stepper's "Siklus 23 Okt – 24 Nov" / "Cycle 23 Oct – 24 Nov", and the dialog's "Rabu, 4 November pukul 09.12" / "Wednesday, 4 November at 09.12".
- [ ] 4.3 Add `/ledger` to `lib/i18n/translatedRoutes.ts` and remove it from `components/workspace/untranslatedPages.ts`, with the title block reading Transaksi's title from the catalogue (amended 2026-11-08). Move `add-language-system`'s untranslated-page test example from Transaksi to Analisis (`/insights`). Add the roots of D20 to the copy register's translated roots. Evidence:
  - an English Transaksi and each of its dialogs have no "This page is in Indonesian for now." line and no `lang="id"` region;
  - an English Analisis still has both;
  - `InvestmentTopupModal` opened from an English Transaksi carries `lang="id"` and the notice in its header;
  - the copy register passes in both languages, and one planted fixture per language fails: an Indonesian "Belanja" figure label in `transactions`, and an English "Expenses" or "Ledger" label in `transactions`.
- [ ] 4.4 Check the copy. Evidence:
  - the Indonesian rendered-text snapshot after the change equals `copy-before-id.json` for every unchanged key;
  - the differences are exactly the intended ones of D20, listed in `verification.md`;
  - every English string is checked against Appendix B, and no English label uses "Expense", "Spent", "Outflow", "Inflow", "Ledger", "you/your" or slang.

Expected Result: Transaksi's copy comes from the catalogues in both languages, Indonesian reads as today except the listed changes, and `/ledger` is a translated route.

## 5. Transaksi page and dialogs

- [ ] 5.1 Compose the page and its breakpoints (D1, D2): the title block with the stepper and the "…" menu, the hero, `.tx` with the 1200 px and 768 px viewport queries. Remove Transaksi's `useWidthAtMost`, `PHONE_LIST_WIDTH` and `RESIZE_SETTLE_MS`, and "Rutin" (or the fallback of task 1.1). Evidence: `page.test.tsx` covers:
  - one `h1`, DOM order title → hero → rail → toolbar → list;
  - the "…" menu with "Gabungkan dua transaksi" / "Batal gabungkan" and "Filter transaksi", arrow keys, Esc and focus return;
  - spec scenarios "Laptop width keeps the desktop rows" (1024 and 1100 px mocked) and "Rail from 1200 px";
  - a regression test that at a mocked 1024 px viewport with a 456 px list region the desktop rows render (the old width rule's failing case).
- [ ] 5.2 Build the hero (D4). Evidence: `page.test.tsx` covers spec scenarios "Cycle figures", "Saved part named", "Older API without spending" and "Phone hero", the reimbursement line, the "· semua waktu" and "· {range}" labels, and the values taken from `summary.period` or `summary.cumulative` as today.
- [ ] 5.3 Build the rail and the type control from one `LEDGER_TYPES` list with seven types, "Kredit" included (D5, D6, D28, D30). Evidence: `page.test.tsx` covers spec scenarios "Choosing a type", "Choosing a period mode", "Counts while loading", "Savings hero" and "Savings hero with another filter"; the counts equal `type_counts`, including `saving`; `ledgerFilters.test.ts` parses and writes `type=saving` and sends it to the API, and `listsLogicalMovements` keeps movements consolidated for it; a test adds a seventh entry to `LEDGER_TYPES` in a fixture and shows it in the rail and the type control without other code changes (the "Kredit" room of D30); at 1100 and 390 px the type control replaces the rail with the same counts and handlers.
- [ ] 5.4 Build the toolbar, chips and the Filter dialog (D7, D8), and remove `FilterPopover`, `LEDGER_CHIPS`, `chipOf` and `withChip`. Evidence:
  - `page.test.tsx` covers spec scenarios "Searching transactions by note", "Filtering transactions by account", "Apply several filters at once", "Cancel keeps the filters", "Duplicate filter from Laporan" and "Period below 1200 px";
  - chip removal by keyboard moves focus to the next chip or the search field;
  - `ledgerFilters.test.ts` keeps every parse, URL and API case;
  - every deep link of D19 opens with its filters as chips, including `?type=expense&kakeibo=unclassified` and `?from=&to=&q=`.
- [ ] 5.5 Build the list: day groups, rows, loading more and the end line (D10, D11, D12). Rewrite `LedgerList.tsx` on `ListRow` and `DayHeading`. Evidence: `page.test.tsx` covers:
  - spec scenarios "Day heading covers the whole day", "Waiting row is counted in its day", "Day with every kind", "Day without totals from an older API", "Expense shows its category", "Movement has no category", "Row without a category", "Amount carries the kind", "Saving direction fits", "Transfer with a note", "Placeholder note", "Load more of one day on a phone", "Load more announces the rows" and "Load more fails";
  - a credit purchase row reads "dibayar nanti · {provider} · jatuh tempo {date}" with its amount red, and a credit payment row adds "bayar tagihan" (D30);
  - the movement scenarios of "Consolidated Internal Movement Ledger Representation" that name the row (routes, inferred pairs, account directions, unlinked legs);
  - masking with hidden balances on rows and day totals;
  - one `button` per row with `data-row`, and the row whose dialog is open marked `data-selected`.
- [ ] 5.6 Build the merge selection and its bar (D9). Evidence: `page.test.tsx` covers spec scenarios "Selecting transactions from the menu", "Selecting transactions by holding a ledger row", "Keeping durable movement status during selection", "Converting two equal-value transactions directly", "Rejecting unequal transaction amounts before conversion", "Pindah saldo needs a pair" and "Merge on a phone"; the bar is sticky under the top bar while selecting; the request-level merge tests pass with the same `POST /movements/merge` body.
- [ ] 5.7 Build the transaction dialog and remove the drawer use (D14). Point Beranda, Kalender and Peta Aliran to `TransactionDialog`. Evidence:
  - `page.test.tsx` and dialog tests cover spec scenarios "Detail opens centered on a wide window", "Drawer on a phone", "Waiting row points to Tinjau", "Delete asks first", "Deleting a top-up row", "Expense amount in the dialog", "Movement with a fee", the transfer's "Tabungan & investasi" row for each `saving_flow` value, "Inbound leg titled in its direction", "Reviewing an automatic row" and "Confirm and undo";
  - the `.dl` rows per kind, the "Bukti" states, the original-text box and the missing and failed states;
  - every existing request-level drawer test passes with the same requests (`/review/bulk`, the three delete paths);
  - axe reports no violation with the dialog open at 1440 and 390 px.
- [ ] 5.8 Build "Ubah transaksi" (D15) from `TransactionEditForm`. Evidence:
  - tests cover spec scenarios "Modifying transaction amount", "Edit returns to the detail", "Editing the merchant", "Editing a timestamp with seconds", "Editing an allocated payment", "Correcting an incomplete allocation", "Convert to a top-up" and "Not eligible for a top-up";
  - the existing `TransactionEditForm.test.tsx` cases pass against the dialog with the same `PATCH` bodies and receipt upload;
  - the Pilar segment and its hint, with no `InfoHelp` icon.
- [ ] 5.9 Build the movement mode and the top-up conversion dialog (D16, D17). Remove `InternalMovementModal.tsx` and its test after moving every case. Evidence:
  - the moved cases pass: `PATCH /movements/{id}` with both accounts, split with its confirmation, the same-account and credit-on-both-sides checks;
  - spec scenarios "Atomically editing a movement" and "Movement edit error in the interface language" (a planted server code renders in `id` and in `en`, and the English server text never appears in the Indonesian interface);
  - the conversion dialog test sends today's conversion request and records "Dicatat sebagai top up investasi." with Urungkan.
- [ ] 5.10 Build the states and keep keys, focus and URL (D13, D18, D19). Evidence:
  - `page.test.tsx` covers spec scenarios "Loading", "Failure and retry", "Nothing recorded yet", "Empty result", "Walk the list", "Typing in search", "Switched off", "Walk while selecting", "Back to the same item", "Item resolved meanwhile" and "Tampered return";
  - focus after a delete, after "Terapkan" and after closing the edit, as D18;
  - the existing `fokus=cari`, due-plan and period-sharing tests pass unchanged;
  - a test shows `scroll-padding-top` keeps a focused row below the top bar, a day heading and the selection bar.
- [ ] 5.11 Remove what the change made obsolete (D22, D23). Evidence:
  - `rg` finds no `KpiCard`, `ws-kcard`, `LedgerTable`, `LedgerFeed`, `KIND_PILL`, `RowAvatar`, `ActivityAmount`, `FilterPopover`, `LEDGER_CHIPS`, `chipOf`, `withChip`, `InternalMovementModal`, `TransactionDrawer`, `LedgerRowMarks`, `ReviewStatusMark`, `HabitMark`, `feeLinkText`, `PHONE_LIST_WIDTH` or any removed `ws-tx-*` rule;
  - `rg "Tidak dihitung"` finds no transfer or top-up label path (income keeps "Uang masuk");
  - `.ws-kk`, `.ws-ph-list`, `.ws-ph-row`, `.ws-amt-*`, `.ws-ava` and `.ws-tone-*` are removed, or each is listed with its remaining user;
  - each class kept in D22 (`.ws-tx-error`, `.ws-tx-actions`, `.ws-chip`, `.ws-tbl`, `.ws-who`, `.ws-seg`) still has a user;
  - `npm test`, `npm run type-check` and `npm run lint` pass.

- [ ] 5.12 Build the credit views of H5 (D30): the provider chooser under "Kredit", the provider header from `GET /api/credit/accounts/{id}`, the "Kredit" group in the Filter dialog's Rekening select, the credit facts and instalments in the transaction dialog, and "Cara bayar" in "Ubah transaksi" with `PUT`/`DELETE …/credit-terms`. Evidence: tests for the spec scenarios "Credit purchase meta", "Credit payment meta", "Provider view" and "Change the instalments"; the terms requests carry exactly `installment_count` and `first_due_date`; a parity check shows the header figures equal the endpoint's.

Expected Result: Transaksi and its dialogs are built from the shared pieces, every requirement in the deltas has a passing test, every action sends today's request, and nothing obsolete remains.

## 6. Verification gate

- [ ] 6.1 Side by side, on the parity seed: prototype and app captures of every D24 scenario at 1440 × 900, 1100 × 800, 1024 × 768 and 390 × 844, light and dark, the app in `en` and `id`, plus composed left/right images. Add the overflow widths 1280, 1200, 1199, 768, 767 and 320, and the after-captures of the shared-part pages of task 1.3. Evidence: the images in `research/evidence/apply-markas-transaksi/after/`, tied to the branch revision, with every visible difference listed in `verification.md` and each one fixed, named as a deviation of the proposal, or added to the owner's end-review list.
- [ ] 6.2 The SPEC measurement checklist and the SPEC §13 global rules (D24), in both languages. Evidence: the checklist JSON with pass and fail counts in `verification.md`. It must show zero fails, or each fail with its reason, including:
  - `.tx` columns, gap and sticky top at 1440, and one column at 1199, 1100 and 1024;
  - first content `y = 192`;
  - one 44 px figure and no other ≥ 33 px;
  - row, chip, toolbar, selection bar, day heading, rail and dialog measurements;
  - sizes, weights and ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the bars and focus;
  - no horizontal overflow at every width of 5.1;
  - the English and Indonesian pillar (136) and category (168) columns, including "Diambil dari tabungan" and "Taken from savings", rail labels and the six-item type control at 1100, 1024, 390 and 320;
  - the computed colour of every expense amount on Transaksi, the dialog, Beranda's Aktivitas terbaru and Perlu tindakan, and the Rekening detail equals `--out-fg`, at rest, hovered and selected, in both themes.
- [ ] 6.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings, in both themes and both languages, at 1440, 1100, 1024 and 390:
  - the page in cycle, month and all-time mode, with active chips, and in selection mode;
  - each dialog open (detail of each kind, delete confirmation, edit, movement edit, Filter, conversion, "Nominal tidak sama") and the "…" menu open;
  - with hidden balances;
  - in the loading, failure and both empty states.
  Zero console errors. `lang` attributes are correct (no `lang="id"` region on an English Transaksi). Results in `verification.md`.
- [ ] 6.4 Keyboard flows: `transaksi-keys.mjs` runs the fifteen flows of D24 in both languages at 1440 and 390. Evidence: every flow passes, recorded in `verification.md` with the revision.
- [ ] 6.5 Figure parity. Evidence: `parity.mjs` and a `verification.md` table with zero changed values:
  - before (task 1.3) against after, with the same seed within the same minutes: the hero and supporting figures, the type counts, each day heading's totals and remaining count, the row amounts, times and dates, and the dialog figures of one row per kind; the intended string differences ("+" on Masuk and Bersih, the merged KPI caption) are listed separately with equal values;
  - `en` against `id` (region `ID`) on the same data, byte-identical figure strings;
  - the Beranda, Kalender, Peta Aliran and Rekening detail figures of task 1.3, unchanged;
  - savings parity (owner feedback 4): for the open cycle, the previous cycle and the current month, the hero of `/ledger?type=saving&period=…` equals Beranda's "Ditabung & investasi" of task 1.3 byte for byte, in `en` and `id`, and the listed rows are exactly those with a counted `saving_flow`.
- [ ] 6.6 Stress and USD seed at 1440, 1100, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for 60-character merchants and notes, long pocket routes, Rp 1.234.567.890 and the USD display; the 1,000-row list paged to the end with each row exactly once and the end line; the CDP trace of scrolling 200 loaded rows with no main-thread task over 50 ms; recorded in `verification.md`.
- [ ] 6.7 Frontend and repository checks on the exact revision. Evidence: every result recorded in `verification.md` with the commit hash:
  - `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass;
  - the full backend suite passes on migrated disposable PostgreSQL with its count recorded;
  - `git diff --stat origin/main -- db mobile nginx` is empty, and `git diff --stat origin/main -- backend` lists only `services/saving_flow.py`, `routers/dashboard.py`, `routers/transactions.py` and their tests.
- [ ] 6.8 Complete `verification.md` and present the result. Record the owner's standing instruction of 2026-11-06 as the acceptance basis, and add the deviations, the flagged outcomes and the shared-part changes on other pages to the end-review list in `PROJECT_STATE.md`. Evidence:
  - `verification.md` names what changed, the gate counts, the deviations (including the owner-directed ones), the flag decisions of 2026-11-06, the backend additions and what was not verified and why;
  - the report to the owner quotes them.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-transaksi`, then `openspec validate --all --strict` must pass. The archive path is under `openspec/changes/archive/`.
- Commit with a subject-only Conventional Commits message, and push `feat/markas-transaksi`.
- Create the pull request into `main` (the owner's standing instruction of 2026-11-06), and report its URL with the pushed revision. The description names the additive, read-only backend fields. Merging stays with the owner.
- Record in `PROJECT_STATE.md` that `add-credit-spending-flow` starts from this phase's merged state (D30).
