## 1. Approval, dependencies and baseline

- [x] 1.1 Record the approval and the flagged review:
  - in `proposal.md` ("Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08) …", already written) and in `PROJECT_STATE.md`, with the date;
  - the review outcome of the seven flagged items (D3, D5 ×2, D6, D8, D11, D12): default, or the recorded fallback, in `design.md` "Decisions and their defaults" and the spec delta, before any code;
  - the open item "delete `fix/selection-bars-and-tinjau` and the `cash-flow-tracker-ui-refine` worktree after this phase merges" in `PROJECT_STATE.md`;
  - the phase 9 contract (`ProgressBar`, `GainText`, `credit.ts`, `AccountDetailDialog`, `openMovementCapture`, the new `terms`) in `PROJECT_STATE.md`.

  Evidence: the files name the date and each flag's outcome; `openspec validate apply-markas-rekening --strict` passes after the edits.
- [x] 1.2 Confirm the dependencies and update the branch (D24). `add-language-system`, `localize-server-text`, `apply-markas-transaksi` and `add-credit-spending-flow` must be merged into `main`; stop and report if one is not. Then update `feat/markas-rekening` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for the four merge commits;
  - `components/transactions/rows.tsx` exports `RowAmount`, and `.dl` and `.box` exist in `globals.css`;
  - `GET /api/accounts` returns `statement_day`, `limit_left` and `next_due` on a credit row, and `GET /api/credit/accounts/{id}` returns `bills[].items[]` with `days_to_settle` and `settled_on_time`, on a migrated disposable database;
  - the `credit-accounts` "Credit Account Type" text on `main` is re-copied into this change's delta, with only the tile scenario changed;
  - an English `/accounts` still shows "This page is in Indonesian for now.";
  - every difference between the merged APIs and this design is written to `verification.md`;
  - `openspec validate --all --strict` passes, and the revision is recorded in `verification.md`.
- [x] 1.3 Capture the "before" state on that `main` revision, tied to its hash:
  - Rekening with the parity seed and the audit seed, in `en` and `id`, at 1440, 1280, 1200, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances;
  - Kartu in both groupings, Grafik, the phone reorder, and every dialog of D22 that exists before the change;
  - the trade and top-up dialogs opened from Transaksi and Tinjau, because they are shared (D10);
  - `parity-before.json`: every amount, percent, count and gain on the page and in each detail, plus `net_worth`, `total_assets`, `total_liabilities` and `credit_owed` from the API.

  Evidence: captures and the JSON in `research/evidence/apply-markas-rekening/before/`; the Indonesian rendered-text snapshot `copy-before-id.json` for task 3.4.
- [x] 1.4 Extend the seeds (D22), local and disposable only:
  - `seed_parity.py` gains the credit accounts, bills, instalments and fee; the platforms with gains, losses and a missing cost basis; an amount-tracked reksa dana; two obligations; a flagged pocket; an awaiting leg; and a parent with its own balance;
  - the stress and USD seed gains 40 products, a 60-character pocket name, Rp 1.234.567.890 and the USD display currency on top of the audit seed.

  Evidence:
  - `test_seed_parity.py` passes;
  - the seed report lists every addition and every skip with its reason;
  - `GET /api/credit/accounts/{id}` on the seed returns one bill of each status the seed sets, and an item with `settled_on_time` false;
  - `git diff --stat origin/main -- backend db mobile` is empty.
- [x] 1.5 Add the Rekening scenarios of D22 to `scenarios-app.json` and `scenarios-prototype.json` (`en` and `id` for the app), `rekening-checklist.js` for `measure.js`, and `rekening-keys.mjs`. Evidence: a dry run over the prototype at 1440, 1024 and 390 in both themes writes captures and a checklist JSON whose expected values equal D1 to D14 and the prototype's computed values; `rekening-keys.mjs --dry-run` lists the fourteen flows of D22.

Expected Result: the dependencies are on the branch, every account kind, credit state and dialog can be seeded and captured, and the pre-change state is recorded in both languages.

## 2. Shared pieces

- [x] 2.1 Add `ProgressBar` and `GainText` to `components/workspace/figures.tsx` (D6, D18), with `.prog`, `.prog.is-thick` and `.gain` from `styles.css`. Evidence: unit tests for the 4 and 8 px heights, the square fill and `aria-hidden`; `GainText` renders "▲ +Rp 396.000 · +8,0%", "▼ −Rp 164.000 · −10,7%", the hidden "untung"/"rugi", "Modal belum diisi", nothing without products, and "Rp ••••••" with hidden balances; `/komponen` shows both in both themes.
- [x] 2.2 Add `components/accounts/credit.ts` (`creditFacts`, bill status labels and tones), `Tile.tsx` and `InstitutionMark` (D6, D7). Evidence: unit tests for owed, limit left, over limit, overpaid, no limit, no due day and each `next_due.status`; the initials pick white or ink by a 4.5:1 test for the nine colours in both themes.
- [x] 2.3 Add `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` to the workspace context, backed by `AppLayout`'s movement defaults plus the amount (D8). Evidence: a test that it opens the Perpindahan form with "Ke" and the amount filled and posts nothing until "Simpan"; `handleOpenMovement`'s existing callers are unchanged.
- [x] 2.4 Add the CSS of D18 to `globals.css` with the phase 1 token names, and the tile, group and bar samples to `/komponen`. Evidence: `measure.js` on `/komponen` matches the prototype `#/komponen` tile values; `selectionMarks.test.ts` passes with the legend assertion moved to the new Grafik legend.

Expected Result: the pieces phase 9 needs exist with tests, and the page can be built from them.

## 3. Language

- [x] 3.1 Add the `accounts` namespace and the new `terms` (D16, Appendix A) to `messages/en.json` and `id.json`. Evidence: catalogue parity passes; the copy register passes for both languages; no "kamu", "Anda", "Expense", "Outflow" or "Ledger".
- [x] 3.2 Move every string of `components/accounts`, `app/accounts` and the four `components/ui` money dialogs to keys, with whole ICU sentences, plurals and formatted arguments, and the server errors to `errors.<code>`. Evidence: `rg` finds no Indonesian literal in those roots outside tests; `useFormat()` formats every amount, date and percent.
- [x] 3.3 Add `/accounts` to `TRANSLATED_ROUTES`, remove its `untranslatedPages.ts` entry, add the roots to the copy register, and move the untranslated-page test examples to `/settings`. Evidence: an English `/accounts` shows "Accounts & Pockets" and no notice; `lang` is not `id` on the page; the moved test passes.
- [x] 3.4 Check the Indonesian text against today's. Evidence: the rendered-text snapshot after the change equals `copy-before-id.json` except the intended changes listed in D16, recorded in `verification.md`.

Expected Result: Rekening & Kantong and its dialogs read in either language, and the Indonesian text is unchanged except the listed intended changes.

## 4. Rekening page and dialogs

- [x] 4.1 Page structure, title actions, viewport breakpoints and the removal of `useWidthAtMost` on `/accounts` (D1, D2). Evidence: tests for the two actions, the "…" items (Atur urutan, Rencana, phone-only Perbarui harga); captures at 1024 and 767 show the desktop and phone layouts at the right widths.
- [x] 4.2 Hero, Rincian and the split bar (D3). Evidence: tests that the hero reads `net_worth`, "dari aset {total_assets}", the legend shares and the split bar's accessible name; the Rincian lists each server line with the labels of D3 and totals `net_worth`; the failure box with no Rp 0 while the groups still show; exactly one "Rincian" on the page.
- [x] 4.3 Controls (D4). Evidence: tests for `aria-pressed` segments, `?tampilan=grafik` written and read, the grouping kept per browser and Per kegunaan when storage throws, and the grouping hidden on Grafik.
- [x] 4.4 Groups, tiles and Investasi lines (D5, D6). Evidence:
  - tests for the five groups and their totals and counts, the rails' colours, the per-kind lines, and the Per rekening subtotals in words;
  - the one note line and its priority;
  - a parent counted once;
  - "Modal belum diisi" with no partial sum;
  - no `--card-accent` and no pills (`rg`).
- [x] 4.5 Utang without obligations (D5, owner request). Evidence: tests that no obligation tile renders in either grouping, the Utang total is `−credit_owed`, the Tagihan row shows the count and `total_liabilities − credit_owed` with the name "Tagihan −Rp X di Target & Tagihan" and opens `/goals`, it is hidden without obligations, and no "Utang lain" group exists.
- [x] 4.6 Credit tile (D7, H8 "Credit Account Tile"). Evidence: tests for −Rp owed, "Sisa limit", "Melewati limit", "Tanpa limit", "Lebih bayar", the next bill with "hari ini" and "lewat jatuh tempo", and "Tanggal jatuh tempo belum diisi"; the bar only with a limit.
- [x] 4.7 Account detail dialog (D8, H8 "Credit Account Detail"). Evidence:
  - tests for each kind's figure, `.dl`, pockets, products, "Juga dilaporkan" and recent transactions with the red expense;
  - the footer table of D8 per kind, "Lainnya" by keyboard;
  - the credit bills with statuses, an expanded item with "cicilan 2 dari 3", "lunas 9 Des · 35 hari · tepat waktu" and a late item;
  - the cycle facts;
  - "Lihat di Transaksi" to `/ledger?account={id}`;
  - the failure and no-due-day lines;
  - "Bayar tagihan" opening Perpindahan with the account and `left_to_pay`;
  - `?akun=` open and clear;
  - the dialog opened outside `/accounts` in a test harness;
  - no Cocokkan saldo, Sesuaikan saldo or Tambah kantong for credit.
- [x] 4.8 Account and pocket forms (D9, H8 "Credit Days in the Account Form"). Evidence: tests for Jenis as a radiogroup with today's `type` values, the role rules, the nine colours with the outline and focus ring, `statement_day` and `payment_day` sent for credit only, the range error under the field, the hint text, and the request bodies equal to today's for every other field.
- [x] 4.9 Money dialogs on `ShellDialog` (D10). Evidence: the existing tests of `InvestmentTradeModal`, `InvestmentTopupModal`, `AmountTrackingSwitchModal`, `BalanceCheckDialog`, Perbarui nilai and Sesuaikan saldo pass with identical request assertions; the widths 520 and 560; the over-sale error on the units field; Cocokkan saldo's matched and difference results with the causes and links.
- [x] 4.10 Grafik (D11). Evidence: tests for one ring with the centre total, the legend toggles with `aria-pressed` and the straight bar, the default choice, the Investasi bars per kind and per product with gains, the bars per kantong for another use, the platform rails under Per rekening, the product bar opening its detail, and the phone stacking.
- [x] 4.11 Atur urutan (D12). Evidence:
  - tests for the mode bar and "Selesai";
  - no drag or Alt move outside the mode;
  - Alt + arrows on tiles and handles with the announcement and focus kept;
  - phone move buttons with disabled boundaries;
  - the layout PUT per grouping and the restore with "Urutan gagal disimpan";
  - a pointer drag in the browser run;
  - `ReorderPanel` removed.
- [x] 4.12 Phone, states, prices and hidden balances (D13, D14, D15, D17), and the held branch's account tests adapted (D19). Evidence:
  - tests for the wallet row, one tile per row, the skeleton with `aria-busy`, both error boxes, the empty state, the price toasts, and `MaskedMoney` on every amount of D17;
  - the held branch's assertions (no bill cards, Tagihan link, straight legend bar) pass in their adapted form.
- [x] 4.13 Remove what D18 and D20 list. Evidence: `rg` finds no use of `WalletSummary`, `MiniRing`, `RekeningComposition`, `GainChip`, `ReorderPanel`, `BillCard`, `BillsRow`, `billsOf`, `billsIn`, `BILLS_ROW`, `focusGroups`, `AccountDetail` (old), the removed module classes or `PHONE_WIDTH`; `money.module.css` keeps only rules a remaining page uses.

Expected Result: Rekening & Kantong and its dialogs are rebuilt to the prototype with the recorded deviations, H8 is covered, obligations live only in Target & Tagihan, and nothing obsolete remains.

## 5. Verification gate

- [x] 5.1 Side by side, on the parity seed: prototype and app captures of every D22 scenario at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, the app in `en` and `id`, plus composed left/right images. Add the widths 1280, 1200, 768, 767 and 320. Evidence: the images in `research/evidence/apply-markas-rekening/after/`, tied to the branch revision, with every visible difference listed in `verification.md` and each one fixed, named as a deviation of the proposal, or added to the owner's end-review list.
- [x] 5.2 The SPEC measurement checklist and the §13 global rules (D22), in both languages. Evidence: the checklist JSON with pass and fail counts in `verification.md`, zero fails or each fail with its reason, including:
  - first content `y = 192`;
  - one 44 px figure and no other ≥ 33 px;
  - tiles, groups, `.hbar` and dialog geometry;
  - sizes, weights and ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the rails, bars and focus;
  - no horizontal overflow at 1440, 1024, 390 and 320;
  - the English lengths of D16.
- [x] 5.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings, in both themes and both languages, at 1440, 1024 and 390:
  - on Kartu in both groupings, Grafik and Atur urutan;
  - with each dialog and menu open, and the phone sheets;
  - with hidden balances;
  - in the loading, failure and empty states.
  Zero console errors. `lang` attributes correct. Results in `verification.md`.
- [x] 5.4 Keyboard flows: `rekening-keys.mjs` runs the fourteen flows of D22 in both languages at 1440 and 390, including drag reorder by keyboard (Alt + arrows on tiles and row handles) and the phone move buttons. Evidence: every flow passes, recorded in `verification.md` with the revision.
- [x] 5.5 Figure parity. Evidence: `parity.mjs` and a `verification.md` table:
  - before (task 1.3) against after with the same seed within the same minutes, with zero unexplained differences. The named differences are the one-decimal percent, the Utang total as `−credit_owed`, the Tagihan row, and the summary figures leaving;
  - Utang + Tagihan = `total_liabilities`, hero = `net_worth`, and legend shares = the previous share bar;
  - `en` against `id` (region `ID`) on the same data, with byte-identical figure strings.
- [x] 5.6 Stress and USD seed at 1440, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for 17 pockets, 40 products, the 60-character name, Rp 1.234.567.890 and the USD display, recorded in `verification.md`.
- [x] 5.7 Frontend and repository checks on the exact revision. Evidence: every result recorded in `verification.md` with the commit hash:
  - `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass;
  - `git diff --stat origin/main -- backend db mobile nginx` is empty.
- [x] 5.8 Complete `verification.md` and present the result. Record the owner's standing instruction (2026-11-06, renewed 2026-11-08) as the acceptance basis, and add the deviations and the flagged items to the end-review list in `PROJECT_STATE.md`. Evidence:
  - `verification.md` names what changed, the gate counts, the deviations, each flag's outcome, the held branch parts taken, the H8 coverage, and what was not verified and why;
  - the report to the owner quotes them.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation and flag recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-rekening`, then `openspec validate --all --strict` must pass. The archive path is under `openspec/changes/archive/`.
- Commit with a subject-only Conventional Commits message, and push `feat/markas-rekening`.
- Create the pull request into `main` (the owner's standing instruction), and report its URL with the pushed revision. Merging stays with the owner.
- After this phase merges, check that each "Taken" part of D19 is on `main`, then delete `fix/selection-bars-and-tinjau` locally and on the remote and remove the `cash-flow-tracker-ui-refine` worktree.
