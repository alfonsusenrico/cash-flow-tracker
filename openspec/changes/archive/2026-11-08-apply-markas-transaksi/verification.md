# Verification: apply-markas-transaksi (Markas+ phase 5)

Date: 2026-11-08 · Branch `feat/markas-transaksi` · Base: `origin/main` `f855951` (PR #92, phase 4, merged during this
work) · Code under test: `a8d879d` (the checks below name their revision where it differs).

## Approval, amendment and dependencies (tasks 1.1, 1.2)

- Approval: the owner's standing instruction "complete all until finished" (2026-11-06), renewed 2026-11-08. Every
  decision takes its recommended default; the flag decisions of 2026-11-06 are in `design.md` "Flag decisions".
- **Amendment of 2026-11-08 (coordinator, under the same instruction):** `add-credit-spending-flow` merged in #91, so its
  hand-off H5 is built here (proposal "Credit hand-off H5", design D30, spec requirements "Credit Purchase Rows", "Credit
  Payment Rows", "Credit Provider View", "Credit Terms in the Transaction Dialog", tasks 2.4 and 5.12). Credit purchase
  amounts are red, overriding H5's "ink" (owner rule 2026-11-06). `/ledger` also leaves `untranslatedPages.ts`
  (task 4.3). Committed separately as `b599c5f`.
- **Base:** the branch first merged `origin/feat/markas-tinjau` (`0df677b`) at the coordinator's instruction; PR #92 then
  merged into `main` (`f855951`) and the branch was updated from `origin/main` (`16f2aff`) before any implementation
  (D26 amended). `git merge-base --is-ancestor f855951 HEAD` holds; `#90` (localize-server-text) and `#91` (credit) are
  ancestors of `f855951`.
- Dependencies present: `components/workspace/rail.tsx`, `ListRow` `rowKey`, `messages/en.json`, `lib/format.ts`,
  `lib/i18n/translatedRoutes.ts`; `PATCH /movements/{id}` answers `same_account` as `{code, params, message}` (backend
  `ErrorCode.SAME_ACCOUNT`, both web catalogues hold `errors.same_account`).
- Phase 4's merged API against design D22: `Rail` had one visible label, so it gained `showLabel` and `RailLabel` for
  JENIS and PERIODE; `RailItem` gained `pressed`; `HeroFigure` gained `aside`, an optional `line` and `busy`;
  `FigureBlock` gained `bar` and `className`; `ListRow` gained `selectable` and `pressHandlers`; `ShellDialog` gained
  `scrollFocusable`. No phase 4 piece was missing.

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these over:
- Phase 5 (`apply-markas-transaksi`) built 2026-11-08 under the standing instruction; amended the same day with the
  credit hand-off H5 (credit rows, the "Kredit" type, the provider view and header, credit terms in the dialogs).
- Owner feedback of 2026-11-06 applied: expense amounts red everywhere ("−Rp" in `--out-fg`, Beranda included), transfers
  titled by their note, saving directions instead of "Tidak dihitung", the "Tabungan & investasi" type whose cycle hero
  equals Beranda's "Ditabung & investasi".
- Flag decisions of 2026-11-06 hold (D2 "Rutin" out of "…", D4 hero arithmetic, D8 independent filters and Periode in
  Filter, D11 no duplicate badge and the 136 px pillar column, D14 no delete undo, D27 labels only where Beranda counts,
  D28 all four parts listed).
- End-review list: the deviations below and the shared-part changes on other pages (stepper on Analisis and Laporan,
  the red `RowAmount` on Beranda and the Rekening detail, the transaction dialog on Beranda, Kalender and Peta Aliran).
- Open item: `add-credit-spending-flow`'s H8, H9 and H11 remain for phases 8, 9 and 11.

## Environment

- Local only. PostgreSQL 16 container `cft-transaksi-pg` on `127.0.0.1:55495` with a throwaway password; Flyway
  `flyway/flyway:10`. No production system was touched; no paid service or AI run.
  - `cft_markas_tx`: migrated to V51, `seed_parity.py` (`dimas_contoh`, data.js shifted to today, 188 transactions),
    migrated to V54, then `seed_transaksi.py` through the API (70 additions, 0 skips); `cft_markas_tx_en` is its copy
    with the account in English.
  - `cft_markas_tx_stress` and `_en`: the same plus `seed_transaksi.py --stress` (1,122 additions, 0 skips: 1,050 rows
    of a 60-character note, one Rp 1.234.567.890 expense), later switched to USD and back.
- Backend from this worktree in a `uv` Python 3.12 environment; background jobs, AI and outbound HTTP off.
- "After": this worktree's production build behind loopback proxies (3721 id, 3722 en, 3723 holding the list for the
  loading state, 3724 failing it, 3725/3726 the stress owner). "Before": `f855951`'s `frontend/` exported with
  `git archive` and built the same way (3710), against the same backend and data. Prototype `design-options/` on 3781.
- Headless browser over CDP (`scripts/verify/markas/cdp.mjs`), device scale 1; axe-core 4 (wcag2a, wcag2aa, wcag21a,
  wcag21aa, wcag22aa). Captures, checklists and JSON stay in the session scratchpad
  (`…/scratchpad/markas-transaksi/`), not in `research/evidence/`, as in phases 2 to 4.

## Seeds (task 1.4)

`scripts/verify/markas/seed_transaksi.py` (new; loopback only, writes through the app's own endpoints so balances,
movement pairs, top-ups and credit bills stay consistent) adds to the parity owner: harian → tabungan with a note
(`saved`), tabungan → harian without (`taken`), harian → RDN (`dana_investasi`, null), harian → harian with and without a
note, a goal linked to the Transport pocket and a move into it (`saved`), tabungan → product top-up (null), tabungan →
SPayLater (`taken`), harian → SPayLater with a Rp 2.500 fee (`debt_payment`, a fee-linked row), an expense paid from the
tabungan pocket (`paid_from_savings`), a saving-pillar expense (`saving_pillar`), a SPayLater purchase in 3 instalments
and one paid in full, and a day of 56 rows so a day continues on the next page. The parity seed already holds an RDN →
product top-up (`invested`), SPayLater purchases and a payment, a reimbursement and waiting rows. On the seeded owner
`GET /api/transactions?include=type_counts` gives every `saving_flow` value and credit role. Deviation: the plan put
these in `seed_parity.py`; a separate API-driven script keeps the data consistent with the credit schedule's
invariants without re-implementing them in SQL.

## What changed

- **Backend (additive, read-only; D27, D28, D30):** `services/saving_flow.py` holds the savings predicates that
  `get_kakeibo_breakdown` used inline (`dashboard.py` shows only the move and its import); rows of `GET /transactions`
  and `/{id}` carry `saving_flow`; `type=saving` and `type=credit` filter, `type_counts` gains `saving` and `credit`,
  `logical_movements` keeps one row per movement for both, and `summary.cumulative`/`period` carry `saving_net` (only
  for `type=saving` without other row filters, else `null`). No schema, migration, write path or existing figure
  changed. `test_ledger_day_totals.py`'s exact `type_counts` expectation gains the two keys.
- **Transaksi page:** title from the catalogue; the restyled stepper and one "…" (Gabungkan dua transaksi, Filter
  transaksi; "Rutin" removed); the split hero (Pengeluaran with its one qualifier line, Masuk "+", Bersih signed; the
  savings type shows Ditabung & investasi with `saving_net`; the credit type adds "Dibayar nanti: uang keluar saat tagihan
  dibayar."); `.tx` with the 200 px sticky rail of seven types and three period modes from 1200 px, the scrolling type
  control below; the provider chooser under "Kredit" and the provider header from `GET /api/credit/accounts/{id}`; the
  toolbar with search, "Filter · N" and value-only chips; the sticky selection bar; day groups with muted totals; one-line
  rows on the shared `ListRow` (two lines on the phone); "N lagi di hari ini · Muat lagi", "Muat N lagi", the end line;
  skeleton, failure and two empty states. Layout switches are viewport media queries; `useWidthAtMost` is gone from
  Transaksi.
- **Dialogs:** `TransactionDialog` (560, replaces the drawer on Transaksi, Beranda, Kalender and Peta Aliran) with the
  `.dl` per kind, fee links, pair status, saving direction, credit terms and instalments, provenance, the Tinjau box and
  the footer delete confirmation with its effects; `TransactionEditDialog` (600) with segmented Jenis and Pilar, hints,
  and "Cara bayar" for credit purchases (`PUT`/`DELETE …/credit-terms` only when changed); `MovementEditDialog` (600)
  replacing `InternalMovementModal`; `LedgerFilterDialog` (640); the top-up conversion and the "Nominal tidak sama"
  dialog on `ShellDialog`. Every request is today's.
- **Shared:** `components/transactions/rows.tsx` (row helpers, the one red `RowAmount`/`KindAmount`, `RowStatus`,
  credit and saving lines, `bucketName`), used by Beranda (Aktivitas terbaru and Perlu tindakan), Transaksi, the dialog,
  Kalender and the Rekening detail (the last two with `lib/untranslatedRowLabels.ts` until their phases);
  `RadioSegments`; `AccountSelectOptions` and `DebtAllocationEditor` take their words from the catalogue on Transaksi;
  single-key shortcuts now pass through checkboxes and radios, so J/K walk merge checkboxes.
- **Language:** the `transactions` namespace (333 messages per catalogue), `terms.net`, `terms.calendarMonth`,
  `terms.savingsAndInvestments`, `common.period.label`; the Beranda row keys moved from `home`; `/ledger` in
  `TRANSLATED_ROUTES` and out of `untranslatedPages.ts`; the untranslated-page test example moved to `/insights`; the
  copy register's translated roots gain `app/ledger`, `components/transactions`, `ExpenseTopupConversionModal`,
  `lib/ledgerFilters.ts` and `lib/ledgerDays.ts`, and its spending-term rule covers `transactions` with planted fixtures.
- **Removed:** `KpiCard` and `.ws-kcard*`, `LedgerTable`/`LedgerFeed`, both `KIND_PILL`s, `RowAvatar`, `ActivityAmount`,
  `FilterPopover`, `LEDGER_CHIPS`/`chipOf`/`withChip`, `TransactionDrawer`, `TransactionEditForm`,
  `InternalMovementModal` and its test, `components/ledger/LedgerRowMarks.tsx`, `rowLabels.ts`, the Transaksi `ws-tx-*`
  rules (`.ws-tx-error` and `.ws-tx-actions` kept for Catat), `.ws-kk`, `.ws-ava`, `.ws-amt-*`, `.ws-ph-*`, two duplicate
  error keys in each catalogue. `rg` finds none of the removed names in `frontend/src` (one comment names
  `InternalMovementModal` as what the movement dialog replaces).

## Backend additions (tasks 2.1 to 2.5)

- Pure move: `git diff f855951 -- backend/app/routers/dashboard.py` is 5 insertions and 22 deletions (the import, the
  two predicates, the inline top-up/legacy rule and the paid-from-savings predicate replaced by the shared helpers).
- Before and after on one synthetic owner (HEAD's `backend/app` exported with `git archive` against the worktree's): 71
  requests (cycle-summary, overview and analytics for the cycle, the previous cycle and the month, period comparison,
  flow, 12 ledger lists, 50 details); with the new keys removed no response differs (cycle-summary only in `as_of`,
  period comparison only in its request-time `end`).
- `tests/test_ledger_saving_flow.py` (16 cases): one per `saving_flow` value including the decided cases (into
  `dana_investasi` null, tabungan → product top-up null, tabungan → credit `taken`, harian → credit `debt_payment` and not
  listed), a goal-linked harian pocket `saved`, a Beli trade null, `paid_from_savings` before `saving_pillar`, both legs
  equal, consolidated and account-filtered views, another owner never read, "Aggregate unchanged" (`saved` + `invested`
  = `saving_into`, `taken` = `saving_out`), "Savings filter equals Beranda" (Rp 850.000), `saving_net` equal to
  `tabungan.total` for the open cycle, the previous cycle, the month and all time with a reimbursement offsetting the
  saving pillar, `saving_net` null with each of ten other filters, tab counts with `saving` and `credit`, the credit filter
  (purchase, fee, refund, one payment row, the cash-advance leg; not an ordinary expense, not another owner's), unknown
  types 422.
- Query cost, `GET /transactions` p95 of 30 runs on the stress seed (1,279 rows), main's backend against this branch's,
  same database: all rows with both includes 48.7 → 33.1 ms; cycle 26.8 → 33.0; `type=movement` 35.5 → 40.4;
  `type=saving&period=cycle` 47.6 and `type=credit&period=cycle` 47.5 (new). No rise over 50 ms. On a 20,000-row owner
  the backend run measured +51 ms p95 for the all-rows view (PostgreSQL JIT starts on the larger type-counts plan);
  recorded as a risk, not changed (options: `SET LOCAL jit = off` for the list, or caching the account classification).
- Full suite on a fresh `postgres:16` (`cft-transaksi-pytest`, 127.0.0.1:55491) migrated V1 → V54 by
  `flyway/flyway:10`: **2179 passed** (`PYTEST-EXIT 0`). `git diff --stat f855951 -- db mobile nginx` is empty; no
  migration file.

## Side by side and measurement (tasks 5.1, 6.1, 6.2)

`scripts/verify/markas/transaksi-checklist.js` (D24: `.tx` 200 · 808 and 32 at 1440, sticky rail top 80, one column with
the seven-type control below 1200 px, first content at y = 192, the split hero gap 24 48 and margin 32, one 44 px figure,
rail items 36 with padding 12 and radius 6, the pressed 2 × 20 bar at −8, the 1 px rule, search 36 and ≥ 200, chips 28
with padding 12 4 and the 24 px remove, the sticky 48 px selection bar, day headings 40 sticky at 56 (104 while
selecting), rows ≥ 56 (64 on the phone) with padding 12 and gap 16, time 40, category 168, pillar 136, amount ≥ 120, every
"−Rp" amount in `--out-fg`, tabular amounts, dialogs centred at their width with radius 16 and 24 24 16 / 16 24 padding or
full-width sheets, `.dl` 160, stepper 36 (44 on the phone) and label ≥ 168, sizes ⊆ {12, 14, 16, 20, 24, 32, 44}, weights
⊆ {400, 500, 600}, no other text ≥ 33 px, no border ≥ 2 px, no horizontal overflow), with `measure.js`, axe, console and
language records on every capture (`capture.mjs`, captures in `…/markas-transaksi/shots/`):

| Set | Captures | Transaksi checklist | axe serious/critical | Console | Overflow | `lang="id"` or notice |
| --- | --- | --- | --- | --- | --- | --- |
| App, Indonesian, 24 scenarios at 1440/1024/390/320 × 2 themes (`final-id`) | 192 | **4,276 of 4,276** | 0 | 0 | 0 | 0 |
| App, English, the same (`final-en`) | 192 | **4,276 of 4,276** | 0 | 0 | 0 | 0 |
| App, 1280, 1200, 1199, 1100, 768, 767 (`final-widths`) | 30 | 814 of 814 | 0 | 0 | 0 | 0 |
| Loading (list held) and failure (list 500), 1440 × 2 and 390 | 3 + 3 | 67 of 67, 56 of 56 | 0 | 3 (the induced 500) | 0 | 0 |
| Stress, Indonesian / English (`final-stress-*`) | 32 / 32 | 668 / 668 each | 0 | 0 | 0 | 0 |
| USD, Indonesian / English (`final-usd-*`) | 32 / 32 | 668 / 668 each | 0 | 0 | 0 | 0 |
| Before (`f855951`), 12 widths (`final-before`) | 12 | 60 of 60 (checks that apply) | 0 | 0 | 0 | 0 |
| Prototype, 1440/1100/1024/390 (`final-prototype`) | 30 | 620 of 660 | 0 | 0 | 0 | 0 |

- Scenarios: cycle, all time, month, Pengeluaran, Tabungan & investasi, Kredit, the SPayLater provider view, one
  account (Liburan), a search, three active filters, hidden balances, merge selection, no match, the "…" menu, Filter,
  and the dialogs of a waiting expense, an income, a credit payment, a top-up, a credit purchase in instalments, the
  delete confirmation, "Ubah transaksi", "Ubah transaksi" of a credit purchase and "Ubah pindah saldo".
- The prototype's 40 differences are deviations 1 and 16 by design: its rail stays down to 1024 px and it has five
  types.
- Visible differences from the prototype besides the recorded deviations: the app's own seeded data; the provider view
  and credit metas (H5), which the prototype has no screen for; "Rutin" absent from "…" (D2).
- Composed left/right images were not made (see "Not verified").

## Accessibility and keyboard (tasks 6.3, 6.4)

- axe-core: **0 serious or critical** on all 579 app captures (both languages, both themes, every width above, every
  dialog in the list, the "…" menu, merge selection, hidden balances, loading, failure, no match, stress and USD). The
  first gate found `scrollable-region-focusable` on the delete confirmation at 1024 and 390 px; the dialog body is now a
  focusable region when it scrolls (`3a018a8`). Unit tests run axe with the page, the dialog and the edit dialog open.
- Language: no capture of an English Transaksi has a `lang="id"` element or the untranslated notice.
- `transaksi-keys.mjs` (15 flows) at 1440 and 390: **Indonesian 18 of 18, English 18 of 18** on `749f7c9`. The first run
  found that J did nothing on a focused merge checkbox (checkboxes counted as typing targets) and that J could land on a
  disabled checkbox; single keys now pass through checkboxes and radios, and J/K skip rows that cannot join a merge
  (`749f7c9`, with unit tests). Flow 10 marked a row checked and Z restored it (`review_status` back to `auto`).
- A full scroll of 200 loaded rows of the stress owner at 1440 px recorded no long task (`longtask` observer, 0 entries).

## Figure parity (task 6.5)

`scripts/verify/markas/transaksi-parity.mjs` on the parity owner, before (`f855951` build) and after (`3a018a8` build)
within the same minutes, reading every amount and time of day on six views (cycle, month, all time, Pengeluaran, Pindah
saldo, Masuk; first page each):

| View | Amounts before / after | Changed values | Sign-only differences (intended) |
| --- | --- | --- | --- |
| `period=cycle` | 61 / 61 | 0 | Masuk and Bersih gain "+" |
| `period=month` | 61 / 61 | 0 | Bersih "−" is U+2212 (was "-") |
| all time | 61 / 61 | 0 | Masuk and Bersih gain "+" |
| `type=expense` | 58 / 58 | 0 | Bersih "−" |
| `type=movement` | 31 / 31 | 0 | none |
| `type=income` | 7 / 7 | 0 | Masuk "+" |

- **0 changed values**; times of day unchanged on every view.
- **English against Indonesian** (region ID, same data): every amount and time byte-identical on all six views
  (459 values).
- **Savings parity (owner feedback 4):** for the open cycle the hero of `/ledger?type=saving&period=cycle` reads
  "Rp 3.239.000", byte-identical to Beranda's "Ditabung & investasi"; `summary.period.saving_net` equals
  `cycle-summary` `tabungan.total` (3,239,000) and `type_counts.saving` equals the 10 listed rows. The previous cycle, the
  month and all time are proven by the backend tests (Beranda shows only the current period).
- Stress list: 1,279 rows paged through the API 50 at a time, as the page pages them, each exactly once (26 pages).

## Copy (tasks 4.1 to 4.4)

- Catalogues: `transactions` with 333 messages in each language; the parity test passes (same keys and arguments,
  English counts with `one`/`other`). `npm run type-check` rejects a key the catalogue lacks (TS2345, hit and fixed for
  three dynamic keys during this work).
- Copy register: the Transaksi roots hold no literal copy; the spending-term rule now covers `transactions.*`, and the
  planted fixtures fail as required (Indonesian "Belanja", English "Expenses" and "Ledger").
- Indonesian check: each `transactions` message was searched, arguments as wildcards, in Transaksi's sources at
  `f855951` (page, list, drawer, edit form, movement modal, row helpers, conversion modal, debt editor, provenance, filters,
  catalogue): 254 found as they were; the rest are the intended new or changed strings of D20 and H5 (hero labels and
  lines, rail, Filter dialog, provider, merge count and "Gabungkan jadi pindah saldo", "Belum ada kategori", saving
  directions, credit metas and terms, load-more and end lines, states, dialog "pukul" meta, pair status, receipt words,
  the movement dialog's split, the conversion dialog) or the same text composed differently (subtitle, "Tabungan
  bertambah", the type hint, account option labels).
- English was checked against Appendix B: "Transactions", "Spending", "Income", "Net", "Transfer"; no "Expense",
  "Spent", "Outflow", "Inflow", "Ledger" or "you" (catalogue register and the English page test).

## Frontend and repository checks (task 6.7)

On `a8d879d`:
- `npm test`: **95 files, 967 tests passed**; `npm run type-check` passed; `npm run lint` passed (0 warnings);
  `npm run build` passed.
- Backend: 2179 passed (above; the backend is unchanged since `bae149e`).
- `git diff --check f855951 HEAD`: clean. `openspec validate --all --strict`: 85 passed before archiving, 84 of 84 after (one long requirement shortened).
- `git diff --stat f855951 -- backend` lists only `routers/dashboard.py`, `routers/transactions.py`,
  `services/saving_flow.py`, `tests/test_ledger_saving_flow.py` and `tests/test_ledger_day_totals.py`.
- No package file changed, so no lockfile step was needed.


## Deviations and notes for the owner's end review

The proposal's seventeen deviations hold. Further:
1. **Credit (H5):** credit purchases are red, not ink (owner rule). The provider's "Tagihan berikutnya" shows the amount
   left with "jatuh tempo {date}" under it. The hero of the "Kredit" type is the ledger summary of the listed rows, as for
   every type; with a provider chosen it can differ from the header's "Pengeluaran pakai kredit", which is the credit
   overview's figure over whole local days (a purchase timed later today is counted there but not yet in the cycle
   list). Both are server figures; nothing is computed in the browser.
2. **First due date:** the edit's "Jatuh tempo pertama" starts empty and names the current first due date in its hint;
   changing only the instalment count sends `first_due_date: null`, so a date the owner set earlier is replaced by the
   derived one (stated by the hint "Kosongkan agar mengikuti tanggal tagihan rekening").
3. `lib/movementFormSchema.ts` keeps its Indonesian messages because Catat (phase 11) shares it; the movement dialog maps
   each schema issue to a `transactions.movement.errors.*` key, so the rules stay in one place.
4. The Filter button and the "…" item use the `sliders` icon; the app has no `filter` icon.
5. A dialog body that scrolls without its own controls is a tab stop (`ShellDialog scrollFocusable`, WCAG 2.1.1).
6. Dead Analisis rules (`.cmpName`, `.bench`) read `--k-none` instead of the undeclared `--kc` that only `.ws-kk`
   declared; no element uses them.
7. English: rail labels such as "Savings & investments" ellipsize in the 200 px rail at 1440 (full names in the accessible
   names), as phase 4 allowed.
8. `TodoSection`'s plan amounts stay plain; only "−Rp" spending is red, by `KindAmount`.

## Not verified

- Zoom to 400 % and a real screen reader; the 320 px captures cover reflow.
- Composed side-by-side images were not made; prototype and app captures are in `shots/final-prototype` and
  `shots/final-id`/`final-en`. The prototype's own checklist differences are its 1024–1199 px rail and five types
  (deviations 1 and 16).
- The "Belum ada transaksi" state for an owner with no rows, "Nominal tidak sama" and the top-up conversion were checked
  in unit tests (with axe), not captured; the seeded owner has rows, and the conversion needs an eligible debit.
- A pixel compare of Beranda before and after; Beranda's changes (red spending, note-first transfers, saving directions)
  are covered by its page tests.
- A real device; phone captures are the browser's device mode.

## Owner acceptance

Accepted under the owner's standing instruction of 2026-11-06, renewed 2026-11-08 ("complete all until finished"). No
separate review of this phase was requested before archiving; the deviations above stay on the end-review list.
