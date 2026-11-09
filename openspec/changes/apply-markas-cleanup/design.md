## Context

Phase 12 of the Markas+ plan (`cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md`,
"Legacy cleanup and full acceptance pass") deletes what no component uses any more, sweeps stale presentation
requirements, runs the full capture matrix and hands the owner the end-review list. The archived phase records add
concrete follow-ups (phase 7 `paid_from_savings`, phase 5 JIT and Kredit header notes, phase 6 "Sept", phase 9
target-size, phase 11 default pair and `credit_terms`). This branch starts from `origin/feat/markas-catat`
(`10cd7e5`, PR #99 not merged); `main` is `5401a03` with phases 4 to 10.

Every item below was investigated in the code first. Evidence was gathered on a disposable `postgres:16`
(`cft-cleanup-pg`, V1 → V51, the parity seed, V54) with a loopback backend (AI disabled, no key); the container
was removed afterwards.

**Goals:** remove interim and dead code without a visible change; fix the four recorded inconsistencies in their
owning component; make every spec describe the shipped state; give the owner one end-review list.

**Non-goals:** new behaviour beyond the listed fixes; the AI, benchmark, companion and capture work; production.

## Investigated

| # | Item | Finding in the code | Outcome |
| --- | --- | --- | --- |
| 1 | Untranslated registry | `PAGE_TITLES` lists only `/komponen`; `TRANSLATED_ROUTES` covers every product route; ten dialogs wrap `UntranslatedRegion active={false}`; seven components call `useInterfaceLang`, which returns `undefined` everywhere once no region is active | Remove (D1); `/komponen` decision D2 |
| 2 | Leftovers | Six files no production code imports (`types/domain.ts`, `hooks/useWidthAtMost.ts`, `lib/untranslatedRowLabels.ts`, `ui/ReconciliationFlag.tsx`, `ui/PageHeader.tsx`, `workspace/Disclosure.tsx`); about 30 exports defined but never referenced outside tests; 22 global classes no source names (`ws-banner*`, `ws-tone-*` aside, `ws-rows*`, `ws-who*`, `ws-field*`, `ws-tbl`, `ws-tile-sm`, `ws-layer-in`, `ws-scrim-in`, `ws-sheet-in`, `ws-notice`, `ws-muted`, `ws-caption`, `ws-bigline`, `ws-chips`, `ws-rowbtn`, `ws-accent-link`, `dlg-head`, `is-col`, `is-wallet`); tokens `--surface-3`, `--nav-bg`, `--nav-hover`, `--ink-hover`, `--ink-fg`, `--accent-soft` unused outside `tokens.css` and Tailwind; of the Tailwind colour keys only `text-out-fg` (6) and `text-warn-fg` (3) are used | Remove (D3, D5, D6) |
| 2 | `Meter` / `ProgressBar` | Both draw `.prog` (`figures.tsx`): `Meter` takes a percent, optional width and an unused `label`; `ProgressBar` takes `value`/`max` and `thick`; both hidden from assistive technology; 5 and 9 callers | Merge (D4) |
| 3 | Beranda checklist | `beranda-checklist.js:187` expects 112 px, `:195` expects ink expense amounts | Update (D7) |
| 4 | Rincian tabungan | Figure = `saving_expenses + net_saving_transfers − paid_from_savings` (`dashboard.py`); the report sends only the first two (`monthly_report.py`), so the two rows miss the third part | Additive field and row (D8) |
| 5 | Type counts | Re-measured, table below: JIT costs about 55 ms at p95 per request | `SET LOCAL jit = off` (D10) |
| 6 | Kredit hero vs header | Reproduced (below): the list's window ends at "now" and starts at the salary instant; the page turns it into local days and the overview widens them to whole days | Same window (D9) |
| 7 | Count-only terms edit | A `system` date exists only when the account's days changed after the bill was due (`keep_past_due_dates`); a count-only edit sends `null`, `write_terms` erases it, and the bill is re-derived from the new days | **Flag F1** |
| 8 | "Sept" | `Intl` `en-ID` and `en-GB` write "Sept"; the server's `periods.month_short` writes "Sep" | Normalise in the formatter (D11) |
| 9 | target-size | Found in phase 9 with the old non-modal Catat over Beranda; phase 11's Catat is a modal `ShellDialog` and its 384 captures (390/320 included) show 0 serious | Re-verify only (D12) |
| 10 | Default Dari/Ke | `ManualView` takes the first liquid account as Dari and the first other as Ke; a parent resolves on the server to its default pocket | New default rule (D13) |
| 11 | `credit_terms` count 1 | `write_terms` deletes and stores nothing for count 1 without a date; Catat and notification purchases both have no terms row | No change; regression test (D14) |
| 12 | Held branch, worktree | `git ls-remote` shows no `fix/selection-bars-and-tinjau`; no `cash-flow-tracker-ui-refine` worktree | Done, recorded only |
| 12 | Stale specs | `interface-language`, `explainable-figures` (dotted figures, removed in phase 11), `monthly-navigation-stepper`, `tactile-pulse-ui`, `theme-presets-switcher`, `clean-core-ledger` (phone drawer) | Sweep (D15) |
| 12 | Stale docs | `AGENTS.md` says untranslated pages stay Indonesian, "every page declares `lang="id"`", and calls `tokens.css` the Coral Terminal palette with Plus Jakarta Sans; `config.yaml` says server text stays Indonesian "until localize-server-text" (archived) | Update (D18) |

**Item 5 measurements** (synthetic owner, 20,116 rows, `GET /api/transactions?limit=50&logical_movements=true`,
30 runs after 3 warm-ups, ms):

| Variant | JIT on p50 / p95 | JIT off p50 / p95 |
| --- | --- | --- |
| all rows, `daily_totals,type_counts` | 114.3 / 123.0 | 65.5 / 68.5 |
| all rows, `daily_totals` only | 49.3 / 57.4 | 33.4 / 34.9 |
| `period=cycle`, both | 23.0 / 26.2 | 21.5 / 26.0 |
| `type=movement`, both | 111.7 / 115.2 | 59.7 / 61.1 |

`EXPLAIN ANALYZE` of the type-counts query alone: 77.2 ms with JIT (35.7 ms of it compiling: generation 3.0,
optimisation 1.6, emission 31.1) against 32.3 ms without. The planner's cost (about 327,000) passes
`jit_above_cost` (100,000) because of the per-row saving-flow subplans; the real work is a 20k-row scan.

**Item 6 reproduction** (parity seed, SPayLater, 12.39 Jakarta): before, list and header both Rp 459.000; after a
Rp 77.000 purchase timed 23.00 today, the list (`type=credit&account_id=…&period=cycle`, window
`2026-10-27T01:02Z → now`) stays Rp 459.000 with 1 row while the header (`from=2026-10-27&to=2026-11-09`) reads
Rp 536.000. The same loss applies at the start: the list starts at the salary instant (08.02 Jakarta on 27 Oct),
the header at 00.00.

## Decisions

**D1 Remove the untranslated machinery.** Delete `untranslatedPages.ts`, `untranslated.tsx`, `translatedRoutes.ts`,
the `lang`/region logic in `AppLayout` and `TitleBlock`, every `UntranslatedRegion active={false}` wrapper, every
`useInterfaceLang` call (it can only return `undefined`), `.untranslated-note`, `notice.untranslated` in both
catalogues, `lib/untranslatedRowLabels.ts` and the tests of those paths (`workspace.test.tsx` untranslated cases,
`PeriodStepper.test.tsx` untranslated case, `sharedLanguage.test.tsx` region cases, `rows.test.tsx` label
parity, `copyRegister.test.ts` `UNTRANSLATED_FILES`). The requirement is removed, and a guard scenario says no route
renders an untranslated notice. Rejected: keeping the registry "for future pages" (no page needs it; a new page
writes its copy in the catalogue).

**D2 (default) Remove `/komponen`.** Delete `app/komponen/`, the `/komponen` branch of `proxy.ts`, the
`NEXT_PUBLIC_COMPONENT_SHEET` env and `outputFileTracingExcludes` in `next.config.mjs`, `komponen.test.ts`, the
`systemCategoryGuard` exclusion, `.ws-drawer*`/`.ws-grab`, `scripts/verify/markas/primitives.mjs`, the sheet block of
`checks.mjs`, and the two app scenarios; the prototype's own `komponen` scenario stays for side-by-side images. The
sheet was a phase-1 tool to match primitives to the prototype; it already lacks phase 7's question block and phase
11's money input and chips, and the primitives are covered by `primitives.test.tsx`, the page checklists and axe.
Rejected: translating it (about 60 development-only strings in both production catalogues); keeping it Indonesian
with its own `lang="id"` (keeps a second maintenance surface that every primitive change must remember).

**D3 (default) Dead-code scope.** Remove every file and export that `rg` proves no production source uses, whether
the redesign or earlier work left it (the six files above; exports such as `isUnpaid`, `pressOnKey`, `kindInfo`,
`ChoiceGroup`, `FormErrorSummary`, `Card`/`CardHeader`/`DeltaChip`, `deleteMovement`, `getCurrentTestSession`,
`updateTransaction`, `useRegion`, `kakeiboGroups`, `kakeiboLabel`, `hasRowFilters`, the six `provenance.ts` label
helpers, `currentMonthYM`, `fromDatetimeLocal`, `greetingName`, `parseClampedNumber`, `PayrollItemPayload`,
`MovementFormValues`, `ReviewSummary`), with tests that only test them. Each removal is re-proved with `rg` at
implementation time and listed in `verification.md`. Exports used inside their own file only lose nothing (left
as they are). Catalogue keys are removed only when this change orphans them or they belong to removed parts
(`notice.untranslated`, `nav.components`, `home.viewBreakdownLabel`, and keys of the removed dotted-figure path
once `rg` confirms them); a full catalogue prune is a follow-up because keys are reached dynamically
(`errors.*` by server code). Rejected: Markas leftovers only (leaves known dead code in a cleanup phase).

**D4 (default) One `ProgressBar`.** `ProgressBar({ value, max = 100, thick, width, color })` renders the
`<span class="prog">` (block display already), hidden from assistive technology; `Meter` callers pass a percent
with the default `max`. The unused `label`/`role="img"` path goes. Rejected: keeping both (identical output, two
names); keeping the name `Meter` (nine of fourteen callers say `ProgressBar`, and the SPEC calls it a progress bar).

**D5 (default) Final token set.** Remove the six unused tokens (and their contrast pairs in `tokens.test.ts`) and
every Tailwind colour key no class uses (keep `out.fg`, `warn.fg`). Add to `tokens.test.ts` a check that every token
declared in `tokens.css` and every Tailwind colour key is used by `frontend/src`. Rejected: keeping the Tailwind
keys as an API (they invite the utility style the workspace replaced; adding a key with its first use is cheap).

**D6 Unused CSS.** Remove the 22 unused global classes plus those this change orphans (`.ws-chip-delta`,
`.ws-drawer*`, `.ws-grab`, `.untranslated-note`, `Card`/`Disclosure` rules once their components go). Visible
pages must stay pixel-identical (D16).

**D7 Beranda checklist.** Status column 136 px; every "−" amount resolves to `--out-fg` (the probe pattern of
`transaksi-checklist.js`). Run it in the gate on id and en.

**D8 (default) "Pengeluaran dari tabungan" in Rincian tabungan.** `GET /api/reports/monthly` `savings` gains
`paid_from_savings` (the Kakeibo breakdown's `saving_paid_from_savings`); the service asserts
`saving_expenses + net_saving_transfers − paid_from_savings = fresh_saving`. Laporan lists, under the two current
rows, "Pengeluaran dari tabungan" / "Spending from savings" as a negative amount with the note "tetap dihitung
sebagai pengeluaran" (Beranda's existing words, `home.breakdown.*`), only when it is not zero. The row is not a
link: the ledger has no filter for `paid_from_savings` rows alone. Rejected: subtracting it inside "Pindah bersih" (hides spending inside a transfer figure); always
showing a Rp 0 row.

**D9 (default) The Kredit header uses the list's window.** `GET /api/credit/accounts/{id}` accepts, besides `from`/
`to` (local days, unchanged for Rekening), the list's parameters: `period` + `period_offset` resolved by the same
`get_timeframe_window` as `GET /api/transactions`, or instant `from_date`/`to_date`. Mixing the two forms, or
`period` with dates, answers 422 like the list (`period_with_dates`). The header sends exactly what the list sends.
`period` in the response keeps its day form. Fees, refunds and settled items stay day-based (not shown in the
header). Rejected: clamping `_period` to "now" (fixes the end but not the start instant, and would break the
custom-range case where the list does not clamp); making the list count rows after "now" (changes every period
figure on every page); computing in the browser (figures are server-owned).

**D10 (default) `SET LOCAL jit = off` for the ledger list.** At the start of `list_transactions`' transaction.
p95 for all rows with counts: 123.0 → 68.5 ms; the list alone also gains (57.4 → 34.9). Rejected: a rewritten
counts query (the saving-flow SQL is shared money logic with 16 parity tests; the remaining 32 ms is the scan);
caching counts (every write and every filter combination invalidates them); turning JIT off server-wide in Compose
(a production configuration change outside this scope; reports would need their own measurement).

**D11 (default) English short months read "Sep".** `createFormat` normalises the English month part "Sept" to
"Sep" through `formatToParts` for every short-month format, matching the server's `periods.month_short`. Rejected:
switching the date locale to `en-US` (changes day-month order for region Indonesia); changing the server to "Sept"
(changes API text consumers already read).

**D12 Target-size re-verification only.** Re-run phase 9's scenario (Perpindahan from Target over Beranda at 390 and
320, id and en) with axe `wcag22aa`; fix only if `target-size` returns, then in the owning control.

**D13 (default) Default Ke never equals Dari.** Ke defaults to the first liquid account that (1) is not Dari, (2) is
not a parent with pockets (the server resolves it to a pocket the form does not show), and (3) when Dari is a
parent, is not the pocket Dari's money resolves to: its `default_pocket_id`, or any of its pockets when none is set.
If nothing qualifies, Ke stays empty and the owner chooses. Dari's default is unchanged; an explicit initial target
(bell, "Bayar tagihan") is unchanged. This changes only the preselected Ke, and therefore the request when the
owner keeps it. Rejected: defaulting Dari to the parent's pocket (changes Dari, which the brief keeps); mirroring the
server's name-based pocket fallback in the browser (duplicates a server heuristic).

**D14 No `credit_terms` change.** Catat's `{installment_count: 1}` without a date stores no row (`write_terms`), the
same as a notification purchase. Add a pytest: `POST /api/transactions` on a credit account with that body leaves
no `credit_purchase_terms` row and the same bill as a purchase without terms. Correct phase 11's risk note in
`verification.md` here. Rejected: sending no terms for count 1 (no stored or figure difference to fix).

**D15 (default) Spec sweep scope.** Modify only requirements that describe a state the code no longer has: the
untranslated page, dotted figures and their popover, the stepper on an untranslated page, icon tiles on "a page not
yet redesigned", Tinjau rows "on a page not yet redesigned", and the phone drawer for Settings. Historical
requirements that still hold stay as they are. OpenSpec 1.14 refuses a MODIFIED block that drops a scenario, so the
two requirements that lose one ("Month-and-Year Stepper Navigation", "Explanation Presentation") are removed and
restated as "Period Stepper" and "Rincian Dialog Presentation" with their other scenarios unchanged. Rejected: a
full rewrite of older specs (no behaviour reason); keeping obsolete scenarios under misleading names.

**D16 (default) Gate: full matrix before and after.** Capture every app scenario (except the removed sheet) at
1440 and 390, light and dark, in id and en on the base build (`10cd7e5`, or the merged base if PR #99 lands) and on
this branch, against one seeded database within the same hour (as the earlier parity runs), and compare them pixel by pixel with a new
`scripts/verify/markas/pixel-diff.mjs` (Chrome canvas, no new dependency). Expected differences are only the listed
visible items (Laporan row, "Sep", Catat default Ke, Kredit header later-today case); anything else fails. Pages
whose code changes are also captured at 1024 and 320 with their checklists, axe, console and overflow.

**D17 (default) Prototype side-by-side for the end review.** Compose the prototype's 76 scenarios with the app's
matching captures (`compare.mjs`) at 1440 and 390 light; no measurement, for the owner's review only.

**D18 Docs.** `AGENTS.md`: language paragraph (every page is translated; document language is the account's),
visual references (Inter/Manrope Markas+ tokens, no Coral Terminal/Plus Jakarta Sans), "Every page declares
`lang="id"`" corrected. `openspec/config.yaml` context: stored generated text keeps the language it was written in.

**D19 (default) `renderWithIntl` keeps `id` as its default.** The language plan said "until phase 12"; switching
would rewrite about 2,000 assertions for no product change. Recorded as a decision in `verification.md`.

**F1 (flag, not decided) Count-only edit of a kept due date.** Today: "Ubah transaksi" does not show a `system`
date, so changing only the count sends `first_due_date: null`; the server erases the kept date and re-derives it
from the account's current days, which can move an already-due bill to a later due date, change which bill holds
the purchase, its left-to-pay and on-time facts. Option A (recommended): the dialog sends the stored date on a
count-only edit whatever its setter, and `write_terms` keeps `first_due_date_set_by` when the date equals the stored
one (so a kept date stays `system`). Option B: keep today's behaviour and document it. A changes the stored outcome
of this edit (the date survives) and so the bill figures relative to today's behaviour; relative to before the
edit nothing moves. Only accounts whose days changed after a bill was due are affected.

## Data, API and failure behaviour

- `GET /api/reports/monthly`: `savings.paid_from_savings` (integer IDR, ≥ 0), additive. If the parts do not add
  up the service raises, as `savings_change` does (a programming error, 500).
- `GET /api/credit/accounts/{id}`: optional `period` (`cycle|month`), `period_offset` (−36…12), `from_date`,
  `to_date` (ISO instants). 422 `period_with_dates` when `period` comes with dates or with `from`/`to`; 422
  `credit_period_invalid` when the start is after the end. Owner scoping unchanged (404 for another owner).
- `GET /api/transactions`: unchanged contract; the transaction runs with JIT off.
- F1 (if accepted): `PUT /api/transactions/{id}/credit-terms` with the stored date keeps its setter.
- No migration. Rollback is reverting the merge; no stored data is rewritten (F1 only stops an erase).

## Risks

- Removing CSS can change a page unnoticed → the before/after pixel diff of the full matrix (D16).
- The Ke rule depends on `default_pocket_id` being present in `GET /api/accounts` → unit tests with parent,
  default pocket, no default pocket, and only one family.
- `SET LOCAL jit` must run inside the request's transaction → a test asserts the setting inside the list request.
- PR #99 or `main` moving during the work → merge `origin/main` before the final gate and re-run it.

## Decisions to confirm

| ID | Topic | Default | Alternative | Kind |
| --- | --- | --- | --- | --- |
| D2 | `/komponen` | Remove the sheet and its tooling | Keep it as a development page | default |
| D3 | Dead code | Everything proven unused, older code included | Markas leftovers only | default |
| D4 | Progress bar | One `ProgressBar` | Keep both | default |
| D5 | Token set | Remove unused tokens and Tailwind keys, add a guard | Keep Tailwind keys | default |
| D8 | Laporan row | "Pengeluaran dari tabungan" when not zero | Always shown | default |
| D9 | Kredit header | List's window via new optional parameters | Clamp to now | default, changes a figure in edge cases |
| D10 | Type counts | `SET LOCAL jit = off` | Query rewrite or cache | default |
| D11 | Month | "Sep" in the web | — | default |
| D13 | Default Ke | Never the same account as Dari | Leave as is | default, changes a preselected pocket |
| D15 | Spec sweep | Interim wording only | Wider rewrite | default |
| D16 | Gate | Full matrix before/after with pixel diff | Affected pages only | default |
| D17 | End review | Prototype side-by-side images | None | default |
| D19 | Test helper | Keep `id` default | Switch to `en` | default |
| F1 | Kept due date | Keep it (option A) | Today's behaviour (B) | **flag: stored date** |
