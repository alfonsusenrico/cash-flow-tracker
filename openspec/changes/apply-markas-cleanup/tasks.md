## 1. Approval and baseline

- [x] 1.1 Record the owner's answers to the defaults D2–D19 and flag F1 in `proposal.md` › Approval with the date. Evidence: the Approval section names each accepted default or override; tasks that an override removes are marked "(dropped: …)". Done: proposal Approval (2026-11-09, coordinator): all defaults accepted, F1 pending with option B kept; F1 moved to group 8.
- [x] 1.2 `git fetch`; record `origin/main` and `origin/feat/markas-catat` heads and whether PR #99 merged. Evidence: the two hashes in `verification.md`. Done: origin/main 5401a03, origin/feat/markas-catat 10cd7e5, PR #99 open (not merged) on 2026-11-09.
- [ ] 1.3 Baseline environment: disposable `postgres:16` `cft-cleanup-pg` (127.0.0.1:9830) migrated V1 → V51, parity seed, V54, the Analisis/Laporan/Rekening/Target/Pengaturan/Catat seeds and an English copy; base build of `10cd7e5` on Next 3830 with backend 8771 (AI disabled, no key). Evidence: seed reports and migration output in the scratchpad, listed in `verification.md`.
- [ ] 1.4 "Before" captures: every app scenario except `komponen*` at 1440 and 390, light and dark, id and en, plus figure parity JSON (`parity-languages.mjs` and the page parity scripts) on the base build. Evidence: capture count and folder per language in `verification.md`.

Expected Result: approved scope, a recorded baseline and a full before-set to compare against.

## 2. Untranslated registry and the component sheet

- [x] 2.1 Remove `untranslatedPages.ts`, `untranslated.tsx`, `lib/i18n/translatedRoutes.ts`, the untranslated logic in `AppLayout.tsx` and `TitleBlock.tsx`, every `UntranslatedRegion active={false}` wrapper and every `useInterfaceLang` use, `.untranslated-note`, `notice.untranslated`, `lib/untranslatedRowLabels.ts`, and their tests (D1). Evidence: `rg -n "ntranslated|useInterfaceLang|TRANSLATED_ROUTES" frontend/src` finds nothing; vitest passes. Done: the grep finds only SentenceView's guard test name; vitest 98 files, 1083 passed; type-check and lint pass.
- [x] 2.2 Add a vitest that renders every product route in English and Indonesian through the shell and asserts no notice line and no `lang` attribute other than the language option (spec "Every Page in the Interface Language"). Evidence: the test passes, and fails when a `lang="id"` wrapper is added back (checked once, then reverted). Done: workspace.test.tsx renders nine routes in en and id (18 cases) through AppLayout; with lang="id" planted on the title all 18 failed, then the plant was removed.
- [x] 2.3 Remove `/komponen` (D2): `app/komponen/`, the `proxy.ts` branch, `NEXT_PUBLIC_COMPONENT_SHEET` and `outputFileTracingExcludes` in `next.config.mjs`, `komponen.test.ts`, the `systemCategoryGuard` exclusion, `.ws-drawer*`, `.ws-grab`, `nav.components`, `scripts/verify/markas/primitives.mjs`, the sheet block of `checks.mjs` and the two app scenarios. Evidence: `rg -n "komponen|COMPONENT_SHEET|ws-drawer|ws-grab" frontend scripts` finds only the prototype scenario; `npm run build` passes; `/komponen` answers 404 in the built app. Done: the grep finds only the prototype's own komponen scenario; the 404 check of the built app is part of 6.2.

Expected Result: no untranslated path and no development sheet remain, and both languages render every page.

## 3. Dead code, one progress bar and the token sweep

- [x] 3.1 Remove the files and exports listed in D3 after re-proving each with `rg` (production sources and tests), with the tests that only cover them. Evidence: the `rg` proof table in `verification.md`; type-check, lint and vitest pass. Done: eight modules (types/domain, useWidthAtMost, ReconciliationFlag, ui/PageHeader, ui/Button with its test, ui/Input, workspace/Disclosure) and 39 exports removed after the scan found no production user; a grep of each name in frontend/src now finds only the breakpoints guard's pattern for useWidthAtMost; list in verification.md. Type-check, lint and vitest (97 files, 1058 tests) pass.
- [x] 3.2 Merge `Meter` into `ProgressBar({ value, max = 100, thick, width, color })` and move its five callers (D4). Evidence: `rg -n "\bMeter\b" frontend/src` finds nothing; a `figures` test covers percent, value/max, clamping, `thick` and `width`; the pixel diff of Analisis and Laporan shows no change. Done: one ProgressBar (span.prog, max 100 by default, width, colour); analysis.test covers percent, value/max, clamping, NaN, width and the span; no Meter left; pixel identity in 6.4.
- [x] 3.3 Remove the 22 unused global classes and the classes this change orphans (D6). Evidence: re-run of the unused-class scan lists none (dynamic `ws-tone-*` kept with its user); pixel diff unchanged. Done: globals.css lost 106 selectors (the 22 listed classes, the classes the removals orphaned: ws-act, ws-bar, ws-cap, ws-card*, ws-chip*, ws-cnt, ws-menu*, ws-pill, ws-pop-anchor, ws-search, ws-tile, ws-tone-*, and their orphan section comments); the rescan finds none unused; tokens.test and checks.mjs point at the current card classes; pixel identity in 6.4.
- [x] 3.4 Remove the six unused tokens with their `tokens.test.ts` pairs and the unused Tailwind colour keys, and add the "every token and key is used" check (D5). Evidence: the new check passes, and fails on a temporarily declared unused token (checked once, then reverted); `tokens.test.ts` passes. Done: fifteen unused tokens removed (the six planned plus --tile, --tile-fg, --in-bg, --tr-bg, --inv-bg, --warn-bg, --ok-bg, --ok-fg and --ink, orphaned by 3.3) and .theme-light; Tailwind colours reduced to out.fg and warn.fg; the new guard passes and failed on a planted --cleanup-probe token and a planted probe.fg key (both reverted).
- [x] 3.5 Remove catalogue keys orphaned by 2.1–3.4 in both catalogues, each confirmed with `rg` (D3). Evidence: catalogue parity test passes; the removed keys are listed in `verification.md`. Done: removed notice.untranslated, nav.components, home.viewBreakdownLabel, home.closeExplanation, home.whyUnavailable, home.itemCount and common.closeDetails, each with no source user; catalogue parity passes.

Expected Result: no proven-dead file, export, class, token or key remains, and pages are pixel-identical.

## 4. Figures and requests

- [ ] 4.1 Backend: `savings.paid_from_savings` in the monthly report with the parts-add-up assertion (D8). Evidence: pytest with a Tabungan-paid expense (spec "Spending paid from savings") and with none; existing monthly-report tests pass.
- [ ] 4.2 Laporan: "Pengeluaran dari tabungan" row when not zero, words from `home.breakdown` (D8). Evidence: `SavingQuestion` test (row shown with −Rp 150.000 and its note; hidden at 0; masked with "Sembunyikan saldo"); Laporan checklist and axe in the gate.
- [ ] 4.3 Backend: `GET /api/credit/accounts/{id}` accepts `period`/`period_offset` and `from_date`/`to_date` through `get_timeframe_window` (D9). Evidence: pytest reproducing the later-today and before-salary-instant purchases (overview equals the `type=credit` list summary), the 422 `period_with_dates` cases, the unchanged `from`/`to` day form, and another owner's account answered 404.
- [ ] 4.4 Web: `CreditHeader` sends the list's own window parameters (D9). Evidence: `CreditHeader`/ledger page test asserting the request for cycle, month and custom range; the provider-view capture shows equal hero and header figures with a later-today purchase seeded.
- [ ] 4.5 Backend: `SET LOCAL jit = off` at the start of the ledger list's transaction (D10). Evidence: pytest asserting `SHOW jit` is `off` inside the request and the response unchanged; re-measure the item-5 table on 20k rows and record it in `verification.md`.
- [ ] 4.6 Web formatter: English short months read "Sep" (D11). Evidence: `format` tests for every short-month format in en-ID and en-US, Indonesian unchanged; Analisis and Laporan English captures show "Sep".
- [ ] 4.7 Catat default Ke rule (D13). Evidence: `ManualView` tests for the four spec scenarios; browser flow on the parity seed saving the default pair succeeds (no `same_account`); `catat-checklist.js` passes.
- [ ] 4.8 Regression test: `POST /api/transactions` on a credit account with `credit_terms {installment_count: 1}` stores no terms row and yields the same bill as a purchase without terms (D14). Evidence: the pytest passes.

Expected Result: every changed figure matches its source of truth, and the requests are the ones the specs name.

## 5. Tooling, specs and docs

- [ ] 5.1 Update `beranda-checklist.js` to 136 px and `--out-fg` expense amounts (D7). Evidence: Beranda checklist passes in full on id and en (count recorded), where it gave 440 of 446 before.
- [ ] 5.2 Add `scripts/verify/markas/pixel-diff.mjs` (Chrome canvas, counts differing pixels per pair, writes a diff image for any pair above 0) (D16). Evidence: it reports 0 for a capture against itself and a non-zero count with a diff image for two different captures.
- [ ] 5.3 Update `AGENTS.md` and `openspec/config.yaml` (D18). Evidence: `rg -n "not translated yet|Plus Jakarta|Coral Terminal|until localize-server-text|declares .lang=\"id\"" AGENTS.md openspec/config.yaml` finds nothing.
- [ ] 5.4 Complete `end-review.md` (phases 4–11 and the owner checks are compiled at planning time): fill its "Phase 12" section with this change's deviations and every visible difference the pixel diff found, and update "Changed by phase 12" with the F1 outcome. Evidence: the section lists each item with its source and diff image.
- [ ] 5.5 `openspec validate --all --strict`. Evidence: all pass, count recorded.

Expected Result: tooling and documents describe the shipped state.

## 6. Gate

- [ ] 6.1 Merge `origin/main` (and PR #99 if merged) with `chore: merge main into chore/markas-cleanup` when it moved; re-run 6.2–6.6 afterwards. Evidence: merge commit or "unchanged" with hashes.
- [ ] 6.2 Frontend `npm run type-check`, `npm run lint`, `npm test`, `npm run build`; lockfile untouched unless `package.json` changed. Evidence: counts in `verification.md`.
- [ ] 6.3 Backend pytest on a fresh `postgres:16` migrated V1 → V54 by `flyway/flyway:10`, venv by `uv venv -p 3.12`. Evidence: pass count, exit 0, skips reported.
- [ ] 6.4 "After" captures of 1.4 and the pixel diff against "before" (D16): zero differences except the listed visible items, each shown in a diff image. Pages whose code changed (Beranda, Transaksi provider view, Laporan, Analisis, Catat, Pengaturan) also at 1024 and 320 with their checklists, axe (`wcag22aa`), console errors and overflow, light and dark, id and en. Evidence: tables with counts in `verification.md`.
- [ ] 6.5 Re-run phase 9's target-size scenario at 390 and 320 in id and en (D12). Evidence: axe `target-size` 0.
- [ ] 6.6 Figure parity before vs after and en vs id for Beranda, Transaksi, Analisis, Laporan, Rekening and Target; keyboard flows for Catat Perpindahan defaults, the provider view and Laporan's savings rows. Evidence: parity JSON with 0 unexpected differences; key-script results.
- [ ] 6.7 Prototype side-by-side images for the end review (D17). Evidence: folder and count in `verification.md` and `end-review.md`.
- [ ] 6.8 `git diff --check origin/main`; review the final diff for dead code, secrets and unrequested changes. Evidence: clean output and a short review note.

Expected Result: the redesign passes the full gate on the final revision.

## 7. Record and hand over

- [ ] 7.1 Write `verification.md` with revisions, commands and counts, environments (synthetic only), what was not verified, and a "For PROJECT_STATE.md" section. Evidence: the file.
- [ ] 7.2 Clean up: stop every server and browser started, remove `cft-cleanup-*` containers and network, confirm ports 8771–8779, 3830–3849 and 9830–9849 are free. Evidence: command output in `verification.md`.

## 8. Flag F1 (kept last; owner chose option A on 2026-11-09)

- [ ] 8.1 F1 option A (owner, 2026-11-09): the dialog sends the stored date on a count-only edit and on "Sekali bayar" when a kept date exists; `write_terms` keeps the setter when the date equals the stored one. Evidence: `TransactionEditDialog` tests and a pytest for the spec scenario "Count changed on a purchase with a kept due date" (date and `system` kept, instalments 11 Nov/11 Dec/11 Jan). Request parity: the recorded `PUT` bodies before and after for count-only, "Sekali bayar", date-changed and no-date cases.

Expected Result: the owner's F1 answer is built or recorded as pending with today's behaviour kept.

## Workflow follow-up

- After the owner's go-ahead for stage 2 and the gate: `openspec archive apply-markas-cleanup -y`, push `chore/markas-cleanup` with upstream, and report the branch. The owner opens and merges the pull request; the agent never merges.
