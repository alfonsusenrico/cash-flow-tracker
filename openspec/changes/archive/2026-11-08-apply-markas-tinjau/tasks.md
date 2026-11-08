## 1. Approval, dependencies and baseline

- [x] 1.1 Record the approval and the flagged review:
  - in `proposal.md` ("Approved 2026-11-06 under the owner's standing instruction …", already written) and in `PROJECT_STATE.md`, with the date;
  - the review outcome of the flagged decision D14 (default, or the recorded fallback) in `design.md` and the spec delta before any code;
  - the open item "delete `fix/selection-bars-and-tinjau` and the `cash-flow-tracker-ui-refine` worktree after phase 8 merges" in `PROJECT_STATE.md`.

  Evidence: the three files name the date and the D14 outcome; `openspec validate apply-markas-tinjau --strict` passes after the edits.

  Done 2026-11-08: D14 reviewed (default) in proposal.md, design.md and the spec delta. `PROJECT_STATE.md` lives untracked in the owner's checkout, which this run does not touch, so the approval, the D14 outcome and the open item for `fix/selection-bars-and-tinjau` are recorded in `verification.md` (as phase 3 did).

- [x] 1.2 Confirm the dependencies and update the branch. `add-language-system` and `localize-server-text` must be merged into `main`; stop and report if either is not. Then update `feat/markas-tinjau` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for both merge commits;
  - `frontend/src/messages/en.json`, `lib/format.ts` and `lib/i18n/translatedRoutes.ts` exist;
  - an English Tinjau shows "This page is in Indonesian for now.";
  - `openspec validate --all --strict` passes on the updated branch;
  - the revision is recorded in `verification.md`.

  Done: the language changes arrived first through the stacked base `origin/feat/localize-server-text` (`af8f95e`, PR #90, on main `0308b7b`); PR #90 then merged (`17d3f4a`) and the branch was updated from `origin/main` `9a2f954` (merge `138005b`). Revisions in `verification.md`.

- [x] 1.3 Capture the "before" state of Tinjau on that `main` revision, tied to its hash:
  - with the parity seed and the audit seed, in `en` and `id`;
  - at 1440, 1280, 1199, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances;
  - the item of every kind, the done state, loading and failure;
  - plus `parity-before.json` (every count, amount and date on the page and in each item).

  Evidence: captures and the JSON in `research/evidence/apply-markas-tinjau/before/`; the Indonesian rendered-text snapshot `copy-before-id.json` for task 3.4.

  Done: captures and parity on the session scratchpad (`…/scratchpad/markas-tinjau/shots/final-before`, `parity/`), not `research/evidence/`, as in phases 2 and 3; the Indonesian copy comparison is a catalogue-to-old-source check (`verification.md` › Copy).

- [x] 1.4 Extend the seeds (D22), local and disposable only:
  - `seed_parity.py` gains an ambiguous leg, an awaiting debit with a learned top-up (its own debit, not `t017`'s), a pending history-suggestion group, a report skipped by an account link, a seen-twice item with a kept leg, and an automatic group;
  - a stress and USD seed: the audit seed, a 250-row group, more than 1,000 waiting rows, 60-character merchants, Rp 1.234.567.890, and the USD display currency.

  Evidence:
  - `test_seed_parity.py` passes;
  - the seed report lists every addition and every remaining skip with its reason;
  - `GET /api/review/queue` on the seeded database returns at least one item of each kind in the D9 table, plus an awaiting debit with a `topup_suggestion`, and `truncated: true` on the stress seed;
  - `git diff --stat origin/main -- backend db mobile` is empty.

  Done: the additions live in `scripts/verify/markas/seed_tinjau.py` (`--kinds`, `--stress`, `--currency`), run on top of the parity and audit seeds; its guards are tested in `test_seed_parity.py`.

- [x] 1.5 Add the Tinjau scenarios of D22 to `scenarios-app.json` and `scenarios-prototype.json` (`en` and `id` for the app), the `tinjau-checklist.js` for `measure.js`, and `tinjau-keys.mjs`. Evidence: a dry run over the prototype at 1440, 1024 and 390 in both themes writes captures and a checklist JSON whose expected values equal D1, D5 to D8 and the prototype's computed values; `tinjau-keys.mjs --dry-run` lists the eleven flows of D22.

  Done: `tinjau-checklist.js` (hooked into `capture.mjs` by the `tinjau` flag), `tinjau-keys.mjs`, `tinjau-parity.mjs`; prototype scenarios exist for the kinds it draws.

Expected Result: the dependencies are on the branch, every review kind can be seeded and captured, and the pre-change state is recorded in both languages.

## 2. Shared pieces

- [x] 2.1 Extend `ListRow` with `layout="stacked"`, `current` and `rowKey` (D6, D19). Evidence:
  - `primitives.test.tsx` cases: the stacked row is one `button` or `a` with the title over the meta; `aria-current="true"` and `data-selected` give `--surface-2` and the 2 px full-height bar at x 0; `data-row` is set;
  - the Beranda row tests pass unchanged;
  - `selectionMarks.test.ts` passes;
  - the `/komponen` sheet shows the stacked row at rest, hover, pressed, selected and focus in both themes, and the checklist values (min-h 64, padding 12, icon column 40) pass there.
- [x] 2.2 Make `HeroFigure`'s `mark` optional (D3, D19). Evidence: a component test renders no `sw-bar` without a mark and keeps the 32 px label row; the Beranda hero tests and captures are unchanged (pixel compare of `beranda-1440-light`).
- [x] 2.3 Add `Rail`, `RailItem` and `RailRule` with the chip line below 768 px (D5, D19). Evidence:
  - component tests: the `nav` is labelled; the current item has `aria-current="true"` and the bar pseudo-element 2 × 20 px at `left: -8px; top: 8px`; the count has its hidden text;
  - at 390 px the chips are 44 px, radius 6, 1 px `--line`, the current chip has `--surface-2` and `--line-strong` with no bar, and the line scrolls inside its box with page overflow 0 at 390 and 320;
  - `selectionMarks.test.ts` covers the rail;
  - the `/komponen` sheet shows rail and chips in both themes, and the checklist passes there.
- [x] 2.4 Add `ShellDialog`'s `fullHeight` variant and the `archive` icon (D13, D19). Evidence: a test that below 768 px the full-height dialog is `100vh` with radius 0 and above it the default dialog is unchanged; `Icon.test.tsx` covers `archive`; the existing `ShellDialog` tests pass unchanged.
- [x] 2.5 Add `Answers.tsx` (`AnswerList`, `Answer`, `KeyCap`, `pressChoice`, `pressMarked`) to replace `Choice.tsx` (D8). Evidence:
  - tests: each answer is one `button` with min-h 56, a key cap only for 1–9, `data-choice`, `data-primary-choice` and `data-topup` markers, `aria-keyshortcuts` with the digit and "E" or "T" where marked, and 45 % opacity when disabled;
  - `pressChoice` scopes to the last open nested list, as today;
  - axe reports no violation on the sheet's answer demo.

Expected Result: every piece Tinjau needs exists once, is measured on the component sheet, and leaves Beranda unchanged.

## 3. Language

- [x] 3.1 Add the `review` namespace and Tinjau's seven `errors` codes to `messages/en.json` and `id.json` (D18, Appendix A). Every existing Tinjau string moves with its Indonesian unchanged, and the full key list is written to `research/evidence/apply-markas-tinjau/copy-keys.md`. Evidence:
  - the catalogue parity test passes (same keys, same arguments, `one`/`other` in English);
  - `npm run type-check` fails on a planted missing key and passes without it.
- [x] 3.2 Move labels out of `lib/triage.ts` into `useTriageCopy()`. Move `lib/review.ts` reason and kind labels, `lib/reviewUndo.ts` messages and `lib/historySuggestions.ts` labels to keys, and every Tinjau date, amount and range to `useFormat()` (D18). Evidence:
  - `triage.test.ts` and `reviewUndo.test.ts` keep their data results;
  - `rg 'id-ID|Intl\.DateTimeFormat' frontend/src/components/review frontend/src/lib/triage.ts frontend/src/lib/review.ts` finds nothing;
  - formatter cases for `id-ID`, `en-ID` and `en-US` render a row's "4 Nov 21.40" and "Nov 4 9:40 PM".
- [x] 3.3 Add `/review` to `lib/i18n/translatedRoutes.ts`. Move `add-language-system`'s untranslated-page test example from Tinjau to Transaksi. Add Tinjau's roots (D18) to the copy register's translated roots. Evidence:
  - an English Tinjau has no "This page is in Indonesian for now." line and no `lang="id"` region, and an English Transaksi still has both;
  - the copy register passes in both languages, and one planted fixture per language fails: an Indonesian "Belanja" figure label in `review`, and an English "Expenses" or "your" label in `review`.
- [x] 3.4 Check the copy. Evidence:
  - the Indonesian rendered-text snapshot after the change equals `copy-before-id.json` for every unchanged key;
  - the differences are exactly the intentional ones listed in `verification.md` ("Ya, top up", the D9 questions, "Saran", the new keys of Appendix A, the removed done-state figures);
  - every English string is checked against Appendix B, and no English label uses "Expense", "Spent", "Outflow", "ledger" or slang.

Expected Result: Tinjau's copy comes from the catalogues in both languages, Indonesian reads as today except the listed changes, and `/review` is a translated route.

## 4. Tinjau page

- [x] 4.1 Compose the page (D1, D2, D3): title block with the "…" menu, hero, and the `.tj` region with the 1200 and 768 breakpoints. Remove `TWO_PANE_MIN` and Tinjau's `useWidthAtMost`. Evidence: `page.test.tsx` covers:
  - one `h1`;
  - one "…" button whose menu holds "Cari saran dari riwayat" and "Bantuan & pintasan", with arrow keys, Esc and focus return;
  - the hero equal to `attention` and the badge, with the line from `unreviewed` and the truncated line;
  - DOM order title → hero → rail → list → item;
  - spec scenarios "Hero equals the badge", "Truncated queue", "Open the suggestion dialog by keyboard" and "Nothing in the pane".
- [x] 4.2 Build the folder rail and chips into Tinjau (D4, D5). Evidence: `page.test.tsx` covers the spec scenarios:
  - "Counts match the badge";
  - "Folder filter";
  - "Empty folders hidden";
  - "Optional folder chosen";
  - "Optional automatic rows";
  - "Automatic rows inside a mixed group";
  - and that `buildTriage` membership and counts are unchanged (the existing triage tests pass unchanged).
- [x] 4.3 Build the item list on the stacked row (D6). Evidence: `page.test.tsx` covers:
  - "Question Row Direction" (both scenarios);
  - the meta line in "Semua" and in other folders;
  - the amount variants (expense, income, transfer without "⇄", empty for a seen-twice item without an amount);
  - masking with hidden balances;
  - focus returning to `data-row` after the phone sheet closes;
  - the empty-folder state.
- [x] 4.4 Build the item pane (D7): kind label, position and ‹ ›, the question as the focused `h2`, meta, warning, reasons line, status line and the notification text box. Evidence: `page.test.tsx` covers:
  - "Three panes on a wide screen", "Two panes on a wide screen", "Item below the list at 1024 px" and "One pane at 768 px" (with the viewport mocked);
  - ‹ › disabled at the ends with `aria-keyshortcuts`;
  - "Hidden balances";
  - the raw box's hidden prefix and no `lang` on the quoted text.
- [x] 4.5 Build each kind's content and answers (D8, D9). Evidence:
  - one `page.test.tsx` case per kind of D9: kind label, question, meta, answers and rows;
  - every existing request-level test passes with each action sending today's request: purpose answer, group accept, recategorise and rename, awaiting completion with the 409 candidates, merge, keep separate, expense, top-up convert and reject with `suggested_target_account_id`, seen-twice acknowledge and record separately with the learned-link toast and "Batalkan", suggestion accept and reject per group and per row, batches of 200 and partial failure;
  - spec scenarios "Keyboard group accept", "Complete an awaiting transfer", "The pair on Tinjau" and "No hint strip".
- [x] 4.6 Build the dialogs (D10, D11, D12) and remove `HistorySuggestionsSection`. Evidence:
  - `page.test.tsx` and dialog tests cover "Recategorise a group", "Rename without input", "Failed save", "Focus return", "Empty answer", "Answer by text", "Estimate then confirm by keyboard", "Start disabled before an estimate", "Cancel the estimate", "Invalid period", "Model unavailable" and "Closed while running";
  - each dialog is centered at 1440 and a bottom sheet at 390, traps Tab, closes on Esc and returns focus to its opener;
  - axe reports no violation with each dialog open.
- [x] 4.7 Build the phone sheet (D13). Evidence:
  - `page.test.tsx` at 390 px covers "Back to the list", "Answer inside the sheet", a deep link opening the sheet with the heading focused, and "Phone folders";
  - the sheet is inside the region (J, 1 and Z act inside it, and J does nothing on the phone header).
- [x] 4.8 Build the states (D14 as reviewed in 1.1, D15). Evidence: `page.test.tsx` covers:
  - "Loading" (no hero, count or amount; `aria-busy`);
  - "Failure and retry";
  - "Nothing waiting";
  - "Nothing at all";
  - "Summary unavailable" (no cycle-summary request);
  - "Not counted as waiting";
  - a failed suggestions request leaving the queue visible without "Saran".
- [x] 4.9 Keep keys, focus, URL and Urungkan as today (D16). Evidence:
  - every existing `Triage Keys`, `Auto-Advance`, `Tinjau Deep Links` and `Urungkan on Tinjau` test passes unchanged, including "Outside the region", "Typing an answer", "Switched off", "Undo by Z after the message closed", "Nothing to undo" and "Tokens are not stored";
  - "Help lists the page keys" passes for `?` and for the menu item;
  - a test shows `scroll-margin-top` keeps a focused row and the heading below the top bar.
- [x] 4.10 Remove what the change made obsolete (D19, D20). Evidence:
  - `rg` finds no `ChipGroup`, `ItemHead`, `Choice`, `TriageRows`, `HistorySuggestionsSection`, `RUN_PANEL_ID`, `TWO_PANE_MIN`, `foldOpen`, `ws-tri-`, `ws-choice`, `ws-picker`, `ws-facts` or `ws-tri-fold`;
  - each class kept in D19 (`.ws-ava`, `.ws-kbd`, `.ws-field`, `.ws-chip`, `.ws-rows` if kept) still has a user;
  - `npm test`, `npm run type-check` and `npm run lint` pass.

Expected Result: Tinjau is built from the shared pieces, every requirement in the deltas has a passing test, every action sends today's request, and nothing obsolete remains.

## 5. Verification gate

- [x] 5.1 Side by side, on the parity seed: prototype and app captures of every D22 scenario at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, the app in `en` and `id`, plus composed left/right images. Add the widths 1280, 1199, 768, 767 and 320. Evidence: the images in `research/evidence/apply-markas-tinjau/after/`, tied to the branch revision, with every visible difference listed in `verification.md` and each one fixed, or named as a deviation of the proposal, or added to the owner's end-review list.
- [x] 5.2 The measurement checklist and the SPEC §13 global rules (D22), in both languages. Evidence: the checklist JSON with pass and fail counts in `verification.md`. It must show zero fails, or each fail with its reason, including:
  - `.tj` columns and gap;
  - equal top edges;
  - first content `y = 192`;
  - one 44 px figure and no other ≥ 33 px;
  - sizes, weights and ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the bars;
  - no horizontal overflow at 1440, 1024, 390 and 320;
  - rail labels and chips in English at 184 px and 390 px.
- [x] 5.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings, in both themes and both languages, at 1440, 1024 and 390:
  - with an item of each kind;
  - with each dialog open, the "…" menu open and the phone sheet open;
  - with hidden balances;
  - in the loading, failure and empty states.
  Zero console errors. `lang` attributes are correct (no `lang="id"` region on an English Tinjau). Results in `verification.md`.
- [x] 5.4 Keyboard flows: `tinjau-keys.mjs` runs the eleven flows of D22 in both languages at 1440 and 390. Evidence: every flow passes, recorded in `verification.md` with the revision.
- [x] 5.5 Figure parity. Evidence: `parity.mjs` and a `verification.md` table with zero changed values and the removed done-state figures listed with Beranda as their place:
  - before (task 1.3) against after, with the same seed within the same minutes: the hero, folder counts, list amounts and dates, each item's meta and answer amounts, and the rows below;
  - `en` against `id` (region `ID`) on the same data, byte-identical figure strings.
- [x] 5.6 Stress and USD seed at 1440, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for 60-character merchants, Rp 1.234.567.890 and the USD display; the 250-row group accepted in two batches; the truncated line shown; recorded in `verification.md`.
- [x] 5.7 Frontend and repository checks on the exact revision. Evidence: every result recorded in `verification.md` with the commit hash:
  - `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass;
  - `git diff --stat origin/main -- backend db mobile nginx` is empty.
- [x] 5.8 Complete `verification.md` and present the result. Record the owner's standing instruction of 2026-11-06 as the acceptance basis, and add the deviations and captured kinds without a prototype screen to the end-review list in `PROJECT_STATE.md`. Evidence:
  - `verification.md` names what changed, the gate counts, the deviations, the D14 outcome and what was not verified and why;
  - the report to the owner quotes them.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-tinjau`, then `openspec validate --all --strict` must pass. The archive path is under `openspec/changes/archive/`.
- Commit with a subject-only Conventional Commits message, and push `feat/markas-tinjau`.
- The pull request is not created by this run: the caller's instruction for this phase (2026-11-08) is to push the branch only. Merging stays with the owner.
- After phase 8 merges, delete `fix/selection-bars-and-tinjau` locally and on the remote and remove the `cash-flow-tracker-ui-refine` worktree, after checking that its parts are on `main` (design D21).
