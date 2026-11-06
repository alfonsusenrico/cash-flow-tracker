## 1. Approval, tooling and baseline

- [x] 1.1 Record the owner's approval and the answers to the thirteen owner decisions in `design.md`, with the date, in `proposal.md` (replacing "Plan approval pending") and in `PROJECT_STATE.md`. Where an answer differs from the default, update `design.md` and the spec deltas first. Evidence: both files name the date and every decision's choice; `openspec validate apply-markas-beranda --strict` passes after the edits.
- [x] 1.2 Capture the "before" state of Beranda on `feat/markas-shell` `539b367`:
  - with the parity seed and with the audit seed;
  - at 1440, 1280, 1199, 1024, 768, 767, 390 and 320 px, in both themes, with and without hidden balances;
  - plus the figure baseline for parity.

  Evidence: captures, the shell checklist result and `parity-before.json` in `research/evidence/apply-markas-beranda/before/`, tied to that revision; the 24 px Kekayaan bersih overflow at 1024 px is reproduced and recorded.
- [x] 1.3 Add the Beranda scenarios (`beranda`, `dlg-rincian-aman` = prototype `#/beranda?d=rincian:safeToSpend`, the phone sheet, hidden balances) to `scenarios-app.json` and `scenarios-prototype.json`. Add a Beranda checklist to `measure.js`, generated from the D3 to D8 tables and SPEC §2, §5 and §7. Evidence: a dry run over the prototype at 1440, 1024 and 390 in both themes writes captures and a checklist JSON whose expected values equal the design tables and the prototype's computed values.

Expected Result: Beranda can be measured against the prototype with the prototype's own data, and the pre-change state, including the 1024 px overflow, is recorded.

## 2. Shared pieces

- [x] 2.1 Add `MaskedMoney` (D11, Owner decision 11). Evidence:
  - a component test: with hidden balances it renders "Rp ••••••" (and "$ ••••••" for USD) with `role="img"` and the name "Disembunyikan", and no digit appears in its text or in any accessible name built from it; without hidden balances it renders the formatted amount as text;
  - axe reports no violation in either state.
- [x] 2.2 Add `FigureBlock` (hero and supporting variants) and `SplitBar` (D3, D4). Evidence:
  - component tests for the split bar's accessible name with rounded percentages, "belum ada uang masuk" with masuk 0, no bar when every segment is 0, zero width for a savings decrease, a 2 px minimum and 2 px gaps;
  - the hero is 32/40 below 360 px;
  - the `/komponen` sheet shows both in both themes, and the checklist values for `.hero`, `.split`, `.legend` and `.fig-block` pass there.
- [x] 2.3 Add `Panel`, `PanelBlock`, `SectionHeader`, `ListRow` (compact and full variants, phone two-line) and `DayHeading` (D7, D8). Evidence:
  - component tests: a row is a single `button` or `a` named by its title, meta and amount; the expense, income, movement and top-up variants have the D7 signs and colour tokens; the phone variant has two lines; the sticky day heading has `top` equal to the top bar's height;
  - `selectionMarks.test.ts` passes;
  - the `/komponen` sheet shows every row variant and state (rest, hover, pressed, selected, focus) in both themes, and the checklist passes there.
- [x] 2.4 Add `ExplanationDialog` on the shared dialog primitive (D5, Owner decision 3): 480 px; header, rule, rows, total, notes, footer link and line disclosures. Evidence:
  - `explain.test.tsx` cases: the dialog is centered at 1440 and a bottom sheet at 390; focus is trapped; Esc and the close button return focus to "Rincian"; a disclosure toggles `aria-expanded` with Enter and Space and shows exactly the lines it is given; amounts are masked with hidden balances;
  - the existing `ExplainableFigure` popover tests pass unchanged;
  - axe reports no violation with the dialog open in both themes.

Expected Result: every piece Beranda needs exists once, is measured on the component sheet, and is ready for phases 4 to 9.

## 3. Beranda

- [x] 3.1 Write the title block line (D2, Owner decision 2). Evidence: `page.test.tsx` covers "Siklus gaji 25 Okt – 24 Nov · hari ke-9", "Siklus gaji sejak 25 Okt · hari ke-9" and the month fallback with the "Atur gaji di Pengaturan" link (spec "Beranda Period"), and the page still has one `h1`.
- [x] 3.2 Build the hero, legend and Rincian content (D3 to D5, Owner decisions 1, 3, 9 and 10). Evidence: `page.test.tsx` covers:
  - each figure equal to its server field;
  - the line for available, unknown, late, no-source and over states, and no per-day amount unless decision 1 is (A);
  - exactly one control named "Rincian", and no dotted figure;
  - the Rincian rows and total equal to today's `remainingExplanation` values;
  - each disclosure listing today's spending, savings-change, scheduled and masuk lines, and the "Menunggu jawaban" disclosure with section and bucket per line (spec "Waiting-for-Answer Figure");
  - the "Tabungan berkurang" caption for a negative savings change.
- [x] 3.3 Build "Perlu tindakan" on the list row (D6, Owner decision 4). Evidence: `page.test.tsx` covers:
  - the first three rows of the D6 ranking (as revised on 2026-11-06): question, awaiting transfers and group for the spec example, and question, overdue plan and due-today plan when plans are due;
  - each row's target (Tinjau anchors, "Rencana jatuh tempo", `/goals`), including that a due plan still opens the centered dialog with its link, skip and record actions;
  - "Buka Tinjau · N", and "Buka Tinjau" without a count at 0;
  - the empty line and the error line;
  - no action pill, no waiting amount and no auto-recorded note.
- [x] 3.4 Build "Aktivitas terbaru" (D7, Owner decisions 5, 6 and 12). Evidence:
  - `activity.test.ts` covers day grouping in Asia/Jakarta across midnight, leaving out `needs_review` rows, the eight-row cap applied after that, and the meta and status wording per kind;
  - the existing helper tests pass unchanged, and Transaksi, Rekening, Kalender and the transaction drawer tests pass unchanged;
  - `page.test.tsx` covers a row opening the shared transaction dialog with focus returned, a row deleted elsewhere, a top-up named "Top up Investasi" in lavender, a movement route with the parent bank, and the expense in ink with "−".
- [x] 3.5 Build the context panel (D8, Owner decisions 7 and 8). Evidence: `page.test.tsx` covers:
  - Kekayaan bersih with aset and utang equal to today's expressions (credit owed included), linking to `/accounts`;
  - Ketahanan Dana for known, unknown (no number), error and `all_liquid` states with "target N bulan", linking to `/goals`;
  - Kakeibo with five rows, no total and no Tabungan row, each label linked to its ledger `href`, and "Perlu dipilih di Tinjau" only when unclassified spending exists.
- [x] 3.6 Compose the page (D1, D9, D13). Remove the `useWidthAtMost` reorder and the two render branches. Add the loading placeholders and the failure state. Evidence: `page.test.tsx` covers:
  - DOM order hero → Perlu tindakan → Aktivitas → panel;
  - `aria-busy` placeholders without amounts;
  - on summary failure, the error box with "Coba lagi" and no Rp 0, while Perlu tindakan, Aktivitas and Ketahanan Dana still render;
  - axe with no violations in the normal, hidden-balance and failure states.
- [x] 3.7 Remove what the change made obsolete (D10, D12): `MoneyBar`, `BerandaKpis`, `MoneyCard`, `KakeiboCard`, `DanaCard`, `BerandaSkeleton`, the Beranda-only CSS rules and the explanation builders no one reads. Evidence: `rg` finds no reference to the removed names or classes; the kept classes (`.ws-kcards`, `.ws-kk*`, `.ws-dana*`, `.ws-ava`, `.ws-xn`) still have users; `npm test`, `npm run type-check` and `npm run lint` pass.
- [x] 3.8 Extend `copyRegister.test.ts` with the new Beranda strings ("Tidak ada yang perlu dijawab.", "Perlu dipilih di Tinjau", "Lihat transaksi siklus ini", the hero lines and captions). Evidence: the test passes, and it fails when a string uses "Keluar" or "Belanja" for spending.

Expected Result: Beranda is built from the shared pieces, every requirement in the deltas has a passing test, and nothing obsolete remains.

## 4. Verification gate

- [x] 4.1 Side by side: prototype and app captures of `beranda` and `dlg-rincian-aman` at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, with the parity seed, plus composed left/right images. Add the extra widths 1280, 1199, 768, 767 and 320. Evidence: the images in `research/evidence/apply-markas-beranda/after/`, tied to the branch revision, with every visible difference listed in `verification.md` and each one either fixed or named as an owner decision.
- [x] 4.2 The measurement checklist and SPEC §13 global rules:
  - every D3 to D8 value, first content `y = 192` at 1440, panel 352 and gap 24;
  - one 44 px figure and no other ≥ 33 px; sizes, weights and ≤ 8 text styles; one "Rincian"; no border ≥ 2 px and no coloured border except the marks;
  - no horizontal overflow at 1440, 1024, 390 and 320; every figure inside its block from 1024 to 1199 px.

  Evidence: the checklist JSON with pass and fail counts in `verification.md`; zero fails, or each fail listed with its reason and owner decision; the phase 2 Kekayaan bersih overflow is shown resolved at 1024, 1100 and 1199 px.
- [x] 4.3 Accessibility and keyboard. Evidence:
  - axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings in both themes at 1440, 1024 and 390, with Rincian open, with a disclosure open and with hidden balances;
  - zero console errors;
  - the scripted keyboard flow of D14 passes (tab order, Rincian open and close with focus return, disclosures, row → transaction dialog → focus return, no focused row under a sticky heading);
  - results in `verification.md`.
- [x] 4.4 Figure parity: `parity.mjs` on `539b367` and on this branch with the same seed within the same minutes. Evidence: `parity.json` and a `verification.md` table with:
  - every kept figure, with equal values;
  - every removed duplicate, named with the kept occurrence it repeated;
  - every moved figure (waiting amount → Rincian; rupiah target → months, the Target & Tagihan value unchanged);
  - zero changed values;
  - the Rincian lines equal to the old explanations' values.
- [x] 4.5 Real-data shape: the audit seed (17 pockets, long names, several platforms), one stress amount of Rp 1.234.567.890 in a supporting figure and in Kekayaan bersih, and the USD display, at 1440, 1024, 390 and 320 in both themes. Evidence: captures with no clipping, overlap or horizontal scroll, and Owner decision 13's outcome (four columns kept, or the measured two-column width) recorded.
- [x] 4.6 Frontend and repository checks on the exact revision. Evidence: `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass, and `git diff --stat origin/main -- backend db mobile` is empty. Every result is recorded in `verification.md` with the commit hash.
- [x] 4.7 Present the verification to the owner: what changed, the gate counts, the deviations with their decisions, what was not verified, and the items added to the end-review list in `PROJECT_STATE.md`. Evidence: the owner's acceptance, quoted with its date in `verification.md`.
- [x] 4.8 After the owner's acceptance, archive and push: run `openspec archive apply-markas-beranda`, then `openspec validate --all --strict`, commit with a subject-only message, push `feat/markas-beranda`, and create the PR into `main` (owner instruction, 2026-11-06). Evidence: the archive path under `openspec/changes/archive/`, the passing validation, the pushed revision and the PR URL in the report.

Expected Result: the phase passes the plan's common gate on its exact revision, and the owner has accepted it before anything is archived or pushed.
