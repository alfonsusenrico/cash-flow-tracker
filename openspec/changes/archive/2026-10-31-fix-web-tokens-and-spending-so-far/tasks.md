# Tasks

## 1. Spending so far on Beranda

- [x] 1.1 Add backend tests for the 31 October case (`current.outflow` 200.000 over 1–30 Oct, `current_to_date.outflow` 1.600.000 over 1–31 Oct, with and without previous data) and for agreement with `/dashboard/overview` `total_outflow` on today's spending for `month` and `cycle`. Evidence: both fail before the fix with `KeyError: 'current_to_date'`.
- [x] 1.2 Report `current_to_date` from `period_comparison`. Evidence: the two tests pass; `test_observed_insights.py` 10 passed.
- [x] 1.3 Use `current_to_date.outflow` for every "sejauh ini" sentence in `NarrativeInsightCard`. Evidence: two new card tests fail on the old card and pass after.

Expected Result: the card's spending so far equals the Beranda summary for the same period.

## 2. Tokens, contrast and gutter

- [x] 2.1 Extend `tokens.test.ts` to component sources with an empty allowlist, and to danger, warning, info and accent text pairs in both themes. Evidence: 9 of 11 fail on the previous tokens and pass after.
- [x] 2.2 Define the missing tokens, point `--foreground` at `--text` and drop the redundant `#66CC55` fallbacks. Evidence: the undefined-property test passes.
- [x] 2.3 Add `--danger-solid`, `--danger-solid-hover` and `--danger-contrast`; use them in `Button` danger, `confirmDangerClass`, `ConfirmActionButton`, "Ya, hapus", "+ Tagihan Baru" and "Catat pembayaran manual"; stop passing the trigger classes to the confirm button. Evidence: the new `ConfirmActionButton` test fails before and passes after; browser measurements at least 4.5:1.
- [x] 2.4 `AppLayout` phone gutter `px-4`. Evidence: main padding 16 px at 390 and 320 px; no overflow or off-screen elements.

Expected Result: no undefined custom properties, destructive actions at AA in both themes, 16 px phone gutter.

## 3. Verification and delivery

- [x] 3.1 Frontend `npm test`, `npm run lint`, `npm run type-check`, `npm run build`. Evidence: exit codes and counts in verification.md.
- [x] 3.2 Backend suite on a fresh PostgreSQL migrated V1→V38, plus the run without a database. Evidence: counts in verification.md.
- [x] 3.3 Browser before and after: seven pages at 1440, 1280, 390 and 320 px in light and dark; destructive actions and primary buttons at 1280 and 390 px in both themes; the Beranda card against `/dashboard/overview`. Evidence: counts and screenshot folders in verification.md.
- [x] 3.4 `git diff --check`, `openspec validate fix-web-tokens-and-spending-so-far --strict`, `openspec validate --all`. Evidence: exit codes in verification.md.
- [x] 3.5 Archive, commit, push `fix/phone-spacing-tokens` and report the compare link. Evidence: pushed branch.

Expected Result: all four defects fixed with tests and browser evidence, pushed for the owner's review.
