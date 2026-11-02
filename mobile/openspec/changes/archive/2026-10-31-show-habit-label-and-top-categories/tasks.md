# Tasks

## 1. Contract and baseline
- [x] 1.1 Record the approval, the baseline and both server contracts. Evidence: proposal.md `## Approval`; 498 unit tests, 0 failed, 6 skipped on 2cd2572 from the JUnit XML; design.md Context citing cash-flow-tracker 26e04af and the empty `git ls-remote` for `chore/small-follow-ups`.

Expected Result: the change starts from a known passing baseline and checked contracts.

## 2. Habit label
- [x] 2.1 Parse the optional `habit` and keep it in the stored result JSON without a Room change. Evidence: `TrackerOptionalFieldsTest` (with, without, incomplete, round trip, detail line), `HabitResultTest` (stored and shown, older tracker, later result without habit).
- [x] 2.2 Show "Dari kebiasaan" on the Aktivitas row and in its TalkBack description, and the habit line in Rincian. Evidence: `HabitLabelUiTest` (4 tests × light/dark × font 1.0/2.0).

Expected Result: a habit-recorded row says so in Aktivitas, TalkBack and Rincian, and nothing changes for older trackers.

## 3. Top categories
- [x] 3.1 Parse the optional `top_categories`, validate it and cache it with the summary. Evidence: `TrackerOptionalFieldsTest` (with, without, empty, contract breaks, round trip), `LedgerSummaryRefreshTest` (cached and stale with the summary, replaced by an empty or absent list, dropped on re-pairing) against MockWebServer.
- [x] 3.2 Add the "Kategori teratas" tile with period, freshness, pillar, bars and the empty state; add the bar tokens; update README. Evidence: `TopCategoriesUiTest` (9 tests × light/dark × font 1.0/2.0), `ContrastTest` with four bar pairs, `HomeActivityUiTest` unchanged and passing.
- [x] 3.3 Version 2.3.3, code 25. Evidence: `aapt2 dump badging` and `dexdump` of the clean APK.

Expected Result: Beranda shows the tracker's top categories with the summary's freshness, or no tile for an older tracker.

## 4. Verification and delivery
- [x] 4.1 Full unit suite, lint, androidTest compile and clean APK. Evidence: verification.md.
- [ ] 4.2 Device screenshots on the Realme with the verification build. Not run: the test phone (Realme) was not attached. Evidence: verification.md.
- [x] 4.3 OpenSpec strict validation, archive, commit and push. Evidence: `openspec validate --all --strict` exit 0.

Expected Result: a reviewable branch with recorded evidence and the change archived.

## 5. Large text follow-up (coordinator, 2026-10-31)
- [x] 5.1 Write `LargeTextAmountsUiTest` (native graphics, font 2.0, 360 and 320 dp, light and dark) and see it fail on the previous layout. Evidence: 8 of 8 failed with the layout of cbf29f8 (verification.md).
- [x] 5.2 Keep Beranda's figure amounts whole: `Figure` becomes a `FlowRow` with an unbreakable value; TalkBack text unchanged. Evidence: `LargeTextAmountsUiTest` Beranda cases; `HomeActivityUiTest` and `CompanionScreensUiTest` content descriptions unchanged and passing.
- [x] 5.3 Wrap Aktivitas' day header between words with the date kept together and the total below it at large text. Evidence: `LargeTextAmountsUiTest` Aktivitas cases; JVM renders at font 1.0 and 2.0.
- [x] 5.4 Add the "Amounts and day headers at large text" scenario; re-run the full suite, lint, clean APK with dexdump and strict validation. Evidence: verification.md.

Expected Result: at font scale 2.0 no amount breaks across lines and day headers read cleanly, at the same version 2.3.3/25.
