# Proposal

## Approval
On 2026-10-31 the owner approved the coordinator's small follow-ups for the companion, "do all the small items too": show on the phone when the tracker recorded a notification from the owner's habits, and bring back "Kategori teratas" on Beranda from the tracker's own figures. It builds on the companion redesign plan the owner approved with defaults on 2026-10-29 (research/notes/2026-10-29-companion-redesign-plan.md) and on the recorded follow-up of `show-server-money-only-on-home`; coordinator-reviewed. It ships as 2.3.3, code 25.

## Why
The tracker (1.8.0 and later) records some notifications from the owner's habits instead of asking, and says so in the notification result with an optional `habit` object. The phone drops that object today, so a habit-recorded payment looks like any other "Tercatat" row and the owner cannot tell why no question was asked. Beranda lost its "Kategori teratas" tile in 2.3.1 because the tracker had no per-category amounts for the summary's period; the tracker now adds `top_categories` to the same cash-flow summary, so the tile can return with figures that match the web.

## Expected Outcome
- A row the tracker recorded from a habit carries a small "Dari kebiasaan" tag in Aktivitas, which TalkBack reads with the row, and Rincian says "Dicatat dari kebiasaan: <kategori> · <n dari m jawaban sebelumnya>".
- Beranda shows "Kategori teratas" with the summary's period and freshness, the top three categories with their amounts and Kakeibo pillar, and bars relative to the largest. Every amount comes from the tracker. An older tracker without the field shows no tile; a period without spending says "Belum ada pengeluaran pada periode ini".

## What Changes
- Parse the optional `habit` (`label`, `basis`) of a recorded expense or income result; it is kept with the row's stored result JSON, so no Room change is needed.
- Aktivitas: a "Dari kebiasaan" tag beside the status badge, and the same words in the row's TalkBack description.
- Rincian: a line "Dicatat dari kebiasaan: … · …" in "Yang dicatat tracker".
- Parse the optional `top_categories` of `GET /api/dashboard/cash-flow-summary`; it is cached with the summary per pairing and shares its loading, cached, stale and offline states.
- Beranda: the "Kategori teratas" tile, hidden when the field is absent or malformed, with an empty state.
- Bar colour tokens for the tile, checked for contrast in both themes.
- Large text (coordinator follow-up, same day): Beranda's label-and-amount figures and Aktivitas' day headers no longer break an amount or a date across lines at font scale 2.0.
- Version 2.3.3, code 25.

## Non-goals
- Any backend change; both fields are the tracker's existing or parallel additive contract.
- Computing categories or habits on the phone.
- Habit tags on typed captures, which the tracker does not classify from habits.
- A Room schema change, or re-reading old results to fill in the habit for rows stored before 2.3.3.
- Installing on the owner's TECNO, or any production access.

## Capabilities
### New Capabilities
None.
### Modified Capabilities
- `companion-interface`: rows recorded from a habit say so in Aktivitas and Rincian; Beranda shows the tracker's top categories for the summary's period, or no tile when the tracker does not send them; amounts and day headers stay readable at font scale 2.0.

## Impact
- Code: `ui/components/Surfaces.kt` (`SectionLabel` takes `softWrap`), `data/network/model/ApiModels.kt` (`RecordedHabit`, `TopCategory`), `ui/model/RowPresentation.kt`, `ui/activity/ActivityScreen.kt`, `ui/detail/DetailScreen.kt`, `ui/home/HomeScreen.kt` and a new `ui/home/TopCategoriesTile.kt`, `ui/theme/CompanionColors.kt`, `app/build.gradle.kts` version.
- Tests: new parsing, cache and UI tests; `ContrastTest` gains the bar pairs.
- API: reads two optional, additive response fields. No database, dependency or permission change.
