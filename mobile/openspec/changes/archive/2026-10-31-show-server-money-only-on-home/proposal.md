# Proposal

## Approval
On 2026-10-31 the owner approved the coordinator's recommendation for Beranda's money figures: "ok do all as recommend". The recommendation: money on Beranda comes only from the tracker's cash-flow summary, the local money tile becomes a tile of counts, and the top categories come from the server or the tile is removed, with no backend change in this change. It builds on the companion redesign plan the owner approved with defaults on 2026-10-29 (research/notes/2026-10-29-companion-redesign-plan.md); coordinator-reviewed. It ships as 2.3.1, code 23.

## Why
The owner reported that the phone's home numbers "aren't logical and don't sync with the web UI". Beranda shows two different money figures for today: "Arus kas tracker", read from the server's `/api/dashboard/cash-flow-summary` and equal to the web, and "Keluar tercatat hari ini", a sum of this phone's notification rows and captures. The local sum misses everything recorded on the web or another device and ignores the server's reimbursement treatment, so it disagrees with the web by design. "Kategori teratas · 3 hari" and the three-day bars are local money too.

## Expected Outcome
The only money amounts on Beranda are the tracker's own, with their period and freshness. The phone's own activity is still visible on Beranda, as counts: how many notifications it captured today, how many wait or are unsent, and how many typed captures wait. Nothing on Beranda can disagree with the web's figures.

## What Changes
- Remove "Keluar tercatat hari ini" with its Masuk and Pindah amounts and the three-day money bars.
- Add "Aktivitas ponsel hari ini": notifications captured today, those waiting or unsent, and "· n catatan" for typed captures waiting, labelled as coming from this phone, opening Aktivitas. A count not yet read from the phone's database shows "—", never 0.
- Remove "Kategori teratas · 3 hari". The server has no per-category amounts for the summary's period (today) behind Bearer auth: `/api/dashboard/overview` and `/api/dashboard/analytics` break categories down only for a salary cycle, 30 or 90 days or a month. A server endpoint is a follow-up.
- Keep "Arus kas tracker" unchanged, with its loading, cached, stale and offline states, and the Pertanyaan, Sinkron, Perlu cek and Di ponsel tiles.
- Remove the local money model, the bar colour tokens and the formatting used only by the removed tiles.
- Version 2.3.1, code 23.

## Non-goals
- A backend endpoint for top categories. It is recorded as a follow-up.
- Aktivitas and Rincian. Each row keeps its own amount, and Aktivitas keeps its per-day money out.
- Room, sync, capture, pairing and the server summary's request, caching and refresh.
- Installing on the owner's TECNO, or any production access.

## Capabilities
### New Capabilities
None.
### Modified Capabilities
- `companion-interface`: Beranda's figures become counts from this phone plus the tracker's cash-flow summary; the categories tile is removed from the home tiles and from large-text layout.
- `mobile-text-capture`: Recorded captures no longer appear in any Beranda money figure; the tracker summary is still fetched again after a capture changes.

## Impact
- Code: `ui/home/HomeScreen.kt`, a new counts model replacing `ui/model/HomeFigures.kt`, `CompanionLists` and `CaptureViewModel` gain a loaded state, `ui/theme/CompanionColors.kt` and `ui/components/Formatting.kt` lose unused members, `app/build.gradle.kts` version.
- Tests: `CompanionModelTest`, `CaptureModelTest`, `CompanionScreensUiTest`, `LargeTextUiTest`, `ContrastTest`, `ScreenHarness`, a new `HomeActivityUiTest`; device tests `LedgerSummaryDeviceTest` and `DeviceScreenshotTest`.
- No API, database, dependency or permission change.
