# Proposal

## Approval
On 2026-11-02 the owner compared three clickable prototypes of the companion (A "Utilitas tenang", B "Kotak pertanyaan", C "Satu layar status") and answered: "C, all four defaults ok". The four defaults he accepted:

1. Full edits of notification-captured transactions move to the web. The phone keeps "Perbaiki label" (category, Kakeibo pillar, merchant), and that correction teaches the AI.
2. Urgent questions (sender name, source pocket, account mapping) interrupt right away. "Apa ini?" (purpose) and habit guesses are batched into one evening reminder.
3. The evening reminder is on by default at 20.00 and can be changed or turned off in Pengaturan.
4. Direction C.

This builds on the Phase 0 approvals of the same day: phone direction B ("a tracker companion with no financial dashboards"), the monorepo move, and the device health contract fixed by the coordinator. It ships as companion 2.4.0.

## Why
The phone is a helper that runs in the background, but its home still reads like a small finance app: a money tile ("Arus kas tracker"), a top-categories tile and four count tiles behind a five-item bottom bar. The owner's direction is that money is read on the web and the phone answers one question first: "is it working?". The failures this app has actually hit (notification access lost after an update, HiOS freezing the listener, a server that refuses the key, a quiet day that is really a broken listener) are invisible on today's home, and setup ends without proving that a notification can be caught.

## What Changes
- **Beranda becomes one status hub** (direction C): a status card ("Lagi jagain notifikasimu", today's capture count, never money, the last capture, the sync line and "Rincian"), one large "Catat transaksi" button, the next question answerable in place with the others stacked behind it, a "Laporan siap" card when the tracker says a monthly report is ready, and three tiles: Riwayat, Pengaturan, Buka web. Everything else opens as a sheet over the hub.
- **Problem states take over the status card** with one full-width fix: notification access lost, the listener disconnected or frozen by a battery saver (with the HiOS/TECNO hint), the server unreachable, the key refused, items waiting to send, and a day with no capture while the listener is on (the fix runs the self-test). Battery, notification permission and auto-sync issues are softer notes in the card.
- **BREAKING (UI):** the bottom bar (Beranda, Aktivitas, Catat, Tanya, Atur) is removed. Aktivitas becomes the Riwayat sheet, Tanya the Pertanyaan sheet, Atur the Pengaturan sheet; Rincian nests inside Riwayat.
- **Money leaves the phone's home.** "Arus kas tracker" and "Kategori teratas" are removed with their model, client call, cache and tests. Riwayat keeps per-item amounts but drops the per-day money out. The server endpoints stay.
- **Setup becomes story cards**: welcome, pair (QR with the manual code and the manual key as fallbacks), notification access, battery and background with OEM tips, notification permission, a **self-test**, and "HP-mu siap". The self-test posts a marked notification that the phone's own listener must catch within 10 s; it is never stored, synced or sent as a capture. Every failure has its fix.
- **Health reporting**: the phone sends `POST /api/devices/health` per the device health contract after setup and the self-test, when the app opens, when the listener state changes, and at most every 15 minutes. An older server's 404 is handled quietly with backoff.
- **Batched light questions**: purpose ("Apa ini?") questions, including habit guesses, no longer interrupt; the recorded alert is shown as before and one evening summary ("Ada 3 pertanyaan ringan hari ini") opens the question sheet at the configured time (default 20.00, changeable or off in Pengaturan; off returns them to immediate prompts). Sender, source-pocket and account questions still interrupt. Every reply path and the 2.3.2 answer-attempt record are kept.
- **Perbaiki label** on a recorded notification edits category, pillar (including "Belum diklasifikasi") and merchant through `PATCH /api/transactions/{id}`, so the server writes its owner-correction feedback. Full edits link to the web ledger (Buku Kas). Typed captures keep "Ubah".
- **Catat understands time**: it declares `question_types` with `time`, answers "Jam berapa?" with chips or text, and shows the summary's `time_label`.
- **Notifications**: the ongoing listener notification in Indonesian with a tap target, the evening summary, and "Laporan siap" from `GET /api/reports/monthly/latest`, which opens the web report.
- **Pengaturan** as plain grouped lists: monitored apps, sending (auto-sync), evening reminder (on/off, time), connection (device, test, re-pair, manual key), permissions and battery (with "Tes HP-ku"), theme, about and version.
- Version 2.4.0.

## Non-goals
- Backend work. `POST /api/devices/health` is built in parallel by the backend agent; this change builds against the coordinator's contract and tolerates its absence.
- The "Aku udah hafal N kebiasaanmu" learning line and the shrinking status card: the phone has no API for the habit count, and the card shrink needs a real-phone trial first.
- The full transaction form for notification rows on the phone, by owner decision 1.
- A release, tag or install on any phone. The owner releases through the `android-v2.4.0` tag after merge.

## Capabilities

### New Capabilities
- `companion-health-reporting`: the phone's health report to the tracker (payload, triggers, rate, tolerance of older servers) and the setup self-test it carries.

### Modified Capabilities
- `companion-interface`: the status hub replaces the tabbed home; problem states; money tiles removed; sheets; Riwayat without day totals; Rincian with "Perbaiki label"; Pertanyaan sheet; Pengaturan grouped lists; "Laporan siap".
- `companion-pairing`: guided setup becomes the story-card flow with the self-test and "HP-mu siap".
- `mobile-purpose-questions`: purpose questions are batched into the evening reminder instead of an immediate prompt.
- `mobile-text-capture`: Catat opens from the hub; the time question and time label; no summary refresh.
- `notification-capture`: owner edits of recorded notifications are label corrections; the self-test notification is never captured.

## Impact
- Code (Android, `mobile/app`): new `ui/hub/*` (hub, status card, sheets, questions, web links, report card), `ui/setup/*` story cards replacing the pairing steps, `data/health/*` (snapshot, reporter, worker, self-test), `data/reminder/*` (evening reminder), `data/report/*` (readiness), label correction in `CompanionViewModel`; listener, notifiers, `NotificationResults`, capture models and `TextCaptures`; removal of `ui/home/*`, `ui/shell/BottomBar.kt`, the ledger summary model, client call and cache.
- API use: adds `POST /api/devices/health` (Bearer), `GET /api/reports/monthly/latest` (Bearer), `question_types` on `POST /api/capture/text`, `kakeibo_type` and `merchant` on `PATCH /api/transactions/{id}`; stops calling `GET /api/dashboard/cash-flow-summary`.
- Data: no Room migration. New preferences for the reminder, the self-test result, the health backoff and the report notice; the stored ledger summary is deleted on upgrade.
- Tests: unit and Robolectric suites for the hub, problems, self-test, health reporting, batching, label correction, Catat time and removed tiles; device tests rewritten for the hub.
- Release: backend first for health and readiness (both tolerated if absent), then the `android-v2.4.0` tag by the owner.
