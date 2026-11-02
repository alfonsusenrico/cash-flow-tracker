# Design

## Context
Companion 2.3.4 opens on a tabbed home (Beranda, Aktivitas, Catat, Tanya, Atur) whose first tiles are the tracker's money ("Arus kas tracker", "Kategori teratas"). On 2026-11-02 the owner chose direction B for the phone (a tracker companion with no financial dashboards), compared three clickable prototypes of the companion, and approved C, "Satu layar status", with four defaults (proposal, `## Approval`). The design source of truth is the local prototype `cash-flow-tracker-prototypes/2026-11-02-companion/` (README, `option-c.html`, `src/`, `screenshots/c-*`); setup reuses the story cards of the chosen web onboarding C. The device health contract was fixed by the coordinator the same day and later tightened (strict payload, 401 `device_key_invalid`, 429 `Retry-After: 60`).

## Goals / Non-goals
- Goals: answer "is it working?" first; turn every known failure into one fix; take money off the phone; prove capture at setup; report health; ask light questions once in the evening; let the owner correct labels so the AI learns; understand time in Catat; keep every existing capability.
- Non-goals: backend changes; the habit-count learning line; shrinking the status card; the full transaction form for notification rows; any release or install.

## Rationale (copied from the prototype README)

#### C. Satu layar status (single status hub), recommended

> One friendly screen that says the helper is working: "Lagi jagain notifikasimu". The pending
> questions are stacked below it and Catat is one big button. Everything else opens as a sheet over
> the hub.

- **Beranda** has three parts:
  - **Status card.** An illustration sits on top of the large status line, today's count, last
    capture and the sync line. When something breaks, the card shows the problem instead: a new
    illustration, the problem title, one full-width fix button, and the soft issues below.
  - **Catat transaksi.** The button drops to secondary while a problem's fix button is showing, so
    two coral buttons never compete.
  - **"Ada 5 pertanyaan".** The top question is answerable in place, with the next cards peeking
    behind it. "Lihat semua" opens the question sheet. Three tiles follow: Riwayat, Pengaturan and
    Buka web.
- **Sheets** hold Pertanyaan, Catat, Riwayat (with Rincian nested and a back arrow), Pengaturan,
  Rincian status and Buka di web.
- **Setup** uses the onboarding C story cards: an illustration per step, five progress segments, and a
  sticky action bar. It visually continues the web onboarding that sent the person here.

#### Why C is the third option as briefed

I considered a fourth shape: no home screen at all, where the app opens straight into whatever needs
attention. I dropped it, because when nothing needs attention it shows nothing, and "is it working?"
is the question this app exists to answer. C already answers that question first.

#### Trade-offs

| | A · Utilitas tenang | B · Kotak pertanyaan | C · Satu layar status |
| --- | --- | --- | --- |
| Feels like a helper, not an app | strongest: looks like part of Android | medium: an inbox reads like a messaging app | strong: one screen about the helper's own state |
| "Is it working?" at a glance | yes, in the status card, but dense and text-only | weakest: one pill, and details need a tap | strongest: large status line plus illustration, problems take over the card |
| Problem with a one-tap fix | in the card, as text and a button | card above the inbox, plus a coral pill | the whole card changes, with a full-width fix |
| Questions | one tap away (row with count) | home | the top question is on home, the rest one tap away |
| As the AI asks less | unchanged | home empties into "Beres semua", so the app's main screen loses its purpose | still meaningful, because status stays and the stack becomes "Nggak ada pertanyaan" with the learning line |
| Catat | a row | small floating pill | one large button |
| Depth | pages (back arrow) | pages plus two sheets | sheets only (one nested level in Riwayat) |
| Warmth and brand | least, almost no brand | medium | most, matches the chosen web onboarding |
| Risk | can feel bare, and the casual copy sits oddly in a settings layout | inbox pressure, so people may feel they owe answers | the status card uses height; at 390 × 844 the first question starts just above the fold |

#### Recommendation

**Choose C, and take two ideas from the others.**

1. C keeps a purpose when the AI improves. The owner's goal is that the AI asks less over time. B's
   home trends to empty, and A's home was never about the person. C's home is the status, which
   stays relevant every day and turns into a clear call to action when something breaks. Those
   failures (access lost, HiOS freezing, no captures) are exactly the ones this app has hit in
   practice.
2. It continues the onboarding C the owner already chose. The same story-card setup, illustrations
   and voice carry from the web straight to the phone.
3. Borrow from A: keep settings as plain grouped lists, which C already does in its sheet. Consider
   A's wizard-style sticky action bar for long setup steps.
4. Borrow from B: the "Perlu sekarang / Nanti malam" grouping and the evening reminder. C already
   uses both in its question sheet.

Follow-up for C: once the status has been calm for several days and questions are waiting, the
status card could shrink to one line so the first question rises above the fold. Try this on a real
phone before deciding.

#### Removed on purpose, or moved to the web

- **Money tiles are removed.** The old Beranda's "Arus kas tracker" (today's inflow, outflow and net)
  and "Kategori teratas" go, per the owner's "no financial dashboards on the phone". "Laporan
  bulanan" on the web and the monthly "Laporan siap" notification replace them.
- **Full edit of notification rows moves to the web.** The old Rincian had a full edit form for a
  recorded notification. The phone now keeps "Perbaiki label", which is the fix that teaches the AI,
  plus "Lihat di Buku Kas (web)" for the full edit. Typed captures keep "Ubah" on the phone.
  **Owner decision:** keep this split, or bring the full form back.
- **Swipe-to-delete becomes a "Hapus dari HP" button** in Rincian, with an in-place "Urungkan" row in
  Riwayat. It is easier to discover, and it works the same by keyboard and with TalkBack.

#### Nada & bahasa

The copy follows the onboarding README's "Nada & bahasa":
- "aku" and "kamu";
- short lines;
- acknowledge and move on;
- at most one reason;
- light humour;
- exact money and rules.

Examples from these prototypes:

| Where | Line |
| --- | --- |
| Status, normal | "Lagi jagain notifikasimu." |
| Status, frozen | "HP-mu nidurin aku." / "Penghemat baterai HiOS ngebekuin aplikasi ini dari jam 09.40. Notifikasi setelah itu bisa kelewat." |
| Zero day | "Hari ini masih sepi." / fix: "Tes sekarang" / result: "Tes oke, aku jalan normal. Mungkin emang belum ada transaksi hari ini." |
| Battery step | "Biar HP nggak nidurin aku." |
| Habit question | "Sepertinya ngopi, ya?" / "8 dari 9 kali di Kopi Kenangan kamu pilih Ngopi." / after: "Lain kali Kopi Kenangan nggak aku tanyain lagi." |
| Unsaved reply | "Tadi kamu bales “patungan futsal” dari notifikasi, tapi belum kesimpan karena HP masih kekunci." |
| Catat | "Tulis aja kayak ngobrol" / "Yang kurang nanti aku tanyain." / "Dibatalin. Nggak ada yang kecatat." |
| Setup done | "HP-mu siap." / "Kamu nggak perlu buka aplikasi ini tiap hari." |
| Learning | "Aku udah hafal 12 kebiasaanmu. Minggu ini aku nanya 5 kali, minggu lalu 11." |

Engineering words stay out of the copy, with one exception: "kunci API", which is already the web's
label. The pairing code, the 5-minute validity and single use, and the 10-second undo are stated
exactly.


## Decisions

### D1. The hub and its sheets
`CompanionApp` composes the hub (`ui/hub/HubScreen.kt`) and, over it, the top entry of a saved `SheetStack` (Pertanyaan, Riwayat, Rincian, Rincian of a capture, Pengaturan, Rincian status, Buka di web). Only Rincian nests (over Riwayat); opening any other sheet replaces what was open. Android's back pops one sheet. The NavHost and bottom bar are removed. The sheet uses a shaped background, not a clip, as the existing Catat sheet does: with a clip layer, taps inside the sheet fell through to the scrim in Robolectric's hit test. Only the strip of hub left above a sheet closes it on a tap. Rejected: Material `ModalBottomSheet` per screen (nested navigation and back handling get awkward, and the existing pages already scroll on their own), and keeping NavHost routes styled as sheets (no hub behind them).

### D2. Status and problems
`HubStatus.of(HubInputs)` is pure and holds every rule; the shell feeds it from Android (access, listener, battery, notifications, network) and from this phone's rows (`HubCounts`). Priority: not paired, key refused, access lost, listener asleep (HiOS/TECNO, or any phone without battery exemption: "HP-mu nidurin aku" with the phone's steps; otherwise "Aku lagi putus dari notifikasi" with a rebind), server unreachable, waiting to send (offline, or more than 15 minutes online with auto-sync on), and a quiet afternoon (no capture after 12.00 and no self-test passed today). Server state comes from a new `server_contact` preference written by notification submission, the health reporter and pairing verification. Missing battery exemption, notifications off and auto-sync off are soft notes. Counts never become 0 before Room answers, and no amount is computed.

### D3. Health reporting
`HealthSnapshot` builds the contract body with explicit nulls and `HealthReporter` sends it on the device key. Triggers: the setup self-test and its retries, `MainActivity.onResume`, listener connect and disconnect, and a 15-minute periodic WorkManager job (scheduled at app start when paired and after pairing). A trigger replaces a waiting one-shot job (exponential backoff from 30 s, at most 5 attempts). Answers: 2xx records the report; 404 is an older tracker, quiet, retried by the one-shot job and paused 6 hours for periodic runs; 401/403 marks this pairing's key revoked, so nothing more is sent until a new pairing (new pairing context) or a successful "Tes Koneksi", and records `refused` so the hub shows "Sambungan ke akunmu putus"; 429 waits for `Retry-After` (60 s default, at most an hour) without marking the server unreachable; network and 5xx mark it unreachable; 422 is logged by reason only and not repeated. A stored self-test result that breaks the status/step/time rules is reported as `not_run`; `app_version` drops characters outside `0-9A-Za-z.+_-` and is cut to 32.

### D4. Self-test
The test notification is posted by this app in its own low-importance channel "Tes pendengar", with tag `cft_self_test` and a one-time token in its extras. `NotificationGate.route` runs first in the listener: this app's own package with the tag and a token answers the waiting `SelfTestCatches` entry and returns before any capture code, so the notification is never classified, stored, synced or sent. Another package carrying the tag goes through the normal gate. The runner waits 10 s, removes the notification, records the result in preferences and reports health; an older tracker's 404 still passes the server step (the phone itself is ready). A failed step is recorded and a background report is requested, so the web learns of it later. "Coba lagi" repeats only the report.

### D5. Batched light questions
`QuestionUrgency` makes purpose questions light. In `NotificationResults.alert`, while the evening reminder is on, a record with an unanswered purpose question posts the recorded-transaction alert instead of the prompt; a refused reply from the notification still re-posts the prompt at once (it answers the owner's own action). The reminder is a one-time WorkManager job with an initial delay to the next 20.00 (or the chosen time), rescheduled at app start and on each setting change, and by the worker itself for the next evening (`APPEND_OR_REPLACE`, so it does not cancel itself). The worker posts "Ada n pertanyaan ringan hari ini" only when light questions wait; its tap opens the question sheet. Turning the reminder off cancels it and returns purpose questions to immediate prompts, the conservative choice so no question goes unannounced.

### D6. Perbaiki label and the owner-edit path
The phone sends only the fields the owner touched: `category_id`, `kakeibo_type` (an explicit JSON `null` for "Belum diklasifikasi", built with `org.json` because Gson drops nulls), and a non-blank `merchant`. Nothing is preselected because the stored result names the category but not its id, pillar or merchant. On the server, `PATCH /api/transactions/{id}` reads `model_fields_set`, so untouched fields keep their values; when the classification changes it sets `review_status = reviewed`, applies the owner-edit assignments and calls `record_owner_corrections`, which appends `classification_feedback` and updates exact merchant memory: the correction teaches the tracker. The phone then re-reads the row's result. Clearing a merchant is left to the web.

### D7. Catat time
`CaptureTextRequest` always sends `question_types` with `time`; `isAnswerable` and the answer check accept `time`; options carry `date`; `time_label` is shown among the known facts and the recorded tags. Answers reuse the existing reply-id outbox.

### D8. Money removal
`LedgerCashFlowSummary`, `TopCategory`, the `cash-flow-summary` client call, the summary cache, its refresh after captures, the tiles, their colour tokens and type styles, and their tests are deleted. On start, 2.4.0 removes a summary an older version cached. Riwayat's day headers lose their money out.

### D9. Laporan siap
`ReportReadiness` reads `GET /api/reports/monthly/latest` on app open and from the periodic health run at most every 6 hours, keeps the label, local start date and `/laporan` path per pairing, and notifies once per start date. The card stays until opened from the hub. Only paths that start with `/laporan` on the build's server are opened.

### D10. Setup
`PairingActivity` runs the story: welcome, pairing (QR, the new typed code folded as the tracker folds it, the manual key), access, battery (with `OemHints` for HiOS, MIUI/HyperOS, ColorOS and One UI), the notification permission (asked once, then Android's app page), the self-test, and "HP-mu siap". A re-pair from Pengaturan, or "Pakai kunci API", uses the pairing cards only. The automatic permission request on every start of `MainActivity` is removed; the setup card and the hub's soft note ask instead.

### D11. Notifications
The ongoing listener notification reads "Lagi jagain notifikasimu" and opens the app; its channel is renamed "Pendengar aktif" without changing the owner's importance. New channels: "Pengingat malam" and "Laporan bulanan". Urgent question notifications and the recorded-transaction alerts are unchanged.

## Risks / Trade-offs
- The quiet-afternoon threshold (12.00) and the 15-minute stuck threshold are judgement calls; both are single constants in `HubStatus`.
- The battery-saver diagnosis cannot see a freeze directly; a disconnected listener without battery exemption, or on HiOS, is treated as frozen.
- Health reports are skipped for the old server for 6 hours at a time, so a backend deployed in between is seen at the next app open or trigger.
- Robolectric verifies layout, semantics and contrast tokens; real rendering, TalkBack speech and HiOS behaviour need the phone.

## Migration
No Room migration. New preferences default safely (reminder on at 20.00, self-test `not_run`). The cached ledger summary is deleted on first start. Release order: the backend with `POST /api/devices/health` (V42) and the readiness endpoint first; the phone tolerates both missing.

## Open decisions for owner
1. Evening reminder off: purpose questions are then prompted at once (chosen), or never notified.
2. The learning line ("Aku udah hafal 12 kebiasaanmu…") needs a habit-count API; not built.
3. The calm status card could shrink after calm days so the first question rises above the fold; try on a real phone first (prototype follow-up).
4. Quiet-afternoon threshold: 12.00 local time.
5. The recorded-transaction alert copy is still English ("Expense recorded • Rp …"); translating it is a small follow-up.
6. Today's count counts rows still on the phone; rows deleted from the phone no longer count.
