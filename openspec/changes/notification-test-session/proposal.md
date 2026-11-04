## Why

Making redundant-notification handling general needs real data from every account combination the owner uses: BCA via myBCA, GoPay as a wallet and as Jago's GoPay Tabungan pocket, Jago pockets and ShopeePay. The owner offered to run all the cases with small amounts in one session. Normal processing would book every test movement and bloat the ledger, though. Switching the backend off makes the companion queue and resend everything later, and switching the companion off captures nothing.

The agreed answer is a capture-only test session. Notifications are stored as raw evidence, labelled with their case, and then replayed outside production through the real pipeline.

## Expected Outcome

- **The owner:**
  - starts "Sesi uji" in Pengaturan and runs the case script (research note, "Sesi uji case script") on the phone;
  - sees every captured notification in a session report, adds the case number and a short note, and downloads the report;
  - finds nothing new in the ledger, Tinjau, questions, memory or habits.
- **The agent:**
  - exports the session read-only;
  - replays it locally through `main` and through `reconcile-linked-account-reports` (option A), optionally dropping one app's reports to simulate one-sided income or expense;
  - scores each case against its expected result;
  - deletes the local copy afterwards.

## Approval

**Planning request, 2026-11-04.** The owner said "ok plan Sesi uji, case list is fine", after the agent's proposal: capture-only instead of switching anything off, the four-step session flow, and the 15-case list. Implementation waits for the owner's approval of this plan.

**Merge order:** this change ships after `reconcile-linked-account-reports` (V47) and `manual-balance-check` (V48), and uses V49. Before archiving, it is rebased on `main` with both merged, and its `currency-conversion-and-settings` delta is refreshed against the main spec.

## What Changes

- **Sessions:**
  - A per-owner test session, with a start, a planned end (default 2 hours, maximum 6) and an early "Akhiri sesi".
  - At most one open session per owner. It ends on its own at its planned end.
  - A banner on every page while it runs.
- **Capture-only ingest:**
  - A notification whose `post_time` falls inside any of the owner's session windows is stored with its raw fields, tagged with the session, and set to `ignored` with `error_code = 'test_session'`.
  - It is not interpreted, booked, paired, asked about or learned from.
  - The response keeps the companion contract, so the phone shows the row as settled ("Diabaikan") and does not poll or retry.
  - A database check keeps tagged events from ever holding a transaction.
- **Session report on the web:**
  - The captured notifications with app, post and arrival time, and the deterministic parse (direction, amount, institution).
  - An editable case number and note per notification.
  - A CSV download for the owner.
  - "Hapus data sesi" deletes the session's events.
- **Replay tooling in `scripts/verify/`, with no data in the repo:**
  - a read-only exporter;
  - a loopback-only replay runner on a chosen revision, with `--drop-app` and optional AI;
  - a scorer against an expected-results file.
- **Pengaturan** gains the section "Sesi uji" at `/settings/sesi-uji`.

## Non-goals

- No change to normal processing outside a session window.
- No companion release: the phone works with today's build.
- No simulation of late notifications or investment trades. They stay covered by synthetic tests and history.
- No sharing of session data across users.

## Capabilities

- **New:** `notification-test-session`.
- **Modified:** `currency-conversion-and-settings` ("Sectioned Settings Page" gains Sesi uji).
- **Project capabilities:** no new one. It touches personal-data (raw notification text, owner-only, deletable), database (V49) and api-consumers (the companion contract is unchanged).

## Impact

- **Migration V49 (additive):** a table `notification_test_sessions`; `notification_events.test_session_id` and `test_case`; a check constraint; an index.
- **Backend:**
  - `services/test_sessions.py` (new);
  - `notification_processing.accept_event`, which branches before any interpretation, source-pocket gate or lock;
  - `routers/ingest.py` (session endpoints, under session auth).
- **Frontend:**
  - Pengaturan section `sesi-uji` (`components/settings/sections.ts`, `TestSessionSettings.tsx`);
  - the banner in `components/layout/AppLayout.tsx`;
  - the report view and the CSV download.
- **Scripts:** `scripts/verify/export_test_session.py`, `replay_test_session.py`, `score_test_session.py`.
- **Companion:** read-only check of `DirectSync`, `RowPresentation`, `HomeFigures` and `ActivityFilter` (no code change).
