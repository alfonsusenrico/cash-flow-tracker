# Verification: notification-test-session ("Sesi uji")

## Revisions

- **Base:** `main` 1320060 (linked accounts V47, balance check V48), merged into the branch as `ee8033f`.
- **Regression tests on the code before the feature:** `86b8562`, with V49 and the tests only.
- **Feature:** `93b0b46` (backend), `5118225` (web) and `cd23ab3` (replay tooling).
- **Checks:** all ran on the tree of `cd23ab3`, plus the user-guide and record commit that holds this file.

## Environment

- **Local only:** Postgres 16 in `cft-sesi-pg` on 55642, migrated by Flyway 10 to V49. The databases are `ledger_test` (pytest), `replay_main` (V48) and `replay_branch` (V49) for the dry run, and `ledger_browser` (the synthetic seed `raka_contoh`) for the browser.
- **Browser stack:** the API on 8342, `next start` on 3343 behind the loopback proxy on 3342, and headless Brave on 9642.
- **No production access.** All data is synthetic.

## Task evidence

| Task | Evidence | Result |
| --- | --- | --- |
| 0.1 | Merges `ae8748c`, `d0d83d1` and `ee8033f`. The settings delta was checked against the merged main spec (eight sections; Sesi uji is the ninth). | `openspec validate --all --strict` 80 passed |
| 1.1 | `test_v49_runs_twice_on_a_database_with_events`; `test_the_database_refuses_to_book_a_session_event` (CheckViolation on a state change); Flyway applies V49 on a fresh database and on the restored seed (V44 → V49) | pass |
| 2.1 | `backend/tests/test_notification_test_sessions.py` at `86b8562` | 14 failed, 3 passed. The 3 that passed cover behaviour that must not change: a pre-session notification books, another owner's session doesn't capture, V49 runs twice. |
| 2.2–2.4 | Same file at `93b0b46` and later | 18 passed |
| 2.3 | `test_with_the_ai_on_a_session_event_never_reaches_the_ai_queue`: with AI enabled, the event is `ignored` and `claim_event` finds nothing, so no provider call can happen | pass |
| 2.5 | `test_the_phone_gets_a_settled_ignored_result`: the result is exactly `payload_hash`, `event_id`, `status: ignored` and `error_code: test_session`, and a re-upload gets the same answer and stores nothing | pass |
| 3.1–3.2 | `TestSessionSection.test.tsx` (7), the AppLayout banner tests (2) and the settings navigation test (nine sections) | pass |
| 3.3 | Browser check, below | pass |
| 4.1 | `test_the_exporter_refuses_a_path_inside_the_repository`; `test_an_export_holds_the_session_and_the_structure` (an export from a local database) | pass |
| 4.2 | `test_the_replay_refuses_a_non_loopback_database`, the replay tests, and the CLI dry run below | pass |
| 4.3 | `test_the_scorer_reports_differences_and_missing_cases`, and the CLI scoring below | pass |

**Companion contract (2.5):** read-only in `financial-tracker-mobile-listener` at `35dfbeb`.
- `NotificationResults` accepts `ignored` without an event id.
- `HomeFigures.SETTLED` contains `ignored`, and `DirectSync` polls only `queued` and `processing`.
- `RowPresentation` shows "Diabaikan", and the detail shows "Diabaikan server" with "Kirim Ulang". A re-send of the same payload gets the same settled answer.
- The companion needs no release.

## Checks

| Check | Result |
| --- | --- |
| Backend pytest, full (V49) | 1839 passed, 0 failed (1810 on the merged base, plus 29 new) |
| Frontend `npm ci`, type-check, lint, build | all exit 0, lint 0 warnings |
| Vitest | 76 files, 684 passed, 0 unhandled errors |
| `openspec validate --all --strict` | 80 passed, 0 warnings |
| `git diff --check` | clean |

## Replay dry run (synthetic export of the 1 Nov sequence)

The synthetic export holds a GoPay "menerima Tabungan by Jago", a Jago "received from GoPay" and a myBCA debit, all case 1.

| Revision | Command variant | Case 1 | Scored against |
| --- | --- | --- | --- |
| `main` 1320060 (`--backend` a `main` worktree, V48 database) | all apps | 1 movement, 1 held, 0 income or expense | `{"movements": 1, "held": 1, "income": 0, "expense": 0}`: pass |
| `main` | `--drop-app com.jago.digitalBanking` | 2 events, 1 movement, 0 held | `{"movements": 1, "held": 0, "events": 2}`: pass |
| this branch (V49) | all apps | same as `main` | pass |
| this branch | drop Jago | same as `main` | pass |

**Other results:**
- The `main` and branch replay outputs are identical in both variants (`diff` of the JSON). Notifications outside a session go through unchanged code paths.
- **Guards:** `--database-url postgresql://…@db.example.com/…` is refused before anything is imported, and `--out` inside the repository is refused before the replay runs.

## Unchanged writes outside a session

- **Membership:** `accept_event` runs the session lookup only after the duplicate lookup. With no window for the owner it falls through to the existing code, unchanged.
- **Equal results:** the identical `main`/branch replays above and the full backend suite (every existing ingest, pairing, linked-account, balance-check and review test) show the same results as on `main`.
- **Tests:** `test_a_notification_posted_before_the_session_books_normally` and `test_capture_resumes_after_the_session` pin the boundaries.

## Browser check

Cells: 1440 light, 1440 dark, 390 dark. In each cell the script, `scratchpad/sesi-web/check.mjs`:
- opens Pengaturan → Sesi uji (idle) and starts a session;
- posts three synthetic notifications through the ingest API;
- opens the report and saves case number 1;
- opens Transaksi to see the banner and ends the session from the banner;
- opens the ended session's report and fetches its CSV.

The last cell also deletes a session's data through the confirmation.

| View | Overflow | Targets under 24 px | axe serious/critical | Console errors |
| --- | --- | --- | --- | --- |
| Idle (3 cells) | 0 | 0 | 0 | 0 |
| Running with report (3) | 0 | 0 | 0 | 0 |
| Banner on Transaksi (3) | 0 | 0 | 0 | 0 |
| Ended report (3) | 0 | 0 | 0 | 0 |

**Results:**
- **Ingest during the session:** `ignored/test_session` for all three, with `created_transactions` 0.
- **Ending the session:** "Akhiri sesi" in the banner removes the banner.
- **CSV:** 200 `text/csv; charset=utf-8`, with a header and the labelled case 1 row.
- **Delete:** 3 sessions before, 2 after.
- **Keyboard:** Tab reaches Durasi, Mulai sesi, Lihat laporan and Unduh CSV.
- **Screenshots:** `scratchpad/sesi-web/shots/` (`sesi-idle-*`, `sesi-running-*`, `banner-*`, `sesi-report-*`, `sesi-delete-confirm-390-dark`, `report.json`), read by the agent. Full-page captures draw the phone's sticky header mid-page, which is a capture artefact, not the page.

**Seen during the check:** the first run posted its notifications a minute before each session started. The first two booked normally, because they were posted before any window existed. Later ones landed in the previous session's window. Both are the designed post-time membership (D2). On the session day, anything done before "Mulai sesi" books as usual.

## Not verified

- A real phone and a screen reader.
- `--ai` in the replay. It needs the owner's key and go-ahead for its cost.
- The owner's real session (task 6.3), which runs after acceptance and deploy.
