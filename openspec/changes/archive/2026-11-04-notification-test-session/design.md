## Context

`accept_event` (`notification_processing.py`) inserts each notification into `notification_events`. It then either queues it for the AI worker or processes it deterministically in the same request. Everything else reads events by state or by `transaction_id`:

| Path | What it picks up |
| --- | --- |
| Worker (`claim_event`) | `queued`, expired `processing`, retryable `failed` |
| Retry endpoint | `needs_review`, `failed`, `needs_confirmation` only |
| Resolve endpoint | unresolved states only |
| `confirm_mapping` / `confirm_source_pocket` / `confirm_sender` | `needs_confirmation` only |
| `reclassify_review_noise` | `needs_review` with no transaction |
| Deferred pairing (`movement_pairing`) and the linked-report rule (`same_arrival`, `account_links.py` on `feat/linked-account-reports`) | transactions, and events that are `recorded` with a transaction |
| Questions, habits, merchant memory, the question rate, the monthly report | events joined through `transaction_id`, or `recorded` events |

The companion (`financial-tracker-mobile-listener`, read-only) does the following with an ingest result:
- **Polling:** `DirectSync.pollResults` polls `/result` only for rows in `queued` or `processing`.
- **Status labels:** `RowPresentation.status` shows `ignored` as "Diabaikan" (muted) and any unknown status as "Terkirim".
- **Waiting counts:** `HomeFigures.SETTLED` is `recorded`, `ignored` and `dismissed`. Every other status counts as waiting today.
- **"Perlu cek":** `ActivityFilter.UNSETTLED` covers `queued`, `processing`, `needs_review`, `needs_confirmation` and `failed`.
- **Retries:** happen only on network errors or `acceptance_failed`.

## Decisions

### D1. A session is a time window per owner

`notification_test_sessions`:
- `id`, `user_id`;
- `started_at`, `planned_end_at`, `ended_at` (null while open), `created_at`.

At most one open session per owner, enforced by a unique partial index on `user_id` where `ended_at` is null.
- **Expiry:** a session whose `planned_end_at` has passed counts as ended everywhere. Starting a new session first closes the expired one (`ended_at = planned_end_at`) in the same transaction.
- **Effective window:** `[started_at, coalesce(ended_at, planned_end_at))`.

*Rejected:* a background job that ends sessions. Expiry by time is exact without one.

### D2. Membership is decided by `post_time`

A notification belongs to a session when its `post_time` falls inside an effective window of that owner. This holds whether the session is open or already ended.
- A notification posted during the session but uploaded after it (phone frozen or offline) is still capture-only, so a late upload never books test movements.
- A notification posted before the session but uploaded during it is processed normally.
- The owner's typed captures (Catat) are a separate endpoint and are not affected.

*Rejected:* arrival time. It would book late uploads of test movements and capture unrelated earlier ones.

### D3. Capture-only is the `ignored` state, guarded by the database

`accept_event` checks membership right after the duplicate lookup. That's before `collect_facts`' ignore branch, the source-pocket gate, any advisory lock and any interpretation. A member is inserted with:
- `processing_state = 'ignored'` and `error_code = 'test_session'`;
- `test_session_id` set;
- `next_attempt_at`, `interpretation` and `active_question` null;
- the same raw fields as today.

**Database guard (V49):** `CHECK (test_session_id IS NULL OR (processing_state = 'ignored' AND transaction_id IS NULL AND movement_id IS NULL))`.

**Why nothing ever processes it:**
- the worker, retry, resolve and confirm paths all skip `ignored` (see the context table);
- the pairing and linked-report rules see no transaction;
- the check makes any later state change or booking fail loudly instead of slipping through.

*Rejected:* a new state `captured`. It would need the existing CHECK widened, and the companion would count it as waiting forever (`HomeFigures`), with no app release planned.

### D4. What the phone sees

The result is `{"status": "ignored", "error_code": "test_session", "event_id": …}`. The phone:
- shows "Diabaikan";
- settles the row;
- sends no alert, question or poll.

The ingest response's `created_transactions` stays 0. Nothing on the phone waits.

### D5. Session endpoints

All under session auth, so the companion's Bearer key gets 403, as for skip patterns:
- `GET /api/ingest/test-sessions` (list, newest first);
- `POST /api/ingest/test-sessions` (`{"duration_minutes": 120}`, 15 to 360);
- `POST …/{id}/end`;
- `GET …/{id}/events`;
- `PATCH …/{id}/events/{event_id}` (`test_case` up to 8 characters, `label_notes` up to 400);
- `GET …/{id}/export.csv`;
- `DELETE …/{id}`.

**Errors:**
- 409 `test_session_open` when one is open;
- 404 for another owner's session or event;
- 422 for an invalid duration or label.

Starting a session, ending it and deleting its data log reason codes only, never notification text.

### D6. The report

The report computes `collect_facts(source_event(event))` on read: institution, direction, amount and parse status. It is the same deterministic parser as ingest, with no AI and no cost.

Columns:
- case number (editable);
- post time and arrival time;
- the gap;
- app;
- direction;
- amount;
- a short text preview, visible only to the owner;
- note (editable).

The CSV carries the same columns. "Hapus data sesi" deletes the session row and its events (`ON DELETE CASCADE` on `test_session_id`), after a confirmation.

### D7. Replay outside production

Three scripts in `scripts/verify/`, with no data in the repo:
- **`export_test_session.py --session <id>`:**
  - runs only through the read-only recipe (`PGOPTIONS=-c default_transaction_read_only=on`, `SET TRANSACTION READ ONLY`);
  - writes the session's events plus the owner's account, pocket, alias, sender-alias, category, account-link and mapping structure to a JSON file;
  - the target path must be outside the repository.
- **`replay_test_session.py --export <file> --revision-db <url>`:**
  - refuses any database URL whose host is not loopback;
  - loads the structure into a fresh local database for a synthetic owner;
  - posts the events through the real `accept_event` in `post_time` order, with their session tags removed;
  - runs the worker deterministically, or with `--ai` when a key is present;
  - `--drop-app <package>` removes one app's reports, simulating one-sided income or expense;
  - prints the resulting transactions, movements, holds and questions.
- **`score_test_session.py --expected <file>`:** compares the outcome per case number with an expected-results file (case → expected booking), and prints pass/fail.

Both revisions, `main` and the linked-accounts branch, run the same export. The local copy and the export file are deleted after scoring.

### D8. Labels

Neutral standard Indonesian:
- **Section:** "Sesi uji".
- **Buttons:** "Mulai sesi" and "Akhiri sesi".
- **Banner:** "Sesi uji berjalan sampai 18.30 · notifikasi disimpan tanpa dicatat".
- **Report:** "Nomor kasus", "Catatan", "Unduh CSV" and "Hapus data sesi".

## Data and migration

V49, additive only:
- `CREATE TABLE notification_test_sessions`;
- `ALTER TABLE notification_events ADD COLUMN test_session_id UUID REFERENCES notification_test_sessions(id) ON DELETE CASCADE`, `ADD COLUMN test_case TEXT`;
- the check from D3;
- a partial index on `(user_id, test_session_id)` where `test_session_id IS NOT NULL`.

**Rollback:** drop the constraint, the columns and the table, after deleting tagged events if wanted. No existing row changes.

## Risks

- **Phone clock skew:** a skewed `post_time` could put a real notification inside a window. Mitigation: sessions are short and started on purpose, and the report shows the post and arrival times, so a stray real transaction is visible and the owner can record it with Catat.
- **A forgotten session:** it ends at its planned end, at most 6 hours.
- **Personal data:** raw text stays in production as today, owner-only, and is deletable per session. The replay copy lives only in the agent's scratchpad and is deleted after use.

## Open decisions (defaults applied unless the owner says otherwise)

| | Decision | Default |
| --- | --- | --- |
| OD150 | Duration | Default 2 hours, choosable 15 minutes to 6 hours, extendable by ending and starting again |
| OD151 | What the phone shows | "Diabaikan" (status `ignored`, `error_code = 'test_session'`). A clearer "Sesi uji" label would need a companion release, which is deferred. |
| OD152 | Reopening an ended session | Not possible. A new session gets its own id, and the report lists sessions separately. |
| OD153 | Retention | Kept until the owner deletes them with "Hapus data sesi". They are excluded from every normal view. |
| OD154 | AI in the report | No. The report uses the deterministic parse only. The AI runs only in the agent's local replay with `--ai`, after the owner's go-ahead for its cost. |
| OD155 | Session membership | By `post_time` (D2) |

## Deviations during implementation

- **Base (task 0.1):** the branch merges `feat/linked-account-reports`, `feat/manual-balance-check` and then `main` (1320060) with ordinary merges, instead of rebasing. The branch was already shared with the plan commits, and the merges keep its history intact. V49 follows V48 on `main`.
- **"Hapus data sesi" keeps the window (D6):** the session's notifications are deleted and the session row stays, marked `data_deleted_at` (a V49 column). A notification posted during that window and uploaded later is answered `ignored`/`test_session` and is not stored, so a late upload neither books nor leaves hidden data behind. Deleting the session row outright, as D6 first said, would have made such a late upload book.
- **Label limits:** `test_case` is up to 8 characters and `label_notes` up to 400, as in D5. An empty value clears the field.
- **Replay ids:** the replay gives every copied row a new id and rewrites the references, so it can share a database with the exported owner (the tests do) and copies fail loudly instead of being skipped.
- **Inspection endpoint:** `GET /api/ingest/notifications` (raw event inspection, used by no web page) lists session events like any other, as `ignored` with `error_code = 'test_session'`. Ledger, Tinjau, reports, questions, habits, memory and the balance check never see them.
