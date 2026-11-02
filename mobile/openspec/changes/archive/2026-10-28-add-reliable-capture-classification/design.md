## Context

`FinancialNotificationListenerService` reads title, text, big text, sub text and summary text from enabled registered packages. It skips rows that are entirely empty and passes the rest to `NotificationProcessor`. The processor:

- drops one-time codes;
- labels rows that its local regexes recognise;
- computes `payload_hash = SHA-256(package|title|text|postTime)`;
- launches `insertOrIgnore` on a bare `CoroutineScope(Dispatchers.IO)`. Exceptions are caught and only logged.

`NotificationSubmission` posts up to 50 unsynced rows in one request. A 400, 404 or 422 marks all of them `invalid_request`, and `getUnsyncedNotifications` then excludes them for the current pairing.

The backend (cash-flow-tracker `backend/app/services/notification_evidence.py`, `collect_facts`) ignores text by these rules:

- `HARD_NOISE_PATTERN`: always applied.
- `PROMOTIONAL_PATTERN` and `MARKET_INFO_PATTERN`: applied unless the text is recognised.
- `HEADLINE_AMOUNT_PATTERN`: when every Rupiah mention is a headline figure.
- No amount.
- Settlement unproven without a direction.

Text counts as **recognised** when either:

- the parser read a transaction with an amount; or
- exactly one Rupiah amount (outside balance phrases) appears together with settlement wording and one direction.

The backend deduplicates on `(user_id, payload_hash)`.

## Goals / Non-Goals

**Goals:**

- Decide on the device what reaches the network.
- Never lose a real transaction.
- Stop duplicate rows from re-posts and from sibling apps.
- Isolate bad rows.
- Expose every app toggle.

**Non-Goals:**

- The "Apa ini?" prompt (phase 4b).
- Backend changes.
- Device installation.
- Changing the payload hash formula or the request contract.

## Decisions

### D1. Three outcomes, decided by a pure classifier that mirrors the backend

`CaptureClassifier` is a pure Kotlin object with no Android types. For each notification it returns an outcome (`DROP`, `SEND`, `KEEP_LOCAL`) and a reason code. It works on the same joined text as the backend: title, text, big text, sub text and summary text, each counted once.

1. **Drop, before storage**, with these reasons:
   - `own_notification`: the package is the listener itself;
   - `ongoing`: `FLAG_ONGOING_EVENT` or `FLAG_FOREGROUND_SERVICE` is set;
   - `group_summary`: a group summary without a title, text or big text of its own;
   - `empty`: no text at all;
   - `one_time_code`: the existing D1 rule of `fix-one-time-code-filter`, unchanged.
2. **Recognised**, when either:
   - the local `extractDetectionSummary` returns a financial row with a positive amount; or
   - the backend's generic rule holds: one distinct parsed Rupiah amount outside balance phrases, plus `SETTLED_PATTERN`, plus exactly one of `OUT_PATTERN` and `IN_PATTERN`.

   Amounts are parsed with a port of `parse_idr_amount`. A recognised row is sent with reason `recognised_transaction`.
3. **Kept on the phone**, when the text is not recognised. The rules are checked in the backend's order, each with its reason:
   - `hard_noise`;
   - `promotional`;
   - `market_info`;
   - `headline_figures`;
   - `no_amount`, when there is no Rupiah mention and no foreign currency;
   - `settlement_unproven`, when there is exactly one amount and neither settlement nor direction wording.
4. **Sent** with reason `money_event` in every other case. These are rows the backend would flag for review rather than ignore: several amounts, foreign currency, unparseable amounts, or one amount with settlement or direction wording.

Deliberate differences from the backend:

- **Hard noise without one-time-code words.** The device's hard-noise pattern omits `OTP`, `one time password` and `kode verifikasi/akses`. One-time codes already have the stricter D1 rule. Safety footers such as "Jangan berikan PIN/OTP" must not hide transactions, which the existing `notification-capture` requirement already states.
- **Hard noise only applies to unrecognised text.** The backend applies it unconditionally, so a recognised transaction with failure or pending words is still sent. The backend ignores it, which costs one request. That is cheaper than holding back a settled payment.

*Rejected alternatives:*

- **Dropping by channel** (for example `promotional_notifications`). Gojek and GoPay post real transactions on that channel.
- **Calling the backend's dry-run endpoint.** It needs the network before capture, which defeats the point.
- **Dropping noise outright instead of keeping it.** A misclassified transaction would then be unrecoverable.

### D2. Which drops are stored

- **Not stored:** drops that the notification itself determines (D1.1). One-time codes must never be stored, and the others carry no ledger fact. They are only counted (`dropped_count` in preferences).
- **Stored with the drop outcome:** drops that depend on other rows, namely `duplicate_source` (D6) and a group summary that restates a captured child (D6). This keeps the decision traceable through `duplicate_of_id`.

Stored drop rows are never sent, and they are pruned with kept rows (D9).

### D3. Durable capture through one serial queue

`NotificationProcessor.capture` is a `suspend` function that runs on a process-wide queue: `Dispatchers.IO.limitedParallelism(1)` with a `SupervisorJob`.

- The posting-identity check, the duplicate pairing and the insert run in one Room transaction, so concurrent posts cannot race.
- A failed write (any exception except cancellation) is retried twice, after 200 ms and then 800 ms.
- After the final failure, the processor:
  1. increments `capture_failure_count`;
  2. stores the time in `last_capture_failure_at`;
  3. logs the reason code `store_failed`.
- `CancellationException` is rethrown.
- Notifications still in the drawer are captured again by the rescan in `onListenerConnected`, so a failure there is recoverable.

*Rejected alternatives:*

- **`runBlocking` on the listener's main thread.** It blocks the binder callback and risks an ANR while a sync holds the database.
- **A WorkManager job per capture.** HiOS defers jobs for this app (see `DirectSync`).

### D4. Inbox lines and messaging-style messages fold into big text

The listener reads `EXTRA_TEXT_LINES` (inbox style) and the `EXTRA_MESSAGES` bundles (messaging style, each message's `text`). It keeps every non-blank value that differs from the title and text. The stored big text is the distinct join, with newlines, of the original big text, the lines and the messages.

- Group summaries skip the lines, because they repeat the children.
- The payload hash uses package, title, text and `postTime`, so it is unchanged for every existing shape.
- The backend reads `big_text` already, so the request contract does not change.

*Rejected alternative:* a new request field. The backend would ignore it until a backend release.

### D5. Posting identity: key, content and `Notification.when`

A capture matches an existing row of the same package when all of these hold:

- the same `notification_key`;
- the same content fingerprint, which is SHA-256 over the exact title, text and big text;
- the same `notification_when`, the app-set `Notification.when`.

A match updates `last_posted_at` and `repost_count` and creates nothing. The row keeps its first `post_time` and payload hash, so the backend identity is stable.

- **Re-posts.** The system re-posts the same `Notification` object when it auto-groups children, unsnoozes, or the app calls `notify` again, and that object keeps its `when`.
- **New payments.** A new payment is built as a new notification with a new `when`. Two identical "You spent IDR 25,000" payments under one reused key therefore stay separate rows, with distinct payload hashes from their own `postTime`.
- **Legacy rows** have no `notification_when`, so they never match. Their rescans are still deduplicated by the unique payload hash, as before.

*Rejected alternatives:*

- **Removing `postTime` from the hash.** It merges genuinely repeated payments and breaks backward compatibility with stored hashes.
- **Key plus content only.** Apps that reuse one notification id would merge repeated payments.
- **A time window per key.** It has the same merge risk inside the window.

### D6. Cross-app duplicates: institution groups, 2 minutes, one-to-one pairing

**Groups.** Packages are grouped by institution, mirroring the backend's `SUPPORTED_PACKAGES`:

- `bca`: myBCA (both ids) and BCA mobile;
- `jago`;
- `gopay`: `com.gojek.gopay`, `com.gojek.app` and `com.gopay.wallet`;
- `shopeepay`: ShopeePay and Shopee;
- `stockbit`.

**Normalised text.** Title plus effective text (big text if present, else text), lowercased with whitespace collapsed. Its SHA-256 is stored as `normalized_fingerprint`.

**Pairing.** A new capture whose normalised fingerprint matches a row that meets all of these conditions is stored with the drop outcome, reason `duplicate_source`, and `duplicate_of_id` pointing at the earlier row:

- it comes from another package in the same group;
- its outcome is not drop;
- its `post_time` is within 2 minutes;
- no other `duplicate_source` row claims it yet.

A group summary whose normalised fingerprint matches a non-drop row of its own package in the window is stored as a drop with reason `group_summary`. It is linked but does not claim the row.

**Why 2 minutes.** Both apps are notified from the same server-side payment event, so their posts normally land seconds apart. Two minutes tolerates delayed push delivery. One-to-one pairing keeps two genuinely repeated identical payments apart, because each of them is posted by both apps and pairs separately.

*Rejected alternatives:*

- **Disabling one app by default.** The defaults must stay unchanged, and either app can miss a notification.
- **Deduplicating only on the backend.** Payload hashes differ, so both rows would be recorded.
- **A longer window.** It raises the chance of pairing unrelated identical payments.

### D7. Batch isolation

`submitIsolating(rows, send)` is a pure suspend helper.

1. It sends the batch.
2. If the reply is a 400, 404 or 422 and the batch has more than one row, it sends each row as a single-item batch.
3. The first per-row reply that is not an accepted 2xx and not a 400, 404 or 422 stops the loop. That reply is then attributed to every remaining row, so they stay pending (`server_unavailable`, `network_unavailable`) or stop (`auth_rejected`).
4. It returns the chunks with their replies. `NotificationSubmission` applies the existing handling to each chunk.

Only rows rejected on their own get `invalid_request`.

*Rejected alternative:* bisecting the batch. It saves requests only for large batches, and per-row retries are simpler to reason about at 50 rows or fewer.

### D8. Room schema 6

All changes are additive columns on `raw_notifications`:

| Column | Type | Default |
| --- | --- | --- |
| `capture_outcome` | TEXT NOT NULL | `'send'` |
| `capture_reason` | TEXT | |
| `notification_when` | INTEGER | |
| `content_fingerprint` | TEXT | |
| `normalized_fingerprint` | TEXT | |
| `last_posted_at` | INTEGER | |
| `repost_count` | INTEGER NOT NULL | `0` |
| `duplicate_of_id` | INTEGER | |

The migration also adds indexes on `notification_key`, `normalized_fingerprint` and `capture_outcome`.

Existing rows become send rows with a null reason, so their behaviour is unchanged. Queries filter by outcome:

- submission, the unsubmitted count and the pending count use `send` only;
- the main feed and its total count use `send` only;
- a new local flow uses `keep_local`.

**Migration test.** It runs on the JVM with Robolectric, so it needs no device:

1. It builds a populated version 5 database from the exported `5.json`. Robolectric cannot read the schemas as assets, which `MigrationTestHelper` requires.
2. It opens the database with Room and the real migrations. Room's open-time validation compares every migrated table with the version 6 entities, as `runMigrationsAndValidate` does.

The instrumented `CompletionMigrationTest` is extended to version 6 for device runs.

### D9. Inbox, Settings and retention

**Inbox.**

- New filter `LOCAL("Disimpan di ponsel")`.
- The detail sheet of a kept row shows its reason and a "Kirim ke Tracker" button. The button sets `capture_outcome='send'`, `capture_reason='owner_sent'` and clears `sync_error`, then triggers a sync.
- The status badge shows "Di ponsel".

**Retention.** Kept and stored-drop rows older than 7 days by `post_time` are deleted at the start of every submission run (WorkManager or direct sync). Unconfigured devices are included.

**Settings.**

- The source list is built from the registry: every distinct lowercase package, with its `appLabel` and a registry description, and its toggle. `PRIMARY_SELECTABLE_APPS`, `SelectableApp` and the unused `knownKeywords` field are removed, because this change rewrites every registry entry.
- Listener health adds the failed-capture count and the time of the last failure, plus the dropped count.

### D10. Version 1.7.0 (code 14)

The debug APK is built with `--no-build-cache`, and the `"1.7.0"` constant is checked with dexdump.

## Data, API and failure behaviour

- **Request:** no new fields. Kept and dropped rows are never in a request.
- **Capture failure:** retried twice, then counted and visible in Settings. The row is missing until the drawer rescan.
- **Classification:** pure, with no I/O, so it cannot fail at runtime.
- **Submission failure:** handled per row as in D7.

## Migration Plan

1. Room 5 → 6 runs at first launch of 1.7.0. Every existing row becomes a send row.
2. Release order follows the existing rule: the backend first (no dependency here), then the APK, on the owner's instruction.

**Rollback.** Room cannot downgrade 6 → 5, so reinstalling 1.6.1 requires an uninstall, which loses rows that exist only on the phone. Prefer a forward fix (1.7.x). For an emergency rollback:

1. Sync pending rows.
2. Uninstall.
3. Install 1.6.1.
4. Re-enter the server settings.

## Risks / Trade-offs

- **[An unusual real transaction is kept on the phone]** It is stored and visible under "Disimpan di ponsel" for 7 days and can be sent with one tap. The rules mirror the backend, which would ignore the same text anyway.
- **[An app rebuilds a re-posted notification with a new `when`]** A separate row is created, as before this change. The backend may record it twice. The cost is accepted to keep repeated payments distinct.
- **[A sibling app delivers more than 2 minutes late]** This can happen, for example, when HiOS freezes one app. Both rows are sent. The owner can disable one app in Settings.
- **[Robolectric download]** The first test run downloads its Android runtime jar, which is a test-only dependency.
- **[Group-summary restatement]** A group summary is dropped only when it restates a captured child within 2 minutes. A summary with its own different text is classified like any other notification.

## Open decisions for owner

1. **Duplicate window.** It is 2 minutes. Widen it if device evidence shows GoPay and Gojek posts arriving later than that.
2. **Hard-noise wording on recognised transactions.** It is sent, and the backend ignores it. The alternative is to keep it on the phone.
3. **Retention for kept rows.** It is 7 days, as requested. The ignored-by-server retention stays at 3 days.
