# Design: notification-pipeline-reliability

## Context

See proposal.md for motivation. Current flow: mobile `NotificationProcessor` drops anything its regexes do not match → `POST /api/ingest/notifications` → `collect_facts` (deterministic amount/direction/settlement) → AI or deterministic interpretation → `apply_interpretation` records the observed leg and calls `discover_candidates` (±30 s) plus `signatures_compatible` (transfer keyword on both legs, institution mention, owner name). Outcomes are exposed through `GET .../{id}/result`; `POST .../{id}/retry` exists but no client calls it. Evidence from the device DB on 2026-09-29: rows 56/58 (BCA debit ↔ ShopeePay top-up, 3 s apart) were recorded as an unrelated expense and income; rows 59/60 are two incoming legs (Jago and ShopeePay each received Rp7.491.557) whose outbound counterparts were never captured because the phone dropped the unknown outbound formats; row 60 is stuck `model_uncertain`.

Constraints: single owner per deployment, PostgreSQL via Flyway, worker is a single asyncio task, mobile Room DB v3 must not require a destructive migration, and the monitored allocation run needs both sides deployed together.

## Goals / Non-Goals

Goals: every notification from an enabled bank app is visible somewhere; equal-amount opposite legs within minutes become one movement; nothing stays in `needs_review` because of a transient provider problem; the owner can resolve any stuck event from the phone.

Non-Goals: a web notification inbox (follow-up), rewriting historical events, multi-user alias management UI beyond a text field, changing the AI provider or prompt version semantics, the unrelated backend integrity findings (startup DELETE, trade deletion), Bibit/Blu support.

## Decisions

### D1. Mobile forwards everything from enabled packages; backend is the single parser
The Kotlin regex set is kept only to populate `expected_amount`/`expected_direction`/`event_class` hints. Local drop rules shrink to: empty title+text, and OTP/verification-code messages (privacy: codes never leave the device). `is_financial` is sent as `null` when unrecognised. Backend `accept_event` already returns `ignored` without storing noise, and the mobile row is marked ignored and pruned. DAO queries that filter `is_financial = 1 AND expected_amount > 0` change to include unrecognised rows so they sync and appear in the inbox as "Tidak dikenali" until the backend answers.
Alternative rejected: keep the Kotlin gate and mirror every new backend pattern in Kotlin. It doubles work and keeps unknown formats invisible.

### D2. Pairing rule
The existing role-confirmation check (a second notification confirming a synthesized Jago movement leg) keeps its original 30-second scope so the wider window does not turn unrelated same-account payments into `ambiguous_existing_movement`. `discover_candidates` window becomes `settings.notification_pairing_window_seconds` (env `NOTIFICATION_PAIRING_WINDOW_SECONDS`, default 900, bounds 30–86400). `signatures_compatible(left, right)` returns true when either the existing strict rule holds, or all of: both legs `transfer`-eligible under the relaxed definition (notification-origin liquid transaction), opposite directions, different accounts whose root institution accounts differ, equal amount and currency, and neither leg carries an `external_counterparty` marker. The marker is set by `transfer_signature` when the parsed counterparty contains a personal-name-like token (two or more alphabetic words, not an institution word, not matching any owner alias including masked matching). Ambiguity handling is unchanged: more than one candidate → record the leg alone and set `error_code = 'ambiguous_movement'`. Transfer keyword regex adds `irim` so `mengirim`, `mengirimkan`, `dikirim`, `kirim` all match.
Alternative rejected: a fixed 60-second window; BI-Fast and e-wallet withdrawals can settle minutes after the debit, and the outbound legs that would show real latencies have not been captured yet.

### D3. Owner identity aliases with masked matching
Flyway `V21__user_name_aliases.sql` adds `users.name_aliases TEXT[] NOT NULL DEFAULT '{}'`. Effective aliases = `name_aliases` ∪ {`name`} (each ≥ 3 chars). `notification_context.load_context` computes `self_identity_detected` and sanitises `[SELF]` for every alias, plain or masked. Masked matching: split alias and candidate into words; each candidate word with `*` wildcards must match the prefix of the alias word at the same index, at least two literal letters must be present overall, and the candidate must have ≤ alias word count. `PATCH /api/auth/settings` accepts `name_aliases` (max 10, each 3–150 chars, whitespace-normalized, case-insensitively deduplicated) and `GET /api/auth/me` returns them; Settings modal exposes a comma-separated field. The mobile hardcoded `isSelf` checks are removed; the mobile sends `expected_direction` only.

### D4. Fallback and state separation in the worker
`apply_claim` for AI mode: if the proposal outcome is `needs_review`, or the provider raised a non-transient error, attempt `deterministic_interpretation`; if it validates, record it with `error_code = NULL` and `source = "deterministic_fallback"` in the stored interpretation (`provenance_kind` stays `observed_leg`, because it describes movement provenance and must keep the leg eligible for pairing). An AI `record` proposal that fails backend validation also falls back. When the fallback cannot record either, the event is `needs_review` with the deterministic path's error code, which names the backend-proven reason. `fail_claim` becomes: transient or configuration errors → `failed` with `next_attempt_at = NOW() + backoff(attempt)` where backoff = 30 s, 2 min, 10 min, 30 min, 1 h, 3 h (capped), retried automatically by `claim_event` for events younger than 48 h and `attempt_count < 8`; evidence/mapping/model outcomes → `needs_review` with `attempt_count` left as is. The `MAX_ATTEMPTS` lease-recovery rule keeps guarding crashed workers. Retry endpoint remains for `needs_review` and `failed`.

### D5. Manual resolution endpoint
`POST /api/ingest/notifications/{event_id}/resolve` body `{type: expense|income, account_id, category_id, notes?, kakeibo?}`. Preconditions: owner's event in `needs_review` or `failed`, account liquid and owned, category kind matches type. The amount is the notification's single proven amount; an optional `amount` must equal it, and is required when the notification has no single proven amount (e.g. fee plus total). A repeat request on an event already resolved manually returns the recorded result. Records one transaction with `idempotency_key = notification:<event_id>`, timestamp = event `post_time`, marks the event `recorded` with `source = "manual"` in the stored interpretation and a committed snapshot, and runs the normal pairing pass so the counterpart can still link. Mobile uses this from the edit sheet when `processing_status ∈ {needs_review, failed}`.

### D6. Movement split
`POST /api/movements/{movement_id}/split` clears `movement_id`/`movement_role` on both legs, restores each leg's category and Kakeibo from the values stored in its notification interpretation at record time (still-active category of the same kind only; otherwise the leg keeps `Internal Movement`), clears `movement_id`/`confirmed_role` on the associated notification events, and rebuilds each event's result snapshot under its own record key. Ledger movement rows get a "Pisahkan" action next to delete. Investment trades are rejected (`investment_trade_required`).

### D7. Parser cue extensions
`SETTLED_PATTERN` adds `sent|dikirim|mengirim|terkirim|berhasil dikirim`; `OUT_PATTERN` adds `you've sent|you have sent|mengirim|dikirim ke`; `BALANCE_PATTERN` adds `remaining balance|saldo kamu|saldo anda|saldo tersedia`; `BCA_OUTBOUND_*` map category `Account Transfer`/`Transfer Rekening` to `category_hint = "Transfer Keluar"` and set `expected_direction = "out"` on mobile. Because the phone now forwards everything, `collect_facts` ignores (without storing) text with no amount and no settlement cue (promotions), amount-less confirmations the parser recognises (GoPay "Kamu berhasil transfer ke …", which repeats an earlier notification), and amount-bearing text with neither a settlement cue nor a direction (payment instructions). The backend transfer-evidence regex also recognises `mengirim`/`mengirimkan`/`dikirim` and outbound `sent`. New institution-specific patterns wait for captured texts (task 7.3).

### D8. Reason messages
A single Kotlin map `ProcessingReasons.describe(code)` returns Indonesian text for every backend `error_code` (`model_uncertain`, `uncertain_category_mapping`, `ambiguous_movement`, `direction_unproven`, `settlement_unproven`, `provider_unavailable`, …) with a generic fallback that shows the raw code. `StatusBadge` distinguishes `failed` ("Diulang otomatis") from `needs_review` ("Perlu tinjau").

### D9. Neutral movement and self-transfer naming (added 2026-09-29 after the first monitored transfers)
The owner observed a linked BCA → ShopeePay movement still named "Belanja" (the interpreter's label for the debit leg) and an owner-named Jago credit described by the owner's full name. `link_transactions_as_movement` gains an optional `notes` argument; notification pairing passes `movement_note()` = "Pindah saldo ke <destination label>", and `/movements/merge` passes it only when a leg has a notification event, so hand-written notes survive. `resolve_record` and `deterministic_interpretation` describe owner-counterparty legs as "Pindah saldo masuk/keluar". The context gains a boolean `counterparty_is_owner` (no names leave the backend). Existing production rows are not rewritten; a split and re-merge, or a notes edit, renames them.

## Risks / Trade-offs

- [Relaxed pairing merges two unrelated equal-amount transactions within 15 minutes] → external-counterparty exclusion, different-institution requirement, ambiguity flag, and the new split action; window is configurable downward.
- [Forwarding all notifications sends promotional text to the owner's server] → server drops noise before storage; OTP messages never leave the device; server is self-hosted over HTTPS.
- [Deterministic fallback records a transaction the AI doubted] → fallback still runs `validate_interpretation` and the endpoint/category resolution; it only replaces the AI's category choice with rule/hint mapping, otherwise it stays `needs_review`.
- [Auto-retry of `failed` events hammers a dead provider] → capped attempts and hour-scale backoff; retries are one event per second at most.
- [Masked-name matching false positives on short masked tokens] → require ≥ 2 literal letters and word-index alignment.
- [Mobile DAO change shows many unrecognised rows] → rows resolved as `ignored` are pruned by the existing keep-latest rule.

## Migration Plan

1. Merge backend + frontend on a topic branch; Flyway V21 applies on deploy (additive, default `'{}'`). Add `NOTIFICATION_PAIRING_WINDOW_SECONDS` to the deploy workflow env with default 900.
2. Build the mobile debug APK and install with `adb install -r`; Room schema unchanged, so existing rows persist. Verify the listener reconnects and the foreground notification appears.
3. Owner sets aliases in Settings (profile name plus bank renderings), then re-runs allocations while the device DB, logcat, and `GET /api/ingest/notifications` are monitored.
4. Rollback: revert the backend merge (V21 column is harmless), reinstall the previous APK from `app/build/outputs` (kept).

## Open Questions

- Resolved (owner, 2026-09-29): Jago "You've moved … out of your X Pocket" means pocket → Main Pocket. On 2026-09-27 the owner then sent Main Pocket → BCA ATM; that Jago outbound notification was dropped by the old phone parser. The existing reading stays, and the Main Pocket → BCA leg pairs with the BCA credit under D2 (regression test `test_pocket_move_then_main_pocket_to_bca_books_two_movements`). The Jago outbound wording itself is still to be captured.
- Exact texts for Jago outbound, ShopeePay outbound/payment, and GoPay top-up: captured during the monitored run and added as patterns in task 6.2 without changing specs.
