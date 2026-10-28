# Design

## Context

See `proposal.md` for the problem and expected outcome. Inspection on 2026-10-05 found:

- Backend checkout: `fix/investment-topup-label` at `08b0bbd`, with unrelated local edits. The local `origin/main` reference is newer (`4368259`); implementation must start from a refreshed target in isolation, preserving this checkout.
- `notification_processing.py` can write during deterministic `accept_event`, leased `apply_claim`, mapped-account acceptance, fallback, and manual resolution. A guard only in the AI branch would be incomplete.
- `notification_interpretation.py` has overall and account-mapping confidence, but no sender identity contract. The v6 prompt permits generic settled-transfer descriptions. BCA parsing exposes sender text and distinguishes `Account Transfer` categories; RDN own-account transfers have their own proven path.
- `notification_mapping.py` stores names for OWNED accounts. `users.name_aliases` establishes owner identity. Neither is an appropriate place to remember another person.
- Results already support `needs_confirmation`, but its payload and companion validation require `mapping_proposal`. Account confirmation clears `interpretation`. Sender state must therefore persist independently of this transient model output.
- Companion `../financial-tracker-mobile-listener` is clean at `3ae2292`, version 1.4.3/code 10, min API 26, target 35. Existing `MappingConfirmationNotifier` and `MappingConfirmReceiver` support account actions, but the receiver has an eight-second network attempt rather than durable reply delivery. Alert receipts use an event-only confirmation key, which would suppress a second question for that event.
- Room, WorkManager, explicit non-exported receivers, result polling, pairing-context isolation, and a detail sheet already exist. USB ADB was connected and authorized during exploration; runtime verification will recheck the device.
- Durable ingestion/filtering specs contain legacy requirements inconsistent with the released pipeline. This plan references current source and the completed account-confirmation/mobile-completion changes and adds new capability deltas without silently repairing unrelated specs.

## Goals / Non-Goals

**Goals:** Resolve a masked sender with one owner reply, preserve independently proven financial facts, make learning reversible, and make delayed/repeated delivery safe across both repositories.

**Non-Goals:** See proposal scope. Do not add a chat service, contact directory, per-user prompt files, another ingestion pipeline, arbitrary confidence threshold, or a model/version change. Do not make a name answer resolve uncertain money facts.

## Decisions

### 1. Backend policy owns the hold

Add a small sender-clarification service used by the existing processing/application boundary, not a separate workflow engine. Its inputs are backend-extracted sender evidence, resolved receiving account, owner identity, existing movement evidence, and accepted sender state.

The first release targets BCA/myBCA incoming account transfers with an actual masked sender token. Only exact case/space-normalized masks are keys; preserve `*` and other recognized mask positions. Generic labels, missing senders, unmasked names, interest/refunds, RDN own-account transfers, and payments are not new clarification triggers. Extend existing parser facts only as necessary to expose the distinction; model output cannot manufacture a mask or declare ownership.

Resolve the receiving account first. Perform the existing read-only movement-evidence evaluation before the sender hold; independently proven own-account movements retain their normal path. A mere amount/time match or remembered person's name does not independently establish an owned movement. Check the hold before all writes, including immediate deterministic acceptance and fallback. Prevent legacy/manual resolution from silently bypassing an unanswered supported question; offer the explicit sender answer or no-name action instead.

An eligible unconfirmed sender holds even when AI confidence is 0.99. This replaces a proposed sender-confidence threshold with an inspectable rule. Sender uncertainty and account uncertainty are separate; no LLM estimate can raise a masked identity to confirmed.

### 2. Add a typed question without breaking account actions

Continue using `needs_confirmation`. Add a result field `confirmation` for sender questions, with:

| Field | Meaning |
| --- | --- |
| `type` | `sender` |
| `question_id` | Stable UUID for this question revision |
| `masked_sender` | Bounded original sender mask |
| `institution` | Backend-proven institution |
| `receiving_account` | Owned active account ID and display label |
| `amount`, `currency` | Backend-proven financial facts |

Keep `mapping_proposal` for legacy account confirmation. For sender-capable clients, also assign question identifiers to account questions so the companion can deduplicate alerts by pairing + event + question, including transitions between types. Persist the active question; identical refreshes do not invent new identifiers. A materially different account/sender question supersedes the previous identifier.

Use the existing source-version compatibility mechanism: proposed companion release 1.5.0/code 11 is the minimum sender-capable build, subject to rebasing over any intervening release. Older/invalid versions get `needs_review` with `sender_confirmation_required`; they must not receive a sender payload that their parser rejects. Explicit retries already adopt newer companion versions; test this transition. Keep account questions unchanged for older account-capable clients.

### 3. Persist answers outside model interpretation

Use the next available Flyway version after refreshing main; the local checkout ends at V23 while the handoff reports V24 deployed. Mirror additive schema through the existing `init_db.py` startup SQL pattern.

Add owner/event-scoped active-question and sender-resolution storage separate from `interpretation`, and durable answer receipts keyed by event/question. Record the accepted action, normalized name when provided, and replay identity. Add `notification_sender_aliases`, keyed uniquely by owner + institution + receiving account + exact normalized mask, with display mask, confirmed name, active/ambiguous state, and timestamps. Foreign keys and application checks enforce owner/account consistency.

`POST /api/ingest/notifications/{event_id}/confirm-sender` accepts `question_id`, stable client `reply_id`, `action` (`name` or `unknown`), and `name` only for the name action. Trim and normalize Unicode; require 1–80 characters, no control/newline characters, and reject invalid rather than silently truncating. Treat the value as data and apply existing secret-redaction protections; it never becomes prompt instructions.

Under the existing owner advisory lock and event row lock: authorize ownership; first recognize an identical accepted answer; otherwise validate the active question, state and input; persist answer/receipt; update matching memory for a name; clear only the completed question; and queue the same event with a new processing generation and invalidated lease. Commit these operations together. An identical replay returns the current result even after recording. A conflicting accepted answer, reused reply ID with different content, or stale revision returns 409. Cross-owner event access returns 404. Invalid input returns 422.

The worker rereads current facts and active references, then records through normal application and pairing. Provider failure can use deterministic completion after an accepted answer; it cannot erase the answer or bypass an unanswered hold. Preserve accepted sender resolution when account confirmation clears model interpretation or retries reset attempts.

### 4. Learn only scoped, explicit names

A confirmation provides a name for its event and the scoped alias key. For multiple still-pending events, different accepted names for one key mark that key ambiguous rather than silently overwriting it. Each event retains its own answer; future matching events ask again. Explicit Settings correction makes memory active with the corrected name; deletion removes future reuse. Do not mutate already accepted answers or ledger descriptions during memory editing.

Add owner-scoped `GET /api/ingest/sender-aliases`, `PATCH /api/ingest/sender-aliases/{id}` (name correction), and `DELETE /api/ingest/sender-aliases/{id}`. Extend the existing web Settings modal with a compact `Nama pengirim` section showing bank/account, mask, name, ambiguity status, and `Ubah`/`Hapus`; keep existing owned-account mappings distinct.

Provide only the relevant matching alias or event answer in bounded AI context with provenance `owner`. Update the central prompt to use that name solely for sender wording and to treat it as untrusted data. Preserve source evidence quotes and financial validation. At final recording, deterministically ensure the incoming-transfer description is `Transfer masuk dari <confirmed name>` or `Transfer masuk` after explicit unknown. This avoids an extra provider call merely to paraphrase the answer and ensures deterministic fallback also uses it.

This scope cannot guarantee a mask identifies a unique human: two people may share it even for one bank/account. The owner can correct/remove memory; conflicting known evidence must ask again. There is no fuzzy expansion across masks or account scopes.

### 5. Use durable Android reply delivery

Extend existing result consumption to validate sender questions and legacy account proposals. Use Android direct reply through `RemoteInput` and an explicit non-exported receiver. Give each action a distinct event/question identity in the Intent data, not extras alone; use a mutable `PendingIntent` only where direct reply requires it, and immutable intents for other actions. The receiver validates the current row, question, action and pairing before persisting an answer.

Add a Room outbox with stable reply ID, event/question IDs, action, bounded name, pairing-context identity, delivery state and retry metadata. A Room migration adds this without destructive fallback. Persist first, then enqueue unique WorkManager delivery; network work must not depend on the receiver's short execution lifetime. Duplicate taps reuse the same answer; conflicting input requires resolving the current in-app state. Worker retries network errors/5xx with bounded exponential backoff (30 seconds up to one hour); 401/403 waits for pairing recovery; 409 refreshes the latest result and stops stale replay; 422 exposes correction and does not loop. Preserve unresolved replies instead of deleting them at a retry deadline.

Consume the authoritative response through existing completion handling. Accepted queued/processing answers resume polling; only complete `recorded` results trigger the existing logical record alert. Labels distinguish `Balasan menunggu koneksi`, `Mengirim balasan`, processing, review, and recorded states. A pairing change suspends old replies; returning to a compatible pairing explicitly resumes them. Never resubmit a raw event solely to deliver a reply.

Add equivalent controls to `DebugInboxScreen`'s detail sheet/view model and preserve unresolved rows/outbox items through retention. Notification dismissal is not an answer. No indefinite polling of an unanswered question is required; explicit in-app refresh remains available.

### 6. Respect privacy and the current interface

Use concise Indonesian copy with native notification styles and existing Compose controls. Display proven amount/account so two questions are distinguishable, without extra confidence meters or explanatory banners. Keep a private lock-screen public version without financial detail. On API 31+, require authentication for financial notification actions; on API 26–30, reject locked-device acceptance and direct the owner to the unlocked detail sheet. Verify both branches through platform-appropriate tests.

Operational logs contain IDs/status codes only, never reply names, masks, raw notifications or credentials. Extend backup exclusions to cover the outbox within the existing Room DB. Memory remains until owner deletion/account deletion; event answers follow existing event retention. Clear acknowledged outbox name bodies once no longer required for delivery/recovery, retaining only safe delivery identity as needed. Unanswered questions and unsent replies have no automatic expiry.

Sources: [Android direct replies](https://developer.android.com/develop/ui/compose/notifications/create-notification#add-a-direct-reply-action). Use the installed dependencies; no new framework is needed. Preserve accessible labels, focus/error behavior, touch targets, contrast, light/dark rendering and enlarged text. The owner's earlier direction removed a separate TalkBack acceptance gate; semantic checks remain, without reinstating that release blocker.

## Risks / Trade-offs

- Mask collisions → exact scoped reuse, ambiguous-memory handling, visible edit/delete; no claim of verified identity.
- Unanswered transfers omit pending money from ledger balances → show pending status and explicit no-name action; never auto-record on timeout.
- Multiple questions or late replies → stable question IDs, independent persisted answers, locks, leases and idempotent receipts.
- Mixed app/server versions → backend-first deployment and explicit version gate; test old-build review and new-build retry.
- OEM freezes/background restrictions → persistent outbox recovers when Android permits execution, but cannot guarantee prompt delivery while the OEM freezes the app.
- Existing stale specs and unrelated edits → additive deltas, isolated implementation branch/worktree, recheck relevant main source before apply.

## Migration Plan

1. After plan approval, refresh target refs and create isolated topic branches. Adopt the mobile portion into a native companion change with equivalent contracts before mobile source edits; this root's native apply scope alone does not authorize editing a sibling repository.
2. Implement and verify additive backend/Flyway and Room migrations on fictional populated databases. Preserve event, pairing, ledger and alert identity. No historical recorded-event backfill or automatic replay.
3. Run backend PostgreSQL tests, frontend checks/build, Android tests/lint/build, and a USB-device local-backend test using the verification application ID. Verify direct replies, no-name, two-event routing, offline/restart delivery, stale replies, sequential questions, and one ledger record.
4. Push a clean backend/web topic branch and report it for the owner to create/merge the PR. Observe the designated deployment pipeline for that exact merged revision; do not directly access production.
5. Only after backend deployment is confirmed and release/install is authorized, build the matching normal companion APK with `--no-build-cache`, verify its version, retain a rollback artifact, and upgrade via USB `adb install -r`. Preserve the current production pairing; do not overwrite it with local test settings or expose secrets. Record installed version and listener status without reading personal notifications.
6. Prefer a forward fix. App rollback leaves sender events unresolved on the compatible backend. Do not downgrade the backend to a revision that lacks the sender guard while supported clients/pending questions remain; such a rollback needs a separately reviewed compatible procedure. Do not drop the additive schema as a routine rollback.
7. Report evidence and remaining limitations; after owner acceptance, synchronize/archive the relevant native changes.
