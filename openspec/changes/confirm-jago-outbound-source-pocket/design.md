# Design

## Context

See `proposal.md` for the problem. Read-only inspection and a fictional reproduction on 2026-10-06 established:

- Deployed backend source is main `1797708`; `/private/tmp/cft-sender-clarification` contains that implementation plus delivery documentation. The original checkout is older and has unrelated edits, which must remain intact.
- The Jago outbound-transfer parser recognizes an amount and recipient, but may leave `source_pocket` empty. `observed_endpoint` then returns `main_endpoint`, even when multiple child pockets exist. AI confidence does not repair the missing evidence.
- `load_context`, deterministic interpretation and `resolve_record` use that endpoint. Application supports immediate acceptance, leased AI, fallback, manual resolution, and pairing with an already recorded counterpart. A guard only in the AI branch or only after endpoint resolution is insufficient.
- Existing account questions resolve named aliases, can propose three alternatives and can remember an alias. That contract is unsuitable for an unnamed, per-event source choice. Existing typed sender questions and durable answer receipts provide reusable lifecycle mechanics.
- Account context is capped at 200 and marks overflow incomplete. The pocket chooser must not inherit that truncation or the three-alternative mapping limit; existing incomplete-context financial safeguards must remain.
- The delivered companion is `/private/tmp/cft-mobile-sender-replies`, version 1.5.0/code 11, Room 4. The original sibling checkout is older. Sender replies already persist event/question/pairing identity and a stable reply UUID, use WorkManager, and consume authoritative results. Source-pocket replies need a typed account ID, never a name-field workaround.
- The TECNO USB device was connected during exploration. Planning does not change its installed app or pairing. Prior sender changes await their own acceptance/archive.

## Goals / Non-Goals

**Goals:** Make absent source evidence explicit before financial writes; give the owner a complete Jago-only choice; preserve idempotency, offline delivery, current-account checks and bilateral pairing.

**Non-Goals:** See proposal scope. Do not infer the source from recipient, amount, AI confidence, a previous pocket-to-main movement, or a remembered generic transfer label. Do not add a second financial application pipeline or silently rewrite legacy durable requirements unrelated to unnamed outbound transfers.

## Decisions

### 1. Enforce a backend evidence policy before resolving the source

Add a small source-pocket policy to the existing pipeline. Trigger only for a verified settled Jago outbound **transfer**, with a positive independently extracted amount, where the notification lacks source-role pocket evidence. Both registered Jago package spellings remain supported. Evidence naming the source main pocket is informative; an absent source is not.

Classify the trigger from backend facts, not a model-generated description. Preserve recognized outgoing-transfer syntax; parser changes are limited to exposing this distinction consistently. A pocket name appearing only in a destination/recipient is not source evidence. A source explicitly named but not mapped continues through existing named-account confirmation. Incoming Jago notifications, card payments, and the existing named single/dual-pocket internal movements retain their paths.

Check this policy before the default main endpoint is assigned in acceptance/context/interpretation. Unanswered supported events enter `needs_confirmation`; unable-to-form questions enter `needs_review`. Add a final shared application invariant before transaction creation, existing-movement role confirmation, reconciliation changes or movement linking. It must cover deterministic, AI, provider-failure fallback, retry, manual resolution, and concurrent leased work. A supported source question cannot be bypassed through generic `resolve` or `confirm-mapping`; those calls return a source-confirmation-required conflict while the question is unanswered.

An accepted, still-valid event choice becomes the effective observed source throughout context, interpretation validation, transfer signatures and application. The model may categorize/describe the proven transaction, but cannot substitute another source or manufacture a complete internal movement. A conflicting interpretation goes to review without financial writes. Persisted choices survive clearing interpretation and new processing generations. Do not change extracted amount, currency, direction, timestamp or counterparty.

**Alternative considered:** lower AI confidence or add a prompt instruction. Neither prevents deterministic defaulting or fallback, so the hold is a deterministic evidence policy.

### 2. Use a distinct question and current pocket-options API

Continue the existing typed `confirmation` result envelope:

| Field | Meaning |
| --- | --- |
| `type` | `source_pocket` |
| `question_id` | Persisted UUID for this event question revision |
| `institution` | `jago` |
| `jago_account` | Resolved owned active parent ID and sanitized display label |
| `amount`, `currency` | Independently proven financial facts |
| `destination_label` | Optional bounded, sanitized recipient from evidence |

Use `source_pocket_confirmation_required` as the error code. Question meaning includes the event's proven financial facts and Jago parent, not mutable candidate labels/order. Refreshing or adding another pocket does not invent a question UUID; replacing relevant evidence or institution parent supersedes it. Deduplicate alerts by pairing + event + question, as existing sender questions do.

Add `GET /api/ingest/notifications/{event_id}/source-pockets`, returning the active source question ID and the full current list of eligible `{id, label}` options. Query owner-scoped active liquid child accounts under the resolved active Jago parent directly, independently of the model-context window. Include the main child pocket; exclude the aggregate parent while any eligible children exist. If no children exist and the resolved Jago parent itself is an active liquid endpoint, offer that single endpoint for explicit confirmation. Do not auto-accept a one-option list.

Use current backend labels with parent context and stable ordering; no preference/ranking that selects a default. A cached successful list is available offline with its question ID; submitting from it always undergoes current server validation. Adding/removing/renaming a pocket is reflected on refresh. A stale question returns 409 and causes result refresh. Ambiguous/missing Jago parent, no eligible endpoint, or incomplete financial context requires review/configuration and never a cross-bank/primary-account fallback. A safety overflow must not be represented as a complete truncated list.

**Alternative considered:** reuse `mapping_proposal` or Android inline predefined choices. Mapping proposals carry alias learning and a small shortlist. A notification action launching the existing activity supports a full scrollable chooser, whereas system-rendered predefined notification choices do not guarantee that presentation. [Android notification actions](https://developer.android.com/develop/ui/compose/notifications/create-notification#add-an-action), [RemoteInput predefined choices](https://developer.android.com/reference/android/app/RemoteInput.Builder#setChoices(java.lang.CharSequence%5B%5D)).

### 3. Persist an event decision and immutable answer receipt

Use the next available Flyway version after refreshing main, expected V26 at inspection time, and mirror the additive schema in existing startup initialization. Add `source_pocket_resolution` to the event, separate from interpretation and sender resolution. Add owner/event-scoped `notification_source_pocket_answers` with event ID, question UUID, reply UUID, selected account UUID and timestamps. Enforce unique event/question and owner/reply identities plus owner/event referential consistency.

The selected account ID in the immutable receipt is an audit snapshot validated against owned active accounts during acceptance, not a new account-deletion restriction. Current eligibility is checked again during application; receipt identity survives account archival/deletion. Event decisions identify the question, Jago parent, financial evidence revision and selected source with provenance `owner`. They never write `notification_account_aliases`, `default_pocket_id`, sender aliases or user identity.

Add `POST /api/ingest/notifications/{event_id}/confirm-source-pocket` with UUID fields `question_id`, `reply_id`, and `account_id`. Under existing owner advisory/event locks: authorize event ownership, recognize an identical accepted receipt first, validate the active question and current eligible source, persist receipt/decision, clear that question, invalidate stale leases and queue the same event in a new processing generation. Commit atomically. Return the existing `{ok, result}` envelope; an accepted answer may still be queued, processing or under review.

Identical replay of the same reply/question/account returns the current authoritative result even after recording or later archival. A different answer to an accepted question, reply UUID reuse for a different event/content, stale question or ineligible account returns 409 with a bounded code. Malformed UUIDs return 422; another owner's event returns 404; authentication failures preserve 401/403 semantics. Never expose a foreign account's label or existence.

If a chosen account becomes invalid after acknowledgement but before application, hold/review and refresh current choices; do not silently use main. A refreshed source question may supersede the invalid decision with a new question UUID while retaining the old receipt. Other unresolved facts remain unresolved. Retry and provider-failure completion must use the accepted source only when the rest of the existing evidence and account/category checks pass.

**Alternative considered:** reuse the account alias table. There is no source name to learn, and teaching an unnamed transfer a default would recreate the defect on the next event.

### 4. Preserve transfer pairing without guessing the Jago leg

Discovering a receiving-bank counterpart is read-only evidence; it cannot prove the unnamed Jago source. An unanswered Jago event cannot debit any Jago pocket, consume an existing movement role, link a movement, or emit a recorded result. A receiving bank may record its own independently observed credit through the existing pipeline.

After source confirmation, construct the Jago signature using the chosen pocket and existing root-account/counterparty evidence. Retain existing uniqueness, timing/reference and reverse-candidate checks. Pair only proven compatible legs, in either arrival order, updating both result snapshots to the actual chosen source and destination without an extra debit or credit. Do not create a bilateral movement solely from a destination label or amount/time match. Pending, unrecorded Jago questions must not be treated as committed counterpart transactions.

The existing Jago-to-BCA regression assumes main for unnamed transfers after a separate pocket-to-main notification. Update that expectation to require an explicit event choice while preserving the distinct named pocket movement. Also test non-main selection, main selection, two same-amount ambiguous candidates, counterpart arriving during the hold, and replay after pairing.

### 5. Extend durable Android confirmation delivery with typed answers

Add result/request/options models and validate `source_pocket` alongside existing `sender` and `account` questions. Extend the current reply storage with an answer kind and nullable account ID, retaining the legacy table identity and migrating all existing rows to sender kind. Use a bounded shared typed-delivery component; sender and pocket adapters supply their validated payloads. Keep the old scheduled sender worker entry point compatible so existing persisted WorkManager jobs still execute after upgrade. No destructive migration or storing an account ID inside the sender-name field.

Proposed companion release is 1.6.0/code 12 and Room 5, subject to a pre-implementation version audit. Source answers persist row/event/question/reply/pairing IDs, selected account ID and delivery state before network work. Duplicate confirms reuse the same queued answer. Preserve the existing unique work, 30-second to one-hour exponential backoff, response-identity/hash validation and persist-result-before-outbox-ack ordering.

401/403 waits for credential recovery; pairing changes suspend the old answer; 404/409 stops stale delivery and refreshes the current result/options; 422 exposes correction; 429/network/5xx retries durably. Reopening/restarting the app resumes outstanding work only for its matching pairing. No raw-notification resubmission is needed to send an answer. Unknown result types or incomplete payloads cannot produce recorded alerts.

**Alternative considered:** a second copy of the sender worker or an online-only account confirm call. A small typed extension preserves tested recovery semantics without duplicating the delivery lifecycle; an online-only call loses the owner's choice on disconnect/process death.

### 6. Show a compact, explicit chooser

Notification: `Pilih kantong Jago`, a short proven amount/recipient line in private content, and `Pilih kantong` opening the current event's detail sheet. Keep notification identity event/question-specific, use explicit immutable activity intents, and require device unlock before exposing/confirming financial details. Preserve the private lock-screen public version and existing API-specific security handling.

In the existing sheet, show the amount/recipient once, a scrollable single-choice list of Jago pockets, `Segarkan` where needed, and `Konfirmasi`. Nothing is preselected; confirm is disabled until a current or cached eligible option is explicitly selected. Disable generic account/manual resolution controls for an active source question. Keep the selection while submitting or durably queued; display `Pilihan menunggu koneksi` or sending/processing state as applicable. Dismissal, back navigation, denied notification permission and timeout leave the event pending in the inbox.

Use the same question controls in the inbox so notifications are optional. First load without cached options has clear loading/error/retry states and cannot confirm offline. Cached options show that freshness will be checked on submission; stale/removed selection returns to the refreshed chooser without success messaging. A recorded alert occurs only after authoritative ledger completion. Keep sender controls and existing account questions working independently.

Use existing Compose components and Indonesian copy, sufficient touch targets, labeled radio semantics, logical focus/error placement, dark/light contrast and enlarged-text/reflow behavior. Avoid extra explanatory banners/confidence meters. Semantics verification remains required; no separate TalkBack acceptance gate is added.

## Risks / Trade-offs

- Unanswered source choices leave Jago balances incomplete → visible pending state and inbox fallback, no timeout/default recording.
- Multiple Jago parents or malformed setup → review/configuration instead of presenting unrelated pockets.
- Pocket options change while a reply is offline → current owner/parent/active checks at acceptance and application; refresh on conflict.
- AI context overflow → preserve existing review safeguards and never claim a truncated candidate set is complete.
- OEM background freezing → durable work recovers when Android permits execution; immediate background delivery cannot be guaranteed.
- Older apps/backend rollback → version-gated review and backend-first release; a rollback removing the source guard would reintroduce the accounting defect.
- Historical incorrect records → remain untouched; correction requires a separately approved reconciliation task.

## Migration Plan

1. After approval, refresh backend target and identify the accepted companion source; create isolated topic branches and adopt the mobile contract with native OpenSpec before mobile source edits. Preserve unrelated working trees and earlier pending acceptance tasks.
2. Verify additive Flyway/startup and Room upgrades on fictional populated databases, including sender outbox rows and scheduled-worker compatibility. No automatic replay of recorded notifications or destructive downgrade.
3. Complete targeted backend/PostgreSQL regressions, Android unit/instrumentation/lint/build, native validation and an end-to-end USB local-backend test through the separate verification application ID. Keep real production pairing and personal notifications out of test data.
4. Push clean topic branches where a remote exists; report branch/review information for the owner to create and merge the backend PR. The companion currently has no remote: retain a clean committed branch and APK evidence unless the owner supplies one. No direct main push or PR creation.
5. Confirm the exact merged backend revision through the designated deployment pipeline. Once mobile release/install is authorized, build a normal matching APK, retain a matching-signature rollback artifact and install with USB `adb install -r` on the explicitly identified TECNO device. Verify installed version, existing URL/key/pairing preservation with value-free metadata, row preservation and listener binding. Never leave normal app settings pointed at local verification.
6. Prefer a forward fix. Companion rollback may leave source questions in review/pending on the compatible backend; it must not record them by default. Keep additive backend schema and the source hold when rolling back other code. Any downgrade that removes the hold needs a separately reviewed compatible procedure.
7. Report executed evidence and limitations; after owner acceptance, synchronize/archive both native changes. Artifact validation alone does not prove release or device acceptance.
