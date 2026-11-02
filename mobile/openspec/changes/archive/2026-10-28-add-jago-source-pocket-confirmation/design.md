# Design

## Context

This adopts the owner-approved backend change `confirm-jago-outbound-source-pocket`, especially decisions 2, 3, 5 and 6 in its design. Companion source is the installed 1.5.0 build, Room 4, with existing sender questions, durable replies, pairing isolation and result polling.

## Goals / Non-Goals

Deliver an explicit Jago source selection with durable recovery and authoritative completion. Do not learn aliases, guess a default, alter financial facts, inspect real notifications or replace the existing inbox/navigation design.

## Decisions

Use an immutable notification activity action to open the existing detail sheet. Fetch `GET /api/ingest/notifications/{event_id}/source-pockets`, cache the successful question-bound list, and show a scrollable single-choice control without selection. `POST .../confirm-source-pocket` takes question/reply/account UUIDs. Persist before scheduling delivery.

Extend the existing reply table with a typed answer kind and nullable account ID, migrating old rows as sender answers. Keep old worker class identity for scheduled-job compatibility and use shared delivery mechanics. Preserve sender payloads, stable reply IDs, pairing suspension, bounded retries, response identity/hash checks and result-before-ack ordering.

Room 4→5 preserves all notifications, alert receipts and pending sender rows. Proposed APK is 1.6.0/code 12. Loading, empty/error, offline cache, stale refresh, queued, processing and recorded states are explicit. Dismissal or notification denial leaves the inbox question pending. Use existing theme components and verify semantics, touch targets, enlarged text, light/dark and lock-screen protection without adding a separate TalkBack gate.

## Risks / Trade-offs

- Offline options become stale → server validates current ownership/eligibility; conflicts refresh the chooser.
- OEM freezes background work → retain queued choice and recover on app open; no immediate-delivery guarantee.
- Older backend/client combinations → backend-first release and source-question capability gate; no default-main fallback.

## Migration Plan

Test Room upgrade, typed delivery and the fictional local backend using the separate verification application ID on the owner's phone (TECNO) over USB. Retain a rollback APK; normal installation follows successful exact-revision backend deployment and authorization, preserves current pairing/URL/key and uses `adb install -r`. Report evidence and await owner acceptance before native sync/archive.
