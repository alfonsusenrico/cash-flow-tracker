# Design: ai-account-mapping-confirmation

## Context

See proposal.md for motivation. Today `endpoint_context` resolves the observed account or pocket-move endpoints deterministically; a failure sets `facts.mapping_error`, the prompt instructs the model to return `needs_review`, and `resolve_record` rejects any model account that differs from the backend's. The `Interpretation` schema already has an overall `confidence` field, which is unused for decisions. Device evidence: a Jago move to "GoPay Tabungan" stayed in review because no pocket had that name, although the owner's "GoPay" account holds exactly that balance.

## Goals / Non-Goals

Goals: new bank wording maps to the right account with at most one tap; confident mappings need no action; every mapping decision is visible and reversible for future events.

Non-Goals: letting the model choose amounts, directions, categories for movements, or investment positions; mapping names for AI-disabled deployments (deterministic mode keeps `needs_review`); rewriting already-recorded transactions when an alias is removed; a web notification inbox.

## Decisions

### D1. Mapping is a separate, explicit proposal per unresolved name
`endpoint_context` reports each unresolved name as `{"role": "observed"|"source"|"target", "name": "<bank text>"}` in `facts.unresolved_names` (names come from the bank text, which already goes to the model). The schema gains `mappings: list[{role, account_id, confidence, alternatives: list[account_id] (max 3)}]`. The overall `confidence` field keeps its current meaning.
Alternative rejected: reusing the overall `confidence`. It mixes category, description, and mapping certainty, so a confident description could auto-accept a doubtful account.

### D2. Backend guardrails on a proposed account
A proposed account must be active, owned, liquid (not an investment position), and, for a Jago pocket move, different from the other endpoint. For an institution-level name (observed account), the account must be the institution's root or one of its pockets, or a pocket whose name shares a word with the bank text; any liquid account qualifies only for a pocket-move endpoint name. The backend clamps confidence to [0, 1] and treats a missing mapping for an unresolved name as `needs_review` (unchanged).

### D3. Threshold and outcomes
`NOTIFICATION_MAPPING_AUTO_THRESHOLD` (default 0.85; valid 0.5–1.0) is compared with the lowest confidence among the event's mappings.
- ≥ threshold: record through the normal path with the proposed accounts substituted for the unresolved endpoints; store `interpretation.mapping = {..., "mode": "auto"}`; upsert learned aliases with `source = 'ai'`; result snapshot gains `"mapped": [{"name", "account", "mode": "auto"}]`.
- < threshold: set `processing_state = 'needs_confirmation'`, store the proposal in `interpretation.mapping_proposal`, and do not create transactions. The compact result gains `mapping_proposal: {"name", "role", "proposed": {"id", "label"}, "confidence", "alternatives": [{"id", "label"}]}` (at most one unresolved name per proposal; with two unresolved endpoints the backend asks for the lower-confidence one first).

### D4. Learned aliases
Table `notification_account_aliases(id, user_id, institution, name_normalized, account_id → accounts ON DELETE CASCADE, source 'ai'|'owner', created_at, UNIQUE(user_id, institution, name_normalized))`. `institution_parent`, `pocket_endpoint`, and `observed_endpoint` check aliases for the normalized bank name before name matching, so later events are deterministic and cost no mapping call. An owner confirmation overwrites an `ai` alias. Aliases pointing at archived accounts are ignored.

### D5. Confirmation endpoint and state machine
`POST /api/ingest/notifications/{id}/confirm-mapping` with `{"account_id": "<id>"}` (the proposed or an alternative or any eligible account) → validates per D2, upserts the alias as `owner`, and re-queues the event for immediate processing; the worker then resolves the name through the alias and records normally, including pairing. It returns the compact result (usually `queued`, then `recorded` on the next poll). Owner-scoped; 409 unless `needs_confirmation`. `needs_confirmation` events are not auto-retried and are included in retry/resolve eligibility (manual record stays available for non-movement events).

### D6. Mobile actionable notification
`NotificationResults.consume` treats `needs_confirmation` as a waiting state: it saves the proposal in `result_json` and posts a notification on a new channel "Konfirmasi Rekening" (IMPORTANCE_DEFAULT): title "Konfirmasi rekening • Rp …", text "'GoPay Tabungan' → GoPay? (yakin 72%)", actions "Ya, GoPay" (`PendingIntent` → `MappingConfirmReceiver`, which calls the confirm endpoint through `goAsync()` with a short coroutine and schedules completion polling) and "Pilih lain" (opens `MainActivity` with the event selected). The detail sheet shows the proposal with a picker of alternatives and all liquid accounts. Duplicate posting is prevented with a receipt keyed by event id, like existing alerts. Receivers are not exported.

### D7. Web Settings
A "Nama dari notifikasi bank" list (bank name → account, source AI or owner) with remove, backed by `GET /api/ingest/aliases` and `DELETE /api/ingest/aliases/{id}`.

### D8. Prompt and evaluation
Prompt version `notification-interpretation-5` explains unresolved names, eligibility, and calibrated confidence (0.9+ only when the name clearly denotes one account; ≤0.6 when several accounts fit). Synthetic benchmark cases add: exact-but-missing pocket (GoPay Tabungan → GoPay), two plausible pockets (low confidence expected), unrelated name (needs_review). Acceptance: no auto-accepted mapping to a wrong account across repeated runs of these cases.

## Risks / Trade-offs

- [Model confidence is not calibrated] → guardrails in D2, the lowest-confidence rule in D3, benchmark gate in D8, the "dipetakan otomatis" label, and alias removal in Settings. The threshold is configurable upward without a deploy of code.
- [A wrong auto alias repeats] → aliases are visible and removable; removal stops future mapping, and split/edit correct past rows.
- [Notification actions run in the background on an aggressive OEM] → the receiver uses `goAsync()` with a network timeout; on failure it shows "Gagal, buka aplikasi" and leaves the event in `needs_confirmation`.
- [Owner ignores the confirmation] → the event stays in `needs_confirmation` and appears in the in-app "Perlu cek" filter; no transaction is created meanwhile, and a later counterpart leg still records alone.

## Migration Plan

1. Backend with V22 (additive table; constraint replaced to add one state) deploys first; old mobile builds treat unknown states as invalid results only if they receive one, so the confirmation state is emitted only when the request's `source_version` is ≥ 1.3.0; older builds keep receiving `needs_review`.
2. Mobile 1.3.0 installs after the deploy.
3. Rollback: revert the backend merge; events already in `needs_confirmation` can be retried or resolved manually through existing endpoints after rollback because the check constraint change is kept.
