# Proposal

## Why

Notification processing refuses any account the backend cannot match by name, so every new bank wording ("GoPay Tabungan", a renamed pocket) lands in "Perlu tinjau" and the owner must rename accounts to mirror bank text. The AI already sees the owner's accounts and pockets but is not allowed to choose one. The owner wants the AI to pick the nearest account with a confidence score: record automatically when confident, and otherwise ask for a one-tap confirmation from an Android notification, without opening the app.

## What Changes

- When the backend cannot resolve a name in a notification (an institution account, a named pocket, or a pocket-move source or target), the AI proposes the nearest eligible owned account for each unresolved name, with a mapping confidence between 0 and 1.
- **Confidence ≥ 0.85** (configurable): the event is recorded with the proposed account, the result is labelled "dipetakan otomatis", and the name → account pair is remembered as a learned alias so later notifications map deterministically.
- **Confidence < 0.85**: the event enters a new state `needs_confirmation`. The result payload carries the proposal (name, proposed account, confidence, up to three alternatives). The phone posts a notification with "Ya, <account>" and "Pilih lain" actions. "Ya" confirms from the notification shade; "Pilih lain" opens a picker.
- New owner-scoped endpoint `POST /api/ingest/notifications/{id}/confirm-mapping` accepts or corrects the proposal, stores the alias as owner-confirmed, and records the event.
- Learned aliases are listed in web Settings, where the owner can remove a wrong one. Removing an alias does not rewrite recorded transactions.
- Existing strict validation stays for everything else: amounts, directions, and evidence remain backend-proven, and the AI can only choose among the owner's active liquid accounts.

## Capabilities

### New Capabilities
- `notification-account-mapping`: AI-proposed account mapping with confidence, automatic acceptance above a threshold, learned name aliases, and owner confirmation below it.
- `mobile-mapping-confirmation`: actionable Android notifications and an in-app picker to confirm or correct a proposed mapping.

### Modified Capabilities
None in `openspec/specs/`. This relaxes, for unresolved names only, the "Backend Reference and Classification Validation" behaviour described in the unarchived change `add-ai-notification-processing`, which rejected any account the backend could not prove.

## Impact

- Backend: Flyway `V22` (`notification_account_aliases` table; `needs_confirmation` added to the processing-state check constraint); `notification_interpretation.py` (schema and prompt version 5), prompt text, `notification_resolution.py` (alias lookup before name matching), `notification_application.py`, `notification_processing.py`, `routers/ingest.py` (confirm endpoint, alias list and delete), `core/config.py` (`NOTIFICATION_MAPPING_AUTO_THRESHOLD`, default 0.85), synthetic benchmark cases for mapping.
- Frontend: Settings section listing learned aliases with remove.
- Mobile: result model gains `mapping_proposal`; completion handling for `needs_confirmation`; notification with action buttons and a `BroadcastReceiver`; picker in the detail sheet; Room stores the proposal inside the existing `result_json` column (no schema change).
- Cost: at most one extra AI call per unresolved event; no extra calls once an alias is learned.
