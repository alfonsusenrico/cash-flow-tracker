# Proposal

## Why

The 2026-09-29 payroll allocations proved that the automated notification pipeline records cross-account transfers as unrelated expense and income rows (a Rp7.490.557 BCA → ShopeePay top-up became a `Belanja` expense plus an income), strands legitimate events in `needs_review` with no way to act on them from either app, and silently drops every notification format the mobile parser does not already know. The owner is holding the remaining allocations until both sides are fixed and deployed, so the fix must cover the mobile capture policy, the backend pairing and review logic, and the operator controls in one release.

## What Changes

- **Mobile capture policy (BREAKING for the listener's local filter):** the companion app forwards every notification from an enabled registered financial package except empty payloads and one-time-password/verification messages; the local parser only supplies hints (`expected_*`) when it recognises a format. The backend becomes the single authority on what is financial, so unknown formats surface as reviewable events instead of vanishing.
- **Movement pairing rework:** an observed leg is auto-linked to its counterpart when both are notification-recorded liquid transactions with equal amount, opposite direction, different institutions, no goal/debt/recurring linkage, and timestamps within a configurable window (default 15 minutes, previously a hard 30 seconds); a unique match links, multiple matches are flagged `ambiguous_movement`, and a leg naming a third-party counterparty that does not match the owner's aliases is excluded from relaxed pairing. The Indonesian transfer verb family (`mengirim`, `mengirimkan`, `dikirim`) is recognised. A movement created by pairing can be split again from the ledger.
- **Owner identity aliases:** a per-user list of name aliases (default: the profile name) drives self-transfer detection, including bank-masked renderings such as `ALFO**US ***ICO *O`. The mobile app no longer hardcodes personal names.
- **Review controls and graceful degradation:** an AI `needs_review` or provider failure falls back to the deterministic interpretation when it can record safely; provider outages and configuration errors become auto-retried `failed` events with escalating backoff instead of terminal `needs_review`; only evidence, mapping, or genuine ambiguity outcomes remain `needs_review`. The backend gains an owner-scoped resolve endpoint that records a `needs_review` event with owner-chosen account, category, and note. The mobile inbox shows the reason in Indonesian and offers Retry and manual record; the web Settings modal edits the aliases.
- **Parser coverage:** settlement, direction, and balance cue lists are extended for outbound transfer wording (`sent`, `dikirim`, `mengirim`, `remaining balance`, `saldo kamu`), the myBCA `Account Transfer` outbound category is mapped to a transfer hint, and the parser test corpus gains the owner's real captured texts (anonymised). Further institution-specific patterns are added during the monitored allocation run once their texts are captured.
- Mobile `source_version` reports the real app version so the backend can attribute hints to a parser build.

## Capabilities

### New Capabilities
- `mobile-notification-review`: the companion app presents backend outcomes with human-readable reasons and lets the owner retry or manually record unresolved events.

### Modified Capabilities
- `notification-filtering`: listener forwards all notifications from enabled packages (except empty and OTP payloads); backend rejection of noise is unchanged but becomes the only filter.
- `notification-ingestion`: relaxed evidence-bounded movement pairing with a configurable window, owner identity aliases with masked-name matching, deterministic fallback for AI uncertainty, failed-versus-needs-review separation with automatic retry, owner-scoped manual resolution, movement split.

The unarchived change `add-ai-notification-processing` describes "Conservative Automatic Movement Reconciliation" with a strict 30-second window and mandatory transfer evidence on both legs; this change supersedes that requirement. Its retry and inspection requirements are preserved and extended.

## Impact

- Backend: `app/services/notification_resolution.py`, `notification_context.py`, `notification_application.py`, `notification_processing.py`, `notification_evidence.py`, `notification_parser.py`, `routers/ingest.py`, `routers/movements.py`, `routers/auth.py`, `core/config.py`; new Flyway migration `V21` adding `users.name_aliases`; new env `NOTIFICATION_PAIRING_WINDOW_SECONDS`; tests in `backend/tests/test_notification_*.py`, `test_movements.py`.
- Frontend: `SettingsModal.tsx` (aliases field), ledger movement row split action.
- Mobile (`financial-tracker-mobile-listener`): `NotificationProcessor.kt`, `NotificationDao.kt`, `DebugInboxViewModel.kt`, `DebugInboxScreen.kt`, `TransactionEditSheet.kt`, `StatusBadge.kt`, `IngestionApiService.kt`, `ApiModels.kt`, unit tests; no Room schema change.
- Deployment: backend via merge to `main` (GitHub Actions); mobile via debug APK install on the owner's device. The GitHub Actions deploy env gains the pairing window variable with its default.
- Data: no rewrite of historical events; existing unlinked legs from 2026-09-29 can be merged manually in the ledger.
