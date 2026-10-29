# Verification: add-qr-device-pairing

Date: 2026-10-29. Base is origin/main 793d14d. The feature commit was rebased onto it, and every result below comes from that combined tree (feature commit c0bb6fc). All users, device names, ids and keys are synthetic. No production system was contacted.

| Check | Result |
|---|---|
| Fresh `postgres:16`, Flyway V1→V34 | 34 migrations applied |
| Full backend suite | 1062 passed, 0 skipped (1008 on the base; 54 new in `test_device_pairing.py`) |
| Frontend `npm ci`, `npm test` | 223 passed in 45 files (212 on the base; 11 new: 8 in `DevicePairingSection.test.tsx`, 2 in `SettingsModal.test.tsx`, 1 in `Modal.test.tsx`) |
| Frontend `npm run lint`, `npm run type-check`, `npm run build` | exit 0; lint reports one existing warning in `src/app/review/page.tsx`, outside this change |
| `openspec validate --all --strict` | exit 0, 60 items |
| `git diff --check` | clean |
| Dependency | `qrcode` 1.5.4 (published 2024-08-05) and dev `@types/qrcode` 1.5.6 (2025-10-24), exact pins, installed with npm into `frontend/package.json` and `package-lock.json` |

## Mutation checks

Each behaviour was removed in the working tree, `test_device_pairing.py` was run, and the code was restored with `git checkout`:

| Mutation | Failing tests |
|---|---|
| `get_current_user` accepts revoked keys | `test_revoked_phone_gets_401_while_other_devices_keep_working`, `test_a_phone_may_revoke_its_own_key`, `test_manual_key_metadata_ignores_paired_and_revoked_keys` |
| `/api-key/reset` deletes every key (the old behaviour) | `test_rotating_the_manual_key_keeps_paired_phones` |
| No `FOR UPDATE` on the code row (with a 0.3 s pause before commit to widen the race) | `test_concurrent_exchanges_of_one_code_create_exactly_one_key` |
| No failed-attempt limit | `test_code_is_burned_after_five_wrong_attempts` |
| Code route accepts Bearer keys | `test_bearer_clients_cannot_create_codes` |
| Shared `Modal` without the topmost-dialog guard | `Modal > gives the keyboard only to the topmost of two open dialogs`, `settings form > closes only the pairing dialog on Escape and returns focus to its trigger` |

## Evidence by task

| Task | Evidence |
|---|---|
| 1.1 | Fresh V1→V34 migration; `test_v34_marks_existing_keys_manual_and_is_repeatable` (scratch schema migrated to V33, an existing key becomes `manual`, V34 applied twice, the `created_via` check and the one-live-key-per-device index enforced) |
| 1.2 | Codes: `test_code_is_ten_crockford_characters_from_the_os_csprng` (2000 `secrets.choice` draws from the Crockford alphabet), `test_codes_are_read_with_crockford_folding`, `test_new_code_is_stored_only_as_a_hash_and_expires_in_five_minutes`, `test_qr_payload_names_the_configured_server`, `test_created_code_carries_its_qr_payload`, `test_pairing_server_setting`, `test_invalid_pairing_server_disables_new_codes`, `test_runtime_materializer_writes_the_pairing_server`. Session only: `test_new_code_replaces_the_old_one`, `test_bearer_clients_cannot_create_codes` (Bearer-only and Bearer beside a session both 403, no code created), `test_anonymous_clients_cannot_create_codes`, `test_creating_a_code_passes_the_csrf_origin_check` (foreign and missing Origin 403). Exchange: `test_successful_pairing_creates_a_device_key` (key stored as SHA-256 only), `test_typed_code_with_separator_and_lowercase_pairs`, `test_reused_code_returns_410_without_a_key`, `test_expired_code_returns_410_without_a_key`, `test_unknown_and_malformed_codes_get_the_same_410` (identical body for every case), `test_code_survives_four_wrong_attempts`, `test_code_is_burned_after_five_wrong_attempts`, `test_concurrent_exchanges_of_one_code_create_exactly_one_key` (two parallel HTTP exchanges → one 200, one 410, one key), `test_repairing_the_same_device_replaces_its_key`, `test_invalid_device_fields_are_rejected_without_using_the_code`, `test_pair_is_rate_limited_per_client_ip` (11th request → 429, `Retry-After: 60`, key `pair:client:<ip>`). List and revoke: `test_revoked_phone_gets_401_while_other_devices_keep_working`, `test_a_phone_may_revoke_its_own_key`, `test_device_list_and_revoke_are_owner_scoped` (404 for another owner's and unknown ids, 422 for a non-UUID), `test_device_list_names_manual_keys`. Manual key: `test_rotating_the_manual_key_keeps_paired_phones`, `test_manual_key_metadata_ignores_paired_and_revoked_keys`. Logs: `test_logs_hold_no_code_key_or_device_name` |
| 2.1 | Vitest + axe: list with dates and empty state, revoke behind a named confirmation dialog (cancel, Escape, confirm), the QR dialog (QR canvas 240 px drawn from `qr_payload`, code as text, `05:00` → `03:59` countdown, focus inside, Escape returns focus to the trigger), expiry at zero ("Kode kedaluwarsa", QR and timer removed) and "Buat kode baru", regeneration before expiry, the "terhubung" confirmation when a phone pairs, the create error; Settings shows the manual key as "API key manual (cadangan)" below "Perangkat terhubung" and Escape in the pairing dialog closes only that dialog. Browser check below |
| 3.1 | This file |

## Browser check

Headless Brave over CDP against `uvicorn` (127.0.0.1:8018, AI disabled, the disposable database) and `next dev` (127.0.0.1:3018), with a synthetic owner and one phone paired through the real `/api/devices/pair`. Screenshots and `results.txt` are in the session scratch folder, not in the repository.

- 390 and 1280 px, light and dark: Settings shows "Perangkat terhubung" with the paired phone and the manual key section; no horizontal overflow at either width.
- Pairing dialog: canvas 240×240 CSS px with dark modules drawn, accessible name with the code, timer "Berlaku 04:58" two seconds after opening. Escape closes only the pairing dialog, Settings stays open, and focus returns to "Hubungkan ponsel". The revoke dialog behaves the same and returns focus to "Cabut TECNO CN7c".
- Keyboard at 390 px: Enter on "Hubungkan ponsel" opens the dialog; Tab cycles ✕ → "Tutup" → "Buat kode baru" → ✕ without leaving the dialog; the focused control shows the 2 px focus ring; Enter on "Buat kode baru" issues a different code; Escape returns focus to the trigger.
- Console errors: 0.
- Measured text contrast: the lowest ratio in the new section and dialogs is on the shared primary/danger buttons, 3.60:1 in light and 3.34:1 in dark. The existing "Simpan pengaturan" button measures the same. Recorded under "Open decisions for owner" in design.md.

This is not a conformance audit. 200% text, text-spacing overrides and screen-reader passes were not run.

## Not verified

- Scanning the QR code with the companion. The scanner is part of the companion change; the payload format is covered by backend and component tests, and the canvas was checked to render.
- Redis-backed rate limiting. Tests use the in-memory limiter (Redis is disabled in tests); the Redis path is the existing shared limiter code.
- Production. Deployment goes through the pipeline after the owner merges. Revoked keys would authenticate again if the previous code were redeployed (see the migration plan in design.md).
