## Approval

Approved by the owner on 2026-10-29 ("approved, build both"). Owner decisions on 2026-10-29:
- Pair the phone by scanning a QR code from web Settings; typing the API key is only a fallback.
- No Base URL field.
- The device name is detected from the phone, never a placeholder.

## Why

Today the owner copies a long API key from web Settings into the phone. Rotating that key on the web deletes every key, which disconnects the phone. A QR flow with per-device keys is easier and safer, and it lets the owner see and revoke paired phones.

## Expected Outcome

1. In web Settings, "Hubungkan ponsel" shows a QR code that holds a single-use pairing code valid for 5 minutes. A countdown is shown, and the code can be regenerated.
2. The companion scans it and receives its own API key, labelled with the phone's name (for example "TECNO CN7c"). The key is never shown or typed.
3. Web Settings lists "Perangkat terhubung" (name, paired, last seen) with "Cabut" to revoke one device.
4. A manually created key remains available as the fallback. Rotating it no longer disconnects paired phones.

## What Changes

- **Migration V34:**
  - `api_keys` gains `device_name`, `device_id`, `created_via` (`manual` or `pairing`) and `revoked_at`. Existing keys become `manual`.
  - A new `pairing_codes` table holds the code hash, expiry, use time and attempts.
- **Endpoints:**
  - `POST /api/devices/pairing-codes`: session only, CSRF-checked. Creates a code and returns `{code, expires_at, qr_payload}`.
  - `POST /api/devices/pair`: no session. Exchanges `{code, device_id, device_name}` once for a new key. Rate-limited.
  - `GET /api/devices`
  - `DELETE /api/devices/{id}`: revoke.
- **Auth:** revoked keys are rejected.
- **Key reset:** `POST /api/api-key/reset` replaces only `manual` keys.
- **Web Settings:** a "Perangkat terhubung" section with the QR dialog and the device list. The QR code is drawn client-side with the `qrcode` npm package, a new project-local frontend dependency. The existing manual key section stays as "API key manual (cadangan)".

## Non-goals

- The companion scanner UI. It is part of the companion redesign change.
- Multi-user device sharing.
- Expiring device keys automatically.

## Capabilities

### New Capabilities

- `device-pairing`: single-use QR pairing codes, per-device keys, device list and revoke.

### Modified Capabilities

- `web-api-key-management`: rotating replaces only manual keys; the section is renamed as the fallback.

## Impact

- **Backend:** `routers/devices.py`, `services/device_pairing.py`, `services/auth.py` (revoked check), `routers/auth.py` (reset scope), tests.
- **Frontend:** `components/ui/SettingsModal.tsx` (new section) and a `qrcode` dependency in `package.json` / `package-lock.json`, plus tests.
- **Database:** V34. Merge after the capture backend, which owns V33.
- **Security:** single-use codes stored only as hashes; 5-minute expiry; at most 5 exchange attempts per code; IP rate limit on `/pair`; logs carry no code or key.
