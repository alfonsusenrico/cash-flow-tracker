## Context

`api_keys` (`key_hash`, `key_prefix`, `last_used_at`) authenticates the companion through `get_current_user`, which accepts a Bearer token or a session. `/api/api-key/reset` deletes all of the user's keys. The server URL is fixed (finance.alfonsusenrico.com).

## Decisions

### D1. Pairing code

| Property | Value |
|---|---|
| Format | 10 characters, Crockford base32 (no ambiguous letters), from a CSPRNG |
| Storage | SHA-256 hash only |
| Expiry | 5 minutes |
| Uses | Single use |
| Active codes | At most one per user; creating a new one invalidates the previous one |
| Attempts | Counted; after 5 wrong attempts against a code it is burned |

The QR payload is `cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=XXXXXXXXXX`. The phone checks that `server` equals its built-in server and refuses any other value.

*Rejected:* showing the API key as a QR code. It would be long-lived, visible on screen and reusable, and creating it would wipe the other keys.

### D2. Exchange

`POST /api/devices/pair {code, device_id (1–64), device_name (1–80, the phone's own name)}` works as follows:
- It runs in one transaction that locks the code row.
- An expired, used or unknown code returns 410 `pairing_code_invalid`, with the same response for every case.
- On success the code is marked used, and a new key is created with `created_via='pairing'` and the device name and id. The response is `{api_key, key_prefix, device_name}`. The plain key appears only in that response.
- A second phone with the same `device_id` replaces that device's previous key, so re-pairing does not pile up keys.
- The route is rate-limited at 10 per minute per IP, using the existing limiter.

### D3. Device list and revoke

`GET /api/devices` lists the user's non-revoked keys: id, device name (or "API key manual"), `created_via`, prefix, created and last used.

`DELETE /api/devices/{id}` sets `revoked_at`. `get_current_user` ignores revoked keys, so the next request from that phone gets 401, and the companion shows "Perangkat dicabut, pasangkan ulang". Revoking the current session's own key is allowed.

### D4. Manual key fallback

`/api/api-key/reset` deletes and recreates only keys with `created_via='manual'`. The web section is renamed "API key manual (cadangan)", with help text explaining that QR pairing is preferred.

### D5. Web UI

Settings gets a "Perangkat terhubung" card above the manual key, in the existing visual system:
- **"Hubungkan ponsel"** opens a dialog with the QR code (a canvas from the `qrcode` package, size ≥ 240 px, plus the code shown as text for accessibility), a mm:ss countdown, "Buat kode baru", and a note that the code works once.
- **The list** shows each device with its name, "Dipasangkan" date and "Terakhir aktif", and "Cabut" behind a confirmation dialog.

The dialog has focus management, Escape to close, and an accessible name.

### D6. Implementation details settled during apply (2026-10-29)

- **API contract.** Every response carries `ok`. Errors use `{"ok": false, "detail": {"code": "<code>"}}`.
  - `POST /api/devices/pairing-codes` (session only, CSRF-checked) → `200 {ok, code, expires_at, expires_in_seconds: 300, qr_payload}`. `expires_in_seconds` lets the web countdown run on the browser clock, so clock skew cannot shorten it. Any `Authorization` header → `403 session_required` (the CSRF middleware skips Bearer requests, so a Bearer header beside a session cookie must never mint a code); no session → `401`; an invalid `PAIRING_SERVER_URL` → `503 pairing_configuration_invalid`.
  - `POST /api/devices/pair {code (≤32), device_id (^[A-Za-z0-9._:-]{1,64}$), device_name (1–80 printable after whitespace collapsing)}` → `200 {ok, api_key, key_prefix, device_name}`; `410 pairing_code_invalid` for unknown, malformed, expired, used, superseded and burned codes alike; `422` for invalid device fields (the code is not touched); `429 pairing_rate_limited` with `Retry-After: 60` after 10 requests per minute per client IP. The code is read with Crockford folding (case-insensitive; `-` and spaces ignored; O→0, I/L→1).
  - `GET /api/devices` → `200 {ok, devices: [{id, name, created_via, key_prefix, created_at, last_used_at}]}`, newest first; a manual key is named "API key manual".
  - `DELETE /api/devices/{id}` → `200 {ok}`; `404 device_not_found` for another owner's, unknown or already revoked keys; `422` for a non-UUID id. List and revoke accept a session or a Bearer key, so a phone may revoke its own key.
- **Failed attempts.** The phone sends only the code, so a wrong code cannot be tied to one owner's code. Each rejected exchange therefore counts one failed attempt against every live code; a code reaching 5 is burned. Live codes are locked in id order to rule out deadlocks between concurrent wrong guesses.
- **Server setting.** `PAIRING_SERVER_URL` (default `https://finance.alfonsusenrico.com`) must be an https origin without path, query or credentials; it is wired through config.py, .env.example, docker-compose.yml, scripts/materialize_runtime_env.py and deploy.yml.
- **Web.** The device list shows only paired phones; the manual key keeps its own section. The pairing dialog polls `GET /api/devices` every 3 s while a code is live and confirms "<name> terhubung." when a phone pairs. The shared `Modal` now gives the keyboard (Escape, Tab trap) only to the topmost open dialog, so Escape in the pairing or revoke dialog no longer also closes Settings. The QR is drawn black on white in both themes for scanner contrast. `@types/qrcode` 1.5.6 is added as a dev dependency beside `qrcode` 1.5.4.

*Rejected:* counting failed attempts only per IP (the per-IP limit already does that, and it does not bound guesses against a code from many addresses); accepting a Bearer key beside a session on the code route (bypasses the CSRF origin check).

## Open decisions for owner

- **Code burning is global.** Because wrong guesses count against every live code, anyone who can reach `/pair` can burn a live code with 5 junk requests (at most 10 per minute per IP). Nothing leaks; the owner presses "Buat kode baru". The alternative (no burn, only the IP limit) weakens the brute-force bound the approved design asks for, so the conservative option was kept.
- **Primary button contrast.** The existing primary/danger button colour (#FF334B with white text) measures 3.60:1 in light and 3.34:1 in dark, below 4.5:1 for 12 px text. The new "Hubungkan ponsel", "Buat kode baru" and "Cabut perangkat" buttons use the same shared `Button` variants as "Simpan pengaturan"; changing the token is a design-system decision outside this change.

## Risks / Trade-offs

- [The QR code is photographed by someone else within 5 minutes] → It is single use. The owner sees an unexpected device in the list and revokes it.
- [The phone loses its key] → Re-pair; the same `device_id` replaces the old key.

## Migration Plan

V34 is additive, and existing keys become `manual`. Roll back by reverting the code. The extra columns and table are ignored by the old code, but revoked keys would work again under it, so re-revoke after any re-deploy.
