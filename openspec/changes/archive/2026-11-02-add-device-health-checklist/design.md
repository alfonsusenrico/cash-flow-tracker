# Design

## Context

Pairing (`device-pairing`) gives each phone its own revocable row in `api_keys`. The phone exchanges a single-use code for a key, and the web dialog polls `GET /api/devices` every 3 s until a pairing key newer than the code appears. Nothing tells the server whether the phone can actually capture. The coordinator fixed the health contract on 2026-11-02 (endpoint, body, `ready` rule, errors, dialog steps and Settings problems), and the Android job builds against it in parallel. The work is on `feat/device-health-checklist` from origin/main e05cde9. The visual reference is the pairing card of the onboarding prototype, option C (`option-c.html?step=pair`).

## Goals / Non-Goals

Goals: implement the contract exactly; keep one report per device; make the dialog and Settings honest about what the phone said and when.

Non-goals: the Android self-test, any health history or alerting, and copy rewrites outside the dialog and device rows.

## Decisions

### 1. A `device_health` table keyed by the API key

`V42__device_health.sql` creates `device_health (api_key_id UUID PRIMARY KEY REFERENCES api_keys(id) ON DELETE CASCADE, ...)`. It holds one row per key, typed columns for each contract field, and `CHECK` constraints that mirror the validation: status values, `failed_step` present only for `failed`, `self_test_at` present for `passed` and `failed`, `queue_pending >= 0`. A report is an `INSERT ... ON CONFLICT (api_key_id) DO UPDATE`, so only the latest survives. Owner scoping comes from the join to `api_keys.user_id`.

- Re-pairing deletes the old key row, so the cascade drops its health and the new key starts at `null`.
- Revoke keeps the key row (`revoked_at`) for audit, so `revoke_device` deletes the health row in the same transaction. A revoked device's last status is not personal history worth keeping.

Rejected: columns on `api_keys` (they would widen a credential table that every Bearer request reads, and manual keys would carry a dozen null columns); a history table (the contract says latest only).

Rollback: the migration is additive. Old code ignores the table; `DROP TABLE device_health` removes it with no other dependency.

### 2. A Bearer-only dependency that returns the key

`get_current_user` prefers the session and does not expose which key was used, so the router gets its own dependency, `get_bearer_device`. It requires `Authorization: Bearer <key>`, looks up a live key by hash, touches `last_used_at` as other Bearer calls do, and returns `{key_id, user_id}`. No header, an empty token, an unknown key or a revoked key give 401 `{"code": "device_key_invalid"}`. A session cookie is ignored. The CSRF origin check already skips requests that carry an Authorization header.

Manual keys are accepted too. They are live keys in the same device list, and refusing them would make the old companion's reports fail for no safety gain. Settings shows only paired phones, so a manual key's health is visible only through the API.

### 3. Strict validation at the boundary

The body model uses strict booleans and integers, so `"true"` and `1` are 422 rather than coerced. `app_version` matches `^[0-9A-Za-z.+_-]{1,32}$`. Timestamps must be ISO-8601 strings with a zone; numbers and naive times are refused. Self-test consistency: `failed` needs a `failed_step`; `passed` and `not_run` need it null; `passed` and `failed` need `at`. Unknown fields are ignored (pydantic's default), so a newer phone can add fields without breaking older servers. 422 responses use FastAPI's standard validation body (`detail` lists the failing fields).

### 4. `ready` is computed, not stored

`ready = notification_access and listener_connected and self_test_status == "passed"` is computed when a row is read, in one function shared by the POST response and the device list. Storing it would let a rule change leave stale values in the table.

### 5. Rate limit after authentication, per key

`rate_limiter.exceeded(f"device-health:key:{key_id}", 30, 60)` runs after the key resolves, so the bucket belongs to the device rather than to a shared IP. A bad key gets 401 before it can consume a bucket. Over the limit: 429 `{"code": "device_health_rate_limited"}` with `Retry-After: 60`, and nothing is written. A key is 256 bits of randomness, so guessing is not an IP-limit concern.

### 6. Logs carry ids and codes only

One info line per accepted report: `Device health reported key_id=… ready=… self_test=… failed_step=…`. Device names, keys, the app version and timestamps are not logged. 401 and 422 are not logged by this endpoint.

### 7. Dialog: what each step shows

The checklist is derived from the newest device list, not from local events, so a reload or a missed poll shows the same thing.

- Terhubung: done once a pairing key newer than the code exists; until then every step is "Belum" and the header says "Menunggu HP…".
- Each later step is done when its value is true (Tes tangkap: `passed`).
- A step is failed only when its value is false and the self-test has already run (`passed` or `failed`), or, for the test step, when the status is `failed`. Before the self-test runs, a false value means the owner is still granting it on the phone, so the first open step shows "Lagi diatur di HP" and later steps "Belum". This mirrors the prototype's states without alarming the owner halfway through setup.
- Health `null` after pairing means the phone has not reported yet: the next step is "Lagi diatur di HP".
- `battery_unrestricted === false` adds the HiOS/MIUI hint under the background step, in amber, whether or not the phone is ready.
- A failed self-test names its step: post → "HP nggak bisa munculin notifikasi tes.", capture → "Notifikasi tes nggak ketangkep sama aplikasinya.", server → "HP belum bisa ngirim ke server."

States map to existing tokens: done = income soft and text, failed = expense soft and `--danger-text`, in progress = warning soft with text ink, not yet = `--bg` with muted ink. Badge text is `--text` on the soft fills, so contrast does not depend on the hue.

### 8. Dialog: polling window and stop conditions

`refetchInterval` returns 3000 ms while the dialog is mounted, a code has been issued, less than 10 minutes have passed since it was issued (local clock), and the paired device is not ready; otherwise `false`. Unmounting the dialog unmounts its query observer, which stops its interval. The Settings list shares the query key but has no interval. If 10 minutes pass without readiness, the dialog says so and points to Settings. The 5-minute code countdown is unchanged; once a phone has paired, the QR is hidden and only the checklist remains.

### 9. Live region

One visually hidden `role="status"` `aria-live="polite"` element holds a sentence for the latest change (for example "Izin notifikasi beres."). It is derived from the checklist, so it changes only when the checklist changes. Pending, expired and error messages use the same region. The visible headline is not live, so nothing is announced twice.

### 10. Settings row

Each paired phone row adds a readiness badge ("Siap" or "Perlu dicek"), "Lapor terakhir <time>", the app version, and a problem list. A ready phone whose last report is older than 24 hours is "Perlu dicek". Self-test `not_run` is listed as a problem ("Tes tangkap notifikasi belum dijalankan"), because otherwise a not-ready phone would show no reason. A phone that never reported shows "Belum pernah kirim status. Buka aplikasinya di HP, ya." and no badge.

### 11. Copy register

New dialog and Settings strings follow the onboarding prototype's "Nada & bahasa": "aku/kamu", "HP", short sentences. Existing button names and headings that the `device-pairing` spec names ("Hubungkan ponsel", "Buat kode baru", "Cabut", "Perangkat terhubung") are kept unchanged.

## Risks / Trade-offs

- The phone's clock sets `self_test.at` and `last_capture_at`. They are shown as reported, while staleness uses the server's `reported_at`.
- A phone that stops reporting keeps its last "ready" until the 24-hour rule flags it. That is the contract's chosen threshold.
- Polling every 3 s for up to 10 minutes is at most 200 small requests per pairing.

## Migration Plan

Deploy runs V42 through Flyway with the release. The endpoint returns 401/422/429/200 from the first release; phones on older versions simply never report, and their `health` stays `null`. Rollback: redeploy the previous release; the table is ignored. Drop it only if the feature is abandoned.

## Open decisions for owner

- **Migration number.** V42 is the next free number on origin/main e05cde9. If another Phase 0 branch also adds a V42 and merges first, this file must be renumbered before merge.
- **Manual keys may report health (D2).** Conservative in the sense that nothing breaks; say if reports should be limited to paired phones.
- **Strict self-test consistency (D3).** A `failed` report without `failed_step`, or a `passed` one with it, is refused. The Android job builds against the same contract, but this is the place to relax if its reports differ.
