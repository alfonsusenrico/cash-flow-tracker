# Proposal

## Why

After pairing, the web dialog says only "terhubung". Nobody can tell whether the phone can actually read notifications, keeps listening in the background, or reaches the server. A phone that was frozen by the OEM battery manager, or that lost notification access, fails silently until someone notices missing transactions. The companion redesign makes the phone check itself and report, so the web can show a live checklist during pairing and a readiness status in Settings.

## Approval

On 2026-11-02 the owner chose the companion redesign "C, all four defaults ok". The redesign includes pairing that the phone itself confirms: a self-test, followed by a live web checklist that ends in "HP-mu siap". The phone app direction B (a tracker companion with no financial dashboards), the monorepo and the start of Phase 0 were approved the same day ("ok I approve all you recommend", "Phase 0 starts now"). The request/response contract was fixed by the coordinator on 2026-11-02 and is implemented here unchanged. Recorded 2026-11-02.

## Expected Outcome

- A paired phone sends `POST /api/devices/health` with its own key. The server keeps only the latest report for that device and answers with the stored health and a computed `ready`.
- While the pairing dialog is open, the owner watches the checklist fill in: Terhubung, Izin notifikasi, Jalan di latar belakang, Tes tangkap notifikasi. It ends in "HP-mu siap! Mulai sekarang transaksi tercatat sendiri." A failure names the failed step, and a battery restriction shows a non-blocking hint for HiOS and MIUI phones.
- Settings → Perangkat & akses shows, per phone, whether it is ready, when it last reported, its app version, and any problem: notification access off, listener disconnected, self-test failed or not run, or no report for more than 24 hours.

## What Changes

- New endpoint `POST /api/devices/health`: Bearer only, resolved to the calling key; strict validation (422); 30 reports per minute per key (429); a revoked or unknown key gets 401.
- Flyway `V42__device_health.sql`: a `device_health` table with one row per API key (`ON DELETE CASCADE`), no history and no personal data.
- `GET /api/devices` adds `health` to every device, or `null` when the device has never reported. Revoking a device deletes its health row.
- `DevicePairingDialog` polls `GET /api/devices` every 3 s for at most 10 minutes after a code is issued, stops when the device is ready or the dialog closes, and shows the live checklist with a polite live region.
- `DevicePairingSection` shows readiness, last report time, app version and problems for each phone.
- New dialog and Settings copy uses the casual register the owner chose for the app ("aku/kamu", "HP").

## Non-goals

- The Android side (self-test, reporting schedule, local fix-it guidance). Another Phase 0 job builds it against the same contract in `mobile/`.
- Health history, push alerts or email when a phone goes quiet.
- Rewriting existing formal copy outside the pairing dialog and the device list.
- Production access. All verification is local, on disposable `cft-dh-*` containers with synthetic data. Pushing the branch is authorised. Merging and deployment stay with the owner.

## Capabilities

### New Capabilities

- `device-health`: the health report endpoint, its validation, rate limit and storage, the `ready` rule, health in the device list, the live pairing checklist, and device problems in Settings.

### Modified Capabilities

None. `device-pairing` requirements stay true as written. Its device list keeps every listed field and gains `health` through the new capability.

Standards: `database` (additive migration with rollback), `api-consumers` (a versioned phone contract that ignores unknown fields), `multi-user` (owner scoping) and `personal-data` (codes-only logs, no notification content stored) apply. No project capability is added.

## Impact

- Backend: `db/migrations/V42__device_health.sql`, `backend/app/services/device_health.py` (new), `backend/app/services/device_pairing.py`, `backend/app/routers/devices.py`, `backend/tests/test_device_health.py` (new), `backend/tests/test_device_pairing.py` (device field set).
- Frontend: `frontend/src/lib/api.ts`, `frontend/src/lib/deviceHealth.ts` (new), `frontend/src/components/ui/DeviceHealthChecklist.tsx` (new), `frontend/src/components/ui/DevicePairingDialog.tsx`, `frontend/src/components/ui/DevicePairingSection.tsx`, one `minus-circle` icon in `Icon.tsx`, their tests and `frontend/src/test/deviceHealthFixtures.ts`.
- API: one new endpoint and one additive field. No dependency changes.
