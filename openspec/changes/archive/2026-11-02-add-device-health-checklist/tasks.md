# Tasks

## 1. Backend (D1–D6)

- [x] 1.1 Add `db/migrations/V42__device_health.sql`: one row per key, `ON DELETE CASCADE`, checks for status, step and time consistency. Evidence: a fresh `cft-dh-db` migrated V1→V42 by flyway/flyway:10 ("Schema version: 42"); `test_v42_is_repeatable_and_enforces_consistency` applies it twice in a scratch schema and checks seven constraint violations, the primary key and the cascade.
- [x] 1.2 Add `get_bearer_device` and `POST /api/devices/health` with strict validation, the per-key rate limit and the upsert. Evidence: tests for a bad key, a revoked key, no header and a session alone (401); wrong types, unknown status, inconsistent self-test, naive timestamps (422); the 31st report (429, stored report unchanged); unknown fields ignored.
- [x] 1.3 Compute `ready` in one place and return it from the POST and `GET /api/devices`. Evidence: a parametrised test over all 24 combinations of access, listener, self-test status and battery.
- [x] 1.4 Add `health` to `GET /api/devices`, `null` before the first report; delete health on revoke. Evidence: tests for `null`, latest-only after two reports, re-pair starting at `null`, revoke, and two owners seeing only their own devices.
- [x] 1.5 Log only key id and codes. Evidence: a caplog test with a synthetic device name and key.

Expected Result: the full backend suite passes on a fresh database migrated V1→V42, and the health endpoint meets every contract error code.

## 2. Pairing dialog (D7–D9, D11)

- [x] 2.1 Add the health type to `api.ts` and a pure `deviceHealth.ts` that derives the checklist, problems and live message. Evidence: unit tests for waiting, each step in progress, each failure, battery advisory, ready.
- [x] 2.2 Show the live checklist in `DevicePairingDialog`, with the hidden polite live region, the battery hint, failed-step text and "HP-mu siap!". Evidence: component tests with axe for waiting, connected, in progress, failed (notification access, background, self-test), battery advisory and ready.
- [x] 2.3 Poll every 3 s for at most 10 minutes; stop when ready and when the dialog closes. Evidence: a fake-timer test counting device list requests before and after readiness, after closing, and after 10 minutes.

Expected Result: with a phone simulated by `curl`, the dialog goes from waiting through each step to ready, and shows a failure, at 390 and 1280 px in light and dark, keyboard only, with no console error.

## 3. Settings (D10)

- [x] 3.1 Show readiness, last report, app version and problems for each paired phone. Evidence: component tests with axe for a ready phone, each problem, a phone silent for over 24 hours and a phone that never reported.

Expected Result: Settings → Perangkat & akses tells the owner which phone needs attention and why.

## 4. Verification and delivery

- [x] 4.1 Run the backend suite on a fresh Flyway-migrated database with `REQUIRE_TEST_DATABASE=1`; run `npm ci`, test, lint, type-check and build. Evidence: backend 1495 passed, 0 skipped, 0 failed (77 in `test_device_health.py`); frontend 365 tests in 60 files passed, lint 0 errors (1 earlier warning in `review/page.tsx`), type-check and build passed.
- [x] 4.2 Browser check in headless Brave on a loopback stack with a synthetic owner and device key. Evidence: at 390 and 1280 px, light and dark, the dialog went waiting → connected → notification → background with battery hint → failed self-test → ready, with axe 0 violations in all 24 states, 0 overflow, the expected live text, 0 device-list requests in 7 s after ready and 4 s after closing, and keyboard open, focus trap, "Selesai" and focus return. Settings showed ready, problem, silent and never-reported phones with axe 0 violations. 0 console errors.
- [x] 4.3 `git diff --check` and `openspec validate --all --strict`. Evidence: both exit 0.
- [x] 4.4 Archive the change, push `feat/device-health-checklist`, watch the "Checks" run and report the compare link. Opening the pull request and merging stay with the owner. Evidence: the archived change on the pushed branch; the Checks result and compare link are in the handoff report.

Expected Result: the branch is pushed with green checks and an archived change.
