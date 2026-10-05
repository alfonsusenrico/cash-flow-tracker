# Sender clarification verification and rollout

Coordinated OpenSpec change: `add-notification-sender-clarification`.
Companion change: `add-sender-notification-replies`.

The backend holds otherwise recordable incoming BCA transfers with an unresolved
masked sender. A confirmed name resumes the original event and becomes exact,
owner/bank/receiving-account scoped memory. “Catat tanpa nama” completes only that
event and does not learn an alias. Account questions precede sender questions.
Conflicting names make future alias lookup ambiguous; accepted events retain their
own decisions. Settings can correct or remove memory without rewriting history.

## Verification on 2026-10-05

| Check | Result |
| --- | --- |
| Backend, disposable PostgreSQL with versioned V1–V25 | 650 passed |
| Backend CI-style preflight without a database | 445 passed, 205 database cases skipped |
| Frontend tests | 148 passed |
| Frontend type-check, lint, production build | Passed |
| Settings Chrome checks | 1440/390/320 px, light/dark, keyboard/focus and 200% text passed; scoped axe checks found zero violations |
| Android unit tests | 44 passed |
| Android lint | Passed; zero errors, 40 dependency/resource/icon/battery/modifier warnings |
| Android build | Verification and normal debug APKs built with JDK 17 and `--no-build-cache` |
| USB Android instrumentation | 36 passed, six local-backend tests skipped in the ordinary suite |
| New local-backend USB tests, run separately | Five passed across the lifecycle, permission-denied and three restart phases |
| Both native changes, strict validation; both diffs | Passed |

Backend coverage includes populated V24 upgrade and repeat startup, ownership,
atomic rollback, concurrent replies and alias conflicts, stale answers, identical
replays after recording, provider failure, archived accounts, account/sender
sequencing, and unchanged financial facts. Android coverage includes populated
Room versions 1/2/3 → 4, durable outbox identity, bounded retries, 401/403/409/422,
pairing suspension/restoration, result correlation, restricted actions and large
text controls.

The USB lifecycle used only fictional records in a separate `.verification` app
on TECNO CN7c, API 36. It verified hold → direct name → one Rp112.590 record,
future exact reuse, no-name without learned memory, dismissal recovery, two
concurrent transfers, account-then-sender, denied notification permission, and
an offline reply retained across process restart and delivered once on reconnect.
The ordinary suite skips the five phase-driven sender tests and the existing
general backend integration test; those skips are not database or lifecycle
coverage. The five new sender tests were explicitly executed separately.

Earlier attempts failed while the phone dozed or the OEM froze background work.
Waking the phone and foregrounding the verification app resolved those test
conditions. Revoking notification permission during instrumentation kills that
app process; the final permission-denied check revoked it before starting a
separate instrumentation run. These earlier attempts are not counted as passes.

API 31+ action authentication and private/public notification payloads were
asserted on API 36. The receiver rejects locked-device replies on every supported
API, including the API 26–30 fallback. A physical API 26–30 device and an actual
secure-lock-screen user interaction were not tested. OEM background restrictions
can still delay WorkManager delivery. Mask collisions cannot establish identity;
exact memory is an owner-confirmed convenience, and conflicting confirmations
prompt again. No paid model benchmark or separate TalkBack acceptance gate ran.

## Repeating the fictional USB checks

Use a disposable loopback PostgreSQL database named `ledger_test`. Apply the
repository's versioned migrations numerically through V25 before starting the
fixture. Do not point this fixture or tests at production.

```sh
PYTHONPATH=backend TEST_DATABASE_URL="$TEST_DATABASE_URL" SKIP_TEST_MIGRATIONS=1 \
  backend/.venv/bin/pytest backend/tests -q
DATABASE_URL="$TEST_DATABASE_URL" backend/.venv/bin/python scripts/verify_sender_backend.py
```

`SKIP_TEST_MIGRATIONS=1` is appropriate only after the disposable database has
already been migrated. The fixture binds loopback port 18000, installs a
deterministic fictional provider, and deletes only its fictional owner on clean
shutdown. It makes no OpenAI calls. Its endpoints additionally authorize the
fixture owner. Interrupted fixture owners are recovered only by the fixture's
username prefix and fictional key hash.

In the companion repository, use JDK 17 and `-PcompanionVerification=true` for
unit/lint/build and `connectedDebugAndroidTest`. For the backend phases, install
the verification app and test APK persistently and invoke
`SenderBackendIntegrationTest#<method>` with `am instrument -w -e localBackend true`:

1. Forward USB `tcp:18000` to `tcp:18000`; run `namedUnknownConcurrentAndSequentialFlowsRecordOnce`.
2. Revoke `POST_NOTIFICATIONS` for `.verification` before instrumentation and
   set its `user-set user-fixed` flags; run `notificationPermissionDeniedUsesDurableAppAnswer`.
   Restore only that app's permission flags and grant afterward.
3. Run `prepareReplyForOfflineRestart`.
4. Remove only the port forward and force-stop only `.verification`; run `verifyOfflineReplyAfterRestart`.
5. Restore the forward, restart `.verification`, and run `resumeOfflineReplyOnRestoredConnection`.

Do not run all phase methods together: the host must change connectivity between
them. Gradle's connected test task uninstalls the verification app afterward, so
do not insert that task between the restart phases. Keep the phone unlocked and
awake. Never inspect or modify the normal app's personal records or credentials.

## Backend-first rollout

Backend branch: `feat/notification-sender-clarification`, based on main `b682460`.
Companion branch: `feat/sender-notification-replies`, based on accepted `3ae2292`;
the companion repository has no remote. Tested implementation commits are backend
`635fcd1` and companion `2ffc420`; subsequent delivery commits update only docs and
task receipts. Final branch heads are recorded in the session handoff.

The companion APK is version **1.5.0 / code 11**, Room v4, normal debug application
ID `com.alfonsusenrico.financialtracker.listener.debug`, with production as its
default server URL. An upgrade retains existing pairing and local rows. Both
reply receivers are non-exported; only the explicit direct-name PendingIntent is
mutable for RemoteInput. Database and pairing preferences remain backup-excluded.

Prepared artifacts live in the companion worktree's
`app/build/outputs/sender-release/`; normal APK SHA-256:
`c85cb45eac3e1c41735d651632daf09fd5c2c369c612a8ae78c41ae7049eeca8`.
A copy of the installed 1.4.3/code 10 APK is retained there for rollback; both APK
signatures were verified and their signing certificates match. Android
may reject a version downgrade; do not uninstall or clear the normal app to force
rollback because that destroys its local data and pairing.

The owner creates and merges the backend PR. The existing deployment workflow
runs on main or manual dispatch, so a topic push does not run it. Remote Actions
could not be queried because `gh` is not installed; no CI success is claimed.
Verify successful
pipeline execution for the exact merged SHA before installing the normal APK
with `adb install -r`. Then check version, listener and data/config preservation
without printing credentials. Production-server access, main pushes, PR creation,
normal-app installation and owner acceptance are not part of the completed checks.
Synchronize/archive the changes only after owner acceptance.

## Deployment and installed upgrade — 2026-10-05

Owner merged the feature as `1797708c97c268d69fa75e0b98088607164d4f85`.
Its tree exactly matches the verified feature head `47a01a2`.
[Deploy Production run 37266842633](https://github.com/alfonsusenrico/cash-flow-tracker/actions/runs/37266842633)
completed successfully for that exact SHA; both Build & Test Preflight and Deploy
to Production Server succeeded. Evidence was read from GitHub's API; no direct
production connection was used. This resolves the earlier missing-`gh` limitation.

After deployment, the prepared normal APK was upgraded with `adb install -r` on
the original TECNO CN7c USB device. Installed version is 1.5.0/code 11; Room moved
from v3 to v4. All 98 existing local rows and 38 alert receipts were retained.
An on-device check returned only counts and booleans: production URL is selected,
the API key is present, and the URL/key/pairing context/device identity match the
pre-upgrade state. No credential values or personal notification contents were
sent to the host. The listener permission is enabled, its service is foreground,
and Android's system process is bound to it. Notification permission was retained.

The rollback APK remains available. Temporary on-device audit code and comparison
metadata were removed. The second connected phone was not changed. Installed
artifact evidence is in the companion's `app/build/outputs/sender-release/installed-release.json`.
Owner testing of a new masked transfer and acceptance/spec synchronization/archive
remain pending; the existing OEM background and older-device coverage limits apply.
