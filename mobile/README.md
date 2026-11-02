# Financial Tracker Mobile Listener (Android Companion)

Native Android companion application for **Cash Flow Tracker** designed for **Phase 1: Notification Listener & Notification Dataset Capture**. It lives in the `mobile/` folder of the cash-flow-tracker repository; run every Gradle command below from `mobile/`.

This app operates on your Android phone to automatically capture banking and e-wallet notifications from selected financial applications, persist them locally in Room DB, say on one status hub whether it is working and what needs an answer, and synchronize raw events to your private Cash Flow Tracker backend server. Money is read on the web; the phone shows no totals.

---

## 1. Supported Financial Applications (Privacy Allowlist)

The app strictly inspects notifications originating **only** from the following registered package names:

| Application | Package Identifier | Supported Events |
|---|---|---|
| **myBCA** | `id.co.bca.mybca.omni.android` | QRIS, transfer masuk/keluar, debit |
| **BCA mobile** | `com.bca` | m-Transfer, m-BCA, debet/kredit |
| **Bank Jago** | `com.jago.digitalbanking` | Kantong masuk/keluar, transfer, QRIS |
| **GoPay** | `com.gojek.app` / `com.gopay.wallet` | QRIS bayar, transfer, top up, saldo |

> **Privacy Firewall:** Notifications from any application outside this allowlist (e.g. WhatsApp, SMS, personal messengers) are immediately dropped in memory before any title, body text, or metadata is inspected.

---

## 2. Core Architecture

```
Android Status Bar
       │
       ▼
[FinancialNotificationListenerService]
       │
       ├── Privacy Check: Is package in FinancialSourceRegistry?
       │     └─ No ──► Discard immediately
       │     └─ Yes ─► Extract title, body, bigText, channelId, extras
       │
       ▼
[NotificationProcessor] (one ordered capture queue)
       │
       ├── Classify on the device: DROP (one-time codes, ongoing, empty summaries),
       │   SEND (transactions, settled money events) or KEEP_LOCAL (promos, market news, no amount)
       ├── Compute SHA-256 (packageName + "|" + title + "|" + text + "|" + postTime)
       ├── Persist to local Room DB (raw_notifications): a re-post updates its row,
       │   a cross-app duplicate within 2 minutes is stored as DROP (duplicate_source)
       │
       ▼
[SyncScheduler & NotificationSyncWorker (WorkManager)]
       │
       ├── Batches up to 50 SEND rows; a rejected batch is retried row by row
       ├── KEEP_LOCAL rows stay under "Disimpan di ponsel" for 7 days ("Kirim ke Tracker")
       └── POST /api/ingest/notifications
             Authorization: Bearer cfk_...
       │
       ▼
[NotificationResults] (recorded result with an "Apa ini?" purpose question)
       │
       ├── Urgent questions notify at once; "Apa ini?" waits for the evening reminder (on by default, 20.00)
       ├── Riwayat filter "Perlu dijawab" and the question's options in the question sheet
       └── Answer stored with a reply ID, then POST /api/ingest/notifications/{event_id}/answer-purpose
             (retried with backoff; 409 → closed, 404/422 → failed)
```

**Beranda** is one status hub (direction C, 2.4.0). The status card says "Lagi jagain notifikasimu", how many notifications this phone captured today (a count, never money), the last capture and the sync line, with "Rincian" for the listener figures. When something breaks, the card shows one problem with a one-tap fix: notification access lost, the listener disconnected or frozen by a battery saver (with the HiOS/TECNO Phone Master steps), the tracker unreachable, the key refused, items waiting to send, or an afternoon with no capture (the fix runs the self-test). Below it are one large **Catat transaksi** button, "Laporan siap" when `GET /api/reports/monthly/latest` says the monthly report is ready, the next question answerable in place, and the tiles **Riwayat** (the capture log, per-item amounts and no totals), **Pengaturan** and **Buka web**. Every other screen opens as a sheet over the hub.

Sender, source-pocket and account questions notify at once. Purpose questions ("Apa ini?" and habit guesses) wait for one evening reminder ("Ada 3 pertanyaan ringan hari ini", 20.00 by default, changeable or off in Pengaturan; off means they are prompted at once). A recorded notification's Rincian offers **Perbaiki label** (category, Kakeibo pillar including "Belum diklasifikasi", merchant), sent through `PATCH /api/transactions/{id}` so the tracker learns; full edits are on the web (Buku Kas). The phone reports its health with `POST /api/devices/health` after setup, on open, when the listener changes and every 15 minutes; an older tracker's 404 is ignored quietly.

**Catat** (the large hub button) records a transaction typed as one sentence through the tracker's text-capture API, declaring that it answers `time` questions ("Jam berapa?") as well. The capture is stored in Room (`text_captures`, schema 8) before it is sent, and its `client_capture_id` makes every resend idempotent. The tracker records it, or asks one question at a time (amount, time, kind, account, category); each answer is stored with its `reply_id` before sending. A recorded capture can be undone for 10 seconds, edited with the existing form, and later removed with **Hapus dari tracker** (expense or income created by a capture only, after confirmation). Offline or on a server error the capture waits as "Disimpan di antrean"; with auto-sync on it is resent through WorkManager when the network returns, otherwise **Kirim** sends it. Suggestions under the input come only from `GET /api/capture/suggestions`. Captures appear in Riwayat as "Diketik di ponsel", and waiting captures are counted in the hub's sync line.

---

## 3. How to Run & Build

### Prerequisites
- Android Studio (Koala / Ladybug or newer) with JDK 17+. On the command line, point `JAVA_HOME` at a JDK 17 and `ANDROID_HOME` at the Android SDK (or keep an ignored `local.properties` with `sdk.dir`).
- Android Device (Android 8.0+ / API 26+) or Android Emulator

### Steps to Build & Install
1. Open the repository's `mobile/` folder in **Android Studio**.
2. Allow Android Studio to sync Gradle and index files.
3. Connect your Android device via USB (or start an emulator) and enable USB Debugging.
4. Run the app (`app` configuration) or build the debug APK:
   ```bash
   ./gradlew assembleDebug
   ```
5. Install on connected device:
   ```bash
   adb install -r app/build/outputs/apk/debug/app-debug.apk
   ```

---

## 4. Initial Setup in the App

Setup is a story of cards with five steps, "Langkah n dari 5":
1. **Pair.** In web Settings, choose **Hubungkan ponsel**, then **Scan QR** on the phone (camera access is asked only when scanning), or **Ketik kode aja** with the code under the QR, or **Pakai kunci API**. Codes expire after five minutes and work once. The app stores the new key before testing it; if the test fails, **Tes Koneksi** retries without a new code. There is no URL field: local debug builds use `-PcompanionServerUrl=http://127.0.0.1:18000`, release builds the fixed HTTPS server.
2. **Notification access.** Only the listed banking and e-wallet apps are read; everything else is dropped.
3. **Battery and background**, with this phone's battery-saver steps (HiOS Phone Master, MIUI/HyperOS, ColorOS, One UI).
4. **Notification permission**, for questions and confirmations.
5. **Self-test.** The app posts a marked test notification, its own listener must catch it within 10 seconds, and it reports health so web Settings shows the phone as ready. The test notification is never stored, synced or sent. Each failure has a fix.

Each permission step can be skipped and fixed later from the hub or **Pengaturan** (plain grouped lists: monitored apps, sending, evening reminder, connection with re-pair and the manual key, permissions and battery with **Tes HP-ku**, theme, about). A row the tracker recorded from your habits carries a **Dari kebiasaan** tag in Riwayat.

## 5. Pairing verification

Run `./gradlew testDebugUnitTest lintDebug lintRelease assembleDebug assembleRelease` for the local checks. Unit-test applications inject a loopback transport, so fixture credentials never reach the fixed server. Room is at version 8; the 7→8 migration only adds `text_captures`. 2.4.0 adds no migration. When an existing saved URL matches the fixed build server, the pairing context and fingerprints remain unchanged. A different-server credential is cleared and requires pairing again.

Instrumented tests require the separate verification app id and refuse the normal app id. Set `ANDROID_SERIAL` to the intended test phone. All UI actions stay inside Compose tests. Do not use global coordinate input or launch other apps.

For the backend round trip, start a disposable database, then run the tracked harness with the backend's Python environment from this repository (`..` is the repository root). No real AI or account data is used.

```sh
docker run -d --name companion-pairing-test-db -p 127.0.0.1:5548:5432 \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=cft_pairing_test postgres:16-alpine
../backend/.venv/bin/python scripts/verify/pairing_backend.py \
  --backend-dir .. --migrate-empty-database
```

In another terminal, with `ANDROID_SERIAL` set:

```sh
adb -s "$ANDROID_SERIAL" reverse tcp:18000 tcp:18000
./gradlew -PcompanionVerification=true \
  -PcompanionServerUrl=http://127.0.0.1:18000 connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.localBackend=true \
  -Pandroid.testInstrumentationRunnerArguments.class=com.alfonsusenrico.financialtracker.listener.PairingBackendIntegrationTest,com.alfonsusenrico.financialtracker.listener.PairingActivityTest,com.alfonsusenrico.financialtracker.listener.PairingScreenshotsTest
```

The backend test pairs, authenticates, re-pairs, checks the old key is refused, and revokes the new key. The bundled reader also decodes a synthetic image generated with the web app's `qrcode@1.5.4` settings (240 px, margin 2, error correction M). This proves decoding, not an optical camera scan. The Activity tests verify external-link confirmation and camera open/close. Screenshot tests capture only synthetic app content in both themes at font scale 1 and 2. `DeviceScreenshotTest` walks the hub and its sheets and `HubStatesScreenshotTest` saves every hub state, with `-e deviceScreenshots light|dark`. `SelfTestDeviceTest` runs the setup self-test on the phone with `-e selfTest true`, once notification access and the notification permission are granted to the verification app; its health report is faked.

Stop the harness, remove the task's test container and the reverse mapping, and uninstall only the verification packages when finished. These operations remove synthetic data. Never run these tests against an owner's normal app package.

Camera acceptance after an approved install: create a fresh web QR, open the scanner, confirm the detected name, and check success. If camera access is denied, use manual entry. Physical scanning and actual TalkBack speech remain owner acceptance checks.

## 6. Capture verification

The capture harness serves a synthetic backend on `127.0.0.1:18093` from this repository's backend, against a disposable loopback database whose name ends in `_test`, migrated with Flyway. AI stays disabled; it refuses to start with a configured provider or a database that already has users.

```sh
docker network create cft-p3-net
docker run -d --name cft-p3-db --network cft-p3-net -p 127.0.0.1:55591:5432 \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=cft_capture_test postgres:16
docker run --rm --network cft-p3-net -v "$PWD/../db/migrations:/flyway/sql:ro" flyway/flyway:10 \
  -url=jdbc:postgresql://cft-p3-db:5432/cft_capture_test -user=postgres migrate
../backend/.venv/bin/python scripts/verify/capture_backend.py --backend-dir ..
```

The JVM client test runs against it with `CAPTURE_LOCAL_BACKEND=http://127.0.0.1:18093 ./gradlew testDebugUnitTest --tests '*CaptureLocalBackendTest'`. On the verification phone:

```sh
adb -s "$ANDROID_SERIAL" reverse tcp:18093 tcp:18093
./gradlew -PcompanionVerification=true connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.localBackend=true \
  -Pandroid.testInstrumentationRunnerArguments.class=com.alfonsusenrico.financialtracker.listener.CaptureBackendDeviceTest
```

`CaptureScreenshotTest` saves every sheet state with `-e deviceScreenshots light|dark`. `CaptureUpgradeDeviceTest` checks a 2.2.0 → 2.3.0 install seeded by `SyntheticDeviceSeedTest`, with `-e captureUpgrade <pairing context>`. Remove the reverse mapping, the containers and the network, and uninstall only the verification package afterwards.

## 7. Checks and releases

Pull requests and pushes to `main` that touch `mobile/` run [Android checks](../.github/workflows/android-ci.yml): `testDebugUnitTest` (counts read from the JUnit XML by `scripts/ci/junit_summary.py`), `lintDebug` and `assembleDebug`, unsigned.

A tag `android-vX.Y.Z` on a commit of `main` runs [Android release](../.github/workflows/android-release.yml). It repeats the checks, builds `assembleRelease` with `-PcompanionReleaseVersion=X.Y.Z` in the `android-release` environment, which holds the signing key, and publishes `cashflow-companion.apk` with its `.sha256` as the repository's latest release. The stable download is `https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk`; web Settings offers it as a link and QR code.

- `versionName` is the tag's `X.Y.Z`. `versionCode` is `X * 10000 + Y * 100 + Z`, so `Y` and `Z` stay from 0 to 99. A release refuses a tag that is not above every published `android-v*` release or below the source version in `app/build.gradle.kts`.
- The release app id is `com.alfonsusenrico.financialtracker.listener` with the fixed production server. Debug builds keep the `.debug` (or `.verification`) suffix, the source version and no signing key.
- Gradle signs a release only when `CFT_ANDROID_KEYSTORE`, `ANDROID_KEYSTORE_PASSWORD`, `ANDROID_KEY_ALIAS` and `ANDROID_KEY_PASSWORD` are all set; a local `assembleRelease` without them stays unsigned. A `companionReleaseVersion` build without the key fails.
- Only `android-v*` releases may be marked latest. The [latest guard](../.github/workflows/android-latest-guard.yml) restores the newest companion release if another release takes it.
