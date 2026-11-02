# Financial Tracker Mobile Listener (Android Companion)

Native Android companion application for **Cash Flow Tracker** designed for **Phase 1: Notification Listener & Notification Dataset Capture**. It lives in the `mobile/` folder of the cash-flow-tracker repository; run every Gradle command below from `mobile/`.

This app operates on your Android phone to automatically capture banking and e-wallet notifications from selected financial applications, persist them locally in Room DB, show what this phone recorded and what needs an answer (Beranda, Aktivitas, Tanya and Atur), and synchronize raw events to your private Cash Flow Tracker backend server.

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
       ├── One notification per question: two suggestions + "Tulis" (free text), tap opens it in Tanya
       ├── Aktivitas filter "Perlu dijawab" and the question's options in Tanya
       └── Answer stored with a reply ID, then POST /api/ingest/notifications/{event_id}/answer-purpose
             (retried with backoff; 409 → closed, 404/422 → failed)
```

Beranda's **Arus kas tracker** tile shows the tracker's own ledger figures for today from `GET /api/dashboard/cash-flow-summary?period=today`: outflow, inflow, net, and any reimbursement offset, with the period and the server's generation time. The last complete summary is kept in the app preferences for the current pairing only and is labelled as stored until a refresh succeeds. A failed refresh keeps it marked as not updated, a refused key removes it, and re-pairing clears it. It holds the only money amounts on Beranda. **Aktivitas ponsel hari ini** shows counts from this phone only: notifications captured today, those waiting or not yet sent, and typed captures waiting. A count not yet read from Room shows "—", never 0. **Kategori teratas** shows the summary's own `top_categories` (at most three, with their amount, Kakeibo pillar and a bar relative to the largest) with the same period and freshness, because it is cached and refreshed with the summary. The tracker's spending without a category comes as one entry with a null `category_id`; it is shown under the tracker's name, or "Tanpa kategori" when that is blank, with no pillar. A tracker that does not send the field gets no tile, a list that breaks the contract (including two entries without a category) gets none either, and an empty list says "Belum ada pengeluaran pada periode ini". The phone never computes category totals.

**Catat** (the raised button in the bottom bar) records a transaction typed as one sentence through the tracker's text-capture API. The capture is stored in Room (`text_captures`, schema 8) before it is sent, and its `client_capture_id` makes every resend idempotent. The tracker records it, or asks one question at a time (amount, kind, account, category); each answer is stored with its `reply_id` before sending. A recorded capture can be undone for 10 seconds, edited with the existing form, and later removed with **Hapus dari tracker** (expense or income created by a capture only, after confirmation). Offline or on a server error the capture waits as "Disimpan di antrean"; with auto-sync on it is resent through WorkManager when the network returns, otherwise **Kirim** sends it. Suggestions under the input come only from `GET /api/capture/suggestions`. Captures appear in Aktivitas as "Diketik di ponsel", and waiting captures are counted on Beranda.

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

1. **Grant Notification Access:**
   - On first launch, tap **"Buka Akses Notifikasi"** on the onboarding screen (or go to **Atur > Pengaturan Android > Akses notifikasi Android**).
   - In Android Settings > Device & app notifications, toggle **Financial Tracker Capture** to **Allow**.
2. **Battery and background access:**
   - Continue to step 2, exempt the companion from battery optimization, and follow the HiOS note when applicable.
   - Each permission step can be skipped and opened later from Atur.
3. **Connect to the tracker:**
   - In web Settings, choose **Hubungkan ponsel**, then **Pindai QR dari Pengaturan web** on the phone.
   - Camera access is requested only when scanning. Confirm the automatically detected device name with **Hubungkan**.
   - Codes expire after five minutes and work once. A same-device exchange replaces its old key immediately.
   - The app stores the new key before testing authenticated access. If verification fails, use **Tes Koneksi** without scanning the used code again.
   - **Masukkan API key manual** is the fallback. A manual key is checked before it replaces the saved key.
   - The server is fixed by the build. There is no URL field. For local debug builds use `-PcompanionServerUrl=http://127.0.0.1:18000`; release builds always use the fixed HTTPS server.
   - Settings offers **Tes Koneksi** and **Pasangkan ulang**. Old manual keys are removed only after the new key authenticates to the same account. If removal fails, use **Cabut** in web Settings.
4. **Verify Incoming Feeds:**
   - When a transaction occurs in myBCA, BCA mobile, Bank Jago, or GoPay, the notification appears in the **Aktivitas** tab.
   - Tap a row to open **Rincian**: what the tracker recorded, the original notification, and the actions for its state (send, resend, retry, record manually, edit). Questions open in **Tanya**.
   - A row the tracker recorded from your habits (tracker 1.8.0 and later) carries a **Dari kebiasaan** tag, and Rincian says "Dicatat dari kebiasaan: <kategori> · <n dari m jawaban sebelumnya>".


## 5. Pairing verification

Run `./gradlew testDebugUnitTest lintDebug lintRelease assembleDebug assembleRelease` for the local checks. Unit-test applications inject a loopback transport, so fixture credentials never reach the fixed server. Room is at version 8; the 7→8 migration only adds `text_captures`. When an existing saved URL matches the fixed build server, the pairing context and fingerprints remain unchanged. A different-server credential is cleared and requires pairing again.

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

The backend test pairs, authenticates, re-pairs, checks the old key is refused, and revokes the new key. The bundled reader also decodes a synthetic image generated with the web app's `qrcode@1.5.4` settings (240 px, margin 2, error correction M). This proves decoding, not an optical camera scan. The Activity tests verify external-link confirmation and camera open/close. Screenshot tests capture only synthetic app content in both themes at font scale 1 and 2.

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
