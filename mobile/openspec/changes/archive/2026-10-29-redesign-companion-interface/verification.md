# Verification: redesign-companion-interface

Date: 2026-10-29

## Revision

- **Base:** `origin/main` at `8425f68` (1.8.3, code 18, Room 7).
- **Branch:** `feat/companion-interface-redesign`. Results come from the uncommitted working tree that the
  feature commit records.
- **Environment:** JDK 17, local SDK via `ANDROID_HOME`, build-tools 36.0.0, Robolectric 4.14.1 with
  SDK 34, Compose BOM 2024.08.00. Device: the test phone, Realme RMX1921 (Android 15) with the
  `.verification` app id only.
- No production system was contacted. The verification app was paired with a synthetic key and the
  loopback address `http://127.0.0.1:9`, which the network policy refuses, so nothing left the phone.
  Every row on the device and in tests is invented.

## Unit suite

| Run | Tests | Failures | Errors | Skipped | Classes |
| --- | --- | --- | --- | --- | --- |
| Baseline, `8425f68` | 181 | 0 | 0 | 0 | 27 |
| This change | 241 | 0 | 0 | 0 | 32 |

`./gradlew testDebugUnitTest --rerun-tasks --no-build-cache`: exit 0; JUnit XML timestamps
2026-10-29T05:28:29Z to 05:28:35Z. Counts are read from the XML files.

- 60 new tests: `CompanionModelTest` 7, `ContrastTest` 1, `CompanionScreensUiTest` 40 (20 in each
  theme), `CompanionShellUiTest` 7, `LargeTextUiTest` 5.
- Updated, not deleted: `PurposeQuestionUiTest` (4, now against the Tanya controls and "Tulis
  sendiri"), `InboxSendFormUiTest` became `DetailSendFormUiTest` (2, the same two cases on the detail
  page), and `PurposeAnswersTest` expects the 2.0.0 version header.
- `CompanionShellUiTest.aRowSwipedAwayAndRestoredStaysRestored` fails on the 1.8.3 swipe state
  (`expected:<recorded> but was:<dismissed>`) and passes with D11.

## Instrumented UI tests on the Realme

`am instrument` with the 2.0.0 verification app and test APKs: `NotificationStatusUiTest` (10),
`SenderQuestionUiTest` (2), `SourcePocketQuestionUiTest` (6, both themes) and `ActivitySwipeDeleteTest`
(2): **OK (20 tests)**. All four were updated to the new screens (`TransactionEditSheet` and
`InboxScreen` no longer exist); `InboxSwipeDeleteTest` became `ActivitySwipeDeleteTest`. The
backend-integration tests were not run (they need a local backend and skip without it).

`ActivitySwipeDeleteTest.swipeLeftDeletesRowAndUndoRestoresIt` failed on the device before D11: the
log showed the snackbar returning `ActionPerformed`, yet the row was dismissed again.

## Contrast (computed from the tokens by `ContrastTest`)

| Use | Light | Dark | Minimum |
| --- | --- | --- | --- |
| Text on page | 16.27 | 17.50 | 4.5 |
| Text on card or tile | 19.04 | 15.74 | 4.5 |
| Text on option | 14.99 | 13.73 | 4.5 |
| Secondary text on page | 6.09 | 8.70 | 4.5 |
| Secondary text on card | 7.12 | 7.82 | 4.5 |
| Secondary text on option | 5.61 | 6.82 | 4.5 |
| Questions tile, suggested option, tab count | 7.51 | 7.51 | 4.5 |
| Primary button, selected option | 17.03 | 17.50 | 4.5 |
| Selected filter | 16.27 | 17.50 | 4.5 |
| Danger button text on page | 5.62 | 8.85 | 4.5 |
| Danger button text on card | 6.57 | 7.96 | 4.5 |
| Swipe-to-delete label | 6.57 | 8.37 | 4.5 |
| Badge: recorded | 7.47 | 9.41 | 4.5 |
| Badge: review | 8.16 | 9.54 | 4.5 |
| Badge: question | 6.85 | 8.97 | 4.5 |
| Badge: movement | 7.68 | 8.59 | 4.5 |
| Badge: on phone | 7.69 | 9.01 | 4.5 |
| Badge: muted | 6.67 | 7.81 | 4.5 |
| Money in on card | 6.16 | 11.30 | 4.5 |
| Money out on card | 19.04 | 15.74 | 4.5 |
| Money moved on card | 9.56 | 10.54 | 4.5 |
| Connection result on card | 8.87 | 11.58 | 4.5 |
| Accent text on card | 6.49 | 8.47 | 4.5 |
| Snackbar text | 17.03 | 17.50 | 4.5 |
| Snackbar action | 9.16 | 5.81 | 4.5 |
| Field and ghost-button outline on card | 4.01 | 3.70 | 3.0 |
| Field and ghost-button outline on page | 3.43 | 4.11 | 3.0 |
| Switch on, track | 19.04 | 6.81 | 3.0 |
| Switch off, outline and thumb | 4.01 | 3.70 | 3.0 |
| Active tab icon | 6.49 | 8.47 | 3.0 |
| Focus outline | 5.18 | 8.47 | 3.0 |
| Day bar | 19.04 | 2.11 | none (value in text) |
| Today's bar | 2.59 | 6.81 | none (value in text) |
## Build

- `./gradlew clean assembleDebug --no-build-cache`: exit 0. `app-debug.apk` SHA-256
  `9f4fd32727467a7fede0aade08746447b9187749ff59e15f6bbade3b31e6be10`.
- `aapt2 dump badging`: `com.alfonsusenrico.financialtracker.listener.debug`, `versionCode='19'`,
  `versionName='2.0.0'`.
- `dexdump -f`: `BuildConfig.VERSION_CODE` = 19, `VERSION_NAME` = "2.0.0"; no `1.8.3` string in any
  dex.
- The APK carries `res/font/archivo.ttf` (658,596 bytes, SHA-256 `0e094a7d…5053` as in design D1) and
  `assets/licenses/archivo-OFL.txt`.
- `./gradlew compileDebugAndroidTestKotlin -PcompanionVerification`: exit 0.

## Device check (Realme, verification build)

- **Seed:** `SyntheticDeviceSeedTest` (`-e seedSynthetic true`) on a fresh 1.8.3 verification install:
  13 rows covering recorded, movement, income, purpose, sender, source-pocket and mapping questions,
  review, kept, ignored and unsent rows, plus a synthetic pairing, two non-default app toggles and
  auto-sync off.
- **Upgrade 1.8.3 → 2.0.0:** `capture-state` pulled the database and preferences with `run-as` and
  compared digests, never values. Before, after `install -r`, and after the first 2.0.0 launch the
  output was identical: user_version 7, 13 rows with the same core-column digest, statuses unchanged,
  and the same digests for `server_url`, `api_key`, `pairing_context` and `known_pairings`, the same
  `auto_sync_enabled` and app toggles; `theme_mode` absent. This ran on the 2.0.0 build before the last
  UI-only fixes (tile and option semantics, Tanya-tab navigation, large-text bar labels); the final
  build was then installed over it, and the state at the end still had all 13 seeded rows with
  unchanged statuses and the same pairing digests (plus four dismissed rows written by the
  instrumented tests, and a `device_id` created when Atur opened).
- **Screenshots and labels:** `DeviceScreenshotTest` (`-e deviceScreenshots light|dark`) walked every
  screen through the app's own composition and saved 25 screenshots per run, without the status bar,
  with the accessibility labels of each screen: dark at font scale 1.0, light at 1.0 (with listener
  access off, so onboarding and the missing-access banner show), and dark at 2.0. The font scale was
  set with `settings put system font_scale 2.0` and restored to 1.0; it reads 1.0 afterwards.
  Evidence is in the coordinator's scratch folder `phase1-redesign/device/screens/`.
- **TalkBack labels:** `uiautomator dump` failed on this phone ("null root node"): the accessibility
  manager kept the launcher as the active window. The labels were read instead from the app's own
  window through `UiAutomation` with interactive windows, which is the tree TalkBack uses. Tiles,
  feed rows and answer options are single buttons with their spoken summary (for example
  "Pertanyaan, 4 menunggu. Berikutnya: Apa ini? Rp187.500"); titles are headings; icon-only buttons
  read "Kembali", "Hapus dari aplikasi", "Pengaturan", "Pertanyaan sebelumnya/berikutnya"; the
  selected tab and filter report selected. The first read found tile and option labels on a child
  of the clickable node; they were moved onto the clickable node and read again.
- The verification app and its test package were uninstalled at the end.

## OpenSpec and other checks

- `openspec validate redesign-companion-interface --strict`: exit 0.
- `openspec validate --all --strict` before archiving: exit 0, 7 passed, 0 failed.
- `git diff --cached --check`: exit 0. The bundled OFL text keeps its upstream trailing spaces;
  `.gitattributes` exempts `app/src/main/assets/licenses/*.txt` from the whitespace check.
- Room schema unchanged: version 7, no new file under `app/schemas`.
- After `openspec archive redesign-companion-interface --yes`: `openspec validate --all --strict`
  exit 0, 7 passed, 0 failed (the new `companion-interface` spec included).

## Not verified

- A real TalkBack session with speech; labels were read from the accessibility tree only.
- Light theme at font scale 2.0 on the device (covered by `LargeTextUiTest` in light for Tanya,
  Aktivitas; dark on the device).
- The owner's phone. Installing 2.0.0 over the owner's 1.8.3 is for the owner to request.
- Backend-integration instrumented tests (need a local backend).
