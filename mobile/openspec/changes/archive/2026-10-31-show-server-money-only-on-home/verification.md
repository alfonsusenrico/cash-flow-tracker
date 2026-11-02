# Verification

All checks ran on 2026-10-31 in the worktree on branch `feat/server-only-home-money`, from 4a11eb0 with this change applied.

## Automated checks
- **Baseline** on 4a11eb0, `./gradlew testDebugUnitTest --rerun-tasks`, counted from the JUnit XML: 415 tests, 0 failures, 0 errors, 6 skipped (the `CaptureLocalBackendTest` cases, which need a local backend).
- **After**, same command and count: 439 tests, 0 failures, 0 errors, 6 skipped (the same six).
  - Added: 24 `HomeActivityUiTest` cases (no local money amount, counts tile opening Aktivitas, unread counts as "—", zero today, server tile unchanged, no phone-computed categories; each in light and dark at font scale 1.0 and 2.0).
  - Replaced in place, same number of cases: `CompanionModelTest` (2), `CaptureModelTest` (1) and `CompanionScreensUiTest.homeShowsTheServerTotalAndNoLocalNotificationTotal` (2). The other Beranda cases in `CompanionScreensUiTest` and `LargeTextUiTest` were updated to the activity tile.
  - `ContrastTest` passes with the two bar pairs removed; every remaining pair still meets its minimum in both themes.
- `./gradlew lintDebug`: exit 0, no issue in a file this change touched.
- `./gradlew compileDebugAndroidTestKotlin -PcompanionVerification`: exit 0.
- `./gradlew clean assembleDebug --no-build-cache`: exit 0. `aapt2 dump badging`: `com.alfonsusenrico.financialtracker.listener.debug`, versionCode 23, versionName 2.3.1. `dexdump` of `BuildConfig`: `VERSION_CODE` 23, `VERSION_NAME` "2.3.1", and `<clinit>` loads `const-string "https://finance.alfonsusenrico.com"` for `SERVER_URL`.

## Device checks (the test phone, Realme RMX1921, Android 15)
The phone was awake and unlocked (`dumpsys power`, `dumpsys window`) and had no companion installed. Only the verification build (`-PcompanionVerification`, `.verification`) and its test APK were installed, and every step ran as an instrumented test through `am instrument`. No input event, `monkey`, BACK or HOME was sent. The TECNO was not touched.

- `SyntheticDeviceSeedTest` (`-e seedSynthetic true`): 1 test passed. Synthetic rows only; the test application's endpoint is a loopback address, so nothing left the phone.
- `DeviceScreenshotTest` light at font 1.0, first attempt: failed before any screenshot ("No compose hierarchies found"). On Android 15 the activity had asked for the notification permission and Android's permission dialog was in front. `pm grant` gave the verification app `POST_NOTIFICATIONS` and `am force-stop` closed it; the launcher was then in front.
- `DeviceScreenshotTest` then passed four times: light and dark at font 1.0, dark and light at font 2.0 (`settings put system font_scale 2.0`, restored to 1.0 and read back as 1.0). Beranda's labels show "Aktivitas ponsel hari ini, 9 notifikasi tertangkap. Menunggu 3 notifikasi. Dari notifikasi dan catatan di ponsel ini", the tracker tile "Tidak terhubung ke tracker." with no amount, and no local money amount or categories tile. The only amount on Beranda is the next question's own "Apa ini? Rp187.500".
- `LedgerSummaryDeviceTest`: 2 tests passed (the server amount is the only money and the local row is counted; server amounts and the stale state after a failed refresh).
- Both verification packages were uninstalled; `pm list packages` shows no companion package afterwards.
- Evidence: the coordinator's scratch folder `home-money/device/screens/{light,dark}-font{1.0,2.0}/` (`02-home.png`, `03-home-bottom.png` and `labels.txt` for Beranda) and `home-money/device/adb-commands.log`.

## Observed in the first device run
- At font scale 2.0 the bottom bar's labels were clipped ("Berand", "Aktivit"). This predated the change and is fixed in the review fixes below.

## Coordinator review fixes (2026-10-31, same branch, still 2.3.1/23)
- `./gradlew testDebugUnitTest --rerun-tasks`, counted from the JUnit XML: 452 tests, 0 failures, 0 errors, 6 skipped (the same six `CaptureLocalBackendTest` cases). Added since the first run: 4 `BottomBarUiTest`, 8 `HomeActivityUiTest` (2 tests × light/dark × font 1.0/2.0) and 1 `CompanionModelTest`.
- `BottomBarUiTest` runs with Robolectric native graphics so the bundled Archivo font is measured as on a phone; the legacy mode reports about 1 px per character. Against the previous fixed-size label it fails with "Beranda ends on its line expected:<7> but was:<4>".
- `./gradlew lintDebug`: exit 0, no issue in a touched file. `compileDebugAndroidTestKotlin -PcompanionVerification`: exit 0.
- `./gradlew clean assembleDebug --no-build-cache`: exit 0; `aapt2` versionCode 23, versionName 2.3.1; `dexdump` of `BuildConfig`: `VERSION_CODE` 23, `VERSION_NAME` "2.3.1", `const-string "https://finance.alfonsusenrico.com"`.
- Device: not run. `adb devices -l` listed the Realme, but `dumpsys power` reported `mWakefulness=Dozing` and `dumpsys window` `isKeyguardShowing=true` at two checks around 12.20 WIB. Nothing was installed, no setting was changed, and no input was sent; no verification package was present (`pm list packages`). The TECNO was not touched. The 2.0 screenshots of the bottom bar therefore remain to be taken.
- `openspec validate --all --strict`: exit 0, 9 passed, 0 failed, after adding the "Bottom bar at large text" scenario and naming the tiles in "Counts not yet read" in the main spec and this archived delta.

## OpenSpec
- `openspec validate --all --strict` before archiving: exit 0, 10 passed, 0 failed (after shortening one requirement the first run flagged as over 500 characters).
- After `openspec archive --yes`, the merged `companion-interface` spec flagged the modified home-figures requirement as over 500 characters; its text was shortened in the main spec and in this archived delta, with the same rules and scenarios. `openspec validate --all --strict` then exited 0, 9 passed, 0 failed.
