# Tasks

## 1. Registry and package visibility

- [x] 1.1 Label `com.gojek.app` "Gojek" (description "GoPay di aplikasi Gojek", institution `gopay`). Verified: `RegistryTest.gojekAppIsLabelledGojekAndStaysInTheGoPayInstitution` and `testAppLabels`.
- [x] 1.2 Add `REGISTERED_PACKAGES` and `installedSelectableApps(isInstalled)`. Verified: `RegistryTest` installed-only, empty, all-installed and Jago-spelling tests.
- [x] 1.3 Add `InstalledPackages` over `PackageManager`. Verified: `InstalledPackagesTest` with Robolectric's `ShadowPackageManager`.
- [x] 1.4 Declare the 11 registered packages in a manifest `<queries>` block, without `QUERY_ALL_PACKAGES`. Verified: `InstalledPackagesTest.manifestQueriesNameEveryRegisteredPackage`.

## 2. Screens

- [x] 2.1 Pengaturan lists installed apps only, reads again on `ON_RESUME`, and shows the empty line. Verified: `CompanionScreensUiTest.settingsSayWhenNoFinanceAppIsInstalled` and the existing settings test.
- [x] 2.2 Setup's notification-access step names installed apps only, read in `readPermissions()`, with the same empty line. Verified: `PairingUiTest.listenerStepNamesOnlyTheInstalledFinanceApps` and `listenerStepSaysWhenNoFinanceAppIsInstalled`.

## 3. Capture regression and version

- [x] 3.1 Capture matching is unchanged. Verified: `RegistryTest.captureMatchingDoesNotDependOnWhatIsInstalled` and `InstalledPackagesTest.captureFollowsTheSavedToggleWhateverIsInstalled`; `NotificationGate` and `isTargetFinancialApp` have no diff.
- [x] 3.2 Version 2.4.1 with code 20401 from the release scheme. Verified: `aapt2 dump badging` of the debug APK reads `versionCode='20401' versionName='2.4.1'`, and its merged manifest queries the 11 registered packages.

## 4. Verify

- [x] 4.1 `./gradlew --no-daemon testDebugUnitTest`, `lintDebug` and `assembleDebug` (`--no-build-cache`) as CI runs them. Verified: 64 suites, 642 tests, 0 failures, 0 errors, 6 skipped (the previous change recorded 621; this one adds 21); lint and the debug build exit 0, with no new lint issue in the touched files.
- [x] 4.2 `openspec validate --all --strict` exits 0 in `mobile/` (10 items) and the repository root (69 items).
