## 1. Snapshot import and privacy fixes

- [x] 1.1 Export the companion tree at `35dfbeb` into `mobile/` with `git archive` (no history) and drop `docs/`, `.github/`, `.agents/`. Evidence: 380 files extracted; `research/`, `local.properties`, build outputs, IDE files, `*.jks`, `*.keystore` and `*.apk` absent (find check, verification.md).
- [x] 1.2 Replace the real salary figure in `CaptureClassifierTest` and `NotificationProcessorTest` with `7.250.000,00` / `7,250,000.00`, and the owner's name in two `NotificationProcessor.kt` comments with "Raka Purnama". Evidence: privacy scan without `real-figure` or `owner-name` matches; unit tests pass (5.1).
- [x] 1.3 Replace the serials and local paths in the README and 12 archived OpenSpec notes with generic wording. Evidence: a grep of `mobile/` for both phone serials and the local path prefixes returns nothing.
- [x] 1.4 Drop the machine-specific `org.gradle.java.home`, ignore `*.jks` and `*.keystore` in `mobile/.gitignore`, and strip four trailing spaces inside `NotificationDao.kt` SQL strings. Evidence: the Gradle runs in 5.1 use `JAVA_HOME`; `git diff --check` is clean; Room compiles the queries and the unit tests pass.

Expected Result: `mobile/` holds the current app with no history and no known private values.

## 2. Layout, OpenSpec root and documentation

- [x] 2.1 Keep `mobile/openspec` as a second OpenSpec root (design D4) and record the collisions. Evidence: `cd mobile && openspec validate --all --strict` passes 9 of 9 items.
- [x] 2.2 Update the root `AGENTS.md` (companion map, commands, release path), the root README (download, pairing, mobile checks), `mobile/README.md` (monorepo paths, section 7 Checks and releases) and `openspec/config.yaml`. Evidence: diff review. No `/path/to/backend-repo` or sibling-repository instructions remain.

Expected Result: agents and users find the app, its checks and its release path from the root.

## 3. Android checks and signed releases

- [x] 3.1 Add `.github/workflows/android-ci.yml`: unit tests with XML counts from `mobile/scripts/ci/junit_summary.py`, `lintDebug` and `assembleDebug` on JDK 17 with the Gradle cache. Evidence: actionlint 1.7.7 clean; the summary script reads 594 tests locally.
- [x] 3.2 Add the release `signingConfig` and the tag version rule to `mobile/app/build.gradle.kts`, with debug unchanged. Evidence: the throwaway-key `assembleRelease -PcompanionReleaseVersion=2.4.0` passes `apksigner verify`, and aapt2 shows `com.alfonsusenrico.financialtracker.listener` 20400 / 2.4.0. Negative paths: an unsigned versioned build, a malformed version, MINOR over 99, a version below the source, and a partial key set each fail with their message.
- [x] 3.3 Add `.github/workflows/android-release.yml` (checks, version, signing build in `android-release`, publish as latest) and `android-latest-guard.yml`. Evidence: actionlint clean. The extracted version step, run against a stubbed `gh`, accepts first and higher tags and refuses an equal, an older, a malformed and an off-main tag.

Expected Result: an owner tag publishes a signed APK at the stable latest link, and every mobile change is checked.

## 4. Web download offer

- [x] 4.1 Add `CompanionDownload.tsx` to Settings → Perangkat & akses and, collapsed, to the pairing dialog, with the download icon. Evidence: `CompanionDownload.test.tsx` covers the link, release page, Android 8.0, QR payload and settings, QR failure alert, and the collapsed disclosure. It runs axe with 0 violations in Settings and in the dialog.
- [x] 4.2 Name the pairing QR in the existing `DevicePairingSection` image queries. Evidence: the full frontend suite passes (5.2).

Expected Result: a user can download the signed app from Settings before pairing it.

## 5. Verification and delivery

- [x] 5.1 Mobile: `testDebugUnitTest` (XML counts), `lintDebug`, `assembleDebug`, each in its own run. Evidence: verification.md.
- [x] 5.2 Frontend: `npm ci`, `npm test`, `npm run lint`, `npm run type-check`, `npm run build`. Evidence: verification.md.
- [x] 5.3 `openspec validate --all --strict` in both roots, and `git diff --check`. Evidence: verification.md.
- [x] 5.4 Run the pre-push privacy scan of the whole branch diff against `origin/main`, and push only if it is clean. Evidence: verification.md and the scan output in scratch.

## Workflow follow-up

- The owner creates the keystore, the `android-release` environment and its secrets, and pushes the first tag (design.md, Owner actions).
- The owner switches the TECNO to the release app and archives the old repository after the merge.
- Follow-up change: reconcile the duplicated `mobile-*` specs.
