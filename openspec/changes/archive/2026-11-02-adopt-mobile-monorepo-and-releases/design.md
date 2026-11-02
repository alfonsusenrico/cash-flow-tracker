## Context

The companion's private repository is at main `35dfbeb` (2.3.4, code 26, Room v9). Its tree has 390 tracked files. Its OpenSpec root holds 9 main specs and 17 archived changes. Its only workflow published unsigned debug APKs. The public repository has no releases or tags. The headquarter repository's `build-android.yml` is the model for signing: an environment restricted to version tags, four secrets, a temporary keystore file, `apksigner verify` and a sha256 file.

## Goals / Non-Goals

Goals: one repository, a privacy-clean import, unsigned checks on every mobile change, signed tag releases at a stable latest link, and a download offer in web Settings.

Non-goals: app behaviour changes, spec reconciliation, minification, and any tag, release, key or environment created by an agent.

## Decisions

### D1. Snapshot import without history

The tree comes from `git archive 35dfbeb`, extracted into `mobile/`. There is no subtree and no merge of its commits, because the history holds device serials, local paths and older personal evidence that the snapshot fixes do not reach. Rejected: `git subtree add` (it imports the history) and a filtered history rewrite (cost and residual risk for no product value).

### D2. What the import leaves out

The import drops:

- `docs/`: old verification logs with a device serial, local paths and APK hashes.
- `.github/workflows/release-apk.yml`: it published unsigned debug builds, and a nested `.github/` never runs anyway.
- `.agents/`: the root already tracks the same generated OpenSpec skills in `.agents/skills` (apply, archive, explore, propose, sync, update), so a second copy adds nothing.
- `research/`, `local.properties`, build outputs and IDE files: they are untracked in the source and were absent from the archive, which was checked after extraction.

`icon.png` (1.1 MB source art) is kept. Its only metadata is an image-generator C2PA provenance manifest, with no personal data.

### D3. Privacy fixes in the imported tree

The approved fixes:

- `CaptureClassifierTest` uses `7.250.000,00` / `7250000L`.
- Serials and local paths are replaced in 11 archived notes, plus the README and one archived proposal. That is 12 files in the `mobile/openspec` archive plus the README. They now say "the owner's phone (TECNO)" or "the test phone (Realme)", and the README opens "the repository's `mobile/` folder".

The full-tree scan found three more items of the same kinds, which are fixed the same way:

- The same real salary figure also appeared, in US separators, in two `NotificationProcessorTest` cases. They now use `7,250,000.00`.
- The owner's full name appeared in two sample-notification comments in `NotificationProcessor.kt`. They now use "Raka Purnama", the synthetic name the backend parser already uses (archived root change `resolve-closeout-follow-ups`, task 4.2).
- `gradle.properties` pinned `org.gradle.java.home` to a Homebrew path. That breaks hosted runners, so it is dropped; `JAVA_HOME` is documented instead.

The ShopeePay sample `Rp7.490.557` stays: it is already public in the backend parser and its tests. Phone model names, "GoPay Tabungan", "PT TEKNOLOGI KARYA", synthetic masked names, "the owner's phone" notes, and the production domain stay as approved.

### D4. `mobile/openspec` stays a second OpenSpec root

Merging into the root is rejected for three reasons:

- `mobile-notification-completion` and `mobile-sender-replies` exist in both roots, with diverged text. The phone copies have 19 and 15 requirements; the root copies have 5 each, an older server-side wording.
- The archived change `2026-10-28-complete-mobile-notification-results` exists in both archives.
- Merged archives would point at different specs of the same name, so they would stop reading correctly.

A second root keeps every archived change byte-identical apart from the privacy fixes, and it validates on its own: `cd mobile && openspec validate --all --strict`. Phone behaviour belongs to `mobile/openspec`, the server contract to the root. Reconciling the duplicates is a follow-up change.

### D5. Android checks

`android-ci.yml` runs on pull requests and on pushes to `main` that touch `mobile/**` or the workflow itself. It is also callable, so the release reuses it.

- Runner and tools: `ubuntu-24.04`, Temurin JDK 17, and the `setup-java` Gradle cache keyed on the Gradle files.
- Steps: `testDebugUnitTest`, then `lintDebug`, then `assembleDebug`, each in its own Gradle run, because one run with every task has run out of memory before. Signing is not involved.
- Results: `mobile/scripts/ci/junit_summary.py` sums the JUnit XML and fails on zero tests, a failure or an error. A Gradle exit code alone once hid a build that ran no tests.
- `deploy.yml` is not touched. Another change adds `paths-ignore: mobile/**` there.

### D6. Release workflow

`android-release.yml` runs on tags `android-v*` as four jobs: `checks` → `version` → `build` → `publish`.

- `checks` reuses the Android checks on the tagged commit.
- `version` validates the tag. It requires the tagged commit to be on `origin/main`, and it computes the version code and refuses it unless it is above every published `android-v*` release.
- `build` runs in the `android-release` environment with `contents: read`. It names each missing secret and stops. It decodes the keystore into `$RUNNER_TEMP` and removes it on exit. It builds `clean assembleRelease --no-build-cache -PcompanionReleaseVersion=X.Y.Z` with no Gradle cache, because a restored compile output once shipped a stale BuildConfig version. It then checks the APK:
  - `apksigner verify`;
  - the `aapt2 dump badging` package name, version code and version name;
  - the production server URL and the version string inside the shipped dex.

  Last, it writes the `.sha256`.
- `publish` has `contents: write` and no secrets. It re-checks the sha256, then runs `gh release create --verify-tag --latest` with `cashflow-companion.apk` and `cashflow-companion.apk.sha256`, and confirms that the latest release is this tag.

`android-latest-guard.yml` runs on `release` events. If the latest release is not an `android-v*` release, it marks the highest-code companion release latest again. Releases created with `GITHUB_TOKEN` do not trigger it, so it cannot loop with the release workflow. Rejected: relying only on `--latest` (a later manual release would take the stable link).

### D7. Gradle signing and versions

`app/build.gradle.kts` reads `CFT_ANDROID_KEYSTORE`, `ANDROID_KEYSTORE_PASSWORD`, `ANDROID_KEY_ALIAS` and `ANDROID_KEY_PASSWORD`.

- When all four are set, the release build type uses that signing config. When none is set, a local `assembleRelease` stays unsigned, as before. A partial set fails and names the missing variables.
- `-PcompanionReleaseVersion=X.Y.Z` sets `versionName = X.Y.Z` and `versionCode = X * 10000 + Y * 100 + Z`, the headquarter rule. MINOR and PATCH must be 0–99, so the codes sort like the versions.
- The tag version must be at least the source version (2.3.4), because the backend gates features on the reported version. A versioned build without the key fails.
- Debug builds keep 2.3.4 / 26, the `.debug` or `.verification` suffix and their server property.
- The release app id has no suffix, and the server stays the fixed `https://finance.alfonsusenrico.com`.
- `isMinifyEnabled` stays false.

The first release code (20304 or higher) is above the old code 26, and the release app id has never been installed.

### D8. Web download offer

`CompanionDownload.tsx` holds the stable URL `https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk`, the release page, and "Android 8.0" (minSdk 26).

- Settings → Perangkat & akses shows "Unduh aplikasi pendamping" first, because a user installs before pairing.
- The pairing dialog shows the same content in a collapsed `<details>` while no phone has paired. The dialog's main QR is the pairing code, and a second visible QR invites the wrong scan.
- The download QR uses the pairing QR's settings (black on white, margin 2, error correction M) and a distinct accessible name.
- If drawing fails, an alert points to the link.

Rejected: a second always-visible QR in the dialog.

## Risks / Trade-offs

- **Lost signing key.** Without the key, no update installs over a released app; users must uninstall, and that loses the phone's local queue. Mitigation: owner backup steps below.
- **Both apps capturing at once.** If the owner's `.debug` app and the release app capture the same notifications, events may be sent twice. Mitigation: the switch steps disable the debug app's notification access before the release app gets it.
- **The first real run is unproven.** The local evidence uses a throwaway key and a simulated version step. The first tag run is the owner's evidence.

## Owner actions (instructions only; agents never run these)

1. **Create the release keystore** on a trusted machine, outside any repository. keytool prompts for the password; for PKCS12, the key password equals the store password.

   ```sh
   keytool -genkeypair -v -keystore cashflow-companion-release.jks -storetype PKCS12 \
     -alias cashflow-companion -keyalg RSA -keysize 4096 -validity 10000 -dname "CN=Cash Flow Companion"
   ```

   Keep two copies of the `.jks` and its password apart, for example in the password manager and on an encrypted offline drive. Never commit it; `mobile/.gitignore` ignores `*.jks` and `*.keystore`. Losing it means no update can install over released apps.

2. **Create the `android-release` environment**, limited to tags `android-v*`. In the web UI: Settings → Environments → New environment `android-release` → Deployment branches and tags → Selected branches and tags → Add rule, type Tag, pattern `android-v*`. Or with the CLI:

   ```sh
   gh api -X PUT repos/alfonsusenrico/cash-flow-tracker/environments/android-release \
     -F 'deployment_branch_policy[protected_branches]=false' -F 'deployment_branch_policy[custom_branch_policies]=true'
   gh api -X POST repos/alfonsusenrico/cash-flow-tracker/environments/android-release/deployment-branch-policies \
     -f name='android-v*' -f type=tag
   ```

   Adding yourself as a required reviewer is optional.

3. **Set the four secrets** without echoing values. `gh secret set` without `--body` prompts with hidden input or reads stdin:

   ```sh
   base64 -i cashflow-companion-release.jks | gh secret set ANDROID_KEYSTORE_BASE64 --env android-release --repo alfonsusenrico/cash-flow-tracker
   gh secret set ANDROID_KEYSTORE_PASSWORD --env android-release --repo alfonsusenrico/cash-flow-tracker
   gh secret set ANDROID_KEY_ALIAS --env android-release --repo alfonsusenrico/cash-flow-tracker
   gh secret set ANDROID_KEY_PASSWORD --env android-release --repo alfonsusenrico/cash-flow-tracker
   ```

4. **Release.** After the merge, push a tag on `main`, for example `git tag android-v2.3.4 origin/main && git push origin android-v2.3.4`. The version must be at least the source version in `mobile/app/build.gradle.kts`, and above every earlier companion release.

5. **Switch the owner's TECNO from the `.debug` app to the release app** (one time). Server data stays: the ledger, mappings and answers live on the server.
   1. In the debug app, answer every pending question in Tanya, and wait until Aktivitas shows nothing waiting or unsent.
   2. In Android Settings → Akses notifikasi, turn off the `.debug` app, so the two apps never capture the same notification.
   3. Install the release app from web Settings → Unduh aplikasi pendamping, after checking the SHA-256 on the release page. Grant notification access and the battery exemption, and set the HiOS per-app background setting again for the new app.
   4. Pair it by QR from web Settings → Hubungkan ponsel.
   5. Later, once the release app has recorded notifications, revoke the old debug key in web Settings (Cabut) and uninstall the `.debug` app. Uninstalling removes only that app's local copy.

6. **Archive the old repository** after this change merges. Archive only; never delete it.

   ```sh
   gh repo archive alfonsusenrico/financial-tracker-mobile-listener
   ```

## Open decisions for owner

1. **Self-hosted servers.** The release APK only talks to the hosted server, yet every server's Settings shows the download. Chosen for now: always show it, because only the hosted server exists. Later this could be hidden or pointed elsewhere by configuration.
2. **Duplicated specs.** Reconcile the duplicated `mobile-notification-completion` and `mobile-sender-replies` specs, server contract versus phone behaviour, in a follow-up change.
3. **The three extra privacy fixes in D3.** They were outside the coordinator's list. They follow the approved fix kinds and the backend's existing synthetic name.
4. **Tag protection.** A tag ruleset could limit who may create `android-v*` tags. Today only the owner can push.

## Migration Plan

No data migration. Rollback: revert the merge. The released APK stays installable, and a later tag publishes a newer one. To stop the stable link, mark an older companion release latest or delete the release; deleting is an owner action.
