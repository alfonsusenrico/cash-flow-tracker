# companion-releases Specification

## Purpose
Check every change to the Android companion in `mobile/`, and publish it only as a signed, versioned APK from an owner tag, at a stable latest-release download link.

## Requirements

### Requirement: Companion Changes Are Checked
A pull request or a push to `main` that changes `mobile/**` SHALL run the companion's unit tests, `lintDebug` and `assembleDebug` on a free hosted runner, unsigned. The unit-test result SHALL be read from the JUnit XML and SHALL fail when no test ran or any test failed or errored.

#### Scenario: A mobile pull request runs the checks
- **WHEN** a pull request changes a file under `mobile/`
- **THEN** the Android checks run unit tests, lint and the debug build, and report the test, failure, error and skip counts from the XML

#### Scenario: A build that ran no tests fails
- **WHEN** the unit-test step leaves no JUnit XML results
- **THEN** the result step fails instead of passing on the Gradle exit code

### Requirement: Signed Release From a Version Tag
A tag `android-vX.Y.Z` SHALL be the only way to publish the companion. The release SHALL first pass the companion checks on the tagged commit and SHALL refuse a tag whose commit is not on `main`. The published APK SHALL have the application id `com.alfonsusenrico.financialtracker.listener`, the fixed production server, a signature that `apksigner verify` accepts, and a SHA-256 checksum file.

#### Scenario: A complete release
- **WHEN** the owner pushes `android-v2.4.0` on a commit of `main` and the environment holds all four secrets
- **THEN** the workflow publishes `cashflow-companion.apk` and `cashflow-companion.apk.sha256`, verified with `apksigner`, with package `com.alfonsusenrico.financialtracker.listener`, version name 2.4.0 and code 20400

#### Scenario: A tag off main is refused
- **WHEN** a tag `android-v2.5.0` points at a commit that is not on `main`
- **THEN** the release fails before signing

### Requirement: Release Signing Stays in Its Environment
Only the release's signing job SHALL read the signing key, in the `android-release` environment limited to `android-v*` tags. It SHALL fail with a message naming each missing secret (`ANDROID_KEYSTORE_BASE64`, `ANDROID_KEYSTORE_PASSWORD`, `ANDROID_KEY_ALIAS`, `ANDROID_KEY_PASSWORD`) and SHALL NOT print secret values. A Gradle build given a release version SHALL fail without the signing variables.

#### Scenario: A missing secret stops the release
- **WHEN** `ANDROID_KEY_PASSWORD` is absent from the environment
- **THEN** the signing job fails before building and names `ANDROID_KEY_PASSWORD`, and nothing is published

#### Scenario: A versioned build without the key fails
- **WHEN** Gradle builds with `companionReleaseVersion` set but without the signing variables
- **THEN** the build fails, so a published version is never unsigned

### Requirement: Monotonic Companion Versions
A release SHALL take `versionName` from its tag `X.Y.Z` and SHALL set `versionCode = X * 10000 + Y * 100 + Z`, with `Y` and `Z` from 0 to 99. It SHALL refuse a tag that is malformed, whose code is not above every published `android-v*` release, or whose version is below the source version. Debug builds SHALL keep the source version, their `.debug` or `.verification` suffix and no signing key.

#### Scenario: A newer tag is accepted
- **WHEN** `android-v2.4.0` is published and the owner pushes `android-v2.4.1`
- **THEN** the release builds version code 20401

#### Scenario: A repeated or older tag is refused
- **WHEN** `android-v2.4.0` is published and a tag `android-v2.4.0` or `android-v2.3.9` runs
- **THEN** the release fails before signing, because the code is not above 20400

#### Scenario: A malformed tag is refused
- **WHEN** a tag `android-v2.4` or `android-v2.100.0` runs
- **THEN** the release fails and states the `MAJOR.MINOR.PATCH` rule

### Requirement: Stable Latest Download
Each companion release SHALL be marked as the repository's latest release, so `https://github.com/alfonsusenrico/cash-flow-tracker/releases/latest/download/cashflow-companion.apk` serves the newest companion. When another release becomes latest, the repository SHALL restore the newest published `android-v*` release as latest.

#### Scenario: The stable link follows the newest release
- **WHEN** `android-v2.4.1` is published after `android-v2.4.0`
- **THEN** the stable link downloads the 2.4.1 APK

#### Scenario: Another release does not take the link
- **WHEN** a release with any other tag is published and marked latest
- **THEN** the guard marks the newest `android-v*` release latest again
