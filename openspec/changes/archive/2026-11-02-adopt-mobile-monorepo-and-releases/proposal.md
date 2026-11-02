## Why

The Android companion lives in a separate private repository, ships unsigned debug builds, and has no download path for new users. The project is now public and multi-user, so the app has to be in the same repository as the server it talks to, and users need a signed, verifiable APK at a stable link.

## What Changes

- Import the companion's current tree from the private repository (main `35dfbeb`, 2.3.4, code 26, Room v9) into `mobile/` as a snapshot with no Git history. Leave out `docs/`, the old `release-apk.yml`, `.agents/`, `research/`, `local.properties`, build outputs and IDE files.
- Apply privacy fixes to the imported tree: replace the owner's real salary figure in two tests, the owner's name in two parser comments, phone serials in 11 archived notes, and local paths in the README and one archived proposal.
- Keep `mobile/openspec` as a second OpenSpec root, because two capability names and one archived change name collide with the root.
- Add Android checks on pull requests and pushes that touch `mobile/**`: unit tests with counts read from the JUnit XML, `lintDebug` and `assembleDebug`.
- Add a signed release workflow for tags `android-vX.Y.Z` that publishes `cashflow-companion.apk` and its `.sha256` as the repository's latest release. Add a guard that keeps "latest" on a companion release.
- Add a release `signingConfig` read from environment variables. `versionName` comes from the tag and `versionCode = X * 10000 + Y * 100 + Z`. Debug builds are unchanged.
- Offer "Unduh aplikasi pendamping" in web Settings → Perangkat & akses and in the pairing dialog: a QR code and link for the stable download URL, a SHA-256 check note, and the minimum Android version.
- Update the root `AGENTS.md`, the READMEs and the OpenSpec context for `mobile/`.

## Capabilities

### New Capabilities

- `companion-releases`: how the companion is checked, versioned, signed and published, and the stable latest download.

### Modified Capabilities

- `device-pairing`: Settings and the pairing dialog offer the companion download (an added requirement; existing requirements are unchanged).

## Impact

- New: `mobile/` (380 imported files, minus exclusions, plus `mobile/scripts/ci/junit_summary.py`), `.github/workflows/android-ci.yml`, `android-release.yml`, `android-latest-guard.yml`, `frontend/src/components/ui/CompanionDownload.tsx` and its test.
- Changed: `mobile/app/build.gradle.kts` (signing and version), `mobile/gradle.properties` (drops a machine-specific `org.gradle.java.home`), `DevicesSettings.tsx`, `DevicePairingDialog.tsx`, `Icon.tsx` (download icon), `DevicePairingSection.test.tsx` (names the pairing QR in its queries), `AGENTS.md`, `README.md`, `openspec/config.yaml`.
- Not changed: `.github/workflows/deploy.yml`, which another change edits. The backend, database, API and ledger behaviour do not change. No migration.
- The release app id is `com.alfonsusenrico.financialtracker.listener`, with no suffix and the fixed production server. Installing it beside the owner's `.debug` app is a one-time owner switch (design.md).

## Expected Outcome

A user opens web Settings, scans "Unduh aplikasi pendamping", installs the signed APK after checking its SHA-256, and pairs it by QR. The owner pushes `android-vX.Y.Z` on `main`, and the link `releases/latest/download/cashflow-companion.apk` serves that version.

## Non-goals

- No tags, releases, signing keys or GitHub environment changes by agents. The owner creates the keystore, the environment and the secrets, and pushes the first tag.
- No production access. Local verification (a throwaway keystore build, unit tests and lint) proves the build and the workflow logic only. The first real release run is owner evidence.
- No behaviour change in the companion app, and no reconciliation of the colliding `mobile-*` specs (recorded follow-up).
- No minification (`isMinifyEnabled` stays false; it is untested).

## Standards and follow-ups

This adds release delivery for a new artifact. Capabilities in `AGENTS.md` are unchanged: `deployed` already covers releases. Follow-ups: reconcile the duplicated `mobile-*` specs; decide whether self-hosted servers get their own download (design.md, open decisions).

## Approval

- 2026-11-02, owner: "1 monorepo". The Android app moves into cash-flow-tracker (`mobile/`).
- 2026-11-02, owner: Option A. Import the current code only, with no history.
- 2026-11-02, owner, on the coordinator's scan findings: "confirm the fixes, PT TEKNOLOGI KARYA is made up". Publishing is approved only with the listed fixes applied and a clean pre-push scan.
- 2026-11-02, owner: "ok I approve all you recommend" (the audit recommendations), phone direction B (a tracker companion with no financial dashboards), personal book only, and "Phase 0 starts now".
