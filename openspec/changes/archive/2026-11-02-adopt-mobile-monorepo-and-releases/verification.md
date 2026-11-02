# Verification: adopt-mobile-monorepo-and-releases

Date: 2026-11-02. Tree: `origin/main` 5004b11 plus this branch's working tree (the committed content). Machine: macOS, JDK 17 (Homebrew Temurin-compatible OpenJDK), Android SDK build-tools 37.0.0, Node 26.9.0 locally (CI uses Node 22).

## Import

- `git archive 35dfbeb | tar -x -C mobile` extracted 380 files, and `docs/`, `.github/` and `.agents/` were removed. A find for `local.properties`, `build/`, `.idea`, `research`, `*.jks`, `*.keystore` and `*.apk` returned nothing.
- Baseline before any edit: `testDebugUnitTest` gave 58 suites, 594 tests, 0 failures, 0 errors, 6 skipped.

## Mobile checks (final tree)

Run from `mobile/`, one Gradle task per run.

| Check | Result |
| --- | --- |
| `testDebugUnitTest --rerun-tasks` | exit 0. From the XML: 58 suites, 594 tests, 0 failures, 0 errors, 6 skipped |
| `lintDebug` | exit 0. 49 warnings, 0 errors, all in imported app code |
| `assembleDebug` | exit 0 |

## Signed release build (synthetic key)

A throwaway PKCS12 key (alias `synthetic`, RSA 3072, valid 2 days, random password never printed) was created in the scratch folder, used once, then deleted. It was never inside the repository and never committed.

- `./gradlew --no-daemon --no-build-cache clean assembleRelease -PcompanionReleaseVersion=2.4.0` with the four signing variables: exit 0.
- The workflow's own verification commands ran on the APK:
  - `apksigner verify --verbose`: Verifies, v2 scheme, 1 signer.
  - `aapt2 dump badging`: `package: name='com.alfonsusenrico.financialtracker.listener' versionCode='20400' versionName='2.4.0'`, targetSdk 35.
  - The dex contains `https://finance.alfonsusenrico.com` and `2.4.0`.
  - The sha256 file was written.
- Gradle guards, each failing with its message:
  - a versioned build without the key;
  - `2.4` and `2.100.0` (malformed);
  - `2.3.3` (below the source 2.3.4);
  - a partial signing set (alias only).

  A build with no properties configures normally.
- The release workflow's version step was extracted and run against a stubbed `gh` and a local Git origin:
  - first tag `android-v2.4.0`: exit 0, code 20400;
  - `android-v2.4.1` above a published 2.4.0: exit 0, code 20401;
  - equal tag, older tag `android-v2.3.9`, malformed tag `android-v2.4`, and a tag off main: each exit 1.

## Workflows

`rhysd/actionlint:1.7.7` (Docker, with shellcheck) on `android-ci.yml`, `android-release.yml` and `android-latest-guard.yml`: exit 0. The one SC2012 note was fixed. `deploy.yml` is unchanged.

## Frontend

| Check | Result |
| --- | --- |
| `npm ci` | exit 0 |
| `npm test` | exit 0. 57 files, 311 tests passed, including `CompanionDownload.test.tsx` (3 tests, axe 0 violations in Settings and the dialog) |
| `npm run lint` | exit 0. 0 errors, 1 warning in `src/app/review/page.tsx`, outside this change |
| `npm run type-check` | exit 0 |
| `npm run build` | exit 0 |

The generated `frontend/next-env.d.ts` that the build rewrites was restored.

## Specs and diff

- `openspec validate --all --strict`: root and `mobile/` both exit 0 (results in the final report).
- `git diff --check` against `origin/main`: clean.

## Privacy gate

The whole branch diff against `origin/main` was scanned for:

- the owner's names (with the handle removed);
- both serials;
- `/Users/`, `/private/tmp` and `100.x.x.x` addresses;
- emails and `+62`/`08` phone numbers;
- 10–16 digit runs outside `app/schemas`;
- non-synthetic `cfk_` keys;
- private keys, keystore files and signing passwords;
- the four real figures.

It also scanned changed file names and the printable strings of binary files. The final output is in the delivery report.

## Not verified

- No browser check (axe ran in jsdom only). Contrast, zoom and reflow of the new subsection were not measured in a browser, and no screenshots were taken.
- No real GitHub run: there is no tag, no environment and no secrets yet. The first `android-v*` run is owner evidence.
- No device install. The TECNO was not touched, and the Realme was not used.
- `lintRelease` was not run.
