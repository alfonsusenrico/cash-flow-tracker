# Verification: fix-one-time-code-filter

Date: 2026-10-28

## Revision

- Base: `e2903203440df1bb08d55b26bb060a96a6b33e95` on branch `fix/one-time-code-filter`, plus the uncommitted working tree that this change commits (`NotificationProcessor.kt`, `NotificationProcessorTest.kt`, `app/build.gradle.kts`, this change folder).
- Environment: `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`, Android build-tools 37.0.0 for dexdump.

## Task 1.2: new tests against the old regex (expected to fail)

Command: `./gradlew testDebugUnitTest`, with the new tests added and the old `ONE_TIME_CODE` regex still in place.

Result: `BUILD FAILED`, 54 tests completed, 4 failed. `NotificationProcessorTest` result XML: `tests="28" failures="4"`, timestamp `2026-10-28T05:58:35`.

| Test | Old regex | Reason |
| --- | --- | --- |
| `testTransactionWithSafetyFooterIsKept` | failed | bare `PIN`/`OTP` in the footer dropped the transaction |
| `testAmountNearPhraseIsNotACode` | failed | `kode OTP` alone dropped the transfer |
| `testSeparatedCodeIsNotDetected` | failed | `Kode OTP` alone dropped it; the new rule documents that a separated code is not detected |
| `testFooterWithDistantReferenceNumberIsKept` | failed | bare `PIN`/`OTP` in the footer dropped the transaction |
| `testCodeInBodyIsDropped` | passed | keyword match |
| `testCodeBeforePhraseIsDropped` | passed | keyword match |
| `testOneTimeCodeTitleIsDropped` | passed | keyword match |
| `testPinWithCodeIsDropped` | passed | keyword match |

The existing `testOneTimeCodesAreDetected` assertions are unchanged and passed in both runs.

## Task 1.1: new rule

Command: `./gradlew testDebugUnitTest` after replacing `ONE_TIME_CODE` with the D1 title rule and code-proximity rule.

Result: `BUILD SUCCESSFUL`, 54 tests, 0 failures, 0 errors, 0 skipped. All nine suite XML files have the fresh timestamp `2026-10-28T05:59:04`. `NotificationProcessorTest`: `tests="28" failures="0"` (20 existing tests plus 8 new ones).

## Task 2.1: build and inlined version

- `app/build.gradle.kts`: `versionCode = 13`, `versionName = "1.6.1"`.
- Command: `./gradlew clean assembleDebug --no-build-cache` gave `BUILD SUCCESSFUL in 44s`.
- APK: `app/build/outputs/apk/debug/app-debug.apk`, 18,367,803 bytes, SHA-256 `f7750a070fcfc3e66003cda427699ac4cd446a217e8ae19e68265111e7278809`.
- `dexdump -d` across all 13 `classes*.dex` files:

```text
classes11.dex  ApiClient.createService$lambda$0
  002ed2: 1a03 8c00   |0043: const-string v3, "1.6.1" // string@008c
classes11.dex  NotificationProcessor.processNotification
  006854: 1a16 8c00   |00fa: const-string v22, "1.6.1" // string@008c
classes5.dex   BuildConfig.VERSION_NAME
  value : "1.6.1"
```

No `"1.6.0"` string remains in any dex file.

## Task 2.2: OpenSpec

Command: `openspec validate fix-one-time-code-filter --strict` returned `Change 'fix-one-time-code-filter' is valid` with exit status 0.

## Other checks

- `git diff --check`: no whitespace errors.

## Not verified

- On-device install and capture are pending the owner's instruction, after the 1.6.0 pull request is merged.
- Instrumented tests were not run.
- Residual risk, consistent with the design: a 4–8 digit number such as a hotline written without separators within 30 characters of `PIN`/`OTP` in a footer would still be dropped. The Jago hotline in the existing tests is written `1500 746`, so its `1500` token is a 4-digit candidate if a Jago footer ever places it near PIN or OTP. The device sample was not re-checked for this case.
