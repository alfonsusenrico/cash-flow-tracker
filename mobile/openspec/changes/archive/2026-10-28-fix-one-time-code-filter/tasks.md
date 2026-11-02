## 1. One-time-code rule

- [x] 1.1 Replace `ONE_TIME_CODE` and `isOneTimeCode` with the D1 title and proximity rules in `NotificationProcessor`.
  - Evidence: `./gradlew testDebugUnitTest` (with `JAVA_HOME` set to JDK 17) passes, with fresh result XML timestamps.
- [x] 1.2 Extend `NotificationProcessorTest` with every `notification-capture` scenario. Keep the existing positive and negative cases, and add the separator-code and reference-number cases from the design risks.
  - Evidence: the new tests fail on the old regex and pass on the new one. Record both runs.

Expected Result: codes are still dropped, and footers no longer hide transactions.

## 2. Build and delivery

- [x] 2.1 Bump to versionName 1.6.1 and versionCode 13. Build with `./gradlew clean assembleDebug --no-build-cache`.
  - Evidence: dexdump shows `"1.6.1"` in `NotificationProcessor` and `ApiClient`.
- [x] 2.2 Run `openspec validate fix-one-time-code-filter --strict`.
  - Evidence: no errors.
- [x] 2.3 Write `verification.md` with the revision, commands and counts, and what was not verified (on-device install is pending owner instruction).
  - Evidence: the file is complete.
- [x] 2.4 After owner acceptance: archive the change, push `fix/one-time-code-filter`, and report the compare link against `main`. This happens after the 1.6.0 pull request is merged.
  - Evidence: archive exit status 0, and the pushed branch URL.
