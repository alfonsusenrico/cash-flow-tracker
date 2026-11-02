# Tasks

## 1. Contract and baseline
- [x] 1.1 Record the approval, the baseline and the backend contract. Evidence: proposal.md `## Approval`; 415 unit tests, 0 failed, 6 skipped on 4a11eb0 from the JUnit XML; design.md decision 4 citing cash-flow-tracker 02f4468.

Expected Result: the change starts from a known passing baseline and a checked server contract.

## 2. Beranda
- [x] 2.1 Replace the local money model with counts and a loaded state for rows and captures. Evidence: `CompanionModelTest.homeFiguresCountTodaysRowsOfThisPhoneWithoutMoney`, `CompanionModelTest.anEmptyPhoneCountsZeroButUnreadRowsAreUnknown`, `CaptureModelTest.homeFiguresCountWaitingCapturesButNeverTheirMoney`.
- [x] 2.2 Remove "Keluar tercatat hari ini", the three-day strip and "Kategori teratas"; add "Aktivitas ponsel hari ini"; keep the server tile unchanged. Evidence: `HomeActivityUiTest` (6 cases × light/dark × font 1.0/2.0), `CompanionScreensUiTest` home cases, `LargeTextUiTest.homeStacksItsTilesAndKeepsThemReachable`.
- [x] 2.3 Remove the bar tokens and formatting only the removed tiles used; update the device tests, README and version 2.3.1/23. Evidence: `ContrastTest`, `compileDebugAndroidTestKotlin -PcompanionVerification`, dexdump of the clean APK.

Expected Result: Beranda's only money is the tracker's, and this phone's activity is shown as truthful counts in both themes and at font scale 2.0.

## 3. Verification and delivery
- [x] 3.1 Full unit suite, lint and clean APK. Evidence: verification.md.
- [x] 3.2 Device checks on the Realme with the verification build only: `DeviceScreenshotTest` light and dark at font 1.0 and 2.0, `LedgerSummaryDeviceTest`, font scale restored, apps uninstalled. Evidence: verification.md.
- [x] 3.3 OpenSpec strict validation, archive, commit and push. Evidence: `openspec validate --all --strict` exit 0.

Expected Result: a reviewable branch with recorded evidence and the change archived.

## 4. Coordinator review fixes (2026-10-31)
- [x] 4.1 Show "—" ("belum dimuat") on "Perlu cek" and "Di ponsel", and "Belum dimuat" on "Pertanyaan", until Room has answered. Evidence: `HomeActivityUiTest.everyCountTileIsUnknownUntilRoomAnswers`, `HomeActivityUiTest.captureCountsWaitForTheirOwnRead` (light/dark × font 1.0/2.0), `CompanionModelTest.tileCountsAreUnknownUntilRowsAndCapturesAreRead`.
- [x] 4.2 Fit every bottom-bar label at font scale 2.0 without cutting, keeping 48 dp targets, the centred Catat button, icons and full TalkBack names. Evidence: `BottomBarUiTest` (native graphics, light/dark × 360 dp and 320 dp: one line, line end at the last letter, no ellipsis, intrinsic width within the laid-out width); the same test fails against the previous fixed-size label ("Beranda ends on its line expected:<7> but was:<4>").
- [ ] 4.3 Re-run `DeviceScreenshotTest` dark and light at font 2.0 on the Realme. Not run: the phone was dozing with the keyguard showing at both checks. Evidence: verification.md.
- [x] 4.4 Full unit suite, lint, clean APK with dexdump, strict validation. Evidence: verification.md.

Expected Result: no Beranda count shows 0 before Room answers, and the bottom bar is readable at font scale 2.0.
