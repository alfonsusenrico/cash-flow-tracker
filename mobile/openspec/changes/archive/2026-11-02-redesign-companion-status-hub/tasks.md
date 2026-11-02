# Tasks

## 1. Baseline
- [x] 1.1 Record the unit-test baseline on e05cde9 from the JUnit XML. Evidence: `testDebugUnitTest --rerun`, `junit_summary.py`: 58 suites, 594 tests, 0 failures, 6 skipped.

## 2. Data and server contracts
- [x] 2.1 Health payload and reporter per the device health contract and the coordinator's strict rules (status/step/time, types, version characters; 401 stops, 429 `Retry-After`, 404 quiet with backoff). Evidence: `HealthReporterTest` against MockWebServer.
- [x] 2.2 Health triggers: setup and self-test, app open, listener connect and disconnect, 15-minute periodic work. Evidence: code review of `HealthReports`, `MainActivity`, `FinancialNotificationListenerService`, `FinancialTrackerApp`, `PairingViewModel`; `HealthReporterTest` covers retry and pause policy.
- [x] 2.3 Self-test runner, marker and listener gate; the marker is never captured, stored or sent. Evidence: `SelfTestTest` (fakes and MockWebServer), `SelfTestListenerTest` (real listener service under Robolectric).
- [x] 2.4 Evening reminder with batching of purpose questions; WorkManager scheduling behind a fake. Evidence: `EveningReminderTest`; prompt-path suites run with the reminder off.
- [x] 2.5 Perbaiki label: changed fields only, explicit null pillar, owner-edit PATCH. Evidence: `LabelCorrectionTest` (MockWebServer), `CompanionScreensUiTest`.
- [x] 2.6 Catat `question_types` with `time`, time answers and `time_label`. Evidence: `CaptureTimeTest` (MockWebServer), `CaptureSheetUiTest`.
- [x] 2.7 Monthly report readiness and its notification. Evidence: `ReportReadinessTest`.
- [x] 2.8 Remove the ledger summary model, client call, cache and refresh; delete an older cache on start. Evidence: compile, `HubScreenUiTest.theMoneyTilesAreGone`.

## 3. Interface
- [x] 3.1 Status hub with calm state, problems and soft issues. Evidence: `HubStatusTest`, `HubScreenUiTest` (both themes, font scale 1 and 2).
- [x] 3.2 Sheets over the hub (Pertanyaan, Riwayat with nested Rincian, Pengaturan, Rincian status, Buka di web). Evidence: `CompanionShellUiTest`.
- [x] 3.3 Riwayat without day totals; Rincian with Perbaiki label and the Buku Kas link. Evidence: `CompanionScreensUiTest`, `LargeTextAmountsUiTest`.
- [x] 3.4 Question sheet with urgent first and the evening tag; top question answerable on the hub. Evidence: `CompanionShellUiTest`, `EveningReminderTest`.
- [x] 3.5 Pengaturan as grouped lists with the evening reminder, manual key and "Tes HP-ku". Evidence: `CompanionScreensUiTest`, `LargeTextUiTest`.
- [x] 3.6 Setup story cards with the typed code and the self-test. Evidence: `PairingUiTest` (both themes, font scale 1 and 2), `PairingViewModelTest`.
- [x] 3.7 Notifications: listener copy and tap, evening summary, "Laporan siap". Evidence: code review; `EveningReminderTest`, `ReportReadinessTest`.
- [x] 3.8 Contrast pairs for the new uses. Evidence: `ContrastTest`.

## 4. Device and checks
- [x] 4.1 Update the instrumented tests (`DeviceScreenshotTest`, new `HubStatesScreenshotTest` and `SelfTestDeviceTest`; remove the ledger summary device tests). Evidence: `compileDebugAndroidTestKotlin` with `-PcompanionVerification=true`.
- [ ] 4.2 Run the device self-test and screenshots on the Realme. Not run: no device was attached.
- [x] 4.3 `testDebugUnitTest`, `lintDebug`, `assembleDebug`, `git diff --check`, `openspec validate --all --strict` in `mobile/` and the root. Evidence: the change's report.
- [x] 4.4 Version 2.4.0 (source code 27) and README.
