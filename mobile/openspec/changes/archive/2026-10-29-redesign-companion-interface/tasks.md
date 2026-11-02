# Tasks

## 1. Visual system and shell

- [x] 1.1 Add the E tokens for light and dark, the Material mapping, the bundled Archivo font with its OFL, and the type scale (D1). Verify: `ContrastTest` passes and prints the table; the font file's SHA-256 matches design.md.
- [x] 1.2 Store and apply the theme choice (D2). Verify: `CompanionShellUiTest.themeChoiceIsStoredAndReadBack`, `CompanionModelTest.storedThemeCodesAreValidatedAndDefaultToTheSystem`.
- [x] 1.3 Add the navigation host and bottom bar with the Tanya count and the free centre slot (D3). Verify: `CompanionShellUiTest.bottomBarSwitchesTabsAndCountsOpenQuestions`.
- [x] 1.4 Open a notification's row in Tanya or Rincian, and open questions as the Tanya tab (D3). Verify: `CompanionShellUiTest.questionNotificationOpensTanyaAndOthersOpenRincian`, `aQuestionOpenedFromTheFeedBelongsToTanya`.

Expected Result: the app shell in E's look, following the system theme until the owner chooses.

## 2. Screens

- [x] 2.1 Beranda bento from this phone's rows (D5). Verify: `CompanionModelTest.homeFiguresCountOnlyRecordedRowsOfThisPhone`, `CompanionScreensUiTest.homeShowsEveryTileFromThisPhoneAndOpensItsList`, `homeWithoutQuestionsOrListenerSaysSo`.
- [x] 2.2 Aktivitas feed with filters, delete with undo, clear ignored and the duplicates note; a row restored after a swipe stays restored (D11). Verify: `CompanionScreensUiTest` activity tests, `CompanionShellUiTest.deletingFromTheFeedCanBeUndone` and `aRowSwipedAwayAndRestoredStaysRestored` (fails with the 1.8.3 swipe state), instrumented `ActivitySwipeDeleteTest`.
- [x] 2.3 Rincian with the state box, inline form and delete (D6). Verify: `CompanionScreensUiTest` detail tests, `DetailSendFormUiTest`, `CompanionShellUiTest.deletingFromRincianGoesBackAndOffersUndo`, instrumented `NotificationStatusUiTest`.
- [x] 2.4 Tanya for purpose, sender, source pocket and mapping questions, with the original notification first, pager, answered and closed states (D4). Verify: `CompanionScreensUiTest` Tanya tests, `PurposeQuestionUiTest`, `CompanionModelTest.everyQuestionTypeIsOpenUntilTheOwnerAnswers`, instrumented `SenderQuestionUiTest` and `SourcePocketQuestionUiTest`.
- [x] 2.5 Atur with health tiles, auto-sync, sync now, apps, connection, Android settings and Tampilan (D8). Verify: `CompanionScreensUiTest.settingsShowHealthAppsAndThemeChoice`.
- [x] 2.6 Restyle the permission screen without changing its flow. Verify: device screenshots.
- [x] 2.7 Remove `ui/inbox` and the sheet. Verify: no reference remains; the unit suite passes.

Expected Result: every 1.8.3 action is reachable on the new screens.

## 3. Accessibility

- [x] 3.1 Labels, headings, 48 dp targets, text with every status, reduced motion, and one-column layouts at large text (D9). Tiles and answer options are one node with their spoken summary. Verify: `LargeTextUiTest`; labels read from the device's accessibility tree by `DeviceScreenshotTest`.

Expected Result: WCAG 2.2 AA in both themes and usable at font scale 2.0.

## 4. Release and verification

- [x] 4.1 Set version 2.0.0 (code 19) and the version assertion. Verify: `clean assembleDebug --no-build-cache`, `aapt2` and `dexdump` show 19 and "2.0.0".
- [x] 4.2 Run the unit suite against the 1.8.3 baseline count. Verify: JUnit XML counts in `verification.md`.
- [x] 4.3 Device check on the Realme with the verification build: synthetic seed (`SyntheticDeviceSeedTest`), screenshots of each screen in light and dark and at font scale 2.0 with the accessibility labels (`DeviceScreenshotTest`), the instrumented UI tests, and an upgrade from 1.8.3 that keeps rows, pairing preferences and pairing context. Verify: `verification.md`.
- [x] 4.4 Validate. Verify: `openspec validate --all --strict` exits 0.

Expected Result: a verified 2.0.0 build that installs over 1.8.3 without losing data.
