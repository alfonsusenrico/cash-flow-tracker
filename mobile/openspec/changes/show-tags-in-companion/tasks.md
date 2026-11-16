## 1. Model

- [x] 1.1 `CaptureTag` and `CaptureTransaction.tags`; a presentation helper that gives the non-blank names in order. Evidence: `CaptureModelTest` parses a view with two tags, an empty list, and a view without the field.
- [x] 1.2 The edit request never carries tag fields. Evidence: a test that serializes `TransactionUpdateRequest` and finds neither `tag_ids` nor `new_tags`.

Expected Result: the companion reads tags and cannot clear them.

## 2. Screens

- [x] 2.1 Result card: "Tag: …" line with a tag icon after the note line, only for an expense or income with tags. Evidence: Compose UI test for one tag, two tags and none.
- [x] 2.2 Rincian: "Tag" row after "Catatan". Evidence: Compose UI test for a tagged capture and a capture stored without the field.

Expected Result: the owner sees on the phone which tags the tracker attached.

## 3. Verify and deliver

- [x] 3.1 `./gradlew testDebugUnitTest lintDebug assembleDebug`; `openspec validate --all --strict` in `mobile/`; `verification.md`.
- [ ] 3.2 After the owner accepts: archive, push, open the PR and report its URL.
- [ ] 3.3 After the merge and green Android checks: push `android-v2.4.5` and verify the APK (checksum, version, signer `CN=Cash Flow Companion`, dex strings).
