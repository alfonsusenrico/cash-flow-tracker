# Tasks

## 1. Contract and baseline
- [x] 1.1 Record the approval, the baseline and the contract. Evidence: proposal.md `## Approval`; 573 unit tests, 0 failed, 6 skipped on 4e0d3e6 from the JUnit XML; design.md Context citing cash-flow-tracker 15dbf61.

Expected Result: the change starts from a known passing baseline and a written contract.

## 2. Uncategorised entry
- [x] 2.1 Write the parsing, UI and cache tests first and run them on the unchanged main code of 4e0d3e6. Evidence: verification.md lists which new cases fail there and which already pass.
- [x] 2.2 Accept one null-id entry, name it "Tanpa kategori" when its name is blank or missing, drop its pillar, and treat two null ids as a contract break. Evidence: `TrackerOptionalFieldsTest` (null id, blank and missing name, pillar dropped, round trip, two null ids and an unknown pillar as breaks), `TopCategoriesUiTest` (4 new tests × light/dark × font 1.0/2.0, including TalkBack "Tanpa kategori, Rp150.000" and no tile for two null ids), `LedgerSummaryRefreshTest` (null id cached and restored through MockWebServer).
- [x] 2.3 Describe the entry in README and set version 2.3.4, code 26. Evidence: README diff; `aapt2 dump badging` and `dexdump` of the clean APK.

Expected Result: Beranda shows the tracker's uncategorised spending as "Tanpa kategori" without a pillar, and a list with two of them gets no tile.

## 3. Verification and delivery
- [x] 3.1 Full unit suite, lint, androidTest compile and clean APK. Evidence: verification.md.
- [ ] 3.2 Device screenshots on the Realme with the verification build. Not run: no device was attached. Evidence: verification.md.
- [x] 3.3 OpenSpec strict validation. Evidence: `openspec validate --all --strict` exit 0.

Expected Result: a reviewable branch with recorded evidence.

## Workflow follow-up
- Archived with `openspec archive show-uncategorised-top-category --yes`; committed and pushed the branch.
