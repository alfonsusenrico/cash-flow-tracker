# Tasks

## 1. Pairing and transport contracts
- [x] 1.1 Record approved decisions, baseline and delta specs. Evidence: OpenSpec validation and baseline JUnit results.
- [x] 1.2 Implement bounded QR validation, automatic device naming and fixed build server. Evidence: parser/name/upgrade regression tests and release/debug manifest checks.
- [x] 1.3 Implement exchange, durable key save, authenticated retry and safe manual-key replacement. Evidence: mock-server tests covering 410, 429, network errors, no redirects, saved-key retry and different-owner isolation.

Expected Result: Pairing logic is safe and independently tested before UI integration.

## 2. Setup and scanner
- [x] 2.1 Add CameraX/ML Kit scanner, permission handling and lifecycle cleanup. Evidence: APK build, scanner lifecycle inspection and instrumented QR decoding and permission-denial regression tests.
- [x] 2.2 Implement guided setup, confirmation, external links and settings connection actions. Evidence: Compose tests in both themes and at font scale 2, with actionable failure states and existing feature tests.
- [x] 2.3 Update version and setup/testing documentation. Evidence: APK version 2.1.0/code 20, README commands and no server field in UI.

Expected Result: Approved setup works with manual fallback and existing screens remain usable.

## 3. Integration and review
- [x] 3.1 Verify pair, authenticate, re-pair and revoke against local backend with synthetic data and verification app only. Evidence: instrumented local-backend result.
- [x] 3.2 Run full unit tests, lint, debug/release builds, OpenSpec strict validation and diff review. Record exact results and accessibility limits in verification.md.
- [x] 3.3 Verify upgrade pairing/data preservation on the Realme verification app and prepare owner camera acceptance instructions. Evidence: before/after synthetic state and QR decoding test. Actual optical camera scan is an owner acceptance check, never claimed as automated evidence.

Expected Result: Reviewable local change with verified artifact and explicit remaining owner checks.

## Workflow follow-up
- Present implementation and evidence for owner acceptance. Push only after acceptance, or explicit owner backup-push instruction.
- Sync and archive following native workflow after review. Owner creates and merges the PR.
- Install on TECNO only on the owner's request. Phase 3 remains separate.
