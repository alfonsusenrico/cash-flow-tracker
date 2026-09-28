# Ledger selection regression verification — 2026-09-28

## Failure and cause

The owner screenshots showed the merge entry control disappearing above a scrolled ledger and a confirmed movement becoming two rows after entering selection mode. Selection mode removed `logical_movements=true` from the query and disabled all consolidation. The ledger action bar also had no sticky positioning.

The correction preserves logical querying and consolidation in selection mode. Confirmed movements remain one row marked `Tergabung` and cannot be selected again. Unlinked inferred pairs retain their `perkiraan` label and support pair selection without a financial write. A partially selected inferred pair remains visible and can be cleared after changing account filters. An inferred row's normal-view action is `Gabungkan`, not `Ubah`.

## Rendered-layout findings

The first sticky implementation assumed a 56px mobile header; Chrome measured 105px and showed the action bar underneath it. Measuring the rendered header height corrected that offset. A reverse-tab experiment then found row controls behind the sticky bar. Measuring the action bar and applying scroll margins to row controls corrected that separate focus obstruction.

## Evidence

- Topic branch: `fix/ledger-merge-selection-visibility`, based on `1b72880`; six corrected frontend source/test files.
- Full frontend suite: 103 tests passed. After the final edit-action label and scroll-margin change, all 16 affected tests passed again. Lint, type-check, host build, Docker build, and whitespace checks passed.
- Final local frontend image: `sha256:1502e5231dd7ea28e9c0df450bb18149c5e482099453c9ab2126b5d7edf96ad7`.
- Chrome checks against that local image passed at 1280px, 390px, and 320px in dark and light themes. Confirmed rows stayed consolidated and non-selectable; inferred pairs remained distinct from confirmed links; scrolled merge controls stayed below the header without horizontal document overflow.
- Forward activation and reverse-tab checks covered normal editing and merge selection. Confirmation initial focus, Escape dismissal, and sampled axe scans of the merge toolbar and confirmation passed without reported violations.
- At 320px with 200% root text size, the measured header and toolbar offsets updated and toolbar buttons remained contained. This is a scoped text-resize check, not a claim of actual browser-toolbar zoom or whole-application conformance.
- A keyboard-selected, explicitly confirmed merge preserved both IDs, accounts, amounts, timestamps, notes, receipt paths, and account balances, and assigned one shared movement ID.
- Every browser run used a disposable local account and removed it and its fixtures afterward. Existing user records were not modified. Only the local frontend service was rebuilt/recreated; no schema, API, or production changes were made.

## Limits and next action

Physical touch, real screen-reader behavior, and actual browser-toolbar zoom remain unverified. The older financial-integrity evidence/review tasks are not closed by this focused correction. The configured CI/release workflow runs on `main`; a topic-branch push does not provide a CI result or authorize production release. Owner testing and a separately authorized release remain next.
