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

## Owner screenshot follow-up (2026-09-28)

The owner reported that entering selection mode split inferred legacy movement rows and added a `Belum tertaut` label, and requested removal of the `perkiraan` qualifier. The current topic branch now keeps uniquely inferred pairs consolidated during selection, selects both underlying records when the pair row is held, and removes both labels. Durable movements retain `Tergabung` in selection mode. Frontend type-check, lint, host production build, strict OpenSpec validation, and `git diff --check` passed. The local frontend image was rebuilt and only the frontend container was recreated; `/auth/login` returned HTTP 200 and other local services were unchanged. Automated frontend tests and browser interaction checks were not run; owner testing is pending.

## Owner interaction revision and regression verification — 2026-09-28

- Replaced the merge toolbar/action and `Aksi` column with one-second hold-to-select, brighter selected rows, click-to-toggle, a maximum of two selections, and a viewport-fixed `Jadikan Pindah Saldo` action. Equal-value selections convert directly; unequal values show a centered warning with only `Ok` and do not call the merge endpoint. Short click still opens edit.
- Confirmed movements remain consolidated, visibly marked, and non-selectable; inferred pairs remain consolidated and select both underlying records. The misleading inferred/unlinked labels are absent. Removed the duplicate mobile `Tergabung` indicator while retaining one durable-link badge.
- Focused desktop/mobile ledger tests pass 14/14; the complete frontend suite passes 106/106. Type-check, lint, production build, strict OpenSpec validation, and `git diff --check` pass. The local Docker API health endpoint returns healthy; this final source state was host-built but not rebuilt into Docker during this checkpoint.
- The test harness uses a bubbling `MouseEvent` with pointer event names because JSDOM has no native `PointerEvent`; this preserves the `button` property required by the production handler. Real touch, assistive-technology, and actual browser-zoom checks remain open. No production action occurred in this checkpoint.
