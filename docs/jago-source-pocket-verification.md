# Jago source-pocket confirmation

## Approved baseline

Owner authorized implementation of `confirm-jago-outbound-source-pocket` on 2026-10-06. Backend target refreshed to main `1797708c97c268d69fa75e0b98088607164d4f85`; next migration V26. Isolated branch `feat/jago-source-pocket-confirmation`, worktree `/private/tmp/cft-jago-source-pocket`.

Companion accepted source `b64cd6e58cf3bb21e9537cd984f61ad644bc4607` (1.5.0/code 11, Room 4), isolated worktree `/private/tmp/cft-mobile-jago-pockets`, same topic branch. Native companion change `add-jago-source-pocket-confirmation` adopts the approved mobile contract. Proposed release 1.6.0/code 12, Room 5. No companion remote exists; the clean local implementation commit is `ed864e8`.

Existing Jago transfer-out facts leave the source absent; the resolver chooses `main_endpoint`. The correction belongs before endpoint resolution and must also enforce the final financial write boundary. Options must use a complete owned Jago query rather than the 200-account model window or mapping shortlist.

Verification uses fictional transactions and a disposable local database plus the separate Android verification application ID. Normal release installation awaits backend deployment. Existing unrelated root edits, previous feature acceptance tasks, historical records and production pairing remain outside corrective writes.

## Executed evidence

Backend coverage includes both Jago package spellings, selected non-main pairing in both arrival orders, same-amount ambiguity, legacy queued recovery, old-client review/upgrade, owner/auth/origin isolation, complete 207-option lists, archived/replayed answers, model-source conflicts, and additive populated V25→V26 upgrades plus startup mirror idempotence. The final full-suite count is recorded below.

Final backend suite: **679 passed** on the independently migrated disposable local PostgreSQL database, with no skipped database tests. Same-amount ambiguity preserves the existing behavior: the owner-confirmed expense records against the selected pocket, but ambiguous counterpart legs remain unlinked. V26 preserves existing notification, sender-answer and transaction rows; the mirrored startup migration is idempotent. Test infrastructure and both temporary verification packages were removed after completion.

Android: 46 unit tests and normal/verification APK builds plus lint passed (zero errors; 40 existing warnings, none in new SourcePocket files). Twenty-three targeted device tests passed with no skips, including populated Room 1/2/3/4→5, persisted sender worker compatibility, typed reply recovery, response identity, auth/conflict/error/429 handling, and enlarged-text/light/dark radio controls. The notification action and stale-pocket end-to-end tests each passed with no skip: Dana Darurat selected explicitly, one authoritative expense, pairing unchanged; an archived cached option was rejected without writing, then a refreshed explicit main choice recorded correctly. Three additional process-restart phases passed: durable answer preparation, force-stopped app restarted with local forwarding removed (network failure left the same typed answer pending), and connection restored (one recorded expense against Dana Darurat). Total: 28 executed device tests.

USB attempts failed because the authorized TECNO disappeared from USB; macOS had no TECNO USB entry. The owner reconnected using wireless ADB `10.0.160.150:34053`; hardware serial was verified as `1694125629002315`. Executed device evidence is wireless, not USB. Initial interactive runs skipped while locked. The test helper navigates the verification app's onboarding without granting notification-listener access. No real bank notification capture was enabled.

Normal APK ready: `/private/tmp/cft-mobile-jago-pockets/app/build/outputs/jago-release/companion-1.6.0.apk`, application ID `com.alfonsusenrico.financialtracker.listener.debug`, version 1.6.0/code 12, SHA-256 `25127ed3b1bd054708fa6ecccce4e78111860abad238f9f0dac929aeea6f2ccb`. Its signing certificate matches the previous 1.5.0 APK (SHA-256 `4ea3580fec00e181075adf59f93f0a086c676f9c0a6d79da117acee2325528a4`). Rollback artifact remains `/private/tmp/cft-mobile-sender-replies/app/build/outputs/sender-release/companion-1.5.0.apk`. The normal production-paired app has not been upgraded or reconfigured.

The inherited notification-ingestion delta includes older `transfer`/`transfer_target_account_id` language. The approved design and current architecture preserve bilateral expense/income movement records; this implementation does not restore obsolete ledger columns/types. Reconcile inherited main-spec wording during the separately accepted synchronization/archive step.

Native strict validation and diff checks pass in both repositories. Owner backend PR creation/merge, exact-revision deployment, normal mobile installation, final acceptance and native sync/archive remain pending.

## Deployed and installed — 2026-10-06

PR #19 merged as `7fcd302c93860f3edfe3a054ddc4e4855f07f13e`; its tree matches the tested `24e1a38` exactly. [Deploy Production run 37424356067](https://github.com/alfonsusenrico/cash-flow-tracker/actions/runs/37424356067) completed successfully for that revision. Evidence was read from GitHub; no production-server connection occurred.

The owner explicitly requested installing the prepared normal APK during the deployment pipeline, overriding the planned wait-before-install order. The same TECNO now has **1.6.0/code 12**, Room **5**. The on-device audit proved **98** existing records and **43** alert receipts retained; production URL/API key/pairing/device identity unchanged. Notification permission was retained; the foreground listener is bound by Android's system process. Credential and personal-notification values never left the phone. Temporary upgrade-check artifacts were removed.

Sanitized installation evidence is beside the retained APK at `/private/tmp/cft-mobile-jago-pockets/app/build/outputs/jago-release/installed-release.json`. Earlier pending release text describes the pre-release checkpoint. Deployment and installation are complete; owner acceptance/spec synchronization/archive and USB transport verification remain pending.
