## 1. Data and learning

- [x] 1.1 Evidence read from existing answers, Tinjau resolutions and moved payments; no migration (design D3). Evidence: database tests for "A web correction teaches" and the manual resolution flow.
- [x] 1.2 `pocket_learning` service: first-word key, latest choice per transaction, weights, window, rule of succession, suggestion. Evidence: unit tests for every scenario of "Pocket Learning Key" and "Learned Pocket Recording", plus a replay of the owner's 10 payments as synthetic data with the same shapes (0 wrong, 1 automatic).
- [x] 1.3 Earlier corrections count from the first deploy. Evidence: database test "A web correction teaches"; a read-only check of the owner's records after deploy.

Expected Result: choices turn into a decision or a suggestion deterministically.

## 2. Notification path

- [x] 2.1 The unnamed-pocket expense path uses a learned decision or asks with the suggestion; income keeps the default pocket. Evidence: database tests for "First payment to a merchant asks", "Incoming money keeps the default pocket", "Older companion", "Three agreeing choices record automatically", "A disagreeing choice keeps asking", "Learned pocket no longer exists" and "Kill switch".
- [x] 2.1a Transfers and money added into a pocket use the same learning. Evidence: database test "Mixed sources keep asking".
- [x] 2.2 Answers and web corrections store choices. Evidence: database tests for "A web correction teaches" and "An answer then a correction of the same payment".
- [x] 2.3 `pocket_habit` in results; existing duplicate-arrival rules still pass. Evidence: full backend suite.

Expected Result: no payment lands on an assumed pocket, and the owner's choices are learned.

## 3. Companion

- [x] 3.1 The recorded detail shows "Kantong dari kebiasaan" with its basis. Evidence: companion UI test and build.

## 4. Verify and deliver

- [x] 4.1 `openspec validate --all --strict`, backend suite, companion tests, `verification.md`.
- [x] 4.2 After `read-unrecognised-notifications` merges, rebase on `main`. After the owner accepts, archive, push, open the PRs and report their URLs.
