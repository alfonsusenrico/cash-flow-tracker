## 1. Data and learning

- [ ] 1.1 Migration V57 `pocket_choices`, append-only. Evidence: Flyway to V57; an update is refused.
- [ ] 1.2 `pocket_learning` service: key normalisation, latest choice per transaction, weights, window, decision and suggestion. Evidence: unit tests for "Station numbers share a key", "Debit card payments share a key", agreement and window.

Expected Result: choices turn into a decision or a suggestion deterministically.

## 2. Notification path

- [ ] 2.1 The unnamed-pocket expense path uses a learned decision or asks with the suggestion; income keeps the default pocket. Evidence: database tests for "First payment to a merchant asks", "Incoming money keeps the default pocket", "Older companion", "Three agreeing choices record automatically", "A disagreeing choice keeps asking", "Learned pocket no longer exists" and "Kill switch".
- [ ] 2.2 Answers and web corrections store choices. Evidence: database tests for "A web correction teaches" and "An answer then a correction of the same payment".
- [ ] 2.3 `pocket_habit` in results; existing duplicate-arrival rules still pass. Evidence: full backend suite.

Expected Result: no payment lands on an assumed pocket, and the owner's choices are learned.

## 3. Companion

- [ ] 3.1 The recorded detail shows "Kantong dari kebiasaan" with its basis. Evidence: companion UI test and build.

## 4. Verify and deliver

- [ ] 4.1 `openspec validate --all --strict`, backend suite, companion tests, `verification.md`.
- [ ] 4.2 After `read-unrecognised-notifications` merges, rebase on `main`. After the owner accepts, archive, push, open the PRs and report their URLs.
