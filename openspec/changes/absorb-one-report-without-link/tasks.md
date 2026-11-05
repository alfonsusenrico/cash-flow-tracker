## 1. Reproduce

- [x] 1.1 Add tests for two top-ups 98 seconds apart where the second top-up's Jago report comes first, one without a link and one with a link. Evidence: both fail on `main` `dd67c58` (the second Jago report repeats the first GoPay leg).

Expected Result: the Sesi uji step 10 regression is reproduced with synthetic data.

## 2. Fix

- [x] 2.1 Add `absorbed_packages` to `SAME_ARRIVAL_SQL` and apply absorb-once in `same_arrival` (D1). Evidence: tests 1.1 pass. Without a link, the second Jago report is held against the second GoPay leg. With a link, it is skipped against it.
- [x] 2.2 Run the backend suite on migrated PostgreSQL. Evidence: pass and fail counts in `verification.md`.
- [x] 2.3 Replay the owner's Sesi uji export locally and score it. Evidence: 18 of 18 cases pass. The export is deleted afterwards, and the score is recorded without personal data.

Expected Result: a leg absorbs at most one report per app, and the field session scores fully.

## 3. Delivery

- [x] 3.1 Write `verification.md`, run `openspec validate --all --strict` and `git diff --check`, then review the diff.
- [ ] 3.2 After the owner accepts, archive the change, push the branch and report the compare link.
