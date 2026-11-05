## 1. Reproduce the order bug

- [x] 1.1 Add `test_gopay_then_mybca_then_jago_books_one_movement` to `backend/tests/test_duplicate_arrivals.py` (synthetic texts, the order of 5 Nov: GoPay 0 s, myBCA 1.1 s, Jago 1.3 s). Evidence: it fails on `main` because the Jago leg is booked on Kantong Utama.

Expected Result: the 5 Nov failure is reproduced on a migrated disposable PostgreSQL.

## 2. Guard fix

- [x] 2.1 Make `SAME_ARRIVAL_SQL` match legs in a movement and select `movement_id` (D1). Evidence: test 1.1 passes, and the Jago event is waiting in `/api/review/queue` `seen_twice`.
- [x] 2.2 Make `replaceable_leg` false for a leg in a movement, and remove the link path's redundant check (D2). Evidence: a new test, "a paired leg is never replaced", keeps the paired Jago leg and its movement.
- [x] 2.3 Add the possible-transfer check for the unlinked guard (D3). Evidence: a new test shows that GoPay credit, then GoPay "udah dikirim ke Bank Jago", then Jago credit pairs the Jago credit with the GoPay debit and holds nothing.
- [x] 2.4 Let the deferred pass decide a pair whose original is in a movement (D4). Evidence: a new test records GoPay, myBCA and Jago with the guard patched off, then runs `repair_unpaired_movements`. The first run holds the Jago report and keeps the movement. A second run changes nothing.
- [x] 2.5 Run the backend suite. Evidence: `pytest backend/tests` against migrated PostgreSQL, with pass and fail counts recorded in `verification.md`.

Expected Result: the second report is held in every order, paired legs are never removed, and real transfers still pair.

## 3. Tinjau question row

- [x] 3.1 Sign, colour and caption a question row by its direction in `TriageList.tsx`, and set `itemTitle`'s fallback in `triage.ts` (D5). Evidence: a new test in `frontend/src/app/review/page.test.tsx` checks an income question row shows "+Rp" with `ws-amt-in` and "Uang masuk", and the existing expense row is unchanged.
- [x] 3.2 Run the frontend checks. Evidence: `npm run type-check`, `npm run lint` and `npx vitest run` pass, and `npm run build` succeeds.
- [x] 3.3 Browser check of the income question row at 1440 px and 390 px, in light and dark, with axe. Evidence: screenshots and axe results in the scratchpad, summarised in `verification.md`.

Expected Result: the Tinjau list shows a question's real direction.

## 4. Delivery

- [x] 4.1 Write `verification.md`, run `openspec validate --all --strict` and `git diff --check`, then review the full diff.
- [ ] 4.2 After the owner accepts, archive the change, push the branch and report the compare link. The owner opens and merges the merge request.
