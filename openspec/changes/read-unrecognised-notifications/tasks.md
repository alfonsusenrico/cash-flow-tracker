## 1. Reproduce

- [x] 1.1 Failing tests from the production wording on synthetic data: "You've added Rp47.619 into Bayar Kredit Locked Pocket." stops at `settlement_unproven` and the parser assumes the main pocket. Evidence: the tests fail for those reasons before the change.

Expected Result: the reported failure is reproduced locally.

## 2. Server

- [x] 2.1 Migration V56 `model_reading`. Evidence: Flyway to V56 on Postgres 16.
- [x] 2.2 Evidence and parser: `needs_reading` candidates; move verb required for the single-pocket Jago rules; code paths without a reading refuse to record. Evidence: unit tests for the scenarios "Unrecognised settled wording is read", "Model unavailable" and "Other 'into' wording is not a main-pocket move"; existing parser, noise and corpus tests pass.
- [x] 2.3 Prompt guidance in both languages and a prompt version bump; reading acceptance and storage; stored reading applied in `collect_facts`; retry clears it. Evidence: database tests with a fake provider for "An unsure reading goes to review", "The model judges unrecognised wording as noise", "Source answer reuses the reading" and "Retry reads afresh".
- [x] 2.4 Source question for a model-read move: target left out, `suggested_account_id`, answer becomes the source, deterministic resume. Evidence: database tests for "Added into a locked pocket asks for the source", "Owner answers the source of the move" and "Unanswered or stale answer never debits the main pocket".
- [x] 2.5 Hand-entered move confirmation. Evidence: database tests for both scenarios of "Hand-Entered Moves Confirmed by an Unnamed-Source Notification", plus a move already referenced and one outside 48 hours.
- [x] 2.6 The production sequence on synthetic data: five "added into" events, four hand-entered moves, the old "Spaylater" name. Result: four confirmations, one mapping question and one source question. Evidence: one database test with a fake provider.
- [x] 2.7 Full backend suite. Evidence: `verification.md`.

Expected Result: unrecognised wording is read once, guarded, and recorded once.

## 3. Benchmark

- [x] 3.1 Synthetic benchmark cases: "added into" moves (named and unnamed source), unrecognised reminders, pending items and promotions that must stay ignored. Evidence: benchmark unit tests pass offline.
- [ ] 3.2 With the owner's OK, one paid benchmark run on the production model covering the new and existing cases. Evidence: scores and cost in `verification.md`; existing cases do not regress.

Expected Result: the model reads the new shape correctly and stays quiet on noise.

## 4. Companion

- [x] 4.1 Source-pocket controls preselect and mark the suggested pocket ("Disarankan"); older servers without the field behave as today. Evidence: companion unit tests and a build.

Expected Result: one tap confirms the suggested source.

## 5. Verify and deliver

- [x] 5.1 `openspec validate --all --strict`, `git diff --check`. Evidence: `verification.md`.
- [ ] 5.2 After the owner accepts, archive, push both branches, open both PRs and report their URLs.
- [ ] 5.3 After merge and deploy, release the companion tag and verify the APK. The owner then installs it and taps "Coba Proses Ulang" on the five events. Evidence: a read-only production check that four confirm hand-entered moves, "Spaylater" asks for a mapping, the Rp47.619 event asks for its source, and nothing is doubled.
