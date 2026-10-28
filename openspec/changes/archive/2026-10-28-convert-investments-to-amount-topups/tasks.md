# Tasks

## 1. Backend

- [x] 1.1 Add Flyway V24 and its mirrored startup SQL for `investment_topups.converted_from`. Verify with a migrated disposable PostgreSQL schema check.
- [x] 1.2 Implement unit-to-amount product conversion and its endpoint with eligibility errors. Verify with database tests for value/cost carry-over, the owner's product numbers, ineligible products, and a following top-up.
- [x] 1.3 Implement expense-to-top-up conversion and its endpoint. Verify with database tests for leg linkage, the unchanged funding balance, the product delta, notification note handling, idempotent repeat, and each refusal case.
- [x] 1.4 Make deletion of a converted top-up restore the original expense, and keep PATCH corrections working. Verify with database tests for restore, product reversal, and reconversion after deletion.

## 2. Web

- [x] 2.1 Add eligibility helpers with unit tests.
- [x] 2.2 Add "Ubah ke pelacakan nominal" with its confirmation on the accounts page. Verify with a component test and a fictional browser check.
- [x] 2.3 Add "Jadikan Top up Investasi" with the product picker in the ledger edit modal. Verify with a component test and a fictional browser check on desktop and mobile widths, plus keyboard focus.

## 3. Verification and delivery

- [x] 3.1 Run the full backend suite on disposable PostgreSQL, plus frontend tests, type-check, lint, and build. Run `openspec validate --strict` and `git diff --check`.
- [ ] 3.2 Commit and push the topic branch and report it for the owner to merge. After deployment, the owner converts the two Bibit products and the two Rp112.590 RDN debits.
