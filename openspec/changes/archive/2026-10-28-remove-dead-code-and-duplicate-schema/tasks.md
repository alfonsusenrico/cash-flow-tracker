## 1. Dead code

- [x] 1.1 Prove and remove `services/ledger/*`, `services/metrics.py`, `services/projection.py`, `services/categories.py`, `models/` and `services/category_rules.py` with `tests/test_category_rules.py`; drop the obsolete `services/ledger/` skip in `test_transaction_provenance.py`.
  - Evidence: repository-wide grep of import paths and symbols (design Context); full backend suite green after removal.

Expected Result: no dead module remains and nothing imported them.

## 2. Schema ownership

- [x] 2.1 Compare Flyway V1→V31 with V31 + `init_db_schema()` on a fresh database (D1).
  - Evidence: `pg_dump --schema-only` diff showing only `default_funding_account_id` and `uq_tx_user_idempotency`.
- [x] 2.2 V32 adopts both, archives paid debts once, and drops `merchant_category_rules` only when empty (D1, D2).
  - Evidence: `test_v32_adopts_startup_schema_archives_paid_debts_and_drops_empty_rules` (applied twice, duplicate idempotency key rejected), `test_v32_refuses_to_drop_merchant_rules_holding_rows` (raises, row kept); fresh V1→V32 dump equals the old V31 + startup dump minus the retired table.
- [x] 2.3 Remove `init_db_schema()`, its call in `main.py`, the eleven mirror files and the calls in fixture scripts; rewrite mirror-equality tests as repeatability tests (D3).
  - Evidence: `test_application_runs_no_startup_schema`; V20, V25–V31 repeatability tests green.
- [x] 2.4 Stop reading merchant category rules in the context and deterministic interpretation; prompt version 10 (D2).
  - Evidence: provider context tests with the shorter query sequence; prompt version assertion; benchmark fixture uses owner memory.
- [x] 2.5 Smoke boot: uvicorn on loopback against a database migrated V1→V32 only.
  - Evidence: `verification.md` (health 200, register 200, authenticated categories and accounts 200, no errors in the log).

Expected Result: the API starts on a Flyway-only database and no deployment can silently lose rules data.

## 3. Follow-ups

- [x] 3.1 Seed "Internal Movement" categories excluded with NULL pillar (D4).
  - Evidence: `test_new_user_movement_categories_are_excluded_without_pillar`; smoke database shows both rows `t` with NULL pillar.
- [x] 3.2 Add `coffee`, `users`, `receipt` and `arrows-right-left` icons and the two picker options (D4).
  - Evidence: `src/components/ui/Icon.test.tsx` (5 tests).

Expected Result: new users' movements never count as spending and seeded categories show their icons.

## 4. OpenSpec hygiene and docs

- [x] 4.1 Audit the sixteen open changes, merge the overlapping top-up requirements into the integrity change, reconcile `big-refactor-v2`, archive ten implemented changes in creation order and write purposes for TBD specs (D5).
  - Evidence: every `openspec archive … --yes` exit 0; `openspec validate --all` exit 0 afterwards; open decisions list the six left open.
- [x] 4.2 Split the eighteen over-long requirements and align the two open changes that modify them (D5).
  - Evidence: `openspec validate remove-dead-code-and-duplicate-schema --strict` exit 0; `openspec validate --all --strict` after archiving (verification.md).
- [x] 4.3 Update `AGENTS.md` (schema owner, companion repository, bot retired, review and automation modules) and the deployment diagram.
  - Evidence: diff review.

Expected Result: the main specs describe current behaviour and pass strict validation.

## 5. Verification

- [x] 5.1 Backend suite on a freshly recreated PostgreSQL migrated V1→V32; frontend tests, lint, type-check and build; `git diff --check`; OpenSpec validation.
  - Evidence: `verification.md` with counts and exit codes.

Expected Result: all suites green on a fresh database.

## Workflow follow-up

- Archive with `openspec archive remove-dead-code-and-duplicate-schema --yes`, re-run `openspec validate --all` and `--strict`.
- Commit, push `chore/automation-cleanup` and report the compare link `main...chore/automation-cleanup`; the owner merges.
