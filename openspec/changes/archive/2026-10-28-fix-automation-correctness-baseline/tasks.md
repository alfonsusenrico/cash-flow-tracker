## 1. Pillar vocabulary, schema and effective pillar

- [x] 1.1 Add `db/migrations/V27__full_kakeibo_and_adjustment_category.sql` and its identical startup mirror `backend/app/db/full_kakeibo_and_adjustment_category.sql`, run after the existing mirrors in `init_db.py` (design D7, D8).
  - Evidence: a migration test on a disposable PostgreSQL migrated V1→V27 proves:
    - the widened constraints accept `culture` and `unexpected` and reject `luxury`;
    - re-running the mirror is a no-op;
    - matching adjustment rows move to "Penyesuaian Saldo" and non-matching rows are unchanged.
- [x] 1.2 Add the shared backend pillar vocabulary (Literal and pattern) and use it in `transactions.py`, `categories.py`, `ingest.py` and `notification_interpretation.py`.
  - Evidence: API tests create and update transactions and categories with each of the five pillars, and get 422 for an unknown value.
- [x] 1.3 Add `effective_pillar_sql` and use it in the ledger list, ledger filter and detail queries, replacing the `is_primary` fallback.
  - Evidence: a test where a transaction with no pillar in a `want` category lists and filters as `want`; the existing transactions tests pass.
- [x] 1.4 Rework `get_kakeibo_breakdown` for five pillars, three groups and the corrected status, keeping the `max(inflow, allocated)` base (D3).
  - Evidence: tests for the delta-spec scenarios: lifestyle warning, essentials elevated, unallocated inflow, empty cycle, and NULL pillar following the category. Existing Kakeibo tests pass.
- [x] 1.5 Change pulse living spending to "effective pillar is not `saving`" (D4).
  - Evidence: pulse tests where culture and unexpected rows count, and a saving-pillar expense does not.
- [x] 1.6 Update the prompt pillar definitions and bump `PROMPT_VERSION` to `notification-interpretation-8` (D9).
  - Evidence: a database-backed notification test where an AI result with `kakeibo='unexpected'` records successfully, with no `failed` state and the prompt version stored.

Expected Result: every backend write path accepts the full pillar set, and every read path resolves pillars with one rule.

## 2. Notification noise classification

- [x] 2.1 Implement the D5 rules in `notification_evidence.collect_facts`: hard noise, promotional and market-info wording without a recognised transaction, headline figures, and no money in any currency. Remove the promotional words from hard noise.
  - Evidence: unit tests with synthetic texts for every scenario in the `notification-filtering` delta, including a settled payment with cashback (kept) and a settled USD card payment (review). The real-corpus tests in `test_real_notification_corpus.py` still pass.
- [x] 2.2 Add the idempotent startup backlog re-evaluation (D6), logging counts only.
  - Evidence: a database test where a noise `needs_review` event becomes `ignored`. Untouched: a real `needs_review` event, a `recorded` event, and a `needs_review` event with a linked transaction. A second run changes nothing.

Expected Result: settled payments are never discarded for marketing wording, and marketing, news and instructions never reach review.

## 3. Reconcile adjustment category

- [x] 3.1 Add `ensure_adjustment_categories` and use it in `reconcile_account` (D7).
  - Evidence: reconcile tests for both directions assert the category, the excluded flag and an unchanged balance result. A dashboard and pulse test shows the adjustment excluded from spending, income and Kakeibo.

Expected Result: reconcile adjustments change balances but never reports.

## 4. Frontend

- [x] 4.1 Add `lib/kakeibo.ts` (pillars, labels, help, tokens) and use it in QuickCapture, ledger edit and the insights category form. Add the culture and unexpected token pairs in both themes in `tokens.css`, plus the chip classes and chart theme (D1, D10).
  - Evidence: vitest tests for the pillar options and the category-to-pillar default. A contrast check of each new text/soft pair at WCAG AA, recorded in `verification.md`.
- [x] 4.2 Update the insights Kakeibo section and `KakeiboPillarCards` for five pillars and three groups, and map the status badge to `healthy`, `warning` and `elevated_needs`.
  - Evidence: vitest tests for badge mapping and group rendering. Browser check at 390 px and 1280 px in light and dark themes, plus keyboard focus through the pillar selects, recorded in `verification.md`.
- [x] 4.3 Update the API types for the additive Kakeibo fields.
  - Evidence: `npm run type-check`, `npm run lint`, `npm test` and `npm run build` pass.

Expected Result: the owner can pick and read all five pillars, and the status badge tells the truth.

## 5. Telegram bot retirement

- [x] 5.1 Remove `telegram-bot/`. Remove its compose service, the volume declaration, and the `depends_on` entry. Remove the nginx server block. Remove `TELEGRAM_*`, `DEEPSEEK_*` and `BOT_SECRET` from deploy env, `materialize_runtime_env.py` and `.env.example`. Remove the `.gitignore` lines, the README section and the docs rows (D11).
  - Evidence:
    - `docker compose config -q` with the example environment succeeds;
    - `nginx -t` in an `nginx:alpine` container with the new config succeeds;
    - `git grep -i -E "telegram|deepseek|bot_secret"` finds only historical OpenSpec archive text and the API-key table comment;
    - the runtime-env script tests pass.

Expected Result: nothing in the repository builds, configures or routes to the bot.

## 6. Verification and delivery

- [x] 6.1 Run the full backend suite against a disposable `postgres:16`, migrated by `flyway/flyway:10` through V27, with `SKIP_TEST_MIGRATIONS=1`.
  - Evidence: pass and skip counts recorded in `verification.md`.
- [x] 6.2 Run `openspec validate fix-automation-correctness-baseline`, then `openspec validate --all` and the `--strict` variant, and run `git diff --check`.
  - Evidence: zero errors. Any strict warnings are recorded as follow-ups.
- [x] 6.3 Write `verification.md`: revision, commands and counts, environments (synthetic data only), browser widths and themes, and what was not verified and why.
  - Evidence: the file is complete and reviewed against the delta specs.
- [x] 6.4 After owner acceptance: sync the delta specs, archive the change, push `fix/automation-correctness-baseline`, and report the compare link. Record in `PROJECT_STATE.md`:
  - the production checks for after the merge (constraint, backlog, adjustments, bot container gone);
  - the owner actions: revoke the bot token, delete the GitHub secrets, remove the webhook tunnel route.
  - Evidence: archive command exit status 0, and the pushed branch URL.
