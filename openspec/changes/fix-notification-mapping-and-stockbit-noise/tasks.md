# Tasks: fix-notification-mapping-and-stockbit-noise

## 1. Account-name precedence and locked Jago pocket normalization

- [x] 1.1 Update `normalize_name`, `aliased_account`, `pocket_endpoint`, and `institution_parent` in `backend/app/services/notification_resolution.py` to strip standalone `"locked"` and `"terkunci"` words in `normalize_name` and resolve in order: owner-confirmed alias → active account name (exact then singular/plural stem) → AI-learned alias. Evidence: unit and integration tests proving (a) an active pocket `"Bayar Kredit"` beats an AI-learned alias pointing to another pocket, (b) an owner-confirmed alias still overrides an account name, and (c) `"Dana Tagihan Locked Pocket"` resolves directly to `"Dana Tagihan"` without AI mapping or confirmation.

Expected Result: locked Jago pockets and active account names resolve deterministically without stale AI aliases hijacking them.

## 2. Stockbit market-news filtering

- [x] 2.1 Update `MARKET_INFO_PATTERN` and `collect_facts` in `backend/app/services/notification_evidence.py` so Stockbit dividend, yield, issuer, and commodity alerts (and unsettled non-recognised Stockbit commentary) classify as `ignored` (`promotional_or_market`) before `FOREIGN_CURRENCY_PATTERN` runs, while keeping real Stockbit trade matches, RDN deposits, and settled foreign-currency bank/card payments unchanged. Evidence: tests covering Stockbit US-dollar dividend/commodity alerts → `ignored`, Stockbit buy/sell match & deposit → recorded, Jago `"You've paid USD 10.00"` → `needs_review` (`unsupported_currency`), and `reclassify_review_noise()` moving unlinked `needs_review` Stockbit news rows to `ignored` without touching linked or non-noise rows.

Expected Result: Stockbit market news stops reaching `needs_review`, and existing stuck news rows clear automatically on startup.

## 3. Verify and deliver

- [x] 3.1 Run the full backend test suite against migrated disposable PostgreSQL (`TEST_DATABASE_URL`, `REQUIRE_TEST_DATABASE=1`, reporting any skips) and `openspec validate --all --strict`; write `verification.md`.
- [ ] 3.2 After owner acceptance: archive the change with `openspec archive fix-notification-mapping-and-stockbit-noise`, commit, push `fix/notification-mapping-and-stockbit-noise`, and when the owner says "open", create the PR (`gh pr create`) and report its URL.

Expected Result: verified PR ready for owner review and merge.
