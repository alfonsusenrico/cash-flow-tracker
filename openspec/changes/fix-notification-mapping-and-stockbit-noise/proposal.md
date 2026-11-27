# Proposal: fix-notification-mapping-and-stockbit-noise

## Approval

Approved by the owner on 2026-11-27 ("ok, do all") after the production audit confirmed that an AI-learned alias overrides an existing pocket name on locked Jago pockets and that Stockbit market-news alerts with foreign currency figures still land in `needs_review` with `unsupported_currency`.

## Why

Two notification-pipeline defects create avoidable `needs_review` items and wrong automatic mappings:
1. In `notification_resolution.py`, `aliased_account` runs before exact account-name matching and `normalize_name` does not strip Bank Jago's `"Locked"` / `"Terkunci"` suffix. When a pocket is locked (for example, `"Bayar Kredit Locked"`), name matching fails and an AI-learned alias can bind it to the wrong pocket (such as another installment pocket) and keep winning even when an active pocket has the exact base name.
2. In `notification_evidence.py`, Stockbit market-news notifications mentioning dividends, yields, commodity sell-offs, or per-share/per-ton foreign currency figures (such as `"US$500 juta"` or `"US$12.471/ton"`) escape `MARKET_INFO_PATTERN` and hit `FOREIGN_CURRENCY_PATTERN`, landing in `needs_review` with `unsupported_currency` instead of being ignored as market news.

## Expected Outcome

- An active account whose normalized name matches a notification's bank/pocket name beats any AI-learned alias, while an explicit owner-confirmed alias still takes priority.
- Bank Jago pocket names carrying `"Locked"` or `"Terkunci"` normalize to their base pocket name and resolve directly to the matching Jago pocket without an AI mapping call.
- Stockbit market-news and commentary notifications (including those quoting `US$` or other foreign-currency headline figures) are classified as `ignored` (`promotional_or_market`), while settled Stockbit trade matches and RDN deposit confirmations continue to record normally.
- Existing unlinked Stockbit market-news rows sitting in `needs_review` are automatically cleared to `ignored` on backend startup via `reclassify_review_noise()`.

## What Changes

- **Account-name precedence and locked-pocket normalization:** Update `normalize_name`, `aliased_account`, `pocket_endpoint`, and `institution_parent` in `backend/app/services/notification_resolution.py` so:
  - `normalize_name` strips trailing/standalone `"locked"` and `"terkunci"` words alongside `"pocket"`, `"kantong"`, `"your"`, and `"my"`.
  - Resolution order is: owner-confirmed alias → active account name match (exact then singular/plural stem) → AI-learned alias.
- **Stockbit market-news filtering:** Update `backend/app/services/notification_evidence.py` so `MARKET_INFO_PATTERN` covers dividend/yield/commodity/issuer news signals (`dividen`, `cum dividen`, `yield`, `emiten`, `sell off`, `/saham`, `/ton`, `kurs`) and non-recognised Stockbit news/commentary is classified as `ignored` before `FOREIGN_CURRENCY_PATTERN` can route it to `needs_review` (`unsupported_currency`), while settled foreign-currency card/bank payments from banks (e.g. Jago USD debit) remain `needs_review` (`unsupported_currency`).

## Non-goals

- Changing how missing outbound Jago source pockets require event-specific confirmation (`confirm-jago-outbound-source-pocket` invariant stays intact).
- Auto-deleting historical rows in `notification_account_aliases` via a migration (owner alias management already exists in Settings, and exact account-name precedence neutralizes stale AI aliases immediately).
- Report calculation or UI changes (handled in `show-daily-spending-at-full-price` and `fix-monthly-report-accuracy`).
- Direct edits or SQL writes in production; verification uses local tests and disposable PostgreSQL with synthetic notifications only.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `notification-account-mapping`: exact/stemmed active account names take precedence over AI-learned aliases (owner-confirmed aliases still win), and Bank Jago `"Locked"`/`"Terkunci"` pocket suffixes normalize to their base pocket name.
- `notification-filtering`: Stockbit market-news notifications (including dividend, yield, and commodity alerts quoting foreign-currency figures) are classified as `ignored` rather than `needs_review` (`unsupported_currency`), while real Stockbit trades/deposits and settled foreign-currency bank payments keep their existing handling.

No project capability is added; existing `money`, `personal-data`, and `llm` standards apply.

## Impact

- **Backend:** `backend/app/services/notification_resolution.py`, `backend/app/services/notification_evidence.py`, and focused unit/integration tests in `backend/tests/`.
- **Startup cleanup:** Existing `reclassify_review_noise()` automatically reclassifies unlinked `needs_review` Stockbit news events to `ignored` on next deploy.
- **API & Data:** No schema migration and no breaking API changes.
