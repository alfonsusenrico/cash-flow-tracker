# Design: fix-notification-mapping-and-stockbit-noise

## Context

See `proposal.md` ("Why"). Notification account resolution lives in `backend/app/services/notification_resolution.py` and noise/evidence gating lives in `backend/app/services/notification_evidence.py`, with startup cleanup in `backend/app/services/notification_processing.py` (`reclassify_review_noise`).

## Goals / Non-Goals

- **Goals:** Ensure an active account's own name beats any AI-learned alias while keeping explicit owner-confirmed overrides authoritative; normalize Bank Jago `"Locked"`/`"Terkunci"` pocket suffixes to their base pocket name; filter Stockbit market-news alerts cleanly as `ignored` before `unsupported_currency` fires.
- **Non-Goals:** Altering missing outbound Jago source-pocket confirmation, adding a database migration to prune historical alias rows, or modifying reporting logic.

## Decisions

### 1. Account resolution precedence: owner alias → active account name → AI alias

In `backend/app/services/notification_resolution.py`:
- Extend `aliased_account(accounts, institution, name_normalized, *, sources: set[str] | None = None)` to filter `learned.get("confirmed_by")` when `sources` is provided (defaulting to all sources for callers that do not split precedence).
- In `pocket_endpoint()` and `institution_parent()`:
  1. Check `aliased_account(..., sources={"owner"})` first so an owner's explicit mapping override always wins.
  2. Next, check active account name matching (exact normalized name, then singular/plural stem match in `pocket_endpoint`, or top-level/child institution match in `institution_parent`).
  3. Only when active account names do not yield a unique match, check `aliased_account(..., sources={"ai"})`.
  4. Otherwise raise `UnresolvedName`.

**Rejected:** Deleting AI aliases only when an account is created or renamed. Checking precedence at resolution time handles already-stored stale AI aliases immediately without a data migration or missed rename paths.

### 2. Normalize Bank Jago `"Locked"` / `"Terkunci"` pocket suffixes

In `normalize_name()` (`backend/app/services/notification_resolution.py`):
- Expand the stripped stopword regex from `\b(?:pocket|kantong|your|my)\b` to `\b(?:pocket|kantong|your|my|locked|terkunci)\b`.
- `"Bayar Kredit Locked"` and `"Bayar Kredit Locked Pocket"` normalize to `"bayar kredit"`, matching the active `"Bayar Kredit"` pocket directly.

**Rejected:** Hardcoding individual pocket names. Stripping the Jago UI lock status word works for every locked pocket of any user.

### 3. Ignore Stockbit market news ahead of foreign-currency checks

In `backend/app/services/notification_evidence.py`:
- Expand `MARKET_INFO_PATTERN` with broker/dividend/commodity news signals: `dividen\s+interim`, `cum\s+dividen`, `ex\s+dividen`, `\byield\b`, `\bemiten\b`, `\bsell[\s-]?off\b`, `/\s*(?:saham|ton|barel|ons)\b`, `asumsi\s+kurs`, `tahun\s+buku`.
- In `collect_facts()`, for `institution == "stockbit"`, when `not recognised` and `not settled`, classify as `ignore_reason = "promotional_or_market"` (`status = "ignored"`) before `FOREIGN_CURRENCY_PATTERN` can set `unsupported_currency`.
- Real Stockbit trade matches (`STOCKBIT_MATCH_PATTERN`) and RDN deposits (`STOCKBIT_DEPOSIT_PATTERN`) set `parsed.is_financial = True` and `recognised = True`, so they continue through the normal recording path.
- Non-Stockbit notifications (e.g. a Bank Jago settled USD debit card payment) still reach `FOREIGN_CURRENCY_PATTERN` and route to `needs_review` with `unsupported_currency`.
- Existing startup `reclassify_review_noise()` moves unlinked Stockbit news events already in `needs_review` to `ignored` automatically on deploy.

**Rejected:** Ignoring all Stockbit notifications unconditionally (would drop real stock trade matches and RDN deposit confirmations).

## Failure Behavior, Compatibility, and Rollback

- **Compatibility:** No API contract or database schema changes; older companion builds work identically.
- **Safety:** `reclassify_review_noise()` touches only `processing_state = 'needs_review' AND transaction_id IS NULL`.
- **Rollback:** Reverting the deploy restores the previous resolution order and filter rules with zero schema impact.

## Risks / Trade-offs

- [If Stockbit introduces a brand-new settled cash-event wording not yet in `SETTLED_PATTERN` or `notification_parser.py`, it could be ignored instead of reviewed] → `SETTLED_PATTERN` already covers Indonesian and English settlement verbs (`berhasil`, `successful`, `completed`, `match di harga`, `sudah dapat digunakan`, `deposit berhasil`, `pemasukan`, `pengeluaran`, `diterima`, `received`), so only non-settled market commentary is ignored.
