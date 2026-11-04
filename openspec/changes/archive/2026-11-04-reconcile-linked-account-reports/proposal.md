## Why

One real money movement can reach the phone as reports from several apps. When one account is seen by two apps, such as a bank pocket that a wallet also shows, both report the same money and the app books it twice. The research note `research/2026-11-04-redundant-notifications.md` (prototypes folder) found 3 confirmed mirrors in 7 weeks, all booked twice and all cleaned by hand, and all from one account observed by two apps.

The per-owner skip patterns of `skip-redundant-notifications` patch this for one app chain. The owner's verdict on 2026-11-04: "this pattern function is more just like a patch for my specific case ... we can't map those into the patterns, even mapped patterns can be replaced and changed anytime ... for public release we should have the resolution for that".

## Expected Outcome

- **Account links:** an account can be linked to the other apps that also report it. The owner confirms this once in Tinjau ("Benar, satu transaksi"). From then on, a second report of that account's money (same direction, same amount, another linked app, within the window) books nothing and asks nothing. It stays visible under "Dilewati otomatis" and can be undone.
- **Generic:** it works for any bank, wallet and pocket combination without hand mapping.
- **Arrival order:** reports book as they arrive, in order. When the report from the other app arrived first and was already booked automatically, it is swapped out safely.
- **Retirement:** skip patterns are converted into account links and retired, together with their Pengaturan page and API.

## Approval

The owner's direction on 2026-11-04, after the research note:
- "2. Ask only is safer and more straight to the point doesn't confuse"
- "4. oke convert and retire"
- "5. ... do the complete implementation, no need option B I think A+C is capable enough"
- "delay in the notification isn't accepted ... the mobile app should show the notification instead of have significant delay ... Delay on the ledger otherwise is accepted"
- "should be per user, don't apply statistic by other user"

**Scope change, 2026-11-04 (option C dropped):** "the C one, I think it's not applicable for real world case as not all apps will give notification realtime, I think preserve transaction order is better then waiting like that. My assumption A is enough". There is no server hold. Each report books as it arrives, and the matching window allows late reports.

The open decisions OD129–OD134 take their recommended defaults (design.md). The agent lists them at acceptance.

**Acceptance, 2026-11-04.** After the summary with its verification, OD129–OD134 and the phone test cases, the owner said "accepted, defaults ok". This change merges first (V47), before `manual-balance-check` (V48).

## What Changes

- **Account links:** a new table `account_report_links` (account, linked institution, source, `stopped_at`), per owner. Links come only from the owner's answer: no presets, and no statistics shared across users.
- **Linked-report rule:** a new rule in `movement_pairing.same_arrival` that replaces `pattern_arrival`:
  - **Match:** two observed legs from different institutions, in the same direction and with the same amount, at most 300 seconds apart. One leg is on the linked account. The other is on it, or names it as its remote. The other's institution is linked to the account.
  - **Result:** the off-account leg becomes a skipped duplicate observation.
  - **Swap:** when the off-account leg arrived first and is untouched and automatic, it is swapped out.
  - **Safeguards:**
    - an opposite-direction candidate (a possible transfer) turns the match into a doubt;
    - a kept leg absorbs at most one report per linked institution;
    - several candidates are doubts;
    - matches between 300 seconds and the pairing window are doubts.
- **Learning:** "Benar, satu transaksi" on "Terlihat dua kali" creates the link (the named account and the other app) instead of a skip pattern. The toast offers "Batalkan".
- **Visibility:**
  - "Dilewati otomatis" lists the reports skipped by a link.
  - The Rekening detail lists each account's linked apps, with "Lepaskan".
  - `GET` and `DELETE /api/ingest/account-links`.
- **Retirement:**
  - migration V47 converts each active skip pattern into a link where the kept app maps to exactly one account, then stops every pattern;
  - pattern matching, `skip_patterns.py`, `/api/ingest/skip-patterns` and the Pengaturan section "Notifikasi ganda" are removed.

## Non-goals

- No server-side hold before booking (option C, dropped by the owner).
- No unified scorer (option B).
- No presets and no cross-user statistics.
- The manual balance check (`manual-balance-check`) is a separate change.
- No companion (Android) change.

## Capabilities

- Modified: `movement-pairing`, `review-queue`, `currency-conversion-and-settings`.
- No new project capability. This builds on the existing money, personal-data and multi-user standards.

## Impact

- **Database:** V47 is additive. It adds a table and a nullable column, and stops patterns. `notification_skip_patterns` and `notification_events.skip_pattern_id` stay, unused, for rollback.
- **Backend:**
  - `services/movement_pairing.py`, `services/notification_application.py`, `services/seen_twice.py`;
  - a new `services/account_links.py`; `services/skip_patterns.py` is removed;
  - `routers/ingest.py`, `routers/review.py`.
- **Frontend:** the Tinjau triage (seen twice, skipped, the toast) and the Rekening detail. The Pengaturan "Notifikasi ganda" page, its API helpers and its query key are removed.
- **Companion:** no change. The ingest response and its timing are unchanged.
