## Context

`recognise-one-arrival-seen-twice` added the one-arrival guard (`same_arrival`). The guard holds a second report in Tinjau when two legs from different apps, in the same direction and with the same amount, within 120 seconds, name each other's accounts. `skip-redundant-notifications` added per-owner skip patterns (redundant institution, direction, kept institution) that skip such reports automatically.

The research found the root cause of mirrors: **one account observed by two apps**. The 1 Nov case: BCA → GoPay Tabungan (a Jago pocket shown in GoPay). GoPay reports "received Tabungan by Jago", which the app books on its Gopay account. Jago reports "received from GoPay", which lands on the default pocket. The owner decided on option A (account links, ask only, per user) and dropped option C (no hold).

**Timing in the research data:**

| Relation | Gap |
| --- | --- |
| Mirror | 0.1–0.6 s |
| Transfer legs (opposite direction) | 0.8–3.8 s |
| Internal hop | 17–45 s |
| Chain hop | 120–150 s |
| Unrelated, same amount | 78–237 s |

## Goals / Non-Goals

- **Goals:**
  - one owner answer per linked account;
  - automatic, reversible skipping afterwards;
  - correctness under any arrival order and under late reports;
  - retire skip patterns without losing the owner's intent.
- **Non-goals:** a hold (C), a scorer (B), presets, cross-user statistics, Android changes.

## Decisions

### D1: an account link is a fact about an account
`account_report_links(user_id, account_id, institution, source, converted_from_pattern_id, created_at, stopped_at)` means "this account is also reported by this institution's app". It holds at most one active link per (owner, account, institution). Sources are `confirmation` (Tinjau) and `converted` (V47).
- **Rejected:** keeping institution pairs, i.e. patterns. They break when an app is reused for another account and don't say which account is shared.

### D2: the linked-report rule
For a new observed leg `L` and a recorded, unlinked observed leg `O`, the pair matches when all of these hold:
- different institutions;
- the same direction and amount;
- post times at most `LINKED_REPORT_WINDOW_SECONDS` apart;
- an active link `(X, I)` exists such that one leg is on `X` exactly ("on X") and the other leg is on `X`, or names `X` (or, when `X` is a root account, any account under it) as its remote ("touches X"), with the touching leg's institution being `I`.

**What a match does:** the leg that is not on `X` is the second report. It becomes a `duplicate_observation` of the on-`X` leg, with `reporting_link_id` and `reviewed_at` set. It books nothing and asks nothing. When both legs are on `X`, the earlier recorded leg is kept.

**Safeguards:**
- **Possible transfer:** when the second report has a recorded, unlinked opposite-direction leg of the same amount on `X` within the pairing window, the pair is a doubt. Both stay recorded, and the new leg is flagged `ambiguous_movement`.
- **Absorb once:** a kept leg that already absorbed a second report from institution `I` is not a candidate for another report from `I`.
- **Uniqueness:** two or more candidates are a doubt.
- **Late reports:** a candidate further than the window but inside `notification_pairing_window_seconds` is a doubt. It is never skipped.

**Fallback:** without a match, the existing one-arrival guard decides as today ("Terlihat dua kali" asks).

### D3: the window allows late reports (OD129)
`LINKED_REPORT_WINDOW_SECONDS = 300`.
- **Why it can be wider:** mirrors arrive within 0.6 s when both apps notify in real time. The owner says "not all apps will give notification realtime", and post times are the posting app's own times, so a late app shifts the gap.
- **Why 300 s is safe:** the linked rule is narrower than the generic guard. It needs the linked account, the same direction, the absorb-once rule and the possible-transfer safeguard. So a coincidence needs two unrelated same-direction, same-amount reports of one linked account within 5 minutes, without either app's own mirror. The research saw unrelated same-amount pairs 78–237 s apart, but none of them on one linked account in one direction.
- **Beyond 300 s:** up to the 900 s pairing window, matches stay doubts that ask.
- **Rejected:** keeping 120 s. A late GoPay report 3 minutes after Jago's would be booked twice.

### D4: arrival order is preserved (option C dropped)
- **As today:** each report books when it is processed.
- **On-account report first:** when the on-`X` report arrives first, the later off-`X` report is skipped at once.
- **Off-account report first:** when it was recorded first, the later on-`X` report keeps the money. The earlier leg's transaction is removed in the same database transaction (the existing `drop_observation`), and its event becomes the skipped duplicate. This happens only while that leg is automatic, unlinked, unanswered and not edited by the owner (`replaceable_leg`). Otherwise the new report waits in "Terlihat dua kali" (unchanged OD119 behaviour).
- **Deferred pass:** the deferred re-pairing pass applies the same rule, from the later observation.

### D5: learning a link from "Benar, satu transaksi" (ask only, per user)
The acknowledgement of a duplicate `D` of kept leg `K` creates the link `(X, I)`:
- **`X` is** `K`'s account, unless `K` was a default-pocket fallback and `D` named its account. Then `X` is `D`'s interpreted account.
- **`I` is** the institution of the other report.
- **Idempotent:** a repeat confirmation creates nothing.
- **Kept leg not from a notification:** nothing is learned.
- **The answer:** the response returns the link with labels. The toast reads "{rekening} juga dilaporkan {aplikasi}. Laporan serupa dilewati otomatis." with "Batalkan", which stops the link.

**OD130, wording:** the existing button "Benar, satu transaksi" stays. The item adds the fact "Rekening yang sama · {rekening} juga terlihat di {aplikasi}", so the question reads "is this one account seen in both apps?" without a new flow.

### D6: visibility and control
**API:**
- `GET /api/ingest/account-links?account_id=` lists active links with labels and skip counts.
- `DELETE /api/ingest/account-links/{id}` stops one.
- Both are session-only (`get_session_user`), so the companion key gets 403. Another owner's link gets 404 `account_link_not_found`.

**OD131, where links are shown:** the Rekening detail dialog shows "Juga dilaporkan" with each linked app and "Lepaskan" (two-step). There is no Pengaturan page and no "add" form, because links come only from answers.

"Dilewati otomatis" lists link-skipped reports for 30 days, with "Catat terpisah" and undo, and names the link.

### D7: converting and retiring the skip patterns (OD132)
V47:
- **Conversion:** for each active pattern `(R, dir, K)`, if the owner's notification aliases of institution `K` resolve to exactly one account `A`, it inserts the link `(A, R, 'converted', pattern)`.
- **Stopping:** every active pattern is stopped (`stopped_at = NOW()`).
- **Skipped events:** they get `reporting_link_id` from their converted pattern.
- **Unresolved patterns:** they convert to nothing. Their next mirror waits in "Terlihat dua kali", and one answer creates the link, so the worst case is one question.
- **Code removed:** pattern matching, `skip_patterns.py`, the API and the Pengaturan section.
- **Production:** 0 patterns and 0 skipped events, checked read-only on 2026-11-04.
- **Rejected:** keeping patterns as a manual override. The owner chose "convert and retire".

### D8: no change to capture timing
The ingest response, the worker and the companion's polling (5, 10, 20, 40, 60 s) are unchanged. The owner said "delay in the notification isn't accepted".

## Data, API and failure behaviour

**Data:**
- `account_report_links` cascades with its account and owner.
- `notification_events.reporting_link_id` is set NULL when its link is deleted.
- Stopping a link keeps the earlier skips.

**Failures:**
- With no link, the guard behaves as before.
- With a link but an unresolved remote, there is no match. The fallback is a visible extra row, never lost money.
- Every skipped report can be recorded separately, with undo.

## Migration and rollback

V47 is additive. It creates a table, an index and a nullable column, converts patterns and stops them. **Rollback:** revert the release. The older release reads the patterns again, but they are stopped, so it would ask in Tinjau. The link table stays, unused. Re-activating patterns is not needed (0 in production).

## Risks

- **Real income dropped:** a real same-direction income touching a linked account within 5 minutes of its mirror could be skipped wrongly. This is mitigated by the absorb-once rule, the possible-transfer doubt and "Catat terpisah" with undo.
- **Wrong link:** an owner might confirm a link wrongly. "Batalkan" and "Lepaskan" stop it.

## Open decisions (defaults applied)

| | Decision | Default |
| --- | --- | --- |
| OD129 | Linked-report window | 300 s auto. Up to the 900 s pairing window, doubt. |
| OD130 | Ask wording | Keep "Benar, satu transaksi" and add the "Rekening yang sama" fact |
| OD131 | Where links are shown | The Rekening detail, with "Lepaskan". No Pengaturan page. |
| OD132 | Unresolved patterns | Stopped without a link. The next mirror asks once. |
| OD133 | Absorb once | A kept leg absorbs at most one report per linked institution |
| OD134 | Removed Notifikasi ganda address | `/settings/notifikasi-ganda` shows the not-found page, like any unknown section |
