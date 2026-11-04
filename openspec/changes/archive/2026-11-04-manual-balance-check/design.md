## Context

- **Reconcile:** `POST /api/accounts/{id}/reconcile` sets one account's own ledger balance to a typed value by inserting a "Penyesuaian Saldo" row (source `reconcile`). The web offers it only for accounts without pockets, because a parent's figure is its own balance plus its pockets, and the adjustment would land on the parent alone.
- **Ledger:** a balance is `initial_balance + income − expense` over the account's own rows.
- **Notification reports that booked nothing** stay in `notification_events`:
  - second observations (`provenance_kind = 'duplicate_observation'`), held in "Terlihat dua kali" or skipped by a pattern;
  - events that wait for an answer, are still queued, or failed.

  Their amount and direction come from `collect_facts`, and their account from `interpretation.account_id`.
- **The research note** (`cash-flow-tracker-prototypes/research/2026-11-04-redundant-notifications.md`) found that, in the owner's data, duplicates are same-direction, same-amount reports seconds apart from two apps, and that notifications almost never carry balances.

## Goals and non-goals

- **Goals:** a check the owner runs at will that names likely causes with links, and a remembered matched point.
- **Non-goals:** automatic correction, scheduled check-ins, AI, investment valuations.

## Decisions

- **D1: read-only check.**
  - `POST /api/accounts/{id}/balance-check` with `{ "actual_balance": int ≥ 0 }` runs in a `READ ONLY` transaction.
  - POST keeps the typed balance out of URLs and access logs. Rejected: GET with a query string.
  - **Response:**
    - `recorded_balance`, `difference = actual − recorded`, `since` (`{at, basis}`, basis `matched`, `adjusted` or `window`);
    - `candidates` (at most 20, newest first) and `unattributed` (reports whose account is unknown);
    - `explanation` (the candidate keys whose effects sum to the difference, or `null`);
    - `last_report_at`, `can_adjust`.
  - **Errors:** another owner's or a missing account answers 404. A product, an amount-tracked account, or a parent with any non-liquid pocket answers 409 `balance_check_not_liquid`. A negative or non-integer balance answers 422.
- **D2: scope of an account.**
  - A standalone account or a pocket is checked on its own rows.
  - A parent is checked as the group the bank app shows: its own rows plus every pocket's. Each row is counted once (no double counting).
  - `can_adjust` is true only without pockets, the same rule the web applies to "Sesuaikan saldo".
- **D3: starting point.** The window starts at the latest of:
  - the account's last matched point (`account_balance_matches`);
  - its last `reconcile` adjustment row. For a parent, the latest adjustment on any account of the group, since each one also proves a known figure.
  - **OD140:** with neither, the window is the last 30 days.

  The response states which basis applied.
- **D4: candidates.** All are deterministic. Each has a signed `effect`: how much the recorded balance would change if the cause were fixed. So the causes that explain a difference `d` are those whose effects sum to `d`.

  | Kind | Found when | Effect |
  | --- | --- | --- |
  | `duplicate` | A notification row on the group is not a movement leg, and another of the owner's rows has the same type and amount within 300 s (OD141), on another account or from another app. | Removing the row: −amount for an income, +amount for an expense |
  | `unpaired` | A row on the group is filed as Internal Movement without a movement or fee link. | Removing it, as above. Valid only if the movement never happened. |
  | `held` | A second observation for an account of the group waits in "Terlihat dua kali". | Recording it: +amount for an income, −amount for an expense |
  | `skipped` | A second observation for an account of the group was skipped by a pattern. | Recording it, as above |
  | `unrecorded` | A report for an account of the group waits for an answer, or failed, with no row. | Recording it, as above |
  | `pending` | A report is still queued or processing. | Recording it, as above, when its account is known |

  - Reports whose account is unknown are listed under `unattributed`, with amount and app, but never used in an explanation.
  - **OD142, capture gap:** when no report from any app arrived for 12 hours, `last_report_at` is shown as a hint that the phone may not be sending notifications.
- **D5: explanation.**
  - The smallest set of one to three attributed candidates whose effects sum exactly to the difference. Among equal sizes, the set with the newest rows wins.
  - With at most 20 candidates the search is at most 1 350 sums.
  - Rejected: nearest-sum or tolerance matches, which would present a guess as a cause.
- **D6: matched point (V48).**
  - `POST /api/accounts/{id}/balance-matches` with `{ "actual_balance" }` recomputes the recorded balance under a lock. A mismatch answers 409 `balance_mismatch` with the `difference`. Otherwise it inserts `{user_id, account_id, balance, matched_at}`.
  - **OD143:** only an exact match is stored. The owner corrects a gap first, either with "Sesuaikan saldo" or by fixing rows.
  - Deleting the account deletes its matched points (`ON DELETE CASCADE`).
- **D7: web dialog.**
  - **Entry:** "Cocokkan saldo" sits in the account detail's actions, next to "Sesuaikan Saldo", for every eligible account (D1, D2).
  - **Input:** one money field, empty at the start (OD144), so the owner types the bank's figure rather than confirming the app's. Then "Periksa".
  - **Result:**
    - three figures: Tercatat, Di aplikasi bank, Selisih;
    - the basis line, for example "Sejak saldo cocok 2 Nov" or "30 hari terakhir";
    - when a set explains the difference, a highlighted line "Selisih ini sama dengan …";
    - the cause list, each with its amount, date, app and a link: a row opens `/ledger?edit=<id>`, and a report opens Tinjau.
  - **Actions:**
    - with a difference of 0: "Saldo cocok";
    - with a difference: "Sesuaikan saldo" (when `can_adjust`) and "Periksa lagi".
  - **OD145:** "Sesuaikan saldo" opened from this dialog records a matched point right after it succeeds, so the next check starts there.
- **D8: privacy.** Responses carry the owner's own figures only. The log records no amounts or texts. Notification texts are never returned. Only app labels, amounts, directions and times are.

## Data and migration

- **V48:** `account_balance_matches(id uuid pk default gen_random_uuid(), user_id uuid not null references users on delete cascade, account_id uuid not null references accounts on delete cascade, balance bigint not null check (balance >= 0), matched_at timestamptz not null default now())`, with an index on `(user_id, account_id, matched_at desc)`.
- It is additive and idempotent (`IF NOT EXISTS`).
- **Rollback:** leave the table in place, because the previous release ignores it.
- **Order:** V48 must deploy after the parallel change's V47 (proposal, Impact).

## Open decisions (defaults applied)

| | Decision | Default |
| --- | --- | --- |
| OD140 | Window without any matched point or adjustment | 30 days |
| OD141 | Time gap for a possible double booking | 300 s, wider than the 120 s guard, because the check only suggests |
| OD142 | Capture-gap hint | No report for 12 hours |
| OD143 | "Saldo cocok" with a difference | Refused (409). Fix the gap first. |
| OD144 | Input start value | Empty, never the recorded balance |
| OD145 | Matched point after "Sesuaikan saldo" from the dialog | Recorded automatically |
| OD146 | Parent with pockets | Checked as a group. "Sesuaikan saldo" stays limited to accounts without pockets. |

## Risks

- **Coincidences:** a coincidence can look like a duplicate, such as two unrelated purchases of the same amount. The dialog says "kemungkinan", and nothing changes until the owner acts.
- **Old rows:** a long window over a busy account can list many candidates. The list is capped at 20, and the matched point keeps later windows short.
