# Verification: skip-redundant-notifications

**Revisions under test**
- `bcecf69`: the regression tests on the old code (main `62285dd` plus the approved plan).
- `ef47e6e`: the backend, migration V46 and evaluation.
- `4461043`: the web.
- The commit holding this record adds only this file, tasks.md and the user guide.

**Environment and data**
- Local only: Postgres 16 in `cft-redundant-pg` (55602), Flyway 10.
- For the browser: the API on 8302, `next start` on 3303 behind the loopback proxy on 3302, and headless Brave on 9602.
- All data is synthetic:
  - the test fixtures ("BCA Harian", "Bank Jago · Kantong Utama", "GoPay");
  - the restored seed of `show-movement-routes-and-centered-dialogs` (owner `raka_contoh`);
  - a scratchpad SQL seed with one waiting duplicate, two skipped reports and one pattern.
- No production write. One read-only production count (7.2).

## 1. Regression tests first (tasks 1.1, 1.2)

`backend/tests/test_duplicate_arrivals.py` gains 11 tests, committed alone as `bcecf69`.

**On the old code** (schema V45, same commit) all 11 fail for the expected reasons:
- 10 fail with `UndefinedTable: relation "notification_skip_patterns" does not exist`, because no pattern can exist;
- `test_without_a_pattern_the_guard_still_holds_the_report` fails with `KeyError: 'skip_pattern_id'`, because the event has no pattern column.

**The 21 existing tests in the file pass on the old code.**

**After the change all of them pass:**
- **1.1 (a):** the incident order (GoPay, Jago 0.13 s later, myBCA 3.4 s later) with the pattern (jago, income, gopay):
  - the Jago event is a `duplicate_observation` with `skip_pattern_id` and `reviewed_at`;
  - there is no `seen_twice` item;
  - myBCA pairs with GoPay;
  - balances are BCA 500 000, GoPay 1 500 000, Kantong Utama 1 000 000 (unchanged).
- **1.1 (b):** Jago first, with the pattern. The Jago leg is removed in the same transaction, and its event is the skipped duplicate of the GoPay leg. The balances are the same as in (a).
- **1.1 (c):** Jago first and edited by the owner. The Jago leg stays, and the GoPay event is an ordinary duplicate (no pattern, no `reviewed_at`) waiting in `seen_twice`.
- **1.2, nothing is skipped in these cases:**
  - no pattern: the guard still holds the report, waiting;
  - a Jago income with no GoPay report: it records;
  - another amount;
  - a pattern for the other direction: the guard still holds, nothing is skipped;
  - an unresolved sender ("from Rina Wulandari"): both record;
  - two possible GoPay reports: `ambiguous_movement`, 3 incomes;
  - a stopped pattern;
  - another owner's pattern.

## 2. The model's time context is removed (tasks 2.1–2.3)

**What changed:**
- The six files the neighbour commit `c151584` added to are restored to their state before it: `notification_context.py`, `config.py`, `notification_processing.py`, `notification_dry_run.py`, `docker-compose.yml` and `.env.example`. No later commit had touched them.
- `notification_interpretation.py` and the prompt are restored to `-10`'s text and schema, with `PROMPT_VERSION = "notification-interpretation-12"`.
- `hold_possible_duplicate`, the `same_money_event_id` parameter and `test_notification_neighbours.py` are removed.

**Evidence:**
- `rg "neighbour|same_money|possible_duplicate_observation|invalid_neighbour_reference|NEIGHBOUR"` over `backend/app`, `backend/evaluation`, `docker-compose.yml` and `.env.example` finds nothing.
- In `frontend/src` the only remaining match is the ledger page's unrelated "neighbour" row variable for focus after a delete.
- `test_ai_unexpected_pillar_is_recorded` asserts the prompt version `notification-interpretation-12`.
- In Tinjau, the held variant ("Catat transaksi ini", "Abaikan laporan ini", `kind: "held"`) and its reason label are gone. `seen_twice` lists only `duplicate_observation` events.

## 3. Patterns and migration (tasks 3.1–3.4)

**V46** `db/migrations/V46__notification_skip_patterns.sql`:
- **Fresh database:** Flyway applies 46 migrations to a fresh database.
- **On a V45 database**, restored at `-target=45` and holding one event `needs_review` with `possible_duplicate_observation` (attempts 2, generation 3) and one unrelated `needs_review` / `model_uncertain` event:
  - V46 applies;
  - the held event becomes `queued` with attempt 0, generation 4, and its interpretation and error cleared;
  - the other event is unchanged.
- **Second run:** the requeue statement run again reports `UPDATE 0`.
- **Seed restores:** the movement-routes seed restored from V44 migrates to V46.

**Behaviour, with its tests:**
- **`skip_patterns.py`:**
  - validation: 422 for the same app twice, an unknown direction, an unknown app, or an app that never sent a notification;
  - the active-pattern unique index, with `ON CONFLICT DO NOTHING`, for idempotence;
  - owner scoping: another owner's DELETE gets 404, and their GET lists nothing.
- **The pattern branch in `same_arrival`:**
  - reverse-order replacement through `replaceable_leg` and `drop_observation`;
  - the same rules in `repair_duplicate_arrivals`: `test_deferred_pass_applies_a_pattern_once` gives 1, then 0 on the second pass, and Kantong Utama stays 1 000 000.
- **Learning:**
  - `test_acknowledging_learns_the_pattern_once`: the first confirmation creates `(jago, income, gopay, confirmation)` with `created: true`, a repeat returns the same id with `created: false`, and exactly one row exists. The next repeat, 10 minutes later, is skipped without a Tinjau item.
  - `test_acknowledging_a_duplicate_of_a_typed_row_learns_nothing`: `pattern: null`.

## 4. API (tasks 4.1, 4.2)

**`backend/tests/test_skip_patterns.py`, 9 tests:**
- list, add (201), stop twice (200, idempotent), and add again after stopping;
- the skipped count and last time;
- 409 `skip_pattern_exists`;
- four 422 `invalid_skip_pattern` cases;
- another owner: an empty list, and 404 `skip_pattern_not_found` for both the owner's id and an unknown id;
- a request with only a Bearer key (the companion) gets 403 `session_required` on GET, POST and DELETE, because the routes use `get_session_user`.

**The queue:**
- `test_skipped_reports_are_listed_and_can_be_recorded_separately`:
  - one item with `kind: "skipped"`, the kept leg's movement route and the pattern labels;
  - `counts.skipped` 1 and `counts.seen_twice` 0;
  - the summary's attention and unreviewed are 0, the same as the queue's;
  - "Catat terpisah" records Kantong Utama's own leg and clears `skip_pattern_id`, and the item leaves `skipped`;
  - "Urungkan" removes the leg and restores the skipped state, with the same pattern and `reviewed_at`.
- `test_another_owner_cannot_record_a_skipped_report`: 404 `seen_twice_not_found`.

## 5. Web (tasks 5.1–5.3)

**Vitest:**
- `lib/triage.test.ts`: the "Dilewati otomatis" folder (`lewat`) is optional, outside "Semua" and the badge, and reachable as `?folder=lewat`. An older queue without `skipped` reads as empty.
- `app/review/page.test.tsx`:
  - "Benar, satu transaksi" shows "Laporan Bank Jago serupa akan dilewati otomatis." with "Batalkan";
  - "Batalkan" calls `DELETE /api/ingest/skip-patterns/pattern-1` and says "Pola dibatalkan. Laporan Bank Jago serupa akan ditanyakan lagi.";
  - a skipped item shows its pattern line and offers only "Catat terpisah", with 0 axe violations.
- `components/settings/SkipPatterns.test.tsx`:
  - the list with its origin and count;
  - the two-step "Berhenti melewati" / "Ya, berhenti";
  - adding by hand, where the kept-app choices exclude the skipped app and only seen apps are offered;
  - the 409 and 422 messages in the card;
  - the two-app requirement;
  - the load error;
  - 0 axe violations.
- `app/settings/layout.test.tsx`: nine sections, "Notifikasi ganda" after "Pemetaan notifikasi" in the Akun group, at `/settings/notifikasi-ganda`.

**Browser check:** run at `4461043` after a rebuild, with one scratchpad script for every cell.
- **Tinjau "Terlihat dua kali":** an item open.
- **Tinjau "Dilewati otomatis":** an item open, with the folds "Terlihat dua kali 1 opsional" and "Dilewati otomatis 2 30 hari terakhir".
- **Pengaturan "Notifikasi ganda":**
  - the list;
  - "Simpan pola" for the existing pattern shows the 409 message "Pola ini sudah ada." in the card;
  - "Berhenti melewati", then "Ya, berhenti", leaves the empty state;
  - adding again lists "Bank Jago, uang masuk · … ditambahkan di sini".

| Cell | Views | axe serious/critical | Overflow | Console errors | Targets < 24 px |
| --- | --- | --- | --- | --- | --- |
| 1440 light | 3 | 0 | 0 | 0 | 0 |
| 1440 dark | 3 | 0 | 0 | 0 | 0 |
| 1024 light | 3 | 0 | 0 | 0 | 0 |
| 390 dark | 3 | 0 | 0 | 0 | 0 |

- **Network log:** the only browser log entry is the expected `409 (Conflict)` network response from the duplicate check. It isn't counted as a console error.
- **Keyboard:**
  - From the card heading, Tab reaches "Berhenti melewati", then the three selects, then the page chrome. "Simpan pola" is disabled until both apps are chosen.
  - A probe with both chosen: Tab from "Aplikasi yang dipakai" lands on the enabled "Simpan pola" with a solid focus outline.
- **Found and fixed during the check:** the empty state used the formal "Anda", which the copy register forbids in Pengaturan. It reads "Belum ada pola. Pola ditambahkan lewat "Benar, satu transaksi" di Tinjau, atau di sini." Design D7 is updated to match.
- **Screenshots** were all read by the agent. They are in the session scratchpad, `redundant-web/shots/`: `tinjau-dua-*`, `tinjau-lewat-*`, `pengaturan-*` and `pengaturan-409-*` for each cell, plus `report.json`.

## 6. Evaluation (tasks 6.1, 6.2)

- **6.1:**
  - the corpus is `synthetic-notifications-5`, and its five sequence cases carry no neighbours and expect plain observed legs;
  - the benchmark drops `same_money`, and its unsafe-write rule is again identical to `-10`'s;
  - `test_notification_cases.py` and `test_notification_benchmark.py`: 105 passed.
- **6.2, the live benchmark:** run on 2026-11-04 with the owner's key, which the owner copied into the two worktrees and which was deleted right after the runs.
  - **Runs:** `--live --model gpt-6-luna --reasoning-efforts none --repeats 3 --partition all --max-estimated-usd 0.65`. `-12` ran at `2968f33` on `synthetic-notifications-5`, and `-11` ran at `62285dd` on `synthetic-notifications-4`. Both trees were clean. `low` was not run: the 2026-11-04 run of `-11` showed `none` scoring higher at half the cost, and production uses `none`.
  - **Cost:** $0.18 at the benchmark's constants, about $0.09 at gpt-6-luna's listed $0.10 / $0.50 per million tokens.

  | Prompt | Composite | Safety gate | Unsafe writes | Record recall | Movement P / R | Classification | Schema | Invalid output | p50 | Input / output tokens |
  | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
  | `-11` | 74.4 | pass | 0 | 79.4% | 81.0% / 63.0% | 57.4% | 80.6% | 21 of 108 | 2.27 s | 379 977 / 16 182 |
  | `-12` | 87.0 | pass | 0 | 96.1% | 100% / 55.6% | 57.4% | 98.1% | 2 of 108 | 2.24 s | 348 345 / 15 484 |

  - **Result:** `-12` is better than `-11`:
    - composite +12.6;
    - invalid outputs fall from 21 to 2;
    - record recall +16.7;
    - movement precision reaches 100%;
    - input tokens fall 8%.

    Movement recall is 7.4 points lower, which the deterministic pairing and guard cover. The quality gate fails for both, as it does for every prompt measured so far.
  - **Variance:** `-11` scored 81.9 with 10 invalid outputs in the earlier run of the same configuration, so three-repeat runs vary by several points. `-12` is ahead in both comparisons.
  - **Sequence cases:** all five record as plain observed legs on `-12`. The myBCA debit failed with invalid output in all three `-11` requests.

## 7. Verification (tasks 7.1, 7.2)

| Check | Revision | Result |
| --- | --- | --- |
| Backend full suite, fresh V46 database | `ef47e6e` | 1798 passed, 0 failed, 0 skipped |
| Frontend `npm ci`, type-check, lint, build | `4461043` | all exit 0, lint 0 warnings |
| Vitest | `4461043` | 74 files, 673 passed, 0 unhandled errors |
| `openspec validate --all --strict` | this commit | 80 passed; after an archive rehearsal on a copy, 79 passed, no warnings, no placeholder Purpose |
| `git diff --check` | this commit | clean |

**Unchanged behaviour without a pattern.** The same seed was restored twice: at V45 for `main` (`62285dd`, its own code) and at V46 for this branch. Each was read and written through its own API with a cookie session:
- the reads `/api/accounts`, `/api/dashboard/cycle-summary`, `/api/review/summary` and `/api/transactions`;
- the ingest of four synthetic notifications: GoPay credit, Jago "received from GoPay" 0.13 s later, myBCA debit 3.4 s later, and a Jago purchase;
- the same reads again.

The results, with ids, timestamps, `record_key`, the cycle's live `end` and the new rows' `partner_id` removed:
- **Reads:** all equal, before and after the ingest.
- **Ingest:** all four results equal. On both releases the Jago credit's result equals the GoPay credit's (the guard's duplicate), and the myBCA debit becomes "BCA · ATM → Gopay".

**7.2, production, read-only:**
- 0 events are `needs_review` with `possible_duplicate_observation` (0 owners), so V46's requeue changes nothing there;
- production is at V45.

## Not verified

- A screen reader, a real phone, and the Android companion's wording for a skipped report. The companion receives the kept leg's result, as for any duplicate observation.
- The live benchmark (6.2), awaiting the key.
