# Verification: recognise-one-arrival-seen-twice

**Revision tested:** code at `272bd2a` on `fix/arrival-seen-by-two-apps`. The documentation commits after it change no code.

**Environment:**
- PostgreSQL 16 in the container `cft-arrival-pg` on 127.0.0.1:55582, migrated by Flyway 10 to V45 as CI does.
- Backend on Python 3.12.12 from `backend/requirements-dev.txt`.
- Frontend on Node 22 after `npm ci`.
- Browser: headless Brave over CDP on 9582. API 8282, Next 3283 behind the loopback proxy on 3282. For the identities, `main` (`0a342a9`) and this branch ran side by side on 8284–8286.

**Data:** synthetic only. The incident is rebuilt with the owner "Raka Purnama", the accounts "BCA Harian", "Bank Jago" with the default pocket "Kantong Utama" (plus "Tabungan"), and "GoPay". The identity seed is a second owner, "Sari Wulan". Production was read only, for the detector and the 04:57 question (D7), in a read-only transaction with `default_transaction_read_only=on`.

## 1. Reproduce

The regression tests in `backend/tests/test_duplicate_arrivals.py` were committed first (`73ce2cc`) and run against the unchanged code. All four failed for the expected reasons:

| Test | Failure on the unchanged code |
| --- | --- |
| `test_gopay_then_jago_then_mybca_books_one_movement` | Three rows: a second income on Kantong Utama, and the myBCA debit left unpaired (`ambiguous_movement`) |
| `test_jago_first_keeps_the_named_gopay_leg` | The same, in reverse order |
| `test_transfer_answer_without_counterpart_keeps_awaiting` | The row is `reviewed`, not `needs_review` with `awaiting_counterpart` |
| `test_transfer_answer_teaches_no_memory` | Memory written for `signature:jago|income|unknown` |

Earlier, an exploratory replay on the unchanged code showed the incident's fingerprint: the Jago event recorded an income on Kantong Utama with `uncertain_category`, `purpose_question` and `ambiguous_movement`, and the myBCA event ended with `error_code = ambiguous_movement`.

All four pass from `9a71d45` (guard) and `f5e27b9` (answers and memory) on.

## 2. Automated checks

| Check | Result |
| --- | --- |
| Backend, full suite (`pytest tests/ -ra`, V45, fresh database) | **1784 passed**, 0 failed, 0 skipped |
| New backend tests | `test_duplicate_arrivals.py` 21, `test_notification_neighbours.py` 11, `test_find_duplicate_arrivals.py` 3 |
| Evaluation corpus and scoring, offline (`test_notification_cases.py`, `test_notification_benchmark.py`) | 107 passed, fixture `synthetic-notifications-4` |
| Frontend `npm ci`, `type-check`, `lint` | pass, 0 lint warnings |
| Frontend `npm test` | 72 files, **641 passed**, 0 unhandled errors |
| Frontend `npm run build` | pass |
| V45 applied twice more on a migrated database | both runs succeed (IF NOT EXISTS, constraint replaced) |
| `openspec validate --all --strict` | 80 passed; after an archive rehearsal 79 passed, 0 warnings, no placeholder Purpose |
| `git diff --check` | clean |

Covered by the new tests:
- **The guard:**
  - Both arrival orders.
  - Negative cases: two real transfers that don't name each other, the same app twice, a mirror at 121 s (flagged), two possible originals (flagged).
  - An owner-edited earlier leg is never removed (OD119).
  - The companion result of a duplicate becomes the movement.
  - The deferred pass applies the rule once and a second pass changes nothing.
- **Transfer answers:**
  - An answer without a counterpart stays `awaiting_counterpart` in Tinjau's `awaiting`.
  - An answer with one counterpart links it, and the answer's undo unlinks both.
- **Memory:**
  - Transfer answers teach nothing.
  - Linking retires model memory, by automatic pairing and by the owner's merge; owner memory survives.
  - Memory in Internal Movement learned earlier is not applied (OD121).
- **Neighbours:**
  - Bounded to 20, nearest first, listed by time, no other event's text.
  - Owner-isolated.
  - A failed query and an over-budget context are incomplete, and the farthest neighbour is dropped first.
  - An invalid window setting is `processor_configuration_invalid`.
- **Model boundary:**
  - A confirmed proposal becomes a duplicate.
  - An unconfirmed one is held and books nothing.
  - An unknown id is rejected and the guard still decides.
  - A queued neighbour is rejected.
  - A null proposal with a guard match becomes a duplicate.
  - A failing model is still guarded.
  - `prompt_version = notification-interpretation-11` is stored.
- **Tinjau API:**
  - `seen_twice` with its kept leg's route.
  - Counts: `unreviewed` +1 and `attention` 0 (OD117).
  - Acknowledge is idempotent.
  - "Catat terpisah" books its own leg, and undo removes it and restores the duplicate.
  - A held item can be recorded.
  - Another owner gets 404.

## 3. Browser

`node flow.mjs` on the production build, with a duplicate item and a held item, each at three cells:

| Cell | Item | axe serious/critical | Overflow | Console errors | Targets under 24 px |
| --- | --- | --- | --- | --- | --- |
| 1440 light | duplicate, held | 0, 0 | no, no | 0, 0 | 0, 0 |
| 1440 dark | duplicate, held | 0, 0 | no, no | 0, 0 | 0, 0 |
| 390 dark | duplicate, held | 0, 0 | no, no | 0, 0 | 0, 0 |

I read every screenshot. The first round showed two faults, both fixed in `272bd2a` and re-shot:
- The facts list used the centered summary grid and sat indented.
- A held report with no match offered "Benar, satu transaksi" first, which would leave the money unbooked.

`node actions.mjs` drove the real UI:
1. "Catat terpisah" booked the Jago leg, which then asked "Apa ini?" (attention 0 → 1).
2. "Urungkan" brought the item back.
3. "Benar, satu transaksi" removed it.
4. No console errors.

## 4. Money identities

Seed A (no second observations) was ingested through `main`. Seed B, the same events, was ingested through this branch. Figures were compared through `/api/accounts`, `/api/dashboard/cycle-summary`, `/api/review/summary` and the ledger rows:

| Comparison | Balances | Cycle (Keluar, Masuk, remaining, spending) | Review counts | Ledger rows (8) |
| --- | --- | --- | --- | --- |
| A read by `main` vs A read by the branch | equal | equal | equal | equal |
| A by `main` vs B ingested and read by the branch | equal | equal | equal | equal |

On the incident seed after the browser actions:
- BCA Harian Rp 1.500.000 (2.000.000 − 500.000).
- GoPay Rp 600.000 (100.000 + 500.000).
- Kantong Utama Rp 300.000, unchanged.
- The held Rp 250.000 booked nothing.

## 5. Detector (production, read-only)

`find_duplicate_arrivals.py --since 2026-09-01` for the owner, piped into `ledger_api` with `default_transaction_read_only=on`:

| List | Count | Cases |
| --- | --- | --- |
| Mirrored pairs still booked twice | 0 | The incident's Jago credit was deleted by the owner |
| Recorded events whose transaction was deleted | 7 | Jago 2026-09-29 14:34, ShopeePay 09-29 14:38, GoPay and Jago 10-23 18:33, Jago 10-30 07:52 and 07:59, Jago 11-01 01:39 (the incident, event `58f42cb8`); times WIB |
| Owner memory in Internal Movement | 4 | `signature:jago|income|unknown`, `merchant:bca` (income); `signature:bca|expense|payment`, `merchant:gopay` (expense) |
| Model memory of a wording whose leg is now a movement | 1 | `merchant:tabungan by jago` in Pendapatan Lain |

With OD121 the four owner entries are no longer applied or shown to the model. `merchant:tabungan by jago` stays a model hint until the next linked report of that wording retires it. No app path deletes a memory entry.

## 6. Not run or not verified

- **4.4 live benchmark of `-11` against `-10`:** not run, awaiting the owner's approval.
  - Command: `cd backend && .venv/bin/python -m evaluation.notification_benchmark --live --reasoning-efforts low --repeats 3 --partition all --max-estimated-usd 0.70`.
  - Run it once on this branch and once at `0a342a9` for `-10`.
  - Upper-bound estimate: $0.60 per prompt version (36 model cases × 3), $1.20 for both. The default cap of $0.25 refuses it.
- **The companion app:** a held event's result arrives as `needs_review` with `possible_duplicate_observation`. The Android companion shows its generic needs-review state; its wording was not checked.
- A screen reader and a real phone were not used.

## 7. Clean-up

All servers on 8282, 8284–8286, 3282, 3283 and 9582 were stopped by port. The container `cft-arrival-pg`, the browser profile and the `main` worktree were removed. The `ps`, `lsof` and `docker ps` output is in the final report.
