# Design: skip redundant notifications

## Context

`recognise-one-arrival-seen-twice` (archived 2026-11-04) is the base.
- **The guard:** `same_arrival` in `movement_pairing.py` finds one recorded observed leg with these properties:
  - same direction and amount;
  - another institution;
  - at most 120 s apart (`SAME_ARRIVAL_WINDOW_SECONDS`);
  - a different account;
  - mutual remote naming (`names_each_other`).
- **What `record_observed_leg` does with it** (`notification_application.py`):
  - marks the new event a `duplicate_observation` (`mark_duplicate_observation`);
  - or, when the recorded leg is an untouched default-pocket fallback and the new one names its account, removes the old leg (`keeps_new_observation`, `drop_observation`) and keeps the new one.
- **The model's part:** it receives `neighbours` from `notification_context.neighbour_context` and may propose `same_money_as`. An unconfirmed proposal is held by `hold_possible_duplicate` with `possible_duplicate_observation`.
- **Tinjau:** `seen_twice.py` lists duplicates and holds that are unacknowledged (`reviewed_at IS NULL`) for 30 days. "Benar, satu transaksi" (`acknowledge`) sets `reviewed_at`. "Catat terpisah" (`record_separately`) records the event with the guard off and an undo of kind `seen_twice_separate`.

V45 added `notification_events.reviewed_at` and `notification_events.duplicate_of_transaction_id`, and the review action kind `seen_twice_separate`. All three stay in use after this change: duplicates, acknowledgement and "Catat terpisah" remain. No column becomes unused, so nothing is dropped.

**Benchmark, 2026-11-04** (`gpt-6-luna`, three repeats):
- `same_money_as` recall was 0% at `none` and `low`, with no false proposals.
- `-11` sends about 3.5 k input tokens per request against 3.2 k for `-10`.
- Production runs `none`.

## Goals

- **Deterministic control:** no model judgement decides whether a notification is redundant.
- **Learn once:** a pattern confirmed once by the owner, or added by hand, skips its repeats without a Tinjau item.
- **Never lose money:** a report is skipped only when the kept app's report of the same money is already recorded. Every skip stays visible and reversible.

## Non-goals

- Skipping by text, sender or merchant.
- Skipping without a recorded report from the kept app.
- Patterns across owners.

## Decisions

### D1. Remove the model's time context

- **Context:** `neighbour_context`, the `neighbours`, `neighbours_incomplete` and `_neighbour_ids` context keys, and the context-budget trimming for neighbours go.
- **Output:** `Interpretation.same_money_as` and the `invalid_neighbour_reference` check in `validate_interpretation` go.
- **Application:** `hold_possible_duplicate` and the `same_money_event_id` parameter of `record_observed_leg` go.
- **Prompt:** `prompts/notification_interpretation.md` loses its neighbours paragraph and becomes `notification-interpretation-12`, otherwise identical to `-11`.
- **Settings:** `notification_neighbour_window_seconds` and `NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS` leave `config.py`, `docker-compose.yml` and `.env.example`. A production environment that still sets the variable is ignored.

Rejected: keeping the context for a later prompt rework. The owner decided against it, it costs tokens on every notification, and the deterministic guard already covers the case.

### D2. Skip patterns

**Table `notification_skip_patterns` (V46):**

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | `uuid` pk | |
| `user_id` | `uuid` fk users, cascade | owner |
| `redundant_institution` | `text` | the app whose report is skipped, e.g. `jago` |
| `direction` | `text` `income`/`expense` | the reports it applies to |
| `kept_institution` | `text` | the app whose report is kept, e.g. `gopay` |
| `source` | `text` `confirmation`/`manual` | how it was added |
| `created_at` | `timestamptz` | |
| `stopped_at` | `timestamptz` null | set by "Berhenti melewati"; a stopped pattern never matches |

- `redundant_institution <> kept_institution`, and both must be in `SUPPORTED_PACKAGES`' institutions.
- At most one active pattern per (owner, redundant, direction, kept): a unique partial index `WHERE stopped_at IS NULL`.
- `notification_events.skip_pattern_id uuid null fk ON DELETE SET NULL` records which pattern skipped an event.

Patterns name institutions (`bca`, `jago`, `gopay`, `shopeepay`, `stockbit`), not packages. Several packages map to one institution in `SUPPORTED_PACKAGES` (for example `com.gojek.gopay`, `com.gojek.app` and `com.gopay.wallet`), and the guard already compares institutions (OD123).

### D3. The pattern match

`same_arrival` gains a pattern branch. For a new leg `N` from institution `R` in direction `d`, with an active pattern (`R`, `d`, `K`), a recorded leg `L` matches when all of these hold:
- `L` is an unlinked `observed_leg`, as in the guard;
- `L.type == d` and `L.amount == N.amount`;
- `L`'s event comes from institution `K`;
- `|L.post_time − N.post_time| ≤ 120 s`;
- `L.account_id != N.account_id`;
- `N`'s remote resolves to `L`'s account or its root (one-sided naming, OD122).

Each count gives a result:
- **Exactly one match:** `N` becomes a `duplicate_observation` of `L` with `skip_pattern_id` set and `reviewed_at = NOW()`. That is "skipped": no transaction, no question, not counted in `unreviewed`.
- **None:** the plain guard runs unchanged. Mutual naming may still make `N` an ordinary duplicate that waits in "Terlihat dua kali". Otherwise `N` records normally.
- **Two or more:** `N` records normally with `ambiguous_movement`, as the guard does for several originals.

The pattern decides which leg is kept: always the kept app's report. Reverse order works like this:
- `N` is from `K`, and an active pattern (`R`, `d`, `K`) has a recorded leg `L` from `R` that matches with the roles swapped.
- If `L` is replaceable (`replaceable_leg`: automatic, untouched, no answer), `drop_observation` removes `L` in the same database transaction, `N` is recorded, and `L`'s event becomes the skipped duplicate of `N`.
- If `L` was touched by the owner, `N` becomes an ordinary duplicate of `L` and waits in "Terlihat dua kali" (OD119 unchanged). The owner's edit wins.

`repair_duplicate_arrivals` (the deferred pass) applies the same pattern branch and stays idempotent.

Rejected: matching on amount and time only (OD122). Two real transfers of the same amount from Jago and GoPay within two minutes would then lose one.

### D4. Learning a pattern

- `acknowledge` ("Benar, satu transaksi") on a `duplicate_observation` creates the active pattern (duplicate's institution, its direction, kept leg's institution) with `source = 'confirmation'`, unless one exists. It returns `pattern: {id, created}`.
- Tinjau's toast then reads "Laporan Bank Jago serupa akan dilewati otomatis." with the action "Batalkan", which stops the pattern.
- **Nothing is learned when:**
  - the item is a "Catat terpisah";
  - the kept leg has no institution (a manual or typed row).
- Learning is idempotent: acknowledging twice does not add a second pattern.

Rejected: asking with a checkbox every time (OD124). The owner said "after first confirmation", and the toast's "Batalkan" plus Pengaturan keep it reversible.

### D5. "Dilewati otomatis" and undo

- `GET /api/review/queue` gains `skipped`: the owner's events with `skip_pattern_id IS NOT NULL` from the last 30 days, newest first, at most 200.
- **Each item:** event id, app label, direction, amount, post time, the kept leg (as `seen_twice` items), and the pattern (`id`, labels).
- **Counts:** `counts.skipped`, which is not part of `unreviewed` or `attention`.
- **"Catat terpisah"** on a skipped item uses the existing `record_separately`:
  - its lock query also accepts skipped events;
  - its undo restores the skipped state, including `skip_pattern_id` and `reviewed_at`, through the V45 restore path with `skip_pattern_id` added to `RESTORED_EVENT_FIELDS`.
- "Catat terpisah" does not stop the pattern. Pengaturan does.

### D6. Removing the held state

- `SEEN_TWICE_SQL` loses its `possible_duplicate_observation` branch.
- `_held_original` and the frontend's held variant go: `kind: "held"`, "Catat transaksi ini" and "Abaikan laporan ini".
- V46 requeues every event still `needs_review` with `error_code = 'possible_duplicate_observation'`:
  - `processing_state = 'queued'`, `attempt_count = 0`, `processing_generation + 1`, `next_attempt_at = NOW()`, `error_code`, `interpretation`, `active_question` and the lease cleared;
  - this is the same reset `confirm_mapping` performs.
  - The processor then decides each one with the deterministic guard, the patterns and the `-12` prompt.
- Before the release a read-only count is taken in production, to record how many events this touches (OD126).

### D7. Settings section and API

- **Pengaturan:** the section "Notifikasi ganda" at `/settings/notifikasi-ganda`, in group Akun after "Pemetaan notifikasi". Its purpose line is "Laporan dari aplikasi yang mengulang laporan aplikasi lain, dilewati otomatis."
- **List:** one row per active pattern:
  - title "Bank Jago, uang masuk";
  - detail "Dilewati bila GoPay sudah melaporkan nominal yang sama · dari Tinjau, 4 Nov · 3 kali dilewati";
  - action "Berhenti melewati", with the existing two-step `ConfirmRemove`.
- **Form "Tambah pola":**
  - "Aplikasi yang dilewati": a select of institutions that have sent the owner at least one notification;
  - "Jenis": "Uang masuk" or "Uang keluar";
  - "Aplikasi yang dipakai": a select, excluding the first choice;
  - "Simpan pola".
- **Empty state:** "Belum ada pola. Pola ditambahkan saat Anda memilih "Benar, satu transaksi" di Tinjau, atau di sini."
- **Endpoints** (session auth `get_current_user`, router `ingest.py` beside `/aliases`):
  - `GET /api/ingest/skip-patterns` returns `{patterns: [...], institutions: [{institution, label}]}`, with active patterns and per-pattern `skipped_count` and `last_skipped_at`.
  - `POST /api/ingest/skip-patterns` takes `{redundant_institution, direction, kept_institution}`:
    - 201 with the pattern;
    - 409 `skip_pattern_exists` for an active duplicate;
    - 422 `invalid_skip_pattern` for an unknown institution, the same institution twice, an institution the owner has never received a notification from, or an invalid direction.
  - `DELETE /api/ingest/skip-patterns/{id}` sets `stopped_at`. It is idempotent, and another owner's or an unknown id gets 404 `skip_pattern_not_found`.
- **Spec length:** adding the ninth section would take "Sectioned Settings Page" past the 500-character limit, so the section-list clause reads "then Akun: …" instead of "and the group Akun with …". The Akun grouping stays specified by the scenario "Grouped list for assistive technology".
- **Companion:** no change. The companion reads a skipped event's result like any duplicate observation (the kept leg's result).

### D8. Evaluation

- **Corpus:** `notification_cases.py` becomes `synthetic-notifications-5`:
  - the five sequence cases stay, without neighbours, each expecting an ordinary observed leg;
  - the duplicate decision belongs to the backend and is tested in `test_duplicate_arrivals.py`.
- **Report:** `notification_benchmark.py` drops the `same_money` metric and the `expected_same_money` fields.
- **Offline:** the deterministic benchmark runs offline.
- **Live:** a live run of `-12` against `-11` (`gpt-6-luna`, `none` and `low`, three repeats) costs about $0.10 at listed prices and runs only with the owner's go-ahead (OD127).

## Compatibility, migration and rollback

**V46 is additive:**
- a new table, a nullable column with a partial index, and the requeue update;
- an older release ignores the table and the column.

**Rollback:**
- Revert the merge, and leave the table and column in place, unused.
- Skipped events keep `provenance_kind = 'duplicate_observation'` and `reviewed_at`, so an older release treats them as acknowledged duplicates. No money is affected.
- Requeued events are processed again by whichever release runs.

**API:**
- `skipped` and the skip-pattern endpoints are new.
- `seen_twice` items no longer carry `kind: "held"`. The only consumer is the web Tinjau, changed in the same release.

**Prompt:** the version is recorded per event, so behaviour is attributable to `-11` or `-12`.

## Risks

- **A real report skipped.** A skip needs:
  - an active owner pattern;
  - a recorded report of the same amount and direction from the kept app within 120 s, on another account;
  - the skipped report naming that account.

  Every skip is listed in "Dilewati otomatis" with "Catat terpisah" for 30 days, and the pattern's count shows in Pengaturan.
- **Remote not resolved.** Without the model, a redundant report whose counterparty text does not resolve to an owned account has no remote. The pattern cannot match it, so it falls back to the guard or records normally. The safe failure is a visible extra row, never a lost one.
- **Requeue load.** V46 requeues only held events from the few hours `-11` was live. The count is taken read-only before the release.

## Open decisions (recommended defaults)

- **OD122: naming in a pattern match.** *Default:* one-sided. The skipped report's remote must resolve to the kept leg's account or its root. A pattern says which app repeats which, so the kept report's own remote (often a default pocket) is not required. The alternatives are mutual naming as in the guard (then a pattern only auto-confirms what the guard already catches), or no naming (amount and time only, unsafe for two real same-amount transfers).
- **OD123: pattern identity.** *Default:* institutions, not packages, because several packages map to one institution and the guard compares institutions. The alternative is packages, matching the owner's wording but splitting GoPay across three packages.
- **OD124: learning.** *Default:* every "Benar, satu transaksi" on a duplicate learns its pattern, with a "Batalkan" toast. The alternative is a checkbox "Lewati laporan seperti ini" on the confirmation.
- **OD125: "Dilewati otomatis".** *Default:* listed for 30 days, counted in `counts.skipped` only, never in `unreviewed` or `attention`. The alternative is counting them in `unreviewed`.
- **OD126: events held by `-11`.** *Default:* V46 requeues them for the deterministic path. The alternative is leaving them `needs_review` for the owner to answer by hand.
- **OD127: live benchmark.** *Default:* run `-12` against `-11`, about $0.10, under the owner's earlier approval of paid tests, and report it before acceptance. The alternative is offline only.
- **OD128: sequence cases.** *Default:* keep them in `synthetic-notifications-5` with plain observed-leg expectations. The alternative is removing them.
