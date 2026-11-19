# Verification: learn-accounts-by-item

Tested on 2026-11-19, branch `feat/learn-accounts-by-item` at `5236ab61` (base `3087c488`) plus the uncommitted implementation. The backend (tasks 1.1–2.1 and the preview API) was built by a delegated agent and reviewed here. The web preview and question, the deploy wiring of the setting, and the benchmark were done here.

## Checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V59) | 2464 passed |
| Frontend type check, lint, tests (vitest), build | 0 errors, lint passed, 1172 passed (102 files), build passed |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 87 passed, 0 failed |
| `docker compose config` | valid |
| `frontend/package-lock.json` | unchanged |

## Benchmark (task 3.1; owner's OK 2026-11-19)

Synthetic only, `gpt-6-luna`, prompt `capture-normalization-4`, Indonesian, 3 repeats. Item grouping: 15 cases (new places, synonyms, brands, a time before the item, new items, a tip that is not a haircut); the gate fails when a capture joins a wrong existing item. Tags: the 13 cases of `capture_tag_benchmark` as a regression check of the new prompt.

| Effort | Item grouping (45) | Joined a wrong item | Tags (39) | Median time | Cost |
| --- | --- | --- | --- | --- | --- |
| `none` | 45 correct | 0 | 39 correct | 1.92 s | US$0.0267 + 0.0251 |
| `low` | 43 correct, 2 fell back to the first word ("janji jiwa") | 0 | 38 correct, 1 unwanted tag | 2.39 s | US$0.0297 + 0.0268 |

`none` is better on every measure, so `CAPTURE_AI_REASONING_EFFORT` stays unset and captures keep `none`. Total cost US$0.108. Outputs: `research/evidence/learn-accounts-by-item/*-benchmark-*.json`.

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| Item groups (new place, different wording, numbers and one-letter words, unproven group) | `tests/test_item_learning.py`; benchmark above |
| Accounts learned by item (cash habit, mixed history asks, correction counts twice, named account wins, two accounts named) | `tests/test_item_learning.py`; on the running app, four parking captures on Tunai at different places, then "parkir lenmarc 10rb" skipped the account question (with the model off it asked only the category) |
| Preview shows the learned account | `test_item_learning.py` (basis `habit`, `mention`, none, no model call); `SentenceView.test.tsx`; browser: "parkir 10rb" reads "Tunai · dari kebiasaan" |
| Suggested account in the question | API: `suggested_account_id`, listed first; web: `SentenceView.test.tsx` "marks the account the owner's habit suggests" |
| Bare clock time after a day word | `test_item_learning.py`; the existing "Capture Time Reading" tests |
| Reasoning setting | `test_item_learning.py` and the lifespan test; passed through `docker-compose.yml`, `deploy.yml` and `materialize_runtime_env.py` as optional |

## Browser check

A production build with the prototype's synthetic dataset migrated to V59 and the model off. Catat with "parkir 10rb" at 1440 and 390 px, light and dark: 0 serious or critical axe violations, 0 console errors. Captures in `research/evidence/learn-accounts-by-item/`.

## Not verified

- **The companion** lists the suggested account first but does not mark it "Disarankan"; it reads no new field. A follow-up companion change can mark it.
- **The owner's own data** in production; it applies after the deploy.

## Known limits

- The preview's category stays "dikenali saat dicatat" for a new phrase: the category still comes from exact-phrase memory or the model at record time.
- Learned names with a leaked time or a stray word ("20 45 parkir ptc", "07 55 e money", "in") remain; they are never reused, and their payments still count for their item.
