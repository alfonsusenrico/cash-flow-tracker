# English output benchmark plan (task 5.3)

**Status: waiting for owner approval (paid).** Nothing in this plan has been run. No provider was called while preparing it, and `LLM_ENGLISH_OUTPUT_ENABLED` stays `false` until the held-out run below passes and the owner decides (task 5.4).

## What is measured

The notification interpreter's English rendering, `notification-interpretation-12-en` (`backend/app/services/prompts/notification_interpretation.en.md`), on the synthetic corpus `synthetic-notifications-5-en`. That corpus is `synthetic-notifications-5` with every case asking for an English description (`evaluation.notification_cases.english_output_cases`): the same notification inputs, accounts, categories, expected facts and references. Only fictional data is used.

| Partition | Cases | Sent to the model | Requests (3 repeats, 1 configuration) |
| --- | --- | --- | --- |
| development | 31 | 19 | 57 |
| held_out | 22 | 17 | 51 |

The other 17 cases are decided before the model (entrance filter, evidence review, deterministic Stockbit) and are scored without a request.

## Commands

Run from `backend/`, development first, then held-out only if development passes. The configuration matches production (`gpt-6-luna`, reasoning effort `none`). The credential is read from the environment or `.env` by the benchmark itself and is never printed.

```bash
cd backend
.venv/bin/python -m evaluation.notification_benchmark --live --output-language en \
  --partition development --model gpt-6-luna --reasoning-efforts none --repeats 3 --max-estimated-usd 0.31
.venv/bin/python -m evaluation.notification_benchmark --live --output-language en \
  --partition held_out --model gpt-6-luna --reasoning-efforts none --repeats 3 --max-estimated-usd 0.28
```

Each run prints one JSON report, which goes into `benchmark-report.md` with its quality, latency and cost.

## Cost estimate

From `estimated_run_cost` (the benchmark's conservative upper bound: one token per input byte and the full 2,048-token output ceiling on every request, at $0.20 input and $1.20 output per million tokens):

| Partition | Upper bound | Rough expected spend |
| --- | --- | --- |
| development | $0.3033 | about $0.06 |
| held_out | $0.2737 | about $0.06 |
| **Both** | **$0.5770** | **about $0.12** |

The rough expected spend assumes about four input bytes per token (about 14.4 KB of input per request) and 300 output tokens. It is not a bound.

**Owner decision needed, besides the paid run itself:** the upper bound of each partition is above the default cap of $0.25 per run, so the benchmark refuses both runs with `benchmark_budget_exceeded` unless `--max-estimated-usd` is raised, and the command line requires at least three repeats. The commands above raise the cap to just over each upper bound ($0.31 and $0.28). The Indonesian corpus has the same upper bounds ($0.3014 and $0.2719), so this is not caused by the English prompt.

## Gates

The same gates as the Indonesian benchmark, plus the English description check:
- `validated_safety_gate_pass`: no validated write with a wrong amount, account or counterpart;
- `quality_gate_pass`: schema completion at least 98% and unambiguous classification at least 90%;
- for English output, at least 90% of proposed records pass the description rubric: 3 to 160 characters, no redaction tokens or long numbers, not a placeholder in either language ("Payment", "Pembayaran", "Transfer in", …) and no Indonesian function words outside proper names;
- every deterministic route passes;
- `eligible_by_evaluation` is true on the held-out partition.

Only a held-out run that passes all gates allows turning `LLM_ENGLISH_OUTPUT_ENABLED` on (owner decision OD2). The capture, purpose and history prompts have no paid benchmark. They follow the same flag and are covered by the offline mocked-provider tests.
