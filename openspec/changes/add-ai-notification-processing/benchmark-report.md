# Synthetic Notification Benchmark Report

Date: 2026-09-29. Backend base commit: `8315255` with the uncommitted AI notification implementation. All tests used wholly fictional repository fixtures; no runtime notification, account, or transaction was sent to the provider. The current operational setting remains `NOTIFICATION_AI_ENABLED=false`.

## Model and evaluation contract

- Model: `gpt-5.6-luna` through the Responses API, tool-less Structured Outputs, `store=false`. The prompt is `notification-interpretation-4` (SHA-256 prefix `2658ac9d605ebde4`); fixtures are `synthetic-notifications-2`. All three final runs used the same implementation hash prefix `7c14335ab6af3dfe` and three repeats per model-eligible case.
- The development partition contains 19 model cases and 12 deterministic cases. The held-out partition contains 12 model cases and five deterministic cases. Every deterministic route passed: development 5 entrance-filter, 6 evidence-review, 1 Stockbit; held-out 2 entrance-filter, 2 evidence-review, 1 Stockbit. None invoked OpenAI.
- The [current official Luna model page](https://developers.openai.com/api/docs/models/gpt-5.6-luna) lists $0.20 per million input tokens and $1.20 per million output tokens. The runner's conservative cost estimate uses those rates, UTF-8 input bytes, and the full 2,048-token output allowance. Each run stayed below the configured $0.25 preflight cap. Observed token-priced amounts are estimates; account billing is authoritative for the owner's $5 total budget.
- Full synthetic raw proposals, trusted decisions, rejection codes, usage, latency, and case outcomes are retained in [development-low-v4.json](evaluation/development-low-v4.json), [held-out-low-v4.json](evaluation/held-out-low-v4.json), and [held-out-none-v4.json](evaluation/held-out-none-v4.json). The held-out `low` and `none` reports have identical prompt, fixture, model, schema, and implementation hashes. They were not used to modify the prompt or fixtures.

## Results

| Partition / effort | Valid output | Unambiguous classification | Composite | Record recall | Movement precision / recall | Correct outcome / abstention | Safety gate | Quality gate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Development / `low` | 56/57 (98.25%) | 100% | 88.95 | 96.08% | 100% / 100% | 96.49% | Pass | Pass |
| Held-out / `low` | 34/36 (94.44%) | 77.78% | 89.72 | 94.44% | 80% / 66.67% | 94.44% | Pass | **Fail** |
| Held-out / `none` | 30/36 (83.33%) | 66.67% | 76.67 | 83.33% | 100% / 25% | 83.33% | Pass | **Fail** |

The validated safety gate found no wrong committed fact/reference or false movement merge in these synthetic runs. This is evidence about the tested fixtures only. The held-out `low` run failed both the 98% schema-validity and 90% unambiguous-classification thresholds; `none` failed both by a larger margin. `low` remains the configured default reasoning effort, but operational AI must stay disabled. The earlier v3 development run (57 requests) scored 79.65 with 54/57 valid outputs and 88.89% unambiguous classification; its detailed result was diagnostic, and v4 was tuned only against the development partition.

The v4 development run had one invalid output. Held-out `low` had two invalid outputs and one proposed record rejected for an invalid category reference. Held-out `none` had six invalid outputs and two invalid-category rejections. Trusted validation turned rejected proposals into review outcomes without ledger writes; those are model errors, not correct model decisions. The `low` run had p50/p95 latency of 4.70/7.06 seconds on held-out cases; `none` had 1.90/2.44 seconds.

| Run | Requests | Input / output tokens | Conservative ceiling | Observed token-priced amount |
| --- | ---: | ---: | ---: | ---: |
| Development `low` v4 | 57 | 118,563 / 11,712 | $0.2371854 | $0.0377670 |
| Held-out `low` v4 | 36 | 78,471 / 14,543 | $0.1514670 | $0.0331458 |
| Held-out `none` v4 | 36 | 78,471 / 4,765 | $0.1514670 | $0.0214122 |

These three v4 runs sent 129 synthetic requests with $0.0923250 in observed token-priced usage. This excludes the earlier v3 diagnostic, previous development tests, and real-notification dry runs. It is not an account-billing total or a claim about remaining credit.

The deterministic description rubric passed for all 49 development and 34 held-out `low` proposed records, but it checks only length and obvious leakage. The owner accepted these representative fictional descriptions on 2026-09-29: `Kedai Awan`, `Transfer masuk`, `Transfer ke BCA`, `Memindahkan dana ke Dana Darurat`, and `Pembayaran`. This qualitative acceptance does not override the failed held-out accuracy gates.

## Prior provider attempt

On 2026-09-28, before OpenAI replaced the unavailable Zen adapter, one request per configured Zen comparison was sent on the wholly synthetic development set. LongCat, MiMo, and Big Pickle returned `provider_access_denied`; Space Bunny timed out after 30.033 seconds. The runner circuit-broke the remaining 92 scheduled results per model. No model quality score or held-out comparison resulted. This historical attempt did not send runtime data and is not an operational fallback.

## Deferred model comparison — 2026-09-29

At the owner's request, a temporary `gpt-4o-mini` candidate at temperature `0.2` ran only the fictional development partition: 57 paid calls, 57/57 schema-valid responses, 0% strict unambiguous classification, composite 82.1053, validated safety gate passed, quality gate failed. Outputs frequently combined invalid income Kakeibo/category decisions or unsupported interpretation evidence. Token-priced observed estimate was $0.02209395 with a conservative ceiling of $0.14286825 under the $0.15 run cap; these are not account billing. The diagnostic JSON is `evaluation/development-mini-v1.json` and describes the temporary candidate, not the reverted release provider.

The owner then deferred further AI benchmarking to the next task and authorized backend main release and a companion APK install. No held-out mini test or further paid call followed that instruction. The release retains the existing Luna model setting and `NOTIFICATION_AI_ENABLED=false`; it does not claim production AI quality passed. Account billing remains authoritative for the owner's $5 total budget.
