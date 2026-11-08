# Verification: localize-server-text

Date: 2026-11-08. Branch `feat/localize-server-text`, on `main` 0308b7b (pull request #89, `add-language-system`). Every check below ran on loopback with synthetic data in disposable databases only. No production system, owner data or paid model API was used. Evidence: `research/evidence/localize-server-text/`.

## Owner acceptance

On 2026-11-06 the owner approved the three-change plan with every recommended default ("ok to all") and gave the standing instruction:

> "complete all until finished" (owner, 2026-11-06)

Work stopped the same day when the owner paused. The owner renewed the instruction:

> "continue all" (owner, 2026-11-08)

That instruction is the acceptance this change is archived under (tasks 6.4 and 6.5), as for `add-language-system`. The owner reviews and merges the pull request.

**Open item:** task 5.3, the paid English benchmark, is waiting for owner approval (paid). It has not run, and `LLM_ENGLISH_OUTPUT_ENABLED` stays `false` (task 5.4).

## What changed

- **Language source (D1):** `app/i18n/` resolves the server text language from the account for every session and Bearer request, and from `Accept-Language` only before authentication. Jobs and services read the owner's row. Every JSON response carries `Content-Language`.
- **Catalogues and money (D2, D3):** `backend/app/messages/{en,id}.json` hold the same 505 messages by area (`errors`, `error_messages`, `questions`, `reasons`, `facts`, `periods`, `labels`, `notes`, `notices`). `format_money` keeps the three shapes (`figure` "Rp 1.234.567", `inline` "Rp1.234.567", `bare` "1.234.567").
- **Error contract (D4):** 296 codes in `ErrorCode`. All 173 plain-string sites, the typed service errors, the CSRF refusal, the 401 and Starlette's own 404 and 405 now send `detail: {code, params, message}`. Existing codes and top-level fields (`available_amount`, `transaction_ids`, `candidates`, …) are kept. FastAPI's 422 list keeps its shape.
- **Web (D4):** the `errors` namespace holds all 296 codes in both languages (257 new), and a register-clean rewording replaces developer text such as "Either account_id or account_name must be provided".
- **Composed text (D5):** questions, suggestion reasons and facts carry `*_code` and `*_params`. Period and system labels are rendered in the account language next to their dates, `key` and `label_source` (decided in task 4.2, spec amended).
- **Stored notes (D6):** written once in the account language at creation.
- **Prompts (D7):** the four Indonesian renderings are byte-identical with their versions. English renderings are `-en` versions behind `LLM_ENGLISH_OUTPUT_ENABLED` (default `false`, also in `.env.example` and `docker-compose.yml`).
- **Logs (D8):** codes, keys, language and counts only. A new log line names the purpose question's key, language and suggestion count.

## Work in progress found on resumption

The stopped run left 141 uncommitted files: the full backend implementation, the catalogues, the four English prompts, the benchmark changes and plan, and a rewritten golden test. Review against the base found it complete and correct apart from the following, all fixed:
- 19 failing tests. 17 asserted the old error shape or an English message for an Indonesian fixture. Two auth overrides (`balance_check`, the notification owner) skipped the language binding that the real dependencies perform, so `tests/signed_in.py` now binds it. A mocked pulse user lacked a language.
- The reconcile notice inserted the raw type ("penyesuaian income"). It is now two keys, "penyesuaian pemasukan" and "penyesuaian pengeluaran", and their English counterparts.
- Three new English texts broke the copy register ("you", "ledger", "reconcile"). They were reworded.
- Missing pieces: the resolver, parity, money, contract, code-parity, completeness, notes, logs and copy-register tests; the web `errors` namespace; the companion check; the figure parity; the browser spot checks.
- `AccountDialogs` matched Indonesian field errors by English words only. "kantong utama" now keeps the default-pocket field focus.

An independent review of `git diff 0308b7b -- backend/app` (status codes, codes, top-level fields, Indonesian messages, language sources, logging, client fields) then found:
- **Fixed, medium:** two sites re-raise a notification refusal code (`api_error(422, exc.code)` in `seen_twice.py` and `notification_processing.py`). Thirteen such codes (`direction_unproven`, `ambiguous_amount`, `missing_amount`, `settlement_unproven`, `model_uncertain`, …) were not in `ErrorCode`, so the base's 422 would have become a 500. They are now contract codes, with the companion's own Indonesian wording, and `test_every_notification_refusal_code_is_an_error_code` scans every `EvidenceError` code.
- **Fixed, low:** the capture placeholder guard used both languages' placeholders for every account, so an Indonesian description such as "Shopping" would have been dropped. The Indonesian guard is back to the base set; English output drops either language's placeholder.
- **Fixed, low:** a settings change that also switched the language answered in the previous language. The response now binds the saved language, so its text and `Content-Language` follow it.
- **Kept, low:** a failed recurring occurrence now stores and returns the specific code (for example `source_account_not_found`) instead of `execution_failed`, and `error_detail` holds the code instead of the English text, because every raise site now has a code. Status codes are unchanged. This is the intended effect of D4 and keeps rendered text out of stored rows.

## Checks

| Check | Result |
| --- | --- |
| Backend full suite (6.3) | FINAL_BACKEND |
| Indonesian golden and byte identity (1.3, 2.3, 3.2, 4.1, 4.2, 5.1) | passed: 73 rendered cases, 81 catalogue cases and the four prompt renderings (SHA-256) byte-identical to the fixture frozen at 2cf8479; every base Indonesian error text kept for its code; 11 codes whose base message was English gain an Indonesian message |
| Server catalogue parity (2.2) | passed: same keys and placeholders, English counts with `one` and `other`; planted missing key, placeholder mismatch and English count without `one` each fail and name the key |
| Error codes in four catalogues (3.3) | passed: `ErrorCode`, server `en`/`id` and web `en`/`id` hold the same 296 codes, and no web message uses a value the server never sends; a planted missing code and a planted unknown value fail and name the code and catalogue |
| Contract scan (3.2) | passed: no `HTTPException` outside `app/errors.py`, no string `detail`; every `message_key` exists in both catalogues; every code renders in both languages with its params |
| Language source (2.1) | passed: an English account with `Accept-Language: id` gets English and `Content-Language: en`; Bearer requests follow the account; signed-out requests follow the header (`id`, none, `fr`); a switch takes effect on the next request; production falls back to Indonesian and logs the key only |
| English completeness (4.2) | passed: every composed case rendered for an English account (region `US`) holds no Indonesian server word outside the synthetic user data |
| Stored notes (4.3) | passed: after a switch to English, the earlier rows keep "Internal Movement" and their `updated_at`; new rows read "Transfer"; `movement_note` gives "Pindah saldo ke BCA Harian" then "Transfer to BCA Harian"; the recurring path writes "Recurring: …" and "Rutin: …" per owner |
| Logs (4.4) | passed in both languages for a purpose question (with its code logged), a capture follow-up, a history run and two error responses; a planted log line with a rendered prompt fails |
| Copy register | passed: server catalogues (questions and reasons keep the conversational voice; two base Indonesian payday texts kept byte-identical); web `copyRegister.test.ts` with the 257 new codes; planted lines fail per language |
| Prompts and offline evaluation (5.1, 5.2) | passed with no network (sockets refused); English output only with the flag on and an English account; "Payment" rejected like "Pembayaran" |
| Companion compatibility (3.4) | 13 captured bodies (both languages, signed out, schema 422s); `serverMessage`, `serverErrorCode` and `PurposeAnswers.errorCode` read the server's `message` and `code` (1 test), and the 10 `PurposeAnswersTest` tests passed, in a scratch clone at companion `main` 35dfbeb |
| Figures untouched (6.1) | API: 52 parameter-free GET endpoints, 2,368 numbers, 0 differing between `main` and this branch in Indonesian and English, 6 numbers added (the `label_params` of facts), all equal across languages. Web `parity.mjs`: 19 of 19 pages identical (650 figures). `parity-languages.mjs`: 97 figures identical in English and Indonesian |
| Frontend | FINAL_FRONTEND |
| `openspec validate --all --strict` | FINAL_OPENSPEC |
| `git diff --check`, `git diff --stat origin/main -- db` | clean, empty |

## Browser spot checks (6.2)

Two synthetic accounts (`teks_english`, `teks_indonesia`) with two open purpose questions each, ingested through the API with a Bearer key. Captures in `research/evidence/localize-server-text/web/`:
- the review question at 1440 light and 390 dark: "What was this for? Rp187.500 from BCA Harian · bank category: Groceries" and "Apa ini? Rp187.500 dari BCA Harian · kategori bank: Groceries";
- the account dialog refusing a duplicate name (`account_name_taken`), rendered from the code: "Account with this name already exists." and "Rekening dengan nama ini sudah ada.".

All six have `<html lang>` matching the account, no horizontal overflow and no serious or critical axe finding. The only console entry is the expected 409 of the refused request. The review page is still an untranslated page, so its own copy stays Indonesian with the "This page is in Indonesian for now" line.

Not verified in the browser: the Rincian note, the bell's report name and the full phase 3 checklist.

## Paid benchmark plan (5.3, waiting for owner approval)

`benchmark-plan.md` holds the commands and the gates. From `estimated_run_cost` (recomputed offline on 2026-11-08):

| Partition | Cases | Upper bound | Rough expected |
| --- | --- | --- | --- |
| development | 31 | $0.3033 | about $0.06 |
| held_out | 22 | $0.2737 | about $0.06 |
| Both | 53 | $0.5770 | about $0.12 |

Each upper bound is above the default cap of $0.25 per run, so the owner's OK also covers raising `--max-estimated-usd` to $0.31 and $0.28. The Indonesian corpus has the same bounds ($0.3014 and $0.2719).

## Not verified, and risks

- **Paid benchmark:** not run, so English AI output stays off and English accounts keep Indonesian AI descriptions.
- **API clients:** every error `detail` is now an object. The web and the companion handle both shapes (checked above). Any other client that reads a string `detail` must be named in the release note.
- **Companion on a device:** not installed or run on a phone; the check used its JVM tests.
- **Untranslated web pages** keep Indonesian copy and their own Indonesian maps for some codes (for example the review page's `stale_question`) until their redesign phases.

## Post-merge production checks (for the owner's release)

- `Content-Language` is present on an API response.
- The owner's (Indonesian) purpose and top-up questions read exactly as before.
- `LLM_ENGLISH_OUTPUT_ENABLED` is `false`.

Implementation and verification commit: FINAL_COMMIT.
