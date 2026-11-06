## Context

After `add-language-system`:
- every account has `language` (`en`/`id`) and `region` (`ID`/`US`);
- the web renders Beranda's system labels and notes from `label_source`, `key` and `note_codes`;
- sign-in, sign-up and settings errors are `{code, params, message}`.

Everything else the server writes is still Indonesian or English regardless of the account. Verified at `feat/markas-beranda` `d2805cb`:

- **Questions.** Templates, not the LLM, compose them, and they are stored with the event:
  - `purpose_questions.purpose_prompt_text` ("Apa ini? Rp187.500 dari BCA Harian", stored in `notification_events.active_question`);
  - `topup_learning` ("Sepertinya top up ke …, kayak bulan lalu?", "Debit … dari … biasanya jadi top up ke …. Yang mana?");
  - `habits` ("Sepertinya …?");
  - `capture_resolution` (choice labels "Pengeluaran", "Pemasukan", "Pindah saldo" and follow-up questions, stored in `text_captures.question`).
- **Labels:** `monthly_comparison` facts, `report_windows` labels ("30 Hari Terakhir", "Siklus 25 Okt – 24 Nov", month names), `money_flow` labels, the `dashboard` legacy narrative and "Belanja tercatat", the cycle-summary system `label`s and `note`s, and the review fallbacks.
- **Stored notes:** "Pindah saldo ke {account}" and "Pindah saldo masuk" (`notification_application`), the `generic_description` fallbacks, and the movement default note "Internal Movement" (`routers/movements.py`).
- **Prompts:** the four files in `services/prompts/` ask for Indonesian output. Their versions are `notification-interpretation-12`, `capture-normalization-2`, `history-suggestions-1` and `purpose-normalization-1`. `capture_normalization.PLACEHOLDER_DESCRIPTIONS` rejects Indonesian placeholders only.
- **Money in sentences.** There are three helpers with different output:
  - `cycle_summary.rupiah` gives "Rp 1.234.567";
  - `purpose_questions.rupiah` gives "Rp1.234.567", with no space;
  - `dashboard.format_idr_compact` gives "1.234.567", with no symbol.
  The research assumed one shape.
- **Errors:** 327 `HTTPException` sites, of which 181 pass a plain-string `detail` (111 unique). The typed errors (`SalaryError`, `refuse(...)`) carry codes with Indonesian or English messages.
- **The companion:**
  - it shows `prompt`, suggestion `label`, capture `question.text` and `prompt`, and the error `detail` verbatim;
  - `ServerMessage.kt` reads `detail` as a string, or `detail.message`, then `detail.code`;
  - it sends no `Accept-Language`.
- **Evaluation:** only the notification interpreter has a benchmark (`evaluation/notification_benchmark.py`). It has a synthetic corpus and a $0.25 cap per configuration, and it needs `OPENAI_API_KEY` (paid). The other three prompts are covered by mocked-provider unit tests.

## Goals / Non-Goals

**Goals:**
- Every text the server writes follows the account's language (or the request's when unauthenticated), with byte-identical Indonesian.
- One error shape on every endpoint.
- English AI output that is evaluated before it is used.
- The backend needs no further language work for phases 4 to 12.

**Non-Goals:**
- Companion UI, English sentence input, translating stored rows, schema changes, timezone and base currency.

## Decisions

### D1. The account decides the server's language

`app/i18n/resolve.py`:
- `language_for_user(user)` returns `users.language`. It is used by every authenticated route (session or Bearer), the companion's ingestion, the cron jobs (recurring, auto-link, monthly report readiness) and the history-suggestion runs. Jobs read the owner's row and never guess.
- `language_for_request(request)` parses `Accept-Language` with the parser from `add-language-system`. It is used only on unauthenticated endpoints (login, register, device pairing exchange, health).
- Middleware sets `Content-Language` on every response that carries server text.

Rejected:
- `Accept-Language` for signed-in requests: a shared device or the companion (which sends none) would flip the account's language.
- A per-request language parameter: it gives two sources of truth.

### D2. Server catalogues mirror the web's

`backend/app/messages/en.json` and `id.json` have identical key trees by area (`errors`, `questions`, `reasons`, `labels`, `notes`, `facts`, `periods`). Values use `str.format` named placeholders. Plurals are explicit `.one` and `.other` keys, chosen by `count`. Indonesian needs `.other` only. The parity test still requires both keys in English and only `.other` in Indonesian.

`t(language, key, **params)` renders. A missing key or parameter raises in tests and logs the key path (never the text) in production, falling back to the Indonesian value.

Month and weekday names come from the catalogue (`periods.month.10` = "Oktober" / "October"), not from the host locale, so output never depends on the container's locale.

Rejected:
- gettext with Babel `.po` files: standard for Python, but its msgid structure would not mirror the web's keys, so one parity test could not cover both.
- Server-side ICU (`pyicu`): a native dependency for a handful of plurals.

### D3. Money in server sentences: one helper, today's shapes

`format_money(amount, region, style)`:
- `style="figure"` gives "Rp 1.234.567" (today's cycle-summary shape);
- `style="inline"` gives "Rp1.234.567" (today's purpose-question shape);
- `style="bare"` gives "1.234.567" (today's dashboard shape).

Region `US` uses comma grouping. Server sentences always state IDR, because stored amounts are IDR. This is unchanged behaviour: the USD display conversion stays a web concern.

This keeps every Indonesian string byte-identical, which follows the owner's rule that Indonesian keeps today's copy. Unifying the shapes is owner decision OD1.

### D4. One error contract on every endpoint

`app/errors.py` defines `ErrorCode` (a `StrEnum` of stable snake_case codes) and `api_error(status, code, *, language, **params) -> HTTPException`, with `detail = {code, params, message}`:
- **`message`** is `t(language, "errors.<code>", **params)` in the resolved language (D1).
- **Existing structured codes** keep their names and gain `params` and `message`.
- **The extra top-level fields** of `insufficient_funds` and similar errors (`available_amount`, `required_amount`) stay, for compatibility, and are copied into `params`.
- **The 181 plain-string sites** each get a code. Today's English text becomes the English message, and an Indonesian text is written to match. Where a plain string is Indonesian today, that Indonesian text is kept byte-identical.
- **The 401 "Not authenticated"** becomes `not_authenticated`.
- **FastAPI's schema-validation 422** (a `detail` list) stays FastAPI's shape. The web maps it to `form_invalid`.
- **Logs** record the code and params keys only.

The web's `errors` namespace gains every code in both languages. A pytest reads `ErrorCode`, both server catalogues and `frontend/src/messages/{en,id}.json`, and fails on any code missing from one of the four, and on any `params` name a message uses that the server never sends.

Rejected:
- Server-rendered messages only, without codes: the web would show the account's language even when the interface language differs before reconciliation, and clients could not branch on an error.
- RFC 9457 `application/problem+json` bodies: correct, but they would change the top-level body shape that every client reads (`detail`). The chosen shape follows its spirit, with a stable code, parameters and a human message, inside `detail`, as AIP-193's `ErrorInfo` does.

### D5. Composed conversation: rendered text plus its code

Every composed text keeps its field (`prompt`, `text`, `label`, `reason`, `explanation`) rendered in the account's language, and gains sibling `code` and `params`. For example, `{prompt: "What was this for? Rp187.500 from BCA Harian", prompt_code: "questions.purpose.expense", prompt_params: {amount: 187500, account: "BCA Harian"}}`.

| Area | Today | Key family |
| --- | --- | --- |
| Purpose questions | `purpose_prompt_text` | `questions.purpose.{expense,expense_hint,income,income_sender}` |
| Top-up suggestions | `topup_learning` prompt and explanation | `questions.topup.*`, `reasons.topup.*` |
| Habit suggestions | `habits` "Sepertinya …?" | `questions.habit.guess` |
| Capture questions and choices | `capture_resolution` | `questions.capture.*`, `labels.kind.*` |
| History suggestion template reasons | `history_suggestions` (memory and rule sources) | `reasons.history.*` |
| Monthly comparison facts | `monthly_comparison` | `facts.comparison.*` |
| Report windows and periods | `report_windows` | `periods.*` |
| Money flow | `money_flow` | `labels.flow.*` |
| Dashboard narrative and cash-flow summary | `routers/dashboard.py` | `labels.dashboard.*` |
| Cycle summary system lines | `cycle_summary` `label`, `note` | `labels.summary.*`, `notes.summary.*` |
| Review and waiting fallbacks | "Tanpa keterangan", "Tanpa kategori" | `labels.fallback.*` |

Questions stored in `notification_events.active_question` and `text_captures.question` are stored in the account's language at creation, like any generated text (D6). A question created before a language switch keeps its language, and so does its code.

The English voice follows `ui-copy-register` "Conversational Voice": "What was this for?", "Looks like {category}?", "Looks like a top-up to {product}, like last month?" Indonesian stays exactly as today, including its casual wording.

Rejected:
- Codes only, with every client rendering: the companion would need a release before English accounts see English, and old APKs would show codes.

### D6. Stored generated text is data

Notes written by templates or by the LLM ("Pindah saldo ke {account}" / "Transfer to {account}", "Pindah saldo masuk" / "Transfer in", generic descriptions, AI descriptions) are written once, in the account's language at creation, and never rewritten. Old rows stay as they are, and a language switch affects new rows only.

The movement default note stays "Internal Movement" for Indonesian accounts (byte-identical) and is "Transfer" for English ones.

### D7. LLM output language, English evaluation first

- **Prompt files become templates** with one `{output_language}` slot and language-specific example and placeholder lists. A test asserts that the Indonesian rendering is byte-identical to today's file (SHA-256 recorded before the change), so the Indonesian prompt versions stay. The English renderings get the versions `<name>-<n>-en`, stored per row in `prompt_version` as today.
- **Input stays as sent.** Notification, capture and history inputs are whatever the bank or the owner wrote. The prompts already treat it as untrusted data, and the boundary rules in `openspec/config.yaml` (code owns amounts, dates and ledger effects) are unchanged.
- **Placeholder guards** (`capture_normalization.PLACEHOLDER_DESCRIPTIONS`, the history guard) get the English placeholders ("Payment", "Shopping", "Spending", "Transaction", "Transfer in", "Bank transaction").
- **English cases come first.**
  - English-output cases are added to the synthetic corpus (`evaluation/notification_cases.py`): the same notification inputs, an English expected-description language, and English placeholder rejection.
  - Mocked-provider unit tests for the capture, purpose and history prompts cover the English rendering, schema validation, the placeholder guard and language checks.
  - These run offline and free.
- **The paid benchmark waits for the owner.** Running `evaluation.notification_benchmark` against the provider with the English rendering costs money, so it needs the owner's explicit OK and stays under the existing $0.25 cap per configuration. The task waits for that OK. Only a passing held-out run, with the same gates as the Indonesian benchmark, allows English output.
- **English output is behind `LLM_ENGLISH_OUTPUT_ENABLED`** (default `false`). While it is off, English accounts get the Indonesian rendering and its existing version, exactly as after `add-language-system`. The other prompts (capture, purpose, history) have no paid benchmark today. They follow the same flag, so one owner decision turns English AI output on everywhere.

Rejected:
- Switching prompts to English as soon as offline tests pass: the interpreter's held-out gates exist because offline checks have missed quality problems before (the 2026-09-29 run).
- Translating Indonesian model output to English after the fact: a second model call per row and a second failure mode.

### D8. Logs never hold rendered text

Rendered text can contain merchant names, account names and amounts. Logs and error reports record:
- message keys and error codes;
- prompt versions and the output language;
- counts.

Never the rendered `message`, `prompt` or `label`. A test captures the logs of a purpose question, a capture follow-up, a history run and an error response in both languages, and asserts that no merchant, account name or amount appears, extending today's capture log leak test.

### D9. The companion keeps working without a release

The companion shows server text verbatim and reads `detail.message`, so an English account's companion shows English questions and errors at once. An Indonesian account's text is byte-identical. The companion's own change later adds `Accept-Language`, reads `language` from `/auth/me`, and may render codes locally.

### D10. Verification proves Indonesian is unchanged and English is complete

- **Golden Indonesian outputs.** Before any edit, a test fixture records today's output for every composed text kind on the parity seed and synthetic cases: questions, reasons, labels, notes, facts, periods, errors and the four prompt renderings. After the change, an Indonesian account must produce byte-identical output.
- **English completeness.** The same fixture run with an English account must produce no Indonesian catalogue value (a dictionary check against `id.json` values, with user data excluded). Each value must come from `en.json`.
- **Parity tests:** the server catalogues (D2), and the error codes across the four catalogues (D4).
- **Language source tests:** an authenticated request with a conflicting `Accept-Language` uses the account; an unauthenticated one uses the header; a cron job uses each owner's language; `Content-Language` is present.
- **Figures untouched.** `parity.mjs` and the API snapshots are unchanged before and after, in both languages.

## Data, API and failure behaviour

- No schema change.
- A missing catalogue key in production falls back to Indonesian and logs the key path once, so a user never sees a key path.
- An unknown `ErrorCode` cannot be raised, because it is an enum.

## Migration and rollback

- No migration. Deploy is the normal release.
- Rollback is a redeploy of the previous release. English rows written in the window stay English, as data. Old code returns string `detail`s again, which both clients handle.
- `LLM_ENGLISH_OUTPUT_ENABLED` can be turned off at runtime without a deploy, which reverts English accounts to Indonesian AI output.

## Owner decisions (recommended default in bold)

1. **OD1, money shape in server sentences:** **keep today's three shapes, so Indonesian stays byte-identical** (the purpose question keeps "Rp187.500"). Alternative: unify on "Rp 187.500" everywhere, which changes one Indonesian string family the companion shows.
2. **OD2, when English AI output turns on:** **only after the owner-approved paid benchmark passes on the held-out partition.** Alternative: turn it on after the offline English cases pass, accepting unmeasured quality for test accounts.
3. **OD3, the paid benchmark run itself:** asked at task 5.3, with the estimate from `estimated_run_cost`. No answer is needed at plan approval.

## Risks / Trade-offs

- [The diff is large and mechanical (181 error sites)] → One helper, one enum, the parity tests and the golden Indonesian fixture. Reviewed per router in commit-sized steps.
- [Indonesian text drifts during extraction] → The golden fixture compares bytes.
- [English AI quality] → Behind the flag until the held-out benchmark passes.
- [A third-party client parses string `detail`] → None is known. The release note names the shape change.

## Open Questions

None beyond OD1 to OD3.
