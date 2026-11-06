## Why

The owner decided on 2026-11-06 that the plan covers "web plus every text the server writes", with the Android companion in a later change. The same day the owner accepted every recommended default of the language research (`cash-flow-tracker-prototypes/research/2026-11-06-language-system.md` §6 and §11) with "defaults ok".

`add-language-system` gives every account a language and translates the web shell and Beranda. The server still writes Indonesian, or mixed English and Indonesian, in many places:
- **Composed questions:** purpose ("Apa ini? Rp187.500 dari BCA Harian"), top-up ("Sepertinya top up ke …, kayak bulan lalu?"), habits ("Sepertinya …?") and capture ("Dari rekening mana?"). The companion shows them verbatim.
- **Generated stored notes:** "Pindah saldo ke …", "Pembayaran QRIS".
- **Labels:** report windows ("Oktober 2026", "Siklus 25 Okt – 24 Nov"), money flow, the legacy dashboard narrative, and the cycle-summary labels that `add-language-system` kept for compatibility.
- **LLM prompts:** the four prompts ask for Indonesian descriptions and reasons.
- **Plain-string errors:** 181 `HTTPException` sites pass a plain string `detail` (111 unique strings, mostly English, some Indonesian). The rest use about 86 structured codes.

Phases 4 to 12 of the Markas+ plan carry a "backend untouched" gate. All server text therefore has to be localized before phase 4 starts. The research recommended running this change back to back with `add-language-system`.

## Expected Outcome

- An English account sees English questions, suggestion reasons, report and period labels, money-flow labels, error messages and AI-written descriptions, on the web and in the companion, without a companion release.
- An Indonesian account, including the owner's, gets byte-identical text to today for every composed message, prompt and label.
- Every error response, on every endpoint, has the same shape `detail: {code, params, message}`. The web renders it from `code` in the interface language, with `message` in the account's or request's language as the fallback.
- New stored notes are written once, in the account's language at creation. Existing rows never change.
- English AI output is switched on only after English evaluation cases pass offline and the owner-approved paid benchmark passes. Until then English accounts keep Indonesian AI descriptions, as after `add-language-system`.

## Plan approval

Plan approval pending. Nothing is implemented until the owner approves this change in the session; the approval and its date are then recorded here and in `PROJECT_STATE.md`.

## What Changes

- **Server language resolution:** the language comes from the account (`users.language`) for every authenticated request, Bearer request, background job, notification ingestion and report. `Accept-Language` is used only on unauthenticated endpoints. Responses with server text carry `Content-Language`.
- **Server catalogues:** `backend/app/messages/en.json` and `id.json`, key-based with named placeholders and explicit plural keys. One renderer, and one money formatter that reproduces today's strings. The Indonesian catalogue is today's text.
- **Error contract:** one error-code enum and one helper. All 181 plain-string sites and the typed service errors return `{code, params, message}`, and the 401 "Not authenticated" becomes `not_authenticated`. A test proves every code exists in both server catalogues and in the web's `errors` namespace.
- **Composed conversation in the account language, always with `code` and `params`** (`notification_sender.py` and `notification_source_pocket.py` hold no question wording at `d2805cb`, so the companion composes those questions; task 1.2 confirms this):
  - purpose, top-up, habit and capture questions;
  - history-suggestion template reasons;
  - monthly comparison facts, report window and period labels, money-flow labels, the cash-flow summary labels the companion shows, and the legacy dashboard narrative;
  - the cycle-summary `label` and `note` of system lines.
- **Stored generated text:** movement notes, generic descriptions and other template-written notes are written in the account's language at creation and never translated later.
- **LLM output language:**
  - the four prompts become templates with an output language;
  - the Indonesian rendering stays byte-identical, and so does its prompt version;
  - the English rendering gets new prompt versions, English placeholder guards, and English synthetic evaluation cases;
  - English output stays behind `LLM_ENGLISH_OUTPUT_ENABLED` (default `false`) until the owner-approved benchmark passes.
- **Logs** hold codes, prompt versions and counts, never rendered text.

## Non-goals

- **The Android companion's own interface,** its `Accept-Language`, per-app language and region formatting. They come in a later companion change. It needs no release for this change, because it shows server text verbatim and already reads `detail.message`.
- **Bank notification input,** parser vocabulary and English sentence capture input. They are not interface text and stay unchanged.
- **The web pages of phases 4 to 12.** They adopt the server codes as they are rebuilt.
- **Translating stored rows,** and changing figures, money rules or schema. There is no migration.
- **Debt for the global product, recorded and not planned here:** a per-account timezone and a real per-account base currency.
- **Public sign-up stays closed** (invite code) until phase 12.
- Running a paid model benchmark without the owner's explicit OK.
- Local verification only, with synthetic data. Release and production stay with the owner through the approved GitHub workflow after acceptance.

## Capabilities

### New Capabilities

None. `interface-language` is created by `add-language-system`, which archives first. This change adds its server requirements to that capability.

### Modified Capabilities

- `interface-language`: server text language, the error contract, composed text with codes, stored generated text, the AI output language gate, and logs without rendered text.
- `ai-notification-processing`: descriptions in the account's language.
- `history-suggestions`: suggestion reasons in the account's language.
- `notification-purpose-questions`: the purpose prompt in the account's language.
- `report-periods-and-drilldown`: window labels in the account's language.

Project capabilities do not change. The change exercises the declared standards:
- `llm`: versioned prompts, evaluation before activation, a cost cap, owner approval for paid runs;
- `api-consumers`: one error shape announced, `message` kept for clients that show it;
- `personal-data`: rendered text with merchants and amounts never logged;
- `background-jobs`: the language read from the account, never guessed;
- `multi-user`: no language leakage between accounts;
- `money`: figures untouched.

## Impact

- **Backend:**
  - new: `app/i18n/` (resolver, catalogue loader, renderer, `format_money`), `app/messages/{en,id}.json`, `app/errors.py`, and `Content-Language` middleware;
  - changed: every router that raises plain-string errors, `services/purpose_questions.py`, `topup_learning.py`, `habits.py`, `capture_resolution.py`, `history_suggestions.py`, `monthly_comparison.py`, `report_windows.py`, `money_flow.py`, `cycle_summary.py`, `routers/dashboard.py`, `notification_application.py`, the four prompt files and their callers, `capture_normalization.PLACEHOLDER_DESCRIPTIONS`, `evaluation/notification_cases.py`, and `core/config.py` (`LLM_ENGLISH_OUTPUT_ENABLED`).
- **Frontend:** the `errors` namespace gains every server code in both languages, and `lib/api.ts` renders `params`. Untranslated pages keep showing server text as before (Indonesian for Indonesian accounts).
- **Database:** none.
- **API contract:**
  - every error `detail` becomes an object (FastAPI's schema-validation 422 list stays as it is);
  - composed-text fields keep their names and gain sibling `code` and `params`;
  - `Content-Language` is added.
  - For Indonesian accounts every text value is byte-identical.
- **Prompt versions:** the Indonesian versions stay (`notification-interpretation-12`, `capture-normalization-2`, `history-suggestions-1`, `purpose-normalization-1`). The English renderings become `notification-interpretation-12-en`, `capture-normalization-2-en`, `history-suggestions-1-en` and `purpose-normalization-1-en`, recorded per row as today.
- **Compatibility and rollback:** no schema change, so rollback is a redeploy of the previous release. Rows written in English during the window stay English, because they are data. Clients that read a string `detail` see an object after this change. The web and the companion already handle both. Any other client is called out in the release note.
