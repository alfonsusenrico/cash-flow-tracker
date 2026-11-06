## Why

On 2026-11-06 the owner decided that the product goes global:
- English is the default interface language: natural, plain, firm terms, no slang.
- Indonesian stays as a switch that keeps today's copy, and user data is never translated.
- A browser that prefers Indonesian starts in Indonesian.
- The choice is saved per account, and the owner's existing account stays Indonesian.
- Formats come from a separate region and currency setting, not from the language.
- This phase comes right after Markas+ phase 3 (Beranda) and before phase 4 (Tinjau). Every later phase ships both languages.
- The scope is the web and the text the server writes. The Android companion follows in a later change.

The same day the owner accepted every recommended default and the glossary of the language research (`cash-flow-tracker-prototypes/research/2026-11-06-language-system.md` §7 and §11) with "defaults ok".

Today the interface cannot change language:
- `app/layout.tsx` hard-codes `<html lang="id">`, and no i18n library is installed;
- copy is inline, often concatenated in Indonesian word order;
- formatting is hard-coded to `id-ID` in 75 places across 44 files;
- the account has no language or region.

## Expected Outcome

- A new account opened from an English browser sees the shell, Beranda with its Rincian, the sign-in and sign-up page and the shared labels in plain English: "Home", "Safe to spend until payday", "Spending", "Saved & invested", "Breakdown".
- Amounts still read "Rp 1.234.567" and dates "Thursday, 5 November" for region Indonesia.
- A browser that prefers Indonesian, and every existing account including the owner's, sees exactly today's Indonesian copy.
- Every number on Beranda is identical in both languages for the same data.
- The language can be switched in Pengaturan › Tampilan and on the sign-in page. The switch applies at once, without a reload or a URL change, and follows the account to another device.
- An English account opening a page that is not redesigned yet (Tinjau to Pengaturan) sees that page in Indonesian inside the English shell, marked `lang="id"`, with one quiet line: "This page is in Indonesian for now."
- New English accounts get English starter categories and accounts, and their figures are correct because system categories are found by key (`add-category-system-keys`).

## Plan approval

Approved by the owner on 2026-11-06: the three-change language plan (`add-category-system-keys`, this change, `localize-server-text`) with every recommended default ("ok to all"), followed the same day by the standing instruction "complete all until finished". The Appendix A copy is accepted unchanged. The owner decisions take their recommended defaults:
- **OD1:** new English accounts get the English starter names of Appendix B.
- **OD2:** IDR in region US reads "Rp 1,234,567".
- **OD3:** existing accounts, the test accounts included, become Indonesian like the owner's.
- **OD4:** merge order `add-category-system-keys` → this change → `localize-server-text`, all before phase 4. The agent creates the pull request.

## What Changes

- **Account language and region (V53):** `users.language` (`en` | `id`) and `users.region` (`ID` | `US`). Existing accounts become `id` and `ID`; new accounts default to `en` and `ID`.
- **API (additive):**
  - `/auth/me` and the login and register responses return `language` and `region`;
  - `PATCH /auth/settings` accepts both;
  - `/auth/register` takes the sign-up page's language and region, and falls back to `Accept-Language` and `ID`.
- **Error codes for this phase's endpoints:** sign-in, sign-up and settings errors become `detail: {code, params, message}`. The web renders every known code from its `errors` catalogue in both languages, including the 27 codes `lib/api.ts` maps today (25 fixed messages plus `insufficient_funds` and `insufficient_units` with amounts).
- **Beranda server text as keys:**
  - cycle-summary lines, review-queue groups and waiting lines carry `label_source` (`user` | `system`), and the web renders system lines from their `key`;
  - notes carry `note_codes` with parameters.
  - The bell names a ready report from its dates.
- **Web language infrastructure:**
  - next-intl 4.14.9 without i18n routing, resolving the language from the account, then the `cft_language` cookie, then `Accept-Language`, then English;
  - message catalogues `messages/en.json` and `messages/id.json` with namespaces, including the `terms` glossary;
  - a dynamic `<html lang>`;
  - one formatter by language and region;
  - a registry of translated routes with the notice for the rest;
  - a `renderWithIntl` test helper.
- **Copy in both languages:** the shell, Beranda with Rincian, sign-in and sign-up with a language switch and a region choice, shared labels (Kakeibo pillars, theme names, period stepper, dialogs, row marks, hidden amounts), the Language control in Pengaturan › Tampilan and the Region control in Pengaturan › Mata uang & anggaran. The English copy is design.md Appendix A. The Indonesian copy is today's.
- **Starter data by language:** English accounts are seeded with English category and account names (design D15). The system-category keys keep their figures correct.
- **Tests and gates:**
  - copy register per language, read from the catalogues;
  - catalogue key and argument parity;
  - the both-language screenshot gate for the shell, Beranda and sign-in at 1440, 1024 and 390 in both themes;
  - the en-versus-id figure-identity check;
  - a pseudo-locale run.
- **BREAKING (spec):** `indonesian-daily-finance-ui` "Default Bahasa Indonesia Localization" is removed and replaced by `interface-language`. Amount inputs group digits by region.

## Non-goals

- **Server-composed text beyond this phase:** the purpose, top-up, habit and capture questions, history-suggestion reasons, report window labels, money-flow and dashboard text, stored notes, the plain-string errors of other endpoints, `Content-Language`, and the LLM prompt language. All of these are in `localize-server-text`, which ships right after this change and before phase 4.
- **The Android companion.** It keeps its Indonesian interface and shows server text verbatim. Its own later change adds `strings.xml`, per-app language and region formatting.
- **Pages 4 to 12** (Tinjau, Transaksi, Analisis, Laporan, Rekening, Target, Pengaturan, Catat). Each phase translates its own page.
- **Debt for the global product, recorded and not planned here:**
  - a per-account timezone (today server-wide `Asia/Jakarta`);
  - a real per-account base currency (today every amount is IDR, and USD is a display conversion).
  Both need their own plan before public multi-country sign-up.
- **Public sign-up stays closed.** Registration keeps requiring the invite code until phase 12.
- English sentence input for text capture, URL locale prefixes, a React 19 upgrade, and next-intl `precompile`.
- Local verification only, with synthetic data in disposable databases. Release and production stay with the owner through the approved GitHub workflow after acceptance.

## Capabilities

### New Capabilities

- `interface-language`: the supported languages, how the language is chosen and remembered, the document language, region-based formatting, catalogues and their parity, translated and not-yet-translated pages, server labels by source, and the rule that user data is never translated.

### Modified Capabilities

- `ui-copy-register`: two registers (Indonesian as today, plain English), read from the catalogues, and the same spending terms in both languages.
- `indonesian-daily-finance-ui`: the default-Indonesian requirement is removed, and thousands grouping follows the region.
- `currency-conversion-and-settings`: settings, registration and the profile carry language and region.
- `custom-category-management`: starter names follow the account language. The Indonesian starter set and its pillars are unchanged.
- `monthly-navigation-stepper`: the stepper names the period in the interface language.

`interface-language` is a new OpenSpec capability, not a new project capability. The framework's project capability list (`database`, `deployed`, `money`, `personal-data`, `background-jobs`, `api-consumers`, `multi-user`, `llm`) has no language entry, and the project already declares every one this change touches, so `openspec/config.yaml` keeps its line. The change switches on these standards:
- the `ui-ux-quality` WCAG 2.2 AA checks, adding 3.1.1 Language of Page and 3.1.2 Language of Parts, plus reflow and text spacing in both languages;
- `database` (versioned migration with backfill, disposable-database tests);
- `api-consumers` (additive fields, error shape announced);
- `multi-user` (per-account setting, no cross-account leakage through the cookie);
- `money` (figures identical across languages);
- `personal-data` (no rendered text with user data in logs or telemetry).

A dedicated localization standard is a framework candidate, recorded for `instruction-maintenance`.

## Impact

- **Database (`db/migrations/V53__account_language_and_region.sql`):**
  - `ALTER TABLE users ADD COLUMN language VARCHAR(8) NOT NULL DEFAULT 'id'`, then `ALTER COLUMN language SET DEFAULT 'en'`, so existing rows become `id` and new rows `en`, with `CHECK (language IN ('en','id'))`;
  - `ADD COLUMN region CHAR(2) NOT NULL DEFAULT 'ID'` with `CHECK (region IN ('ID','US'))`;
  - additive, no row rewritten beyond the defaults.
  - Depends on V52 from `add-category-system-keys`.
- **Backend:** `routers/auth.py`, `services/auth.py` (registration, `get_current_user`, settings), `db/init_db.py` (seeds by language), `services/cycle_summary.py`, `services/waiting_rows.py`, `routers/review.py` (`label_source`, `note_codes`), `notification_application.BANK_CATEGORY_HINT_ALIASES` (English seed names), a small `Accept-Language` parser for unauthenticated endpoints, and tests.
- **Frontend:**
  - new: `src/i18n/request.ts`, `src/messages/{en,id}.json`, `lib/format.ts`, `lib/i18n/` (language cookie, translated-route registry, test helper);
  - changed: `app/layout.tsx`, `providers.tsx`, the shell (`components/workspace`, `components/layout`), `components/beranda`, `components/dashboard/EmergencyCoverage.tsx`, `app/auth/login`, the shared ui labels, `components/settings/AppearanceSettings.tsx`, the budget-and-currency section, `copyRegister.test.ts`, `tsconfig.json` (`allowArbitraryExtensions`) and `next.config.mjs` (the next-intl plugin);
  - the formatting of the 11 shell and Beranda files moves to `lib/format.ts`, and the other 33 move with their phases.
- **Dependencies:** `next-intl` 4.14.9 (exact), `@formatjs/icu-messageformat-parser` as an explicit dev dependency, pinned to the version next-intl 4.14.9 resolves in the lockfile. No paid service.
- **API contract:** additive `language` and `region` fields, plus `label_source` and `note_codes`. The existing `label` and `note` stay with today's Indonesian text until `localize-server-text` renders them in the account language. Sign-in, sign-up and settings errors change from a string `detail` to `{code, params, message}`, which the web already handles. The companion calls these endpoints only in an instrumented test that does not read `detail`.
- **Tooling:** `scripts/verify/markas/` runs every shell, Beranda and sign-in scenario per language, and checks figure identity and the `lang` attributes.
- **Compatibility and rollback:** the columns are additive, and old code ignores them. Rollback is a redeploy of the previous release, after which the web shows Indonesian everywhere as today. The `cft_language` cookie is ignored by old code. Accounts created in English during the window keep English starter names. The previous release handles them correctly because it already reads V52's keys. Rolling back past `add-category-system-keys` is unsafe once an English account exists, and the release note says so.
