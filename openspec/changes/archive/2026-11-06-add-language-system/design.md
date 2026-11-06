## Context

The source of truth is `cash-flow-tracker-prototypes/research/2026-11-06-language-system.md`. The owner accepted all of its §11 defaults and its §7 glossary on 2026-11-06. This design checks the research's claims against the code at `feat/markas-beranda` `d2805cb`.

Frontend:
- Next.js 16.2.6 with React 18.3.1 in `package.json`, Vitest 3.2.4 on jsdom, and `next build --webpack`.
- Almost every page is a client component. Only `app/layout.tsx` is a server component, and it hard-codes `<html lang="id">` and an Indonesian `metadata.description`.
- `id-ID` appears 75 times in 44 non-test files.
- `fmtIDR` writes `"Rp " + toLocaleString("id-ID")` with a normal space. `fmtUSD` writes `en-US` currency ("$1,234.56").
- `lib/api.ts` maps 27 structured server codes to Indonesian messages and passes plain-string `detail` through unchanged. Those strings are mostly English today.
- `copyRegister.test.ts` scans sources for slang, with an allowlist of conversation strings.

Backend:
- `users.currency` is `VARCHAR(10) DEFAULT 'IDR'`, accepted as `IDR` or `USD` only. It is a display conversion: amounts are stored as IDR integers.
- There is no language, region or timezone column, and the latest migration is V51. `add-category-system-keys` takes V52, so this change takes V53.
- Registration requires the invite code (`services/auth.register_user`), so public sign-up is closed today.

Server text on this phase's pages:
- `cycle_summary.py` sends system labels: the Kakeibo bucket names, "Tanpa kategori", "Dana likuid", "Dana investasi", "Investasi", "Saldo minus perlu dicocokkan", "Masuk", "Keluar" and "Terjadwal".
- It also sends notes ("Termasuk uang yang dipindah …", "Sudah dikurangi ganti uang Rp …") next to user data (category, goal, plan, debt and credit names).
- `waiting_rows.py` and `routers/review.py` fall back to "Tanpa keterangan" and "Tanpa kategori".
- The bell shows the monthly report's server `label`.
- Beranda already composes its period line from dates and does not use `period.label`.

Constraints:
- Language changes words only. Figures, rules, layouts and the Markas+ measurements stay.
- Indonesian copy stays byte-identical to today on every surface this change touches.
- User data is never translated and never gets a `lang` override.
- Phases 4 to 12 must not need backend changes for language, so all remaining server text ships in `localize-server-text` right after this change.

## Goals / Non-Goals

**Goals:**
- English as the default language, with an Indonesian switch, saved per account and usable before sign-in.
- The shell, Beranda with Rincian, sign-in and sign-up, and the shared labels complete in both languages.
- The pattern for phases 4 to 12: namespaces, the `terms` glossary, the formatter, the translated-route registry and the both-language gate.

**Non-Goals:**
- Server-composed conversation, the full error contract and prompt language. These are in `localize-server-text`.
- The companion, timezone, base currency, public sign-up, and English sentence capture.

## Decisions

### D1. next-intl 4.14.9 without i18n routing

Install `next-intl@4.14.9` (exact). Use its "without i18n routing" setup:
- `src/i18n/request.ts` (`getRequestConfig`) resolves the language (D2) and loads `messages/<language>.json`;
- `next.config.mjs` wraps the config with `createNextIntlPlugin`;
- the root layout wraps the app in `NextIntlClientProvider`.

`frontend/src/proxy.ts` is unchanged, and no route moves.

Rejected:
- **i18next + react-i18next (+ next-i18next 16):** works without URL locales, but needs `i18next-icu` for ICU plurals, costs about three times the client JavaScript (≈40 KB vs 13 KB min+gzip) and types plurals only partly.
- **Lingui 6:** needs an SWC Wasm plugin matched to Next's `swc_core`. Next 16.2 compatibility is unverified, and its docs show only `app/[lang]`.
- **react-intl (FormatJS):** ICU-native, but its keys are untyped and its App Router guide uses `[locale]` routes.
- **Hand-rolled dictionaries:** no ICU plurals, no argument typing, and no tested fallback.
- **A URL prefix (`/en/…`):** it helps SEO and shareable localized links. Neither matters behind sign-in, and a prefix touches every route, redirect, bookmark and companion deep link.

### D2. Language resolution order

| Context | Order |
| --- | --- |
| Signed in (any page) | account `users.language` |
| Not signed in | `cft_language` cookie → `Accept-Language` → `en` |

Rules:
- **`Accept-Language` matching:** parse the header per RFC 9110 §12.5.4 and pick the supported language with the highest q-value. A primary subtag `id` (or the legacy `in`) maps to `id`, `en` maps to `en`, and anything else is ignored. For example, `id-ID,id;q=0.9,en;q=0.8` gives `id`, `en-US,id;q=0.5` gives `en`, `ms, fr` gives `en`, and a missing or malformed header gives `en`.
- **The account wins.** `AppLayout` already fetches `/auth/me`. When `me.language` differs from the cookie, because the language was changed on another device or another account signed in on this browser, it rewrites the cookie and calls `router.refresh()` once. Signing in does the same from the login response.
- **The cookie** is `cft_language`, `en` or `id`, `Path=/`, `SameSite=Lax`, `Secure` in production and `Max-Age` one year. It is not `HttpOnly`, because the client writes it. It holds no personal data. Signing out keeps it, so the sign-in page stays in the language last used on that device.
- **Unknown cookie values** are ignored and treated as absent.

Rejected:
- `navigator.language` on the client: the first paint would be in the wrong language, and the server could not set `<html lang>`.
- Re-detecting from `Accept-Language` when signed in: a shared device would override the account's choice.

### D3. Message catalogues and namespaces

`frontend/src/messages/en.json` and `id.json` have identical key trees. This phase's namespaces:

| Namespace | Holds |
| --- | --- |
| `terms` | the glossary: every figure and concept name (D4) |
| `common` | actions and words shared everywhere: Close, Cancel, Save, Undo, Try again, Hidden, today, yesterday |
| `nav` | groups, destinations, descriptions, badge words |
| `shell` | top bar, account menu, phone bar, bell, companion status, Bantuan, skip link, metadata |
| `home` | Beranda title line, hero, Perlu tindakan, Aktivitas terbaru, context panel |
| `breakdown` | the Rincian dialog bodies and the Ketahanan Dana basis dialog |
| `kakeibo` | pillar descriptions and group names (the pillar names themselves are `terms`) |
| `auth` | sign-in and sign-up |
| `settings.language`, `settings.region` | the two controls added ahead of phase 10 |
| `errors` | every API error code the web renders (D8) |
| `conversation` | text that may use the conversational voice (`ui-copy-register`) |
| `notice` | the not-yet-translated page line (D10) |

Rules:
- **One catalogue per language is loaded in full** (about 15 KB raw for this phase, about 85 KB raw and 25 KB gzip at phase 12). Splitting waits for a measured problem.
- **Types:** `AppConfig` augmentation types `Messages` from `en.json` and `Locale` as `"en" | "id"`. `createMessagesDeclaration` types ICU arguments, which needs `allowArbitraryExtensions` in `tsconfig.json`. A missing key or argument fails `npm run type-check`.
- **Missing messages:** in tests and development `onError` throws. In production it reports the key path once, with no text and no user data, and `getMessageFallback` returns the key's Indonesian message when it exists, otherwise the key path.
- **Later phases** add one namespace each (`review`, `transactions`, `analysis`, `report`, `accounts`, `goals`, `settings.*`, `capture`).

Rejected:
- One file per namespace and language: more files without a measured need.
- Nested `terms` references inside messages (`@:terms.spending`): ICU has no references, and the parity test (D16) enforces the same spelling instead.

### D4. The `terms` namespace is the glossary

`terms` holds the one name of every concept (design Appendix A, §"Terms"), for example `terms.spending` = "Spending" / "Pengeluaran", `terms.savedInvested` = "Saved & invested" / "Ditabung & investasi", `terms.safeToSpend` = "Safe to spend" / "Aman dipakai", `terms.safetyNet` = "Safety net" / "Ketahanan Dana".

Figure labels render `terms.*` directly, and components have no second label key for a figure. Sentences that name a concept spell it exactly as its term, and the copy test checks that (D16).

"Spending" is the only English name of the spending figure, the transaction kind and its filter. "Expense(s)", "Spend" (noun), "Spent", "Outflow", "Purchases" and "Shopping" are never figure labels.

### D5. Whole ICU sentences

- **No concatenation.** Every sentence is one message with named arguments. The 53 interpolated Beranda strings (for example `` `${uang} dari ${money(masuk.total)} masuk` ``) become messages such as `home.split.name` "{money}, out of {income} income" / "{money} dari {income} masuk".
- **No ICU `number` or `date` formats.** Arguments arrive as formatted strings from `lib/format.ts` (D6), so formats follow the region, not the message language.
- **Plurals select on a raw number argument and display a formatted one.** For example, `{count, plural, one {# day} other {# days}}` uses `#` for integers. A decimal shown in a plural sentence gets both `months` (number, for selection) and `monthsText` (formatted, "5,4"). English needs `one` and `other`; Indonesian needs only `other`, which `Intl.PluralRules("id")` confirms.
- **Same argument names in both languages.** The parity test (D16) enforces it.

### D6. One formatter by language and region

`lib/format.ts` exports `createFormat({ language, region, currency, rate })` for plain functions and `useFormat()` for components (from the next-intl locale and the profile). The format locale is `${language}-${region}`: `en-ID`, `id-ID`, `en-US` or `id-US`.

| Function | `id-ID` | `en-ID` | `en-US` |
| --- | --- | --- | --- |
| `money(1234567)` IDR | Rp 1.234.567 | Rp 1.234.567 | Rp 1,234,567 |
| `money(-1234)` IDR | -Rp 1.234 | -Rp 1.234 | -Rp 1,234 |
| `money(…)` USD display | $1,234.56 | $1,234.56 | $1,234.56 |
| `longDate` | Kamis, 5 November | Thursday, 5 November | Thursday, November 5 |
| `shortDate` | 5 Nov | 5 Nov | Nov 5 |
| `decimal(5.4)` | 5,4 | 5,4 | 5.4 |
| `compact(79000)` | 79 rb | 79K | 79K |
| `relativeDay(-1)` | kemarin | yesterday | yesterday |
| `time` | 18.23 | 18.23 | 6:23 PM |

Rules:
- **Money keeps today's exact strings.** IDR is "Rp", a normal space and the region's digit grouping, with no decimals. USD display is exactly today's `en-US` currency output. `Intl` currency style is not used for IDR, because it emits a no-break space and, for `en-US`, "IDR". The figure-identity gate compares strings.
- **Every `id-ID` use, the `Rp` literals, the weekday array and the "rb"/"jt" helpers move behind the formatter.** This phase moves the 17 uses in 11 shell and Beranda files (Beranda's seven files, `EmergencyCoverage`, `AppLayout`, `CompanionStatus` ×3, `lib/utils.ts`), and the rest move with their phase. `fmtIDR`, `fmtUSD` and `fmtMoney` stay as thin wrappers until their last caller moves, so no un-migrated page changes.
- **The time zone stays `Asia/Jakarta`** from configuration. Per-account time zones are recorded debt.
- **Amount inputs group digits by region**: "20.000.000" for `ID` and "20,000,000" for `US`. Parsing still strips every non-digit.
- **Masked amounts** keep the currency symbol ("Rp ••••••", "$ ••••••"), and their accessible name is `common.hidden` ("Hidden" / "Disembunyikan").

Rejected:
- Formatting by language only (`en` → `en-US`): the owner's English view would show "IDR 1,234,567" and "November 5".
- `Intl` currency style for IDR: the no-break space and "IDR" change the strings the owner reads.

### D7. Server text on this phase's pages: codes and source, not server-rendered words

Each line in the cycle summary, each review-queue group and each waiting line keeps its `key` and `label` and gains `label_source`:
- `user`: a name the user gave (category, goal, plan, debt, credit account, merchant or notes). The web shows `label` as is.
- `system`: fixed wording. The web renders it from `key` through `terms` or `home`, for example `need` → `terms.needs`, `liquid` → `terms.availableCash`, and the fallbacks `uncategorized` → "No category" and `undescribed` → "No description".

Each line's `note` stays, and a new `note_codes: [{code, params}]` carries the same facts, for example `{code: "reduced_by_reimbursements", params: {amount: 250000}}`. The web renders the codes and formats `amount` itself.

The bell names a ready report from its `basis`, `start_date` and `end_date`, which the readiness response already returns, instead of from the server `label`.

`label` and `note` keep today's Indonesian until `localize-server-text` renders them in the account language. They remain the companion's and old clients' text.

Rejected:
- Rendering server text in the account language in this change: it needs the server catalogue, the resolver and their parity test, which `localize-server-text` builds for all server text at once.
- Removing `label` for system lines: it would break older clients and the companion's future use.

### D8. Error contract for this phase

- **Endpoint errors:** the sign-in, sign-up and settings endpoints return `detail: {code, params, message}`, where `params` is `{}` unless a value is needed. `message` is today's English string, unchanged. The codes are `too_many_attempts`, `invalid_credentials`, `username_required`, `password_too_short` (with `params.min`), `invalid_invite_code`, `username_taken`, `display_name_empty`, `no_settings_to_update`, `invalid_language` and `invalid_region`, plus FastAPI's 422 body for schema errors (including an unsupported currency), which the web maps to `form_invalid`. The shared 401 "Not authenticated" of `get_current_user` stays a string here, because every endpoint and the companion receive it. It moves with `localize-server-text`.
- **The web renders every code from `errors.<code>`.** That includes the 27 codes `lib/api.ts` maps today (25 fixed messages plus the parameterised `insufficient_funds` and `insufficient_units`), moved into `errors` with their Indonesian text unchanged and new English text. An unknown code shows `message`. A plain-string `detail` from an endpoint not yet converted is shown as today, and these are mostly English already.
- **The sign-up password-length check** in the browser uses `errors.password_too_short` with `{min}`.

The full contract (`{code, params, message}` on every endpoint, `message` in the resolved language, one exported code list checked against the web catalogue) is `localize-server-text`.

### D9. LLM prompt language is not part of this change

The four prompts keep asking for Indonesian output, so a new English account's AI-written descriptions stay Indonesian until `localize-server-text`. That change adds the language parameter, new prompt versions and English evaluation cases. It runs the paid benchmark only after the owner's OK.

Public sign-up stays closed, so the only English accounts in this window are the owner's test accounts.

### D10. Pages not yet translated show Indonesian inside the English shell

`lib/i18n/translatedRoutes.ts` lists the translated route prefixes (`/` and `/auth/login`). For any other route in an English interface:
- the page's title block and main region carry `lang="id"`;
- one muted line under the title reads `notice.untranslated` "This page is in Indonesian for now." in the interface language, with `lang="en"`;
- the shell, the top bar and its shared controls (including the period stepper) are in English.

In an Indonesian interface nothing changes.

A registry test fails when a translated route still holds literal copy, and each phase removes its routes from the untranslated set. Dialogs opened from the shell on Beranda (Catat, the transaction dialog, "Rencana jatuh tempo") belong to later phases: they carry `lang="id"` and the notice line in their header until then. "Rencana jatuh tempo" is the exception, translated here because Beranda and the bell open it directly.

Rejected:
- Hiding English until phase 12: English would not be the real default, and the owner could not test it.
- Translating every page now: about 1,800 strings in components that phases 4 to 11 rewrite within weeks.
- Forcing untranslated routes fully to Indonesian, including the shell: the language would flip while navigating.

### D11. Choosing the language and the region

- **Pengaturan › Tampilan** gets a "Language" / "Bahasa" control at the top: a two-option radio group, "English" (`lang="en"`) and "Bahasa Indonesia" (`lang="id"`), each named in its own language, with no flags. Choosing saves `PATCH /auth/settings {language}`, writes the cookie and calls `router.refresh()`. There is no reload and no URL change, and focus stays on the chosen option. On failure the choice reverts and `errors.<code>` is announced in a polite live region. The section's Indonesian purpose line adds "bahasa". Phase 10 restyles the control and does not reinvent it.
- **Pengaturan › Mata uang & anggaran** gets a "Region" / "Wilayah" select (Indonesia, United States) above the currency choice, with the hint "Sets number and date style. Language changes words only." / "Mengatur gaya angka dan tanggal. Bahasa hanya mengubah kata." It saves with that section's save, which follows "Section-Scoped Settings Save".
- **The sign-in page** shows the same two language options as links at the foot of the card. They write the cookie and refresh. There is no account to save to before sign-in.
- **The sign-up form** sends the current interface language and a region select, defaulting to Indonesia.

Both controls are translated islands inside pages that stay Indonesian until phase 10, and they carry their own `lang`.

### D12. Document language

- `<html lang>` is the resolved language (`en` or `id`).
- Untranslated page regions carry `lang="id"` (D10).
- Each language option carries its own `lang`.
- User data (names, notes, merchants) and quoted notification text get no `lang` attribute: their language is unknown, and guessing would mislead screen readers more than inheriting.
- `metadata` comes from `generateMetadata` in the resolved language.

This meets WCAG 2.2 3.1.1 Language of Page and 3.1.2 Language of Parts for the interface's own text.

### D13. Migration and backfill (V53)

```sql
ALTER TABLE users ADD COLUMN language VARCHAR(8) NOT NULL DEFAULT 'id';
ALTER TABLE users ALTER COLUMN language SET DEFAULT 'en';
ALTER TABLE users ADD CONSTRAINT ck_users_language CHECK (language IN ('en', 'id'));
ALTER TABLE users ADD COLUMN region CHAR(2) NOT NULL DEFAULT 'ID';
ALTER TABLE users ADD CONSTRAINT ck_users_region CHECK (region IN ('ID', 'US'));
```

- Every existing account, the owner's included, becomes `id`/`ID`, because those accounts were created and used in Indonesian. New accounts default to `en`/`ID`.
- `users.currency` keeps its meaning (display currency, `IDR` or `USD`).
- Registration chooses the language from the request, then `Accept-Language`, then `en`. The region comes from the request, then `ID`.
- The migration test runs on a copy of a synthetic V52 database with three accounts. All three must become `id`/`ID`, a newly inserted row must default to `en`/`ID`, and invalid values must be rejected by the constraints.

Rejected:
- Deriving the language of existing accounts from `currency`: unrelated.
- A nullable language meaning "follow the browser": the owner wants a saved choice per account.

### D14. Rollback

- The columns are additive, and the previous release ignores them. Rollback is a redeploy of that release, after which the web shows Indonesian as today.
- The cookie is ignored by old code, and the new response fields are ignored by old clients.
- Accounts registered in English during the window keep English starter names. The previous release, which contains `add-category-system-keys`, computes their figures correctly. Rolling back past that change is unsafe once such an account exists.
- No down migration.

### D15. Starter data by account language

`seed_user_defaults(user_id, language)` seeds the same categories (icons, colours, kinds, pillars, flags and system keys) with names from a per-language table. Indonesian is today's list. English is Appendix B (owner decision OD1). On-demand system categories (movement, adjustment) are created with the account-language name.

`notification_application.BANK_CATEGORY_HINT_ALIASES` adds the English seed names next to the Indonesian ones (for example `"shopping": ("Belanja", "Shopping")`), so myBCA hints still resolve for English accounts.

Existing accounts' categories are never renamed.

### D16. Tests and gates

- **`renderWithIntl(ui, { language })`** wraps components in `NextIntlClientProvider` with the real catalogue. It defaults to `id` until phase 12, so the existing 2,055 Indonesian assertions stay valid. `onError` throws.
- **Catalogue parity (Vitest):** `en.json` and `id.json` have the same key set and no empty values. Each message has the same argument names in both, parsed with `@formatjs/icu-messageformat-parser`. Every English message with a count has `one` and `other`. No `terms.*` values differ only by case, plural or an "&"/"and" swap.
- **Copy register per language** (`ui-copy-register`):
  - The guard reads the catalogues for translated namespaces and keeps scanning sources for untranslated roots.
  - Translated roots (the shell, Beranda, sign-in) must hold no literal copy. Test ids, class names, keys and routes are exempt.
  - The Indonesian rules are as today, with the conversation allowlist becoming the `conversation` namespace.
  - The English rules: no slang (`gonna|wanna|gotta|yep|nope|kinda|sorta|stuff|awesome|oops|hey|ok(ay)?`); no label jargon (`outflow|inflow|ledger|reconcil\w*|liabilit\w*|runway|cost basis`); no `expenses?|spent|spend\b` in `terms` or figure keys; no "you/your" outside `conversation`.
  - One planted fixture line per language must fail.
- **Formatter table tests** for the four locales and both currencies. They include exact string equality with `fmtIDR` for 0, ±1,234 and 1,234,567,890, and an assertion that no no-break space appears.
- **Language resolution tests:** the D2 headers, cookie over header, account over cookie, an unknown cookie, and refresh-once on mismatch.
- **Backend tests:**
  - the migration (D13);
  - `/auth/me`, login and register return the fields, and `PATCH` accepts valid values and rejects invalid ones with codes;
  - register falls back to `Accept-Language`;
  - English and Indonesian seeds carry the same keys, pillars and flags;
  - `label_source` and `note_codes` on every cycle-summary line kind, review group and waiting line;
  - another account's language never leaks through a shared cookie.
- **Both-language screenshot gate:** `scripts/verify/markas/` runs every shell, Beranda (including Rincian and the phone sheet) and sign-in scenario twice, with an `en` and an `id` account on the same parity seed, at 1440 × 900, 1024 and 390 × 844, in light and dark, plus a 320 overflow pass. It passes when:
  - the Markas+ measurements of phases 2 and 3 hold in both languages;
  - there is no horizontal overflow, clipping or overlap, except the ellipses SPEC allows;
  - sidebar labels fit on one line;
  - the hero captions stay on one line;
  - `lang` attributes are correct;
  - axe reports nothing in either language.
- **Figure identity:** `parity.mjs` reads every amount, count and date number from the DOM of the `en` and the `id` run (region `ID`) and requires them to be identical. Figure strings ("Rp 1.234.567") must also be byte-identical.
- **Pseudo-locale:** a dev-only `en-XA`, generated from `en.json` with about 40% padding and accented letters, runs the gate once to expose literal strings and tight boxes. It is not shipped.

### D17. Shared components follow the interface language everywhere

The period stepper, theme names, the modal and confirm primitives, row marks, masked amounts and Kakeibo labels render in the interface language on every page, including untranslated pages. They are shell-level or shared vocabulary, and one rendering avoids two copies of the same component. This is why `monthly-navigation-stepper` changes in this phase.

## Owner decisions (recommended default in bold)

These four are not settled by the accepted research defaults:

1. **OD1, English starter names** for new English accounts: **Appendix B**. Alternative: keep Indonesian starter names for every account until phase 8 (Rekening) or phase 10 (Pengaturan).
2. **OD2, IDR shape in region US:** **"Rp 1,234,567"** (same symbol, US grouping). Alternative: `Intl`'s "IDR 1,234,567". Region US exists because the accepted default 9 keeps it while USD display stays.
3. **OD3, existing non-owner accounts** (test accounts): **become Indonesian like the owner's**, since they were created in the Indonesian interface. Alternative: English, to exercise the default.
4. **OD4, merge order:** **`add-category-system-keys` on its own branch from `main` first; this change on `feat/language-system`, updated from `main` after phase 3 and the keys merge; then `localize-server-text` on its own branch from `main`.** All three merge before phase 4. Alternative: one stacked branch and one pull request, which would be too large to review.

## Risks / Trade-offs

- [English copy quality] → Appendix A is reviewed by the owner with this change, and the English copy register and glossary tests hold it.
- [Mixed-language pages until phase 12] → Marked with `lang` and the notice. Public sign-up stays closed.
- [AI descriptions stay Indonesian for English accounts until `localize-server-text`] → Only test accounts are English in this window.
- [Captions grow in English ("with goal deposits · 25 Nov", 27 characters)] → The gate measures one-line captions at every width. The fallback, recorded as a deviation for the owner, is "plus goal deposits · 25 Nov" or the phone legend hiding the date. Measured on 2026-11-06: at 1024 px the caption column is 154 px, and both "with goal deposits · 23 Nov" and "plus goal deposits · 23 Nov" take two lines, so the caption is "with deposits · 23 Nov", which fits at every width and is closer to "termasuk setoran".
- [The React 18 test runtime vs React 19 canary in production] → Pre-existing. A separate upgrade change.
- [Cookie and account disagree on a shared device] → The account wins, with one refresh (D2).
- [`label` and `note` stay Indonesian in the API for one change] → The web never renders them for system lines, and `localize-server-text` follows directly.

## Open Questions

None beyond OD1 to OD4.

## Appendix A. English glossary for the shell, Beranda and sign-in (Indonesian → English)

US spelling, sentence case, impersonal labels, and conversational voice only in `conversation`. The Indonesian column is today's text, which the `id` catalogue keeps byte-identical. `{…}` are ICU arguments, and `#` is the plural count.

### Terms (`terms`)

| Indonesian | English |
| --- | --- |
| Aman dipakai sampai gajian | Safe to spend until payday |
| Aman dipakai | Safe to spend |
| Masuk (figure) · Pemasukan | Income |
| Pengeluaran | Spending |
| Ditabung & investasi | Saved & invested |
| Terjadwal | Scheduled |
| Tagihan | Bills |
| Rencana | Plans |
| Kekayaan bersih | Net worth |
| aset · utang | assets · debts |
| Ketahanan Dana | Safety net |
| Dana darurat | Emergency fund |
| Dana likuid | Available cash |
| Dana investasi | Investment cash |
| Investasi | Investments |
| Saldo minus perlu dicocokkan | Negative balance to match |
| Kakeibo | Kakeibo |
| Kebutuhan · Keinginan · Budaya · Tak Terduga · Tabungan | Needs · Wants · Culture · Unexpected · Savings |
| Belum diklasifikasi | No pillar yet |
| Pilar | Pillar |
| Kebutuhan pokok · Pokok · Gaya hidup | Essentials · Essential · Lifestyle |
| Rincian | Breakdown |
| Siklus gaji | Pay cycle |
| Siklus ini · Bulan ini | This cycle · This month |
| Gaji · gajian | Salary · payday |
| Pindah saldo | Transfer |
| Top up Investasi | Investment top-up |
| Setoran target | Goal deposit |
| Ganti uang | Reimbursement |
| Jatuh tempo · Lewat jatuh tempo | Due · Overdue |
| Tanpa kategori | No category |
| Tanpa keterangan | No description |

### Navigation and shell (`nav`, `shell`)

| Indonesian | English |
| --- | --- |
| CashFlow | CashFlow (product name, unchanged) |
| HARIAN · RINGKASAN · SIMPANAN (groups) | DAILY · OVERVIEW · MONEY |
| Beranda · Tinjau · Transaksi | Home · Review · Transactions |
| Analisis · Laporan | Analysis · Reports |
| Rekening & Kantong · Target & Tagihan · Pengaturan | Accounts & Pockets · Goals & Bills · Settings |
| Lainnya · Tutup Lainnya | More · Close More |
| Komponen (local builds only) | Components |
| {count} perlu ditinjau | {count, plural, one {# needs review} other {# need review}} |
| laporan baru siap | new report ready |
| Yang perlu dijawab dan yang dicatat otomatis | What needs an answer and what was recorded automatically |
| Semua uang masuk, pengeluaran, dan pindah saldo | All income, spending and transfers |
| Saldo rekening, kantong, investasi dan utang | Balances of accounts, pockets, investments and debts |
| Dana darurat, tagihan, utang dan target tabungan | Emergency fund, bills, debts and savings goals |
| Ke mana uang digunakan, dibanding periode lalu | Where the money went, compared with the last period |
| Laporan bulanan Kakeibo | Monthly Kakeibo report |
| Lembar komponen Markas+, hanya di build lokal | Markas+ component sheet, local builds only |
| Menu utama · Navigasi samping · Navigasi ponsel | Main menu · Sidebar navigation · Phone navigation |
| Bentangkan menu samping · Ciutkan menu samping | Expand sidebar · Collapse sidebar |
| Lewati ke isi | Skip to content |
| Cari transaksi · Cari transaksi… | Search transactions · Search transactions… |
| + Catat · Catat · Catat transaksi | + Add · Add · Add transaction |
| Isi manual | Fill in manually |
| Sembunyikan saldo · Disembunyikan | Hide balances · Hidden |
| Tema · Terang · Gelap · Ikuti sistem | Theme · Light · Dark · Match system |
| Akun {name} (menu button) · Akun (menu) | Profile, {name} · Profile menu |
| Profil · Keluar | Profile · Sign out |
| Notifikasi · Tutup notifikasi | Notifications · Close notifications |
| Notifikasi, {unread} baru · Notifikasi, tidak ada yang baru | Notifications, {unread} new · Notifications, nothing new |
| Belum ada yang baru. · (baru) | Nothing new yet. · (new) |
| Rencana jatuh tempo | Plans due |
| Rencana · jatuh tempo {day} | Plan · due {day} |
| {count} rencana ditautkan otomatis | {count, plural, one {# plan linked automatically} other {# plans linked automatically}} |
| Laporan {label} siap dibaca | {period} report is ready |
| Periodenya selesai {ended} | Period ended {ended} |
| Tautkan, lewati, atau catat manual | Link, skip, or add manually |
| Belum ada HP terhubung | No phone connected yet |
| Sambungkan HP dengan kode di bawah ini | Connect a phone with the code below |
| HP terhubung · HP perlu dicek | Phone connected · Phone needs a check |
| HP terhubung tapi belum kirim kabar | Phone connected but not reporting yet |
| Terakhir menangkap {time} · kemarin {time} | Last capture {time} · yesterday {time} |
| HP belum siap menangkap notifikasi. | The phone is not ready to capture notifications. |
| Bantuan & pintasan · Tutup bantuan · Bantuan ini | Help & shortcuts · Close help · This help |
| Pintasan keyboard · Tombol · Fungsi | Keyboard shortcuts · Key · Action |
| Tutup lapisan teratas · Di Tinjau · … di Tinjau | Close the top layer · In Review · … in Review |
| HP mengirim notifikasi bank; transaksi tercatat sendiri. | The phone forwards bank notifications, and transactions are recorded automatically. |
| Yang belum pasti masuk ke Tinjau. Jawab sekali, kebiasaan diingat. | Anything uncertain goes to Review. Answer once, and the habit is remembered. |
| Beranda menunjukkan berapa yang aman dipakai sampai gajian. (`conversation`) | Home shows how much is safe to spend until payday. |
| Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan. | Single-key shortcuts can be turned off in Settings › Appearance. |
| Tutup detail · Urungkan · Rincian hitungan | Close details · Undo · Calculation breakdown |
| Catatan uang harian: berapa yang keluar, berapa yang aman dipakai, dan apa yang perlu dijawab. (metadata) | Everyday money notes: what went out, what is safe to spend, and what needs an answer. |
| This page is in Indonesian for now. (`notice`, English only in use) | This page is in Indonesian for now. |

### Beranda (`home`)

| Indonesian | English |
| --- | --- |
| Siklus gaji {span} · hari ke-{n} | Pay cycle {span} · day {n} |
| sejak {date} | since {date} |
| {month} · siklus gaji belum tersedia · Atur gaji di Pengaturan | {month} · pay cycle not available yet · Set up salary in Settings |
| Rincian aman dipakai sampai gajian (button name) | Breakdown of safe to spend until payday |
| {n} hari lagi sampai gajian {date} | {n, plural, one {# day} other {# days}} until payday on {date} |
| Gaji {start} · sejak gaji {start} · sejak 1 {month} | Salary {start} · since salary on {start} · since 1 {month} |
| Tabungan bertambah · Tabungan berkurang | Savings went up · Savings went down |
| tagihan sampai {date} | bills until {date} |
| termasuk setoran · {date} | with deposits · {date} |
| Uang siklus ini · Uang bulan ini (bar name) | Money this cycle · Money this month |
| {money} dari {income} masuk · {money}: belum ada uang masuk | {money}, out of {income} income · {money}: no income yet |
| Angka Beranda belum dapat dimuat | Home figures could not be loaded |
| Saldo dan transaksi tetap aman; coba muat lagi. | Balances and transactions are safe. Try loading again. |
| Perlu tindakan · Buka Tinjau · Buka Tinjau · {n} | Needs action · Open Review · Open Review · {n} |
| Tidak ada yang perlu dijawab. | Nothing needs an answer. |
| Daftar Tinjau belum bisa dimuat. | The Review list could not be loaded. |
| Transaksi tanpa nama | Unnamed transaction |
| Belum dikenali · {when} | Not recognized · {when} |
| hari ini {time} · Besok · {date} · Rencana | today {time} · Tomorrow · {date} · Plan |
| Pasangkan {n} pindah saldo | {n, plural, one {Pair # transfer} other {Pair # transfers}} |
| {n} pasangan pindah saldo belum pasti · Pilih pasangan yang benar | {n, plural, one {# transfer pair is uncertain} other {# transfer pairs are uncertain}} · Choose the right pair |
| Perlu jawaban · {n} transaksi · {when} | Needs an answer · {n, plural, one {# transaction} other {# transactions}} · {when} |
| Cek dan batalkan kalau keliru | Check, and undo if wrong |
| Tidak ada rencana yang jatuh tempo hari ini. | No plans are due today. |
| Aktivitas terbaru · Semua transaksi | Recent activity · All transactions |
| {DAY} · HARI INI · {DAY} · KEMARIN (CSS uppercase) | {DAY} · TODAY · {DAY} · YESTERDAY |
| Belum ada transaksi hari ini dan kemarin. | No transactions today or yesterday. |
| Aktivitas belum bisa dimuat. | Recent activity could not be loaded. |
| Uang masuk (status) · Tabungan (status) | Money in · Savings |
| Tidak dihitung · Menunggu jawaban | Not counted · Waiting for an answer |
| Masuk dari {other} · Keluar ke {other} · Masuk dari … · Keluar ke … | In from {other} · Out to {other} · In from … · Out to … |
| Pasangan belum ditemukan · Pindah saldo belum lengkap | Pair not found yet · Transfer not complete |
| Ringkasan simpanan (panel name) | Money summary |
| {months} bulan biaya pokok | {months, plural, one {{monthsText} month} other {{monthsText} months}} of essential costs |
| dana darurat {amount} · target {n} bulan | emergency fund {amount} · target {n, plural, one {# month} other {# months}} |
| Ketahanan Dana belum bisa dimuat. · Belum bisa dihitung | Safety net could not be loaded. · Cannot be calculated yet |
| Kakeibo siklus ini · Kakeibo bulan ini | Kakeibo this cycle · Kakeibo this month |
| Perlu dipilih di Tinjau | Choose in Review |
| {label} {amount}: lihat rincian · Tutup penjelasan · Kenapa belum ada: | {label} {amount}: view breakdown · Close explanation · Why it is not available: |
| {count} item | {count, plural, one {# item} other {# items}} |

### Rincian and the Ketahanan Dana basis (`breakdown`)

| Indonesian | English |
| --- | --- |
| Tutup rincian | Close breakdown |
| Belum dikenali · Perlu jawaban · Pasangan pindah saldo belum pasti | Not recognized · Needs an answer · Uncertain transfer pair |
| Pindah saldo antar rekening sendiri dan uang yang ditabung atau diinvestasikan tidak dihitung pengeluaran. | Transfers between own accounts and money saved or invested are not counted as spending. |
| Sudah dikurangi ganti uang {amount}. | Reimbursements of {amount} already subtracted. |
| {amount} dari yang menunggu jawaban sudah termasuk di sini. Jawabannya hanya memindahkan pilarnya. | {amount} of what is waiting for an answer is already included here. The answer only changes its pillar. |
| Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan (server note) | Includes money moved to savings pockets or invested |
| Pengeluaran siklus ini · Pengeluaran bulan ini | Spending this cycle · Spending this month |
| Semua pengeluaran periode ini menurut pilar Kakeibo, tanpa yang ditabung, dikurangi ganti uang. | All spending this period by Kakeibo pillar, without what was saved, less reimbursements. |
| Total pengeluaran · Lihat pengeluaran | Total spending · View spending |
| Masuk ke tabungan · Diambil dari tabungan · Pengeluaran dari tabungan | Into savings · Taken from savings · Spending from savings |
| Pengeluaran berpilar Tabungan | Spending in the Savings pillar |
| Uang yang masuk ke kantong tabungan atau investasi, dikurangi yang diambil dan yang dibelanjakan dari tabungan. | Money moved into savings pockets or investments, less what was taken out of or spent from savings. |
| Bersama pengeluaran {spending}, totalnya {total}. | Together with spending of {spending}, the total is {total}. |
| Lihat yang ditabung · Total ditabung | View savings · Total saved |
| Pengeluaran berpilar Tabungan ditambah uang bersih yang dipindah ke kantong tabungan atau diinvestasikan. | Spending in the Savings pillar plus the net money moved into savings pockets or invested. |
| Pindah saldo antar rekening sendiri dan uang pengganti tidak dihitung masuk. | Transfers between own accounts and reimbursements are not counted as income. |
| Masuk siklus ini · Masuk bulan ini | Income this cycle · Income this month |
| Uang yang masuk di periode ini, per kategori. | Income this period, by category. |
| Total masuk · Lihat uang masuk | Total income · View income |
| Tagihan: {label} · Setoran target: {label} | Bill: {label} · Goal deposit: {label} |
| jatuh tempo {date} · jatuh tempo {date}, lewat | due {date} · due {date}, overdue |
| {amount} per bulan, sudah masuk {funded} | {amount} a month, {funded} deposited so far |
| Gaji berikutnya diperkirakan {date}, dari gaji yang dikonfirmasi tanggal {receipts}. Ini perkiraan, bukan batas siklus. | Next salary expected on {date}, based on salaries confirmed on {receipts}. This is an estimate, not the end of the cycle. |
| {count} hal ({total}) masih menunggu jawaban. {included} di antaranya sudah termasuk. | {count, plural, one {# item} other {# items}} ({total}) still waiting for an answer. {included} of it is already included. |
| tanggal tiga gaji terakhir tidak teratur, sehingga tanggal gaji berikutnya tidak ditebak. | the last three salary dates are irregular, so the next salary date is not guessed. |
| sumber gaji belum diatur, sehingga gaji yang dipakai untuk perkiraan belum diketahui. | no salary source is set, so the salary used for the estimate is unknown. |
| belum ada siklus gaji yang terkonfirmasi, jadi Beranda pakai bulan kalender. | no pay cycle is confirmed yet, so Home uses the calendar month. |
| gaji diperkirakan masuk {date}, tetapi gaji baru belum tercatat. | the salary was expected on {date}, but no new salary is recorded yet. |
| Gaji baru belum tercatat · Siklus gaji belum ada | New salary not recorded yet · No pay cycle yet |
| Sumber gaji belum diatur · Tanggal gaji berikutnya belum diketahui | Salary source not set · Next salary date unknown |
| Sampai sehari sebelum perkiraan gaji berikutnya ({date}). | Until the day before the next expected salary ({date}). |
| Tanggal gaji belum diketahui, sehingga dipakai 30 hari ke depan (sampai {date}). | The salary date is unknown, so the next 30 days are used (until {date}). |
| Tagihan yang belum dibayar dan setoran target yang belum masuk, sebelum gaji berikutnya. | Unpaid bills and goal deposits not yet made, before the next salary. |
| Total terjadwal · Terjadwal sampai {date} | Total scheduled · Scheduled until {date} |
| Rencana adalah pengingat, bukan pembayaran. Setelah pembayarannya ditautkan, angkanya pindah ke Pengeluaran. | A plan is a reminder, not a payment. Once its payment is linked, the amount moves to Spending. |
| Buka Target & Tagihan · Jawab di Tinjau | Open Goals & Bills · Answer in Review |
| sudah dihitung di Ditabung & investasi | already counted in Saved & invested |
| sudah dihitung di Pengeluaran sebagai {pillar} | already counted in Spending as {pillar} |
| Masuk dikurangi Pengeluaran, Ditabung & investasi dan tagihan terjadwal sampai akhir {end}. | Income less Spending, Saved & invested and scheduled bills until the end of {end}. |
| Lebih dari uang masuk | More than income |
| Lihat transaksi siklus ini · Lihat transaksi bulan ini | View transactions this cycle · View transactions this month |
| Dasar Ketahanan Dana | Safety net basis |
| Saldo cadangan dibagi biaya hidup pokok bulanan. | Reserve balance divided by monthly essential living costs. |
| Saldo cadangan · Biaya hidup pokok bulanan · Belum diketahui | Reserve balance · Monthly essential living costs · Unknown |
| Belum ada dana darurat khusus — dihitung dari seluruh saldo kas | No dedicated emergency fund yet — counted from all cash balances |
| Dari target dana darurat: {names} | From emergency fund goals: {names} |
| {months}x biaya pokok · {days} hari · 0 hari | {monthsText}× essential costs · {days, plural, one {# day} other {# days}} · 0 days |
| Belum diketahui: belum ada periode lengkap dan belum ada anggaran kategori Kebutuhan. | Unknown: no complete period yet and no budget on Needs categories. |
| Dari anggaran kategori Kebutuhan (belum ada periode lengkap){debt} | From the budgets of Needs categories (no complete period yet){debt} |
| Median {n} periode, kebutuhan (need) dan yang belum diklasifikasi{debt} · + cicilan minimum | Median of {n, plural, one {# period} other {# periods}}, Needs and no pillar yet{debt} · + minimum installments |
| {amount}/bln | {amount}/mo |
| {amount}, lihat transaksi kebutuhan {period} · + belum diklasifikasi | {amount}, view Needs transactions {period} · + no pillar yet |
| {amount}, lihat transaksi belum diklasifikasi {period} | {amount}, view transactions with no pillar yet {period} |

### Kakeibo descriptions (`kakeibo`)

| Indonesian | English |
| --- | --- |
| Pokok sehari-hari: makan harian, belanja dapur, transport rutin, tagihan, cicilan. | Everyday essentials: daily meals, groceries, regular transport, bills, installments. |
| Bisa ditunda: kafe, jajan, makan di luar karena ingin, belanja non-pokok, hiburan. | Can wait: cafés, snacks, eating out for fun, non-essential shopping, entertainment. |
| Pengembangan diri dan relasi: olahraga, kursus, buku, acara, persembahan, hadiah terencana. | Growth and relationships: sport, courses, books, events, offerings, planned gifts. |
| Tidak direncanakan bulan ini: perbaikan, berobat mendadak, kondangan mendadak, denda. | Not planned this month: repairs, sudden medical costs, short-notice weddings, fines. |
| Disisihkan, bukan dibelanjakan. | Set aside, not spent. |
| Belum punya pilar, dari transaksi maupun kategorinya. Tidak dihitung sebagai Kebutuhan. | No pillar from the transaction or its category. Not counted as Needs. |

### Sign-in, sign-up and the two settings controls (`auth`, `settings.language`, `settings.region`)

| Indonesian | English |
| --- | --- |
| Masuk ke akun · Buat akun baru | Sign in · Create an account |
| Masuk (button) · Daftar Akun · Memproses… | Sign in · Create account · Working… |
| Nama Tampilan · Nama Pengguna · Kata Sandi · Kode Undangan | Display name · Username · Password · Invite code |
| Minimal {min} karakter. | At least {min} characters. |
| Kata sandi minimal {min} karakter (`errors.password_too_short`) | Password must have at least {min} characters |
| Belum punya akun? Daftar sekarang | No account yet? Create one |
| Sudah punya akun? Masuk di sini | Already have an account? Sign in here |
| Terjadi kesalahan, silakan coba lagi | Something went wrong. Try again. |
| Bahasa · English · Bahasa Indonesia | Language · English · Bahasa Indonesia (each option in its own language) |
| Wilayah · Indonesia · Amerika Serikat | Region · Indonesia · United States |
| Mengatur gaya angka dan tanggal. Bahasa hanya mengubah kata. | Sets number and date style. Language changes words only. |
| Bahasa disimpan · Bahasa belum tersimpan. Coba lagi. | Language saved · Language not saved. Try again. |

Homonyms are split by meaning: "Masuk" is `terms.income` as a figure and `auth.signIn` on the sign-in page, and "Keluar" is `shell.signOut` in the account menu (it is never a figure label).

## Appendix B. English starter names (OD1)

| Seed key (internal) | Indonesian (unchanged) | English |
| --- | --- | --- |
| food | Makanan & Minuman | Food & drinks |
| snacks | Jajan & Nongkrong | Snacks & going out |
| groceries | Belanja Harian | Groceries |
| transport | Transportasi | Transport |
| bills | Tagihan & Utilitas | Bills & utilities |
| shopping | Belanja | Shopping |
| entertainment | Hiburan | Entertainment |
| health | Kesehatan | Health |
| investment (system key `investment`) | Investasi | Investments |
| movement (system key `internal_movement`, both kinds) | Internal Movement | Transfer |
| salary | Gaji | Salary |
| other income | Pendapatan Lain | Other income |
| reimbursement (`offsets_spending`) | Patungan & Ganti Uang | Reimbursements |
| adjustment (system key `balance_adjustment`, on demand) | Penyesuaian Saldo | Balance adjustment |
| cash account | Uang Tunai | Cash |
| bank account | Rekening Utama | Main account |

These are seed data. Once created they are the user's own names and are never translated.
