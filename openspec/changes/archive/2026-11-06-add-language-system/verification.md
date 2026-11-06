# Verification: add-language-system

Date: 2026-11-06. Branch `feat/language-system`, stacked on `feat/category-system-keys` (b61ad53, pull request #88, open); `main` is f1ee604. Every check below ran on the working tree that became the implementation commit (recorded at the end), on loopback, with synthetic data in disposable databases only. Evidence: `research/evidence/add-language-system/`.

## Owner acceptance

On 2026-11-06 the owner approved the three-change plan with every recommended default ("ok to all") and gave the standing instruction:

> "complete all until finished" (owner, 2026-11-06)

That instruction is the acceptance this change is archived under (tasks 6.7 and 6.8). The owner reviews the pull request before merging.

## What changed

- **Account language and region (V53):**
  - `users.language` (`en` | `id`) and `users.region` (`ID` | `US`);
  - existing rows become `id`/`ID`, new rows default to `en`/`ID`.
  - `/auth/me`, sign-in, sign-up and the settings response return both, and `PATCH /auth/settings` accepts both.
  - Sign-up takes the page's language and the chosen region, and falls back to `Accept-Language` (RFC 9110 q-values) and `ID`.
- **Error codes:** sign-in, sign-up and settings errors are `detail: {code, params, message}`, with `message` unchanged. The web renders every code from `errors` in the interface language: these ten, the 27 codes `lib/api.ts` mapped before, and FastAPI's 422 list as `form_invalid`.
- **Beranda server text:**
  - cycle-summary lines, review groups and waiting lines carry `label_source` (and `label_key` for the fallbacks);
  - Keluar lines carry `note_codes`.
  - `label` and `note` are byte-identical to the baseline response.
- **Starter data by language** (Appendix B):
  - English accounts get English names with the same icons, colours, kinds, pillars, flags and system keys;
  - on-demand system categories are named in the account language;
  - myBCA hints resolve to English starter names.
- **Web language system:**
  - next-intl 4.14.9 without i18n routing and a dynamic `<html lang>`;
  - `messages/{en,id}.json` with 388 keys, typed (`createMessagesDeclaration`);
  - `lib/format.ts` by language and region;
  - the `cft_language` cookie and the account reconciliation (one refresh);
  - the translated-route registry with the notice line and `lang="id"`;
  - `renderWithIntl` and a default Indonesian provider for every test.
- **Copy:**
  - the shell, Beranda with Rincian, sign-in and sign-up, "Rencana jatuh tempo", the test-session line and the shared primitives (stepper, theme names, modal, confirm, masked amounts, Kakeibo names on translated surfaces) in both languages;
  - Language in Pengaturan › Tampilan, and Region in Mata uang & anggaran and at sign-up.
- **Amount inputs** group digits by region. Every dot-only amount parser now strips the region's separator (`stripGrouping`), so US-grouped input parses to the same integer.

## Checks

| Check | Result |
| --- | --- |
| Frontend `npm test` | 94 files, 900 tests passed (was 84 files, 808 tests at the start) |
| Frontend `npm run type-check`, `npm run lint`, `npm run build` | passed, 0 lint findings; the build lists all 22 routes as dynamic (the request reads the cookie) |
| Planted type-check failures (task 3.1) | a missing key fails `tsc` (TS2554); a missing ICU argument fails (TS2345: `{ days: number; date: string }`) |
| Planted missing message in a test (3.5) | throws `MISSING_MESSAGE` |
| Catalogue parity (4.1) | same 388 keys, no empty values, same arguments and tags, English counts with `one`/`other`, no ICU number or date formats; three planted mismatch cases fail and name the key |
| Copy register (5.1, 5.2) | both catalogues clean; translated sources hold no literal copy (AST scan of 55 files); every Beranda figure label is a `terms.*` key; planted lines fail per language: Indonesian slang, English "Expenses" term, "your", "ledger", a near-duplicate term, a literal JSX label, a non-glossary figure key |
| Backend full suite (6.5) | 1990 passed, 0 failed, 0 skipped, on a fresh postgres:16 migrated V1 to V53 with flyway/flyway:10 ("Schema version: 53"), `SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1`. 55 new tests: language parser, 24 account-language API tests (one per error code), label sources, English bank hints, the V53 migration |
| Migration test (2.1) | three synthetic V52 accounts become `id`/`ID`, a new row defaults to `en`/`ID`, and `fr` and `SG` are rejected. On the parity seed, `dimas_contoh` reads `id`/`ID` after V53 |
| Baseline response identity (2.6) | cycle summary, review queue and net worth: 45 `label` and 12 `note` values byte-identical to b61ad53 |
| `openspec validate --all --strict` | 85 passed, 0 failed |
| `git diff --check` | clean |

## Both-language gate (6.1)

The gate runs on the parity seed (`dimas_contoh`, data.js shifted to 2026-11-06). The baseline was captured on b61ad53 (V52); after the change, an `id` account and an `en` account (V53) were captured on identical copies of the same data. The cells were 1440 × 900, 1024 × 768, 390 × 844 and 320 × 844, light and dark. The scenarios were Beranda, Beranda with hidden balances, the Rincian (closed, open with its lines, hidden), every shell scenario (sidebar and rail, top bar, bell, theme, profile menu, Bantuan, Lainnya, every untranslated page in the shell, the test session) and, signed out, sign-in and sign-up.

| Set | Captures | Checks | Failed | Axe serious/critical | Horizontal overflow |
| --- | --- | --- | --- | --- | --- |
| Before (b61ad53), signed in | 150 | 7,090 | 34 | 6 | 0 |
| After, Indonesian, signed in | 150 | 7,090 | 34, same as before | 6, same as before | 0 |
| After, English, signed in | 150 | 7,090 | 34, same checks as before | 6, same as before | 0 |
| Before / Indonesian / English, signed out | 16 each | n/a | 0 | 0 | 0 |

The 34 failures and 6 axe findings are pre-existing and identical in all three sets:
- full-bleed list rows 11 px past the frame on Beranda below 1440;
- Analisis › Peta Aliran content outside the frame;
- Laporan, Rekening and Target at 320;
- the test-session banner in the `shell-sesi-uji` captures;
- `color-contrast` on the Analisis calendar's weekday cells, an untranslated page.

In English the test-session capture adds the notice line, so its "first content y" reads 252 against 212 instead of 232 against 192. That is the same banner offset.

More gate results:
- **No new failure in either language:** the Markas+ measurements of phases 2 and 3 hold, the sidebar labels stay on one line, the hero captions stay on one line, and no figure leaves its block.
- **Document language:**
  - every English capture has `<html lang="en">` and every Indonesian one `lang="id"`;
  - in English, each of the 10 untranslated shell pages has `main[lang=id]`, `h1[lang=id]` and the notice `p[lang=en]`, and shell controls inside them (stepper, more actions) are `lang="en"`;
  - no English Beranda capture has any `lang="id"` region;
  - sign-in has `button[lang=en] English` and `button[lang=id] Bahasa Indonesia`.
- **The gate's own fixes during the run:**
  - the checklists read the expected copy per document language;
  - `capture.mjs` gained signed-out runs, the language cookie, lang reporting, and a cleanup that ends the test session the `shell-sesi-uji` scenario opens;
  - the two runs before that cleanup were discarded.
- **Deviations found by the gate and fixed** (recorded for the owner):
  1. "with goal deposits · 23 Nov" took two lines at 1024 px, where the caption column is 154 px. The design's fallback "plus goal deposits" also took two lines. The caption is now **"with deposits · 23 Nov"**, which fits at every width and is closer to "termasuk setoran". design.md Appendix A and the risk note are updated.
  2. At 320 px the English hero label "SAFE TO SPEND UNTIL PAYDAY" wraps, and the colour mark overlapped it. The mark now sits inside the label, 3 px above its first line, so it never overlaps. The Indonesian measurements are unchanged.
  3. The "More actions" button on untranslated pages now carries `lang="en"`.

Comparison images are in `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/language-system/compare/`:
- `en-vs-id/` (23 pairs);
- `prototype-vs-en/` (9 pairs);
- `before-vs-id/` (3 pairs).

A selection is committed under `research/evidence/add-language-system/compare/`.

## Figure identity (6.2) and Indonesian text identity (4.1)

- **en vs id** (`parity-languages.mjs`), on Beranda, the Rincian, and the Rincian with every line and the waiting rows open:
  - 97 figures (42, 12, 43), 70 of them money strings;
  - 0 differing values, and the money strings are byte-identical.
- **Before vs Indonesian after** (`parity.mjs`, every page route): 18 of 19 pages identical. The difference is Pengaturan › Sesi uji, whose list holds the test sessions the capture runs opened in each database at different times. That is data, not a figure change.
- **Rendered Indonesian text** (`text-identity.mjs`, visible lines and every `aria-label`, `placeholder` and `title`), on Beranda, the Rincian with its lines, the bell, the profile menu, Bantuan, Lainnya, the phone Beranda, sign-in and sign-up:
  - identical, apart from the new controls on sign-in (the language links, and the Region field at sign-up).

## Pseudo-locale (6.3)

`pseudo-locale.mjs` generates en-XA (accents, about 40% padding) into a throwaway copy of the frontend. It is never shipped. It ran at 1440 and 390, light, signed in and out.

- **Literal string found and fixed:** the test-session line under the top bar ("Sesi uji berjalan sampai …") was still literal Indonesian in the shell. It is now in `shell.testSession` and in the source guard's translated roots. The second pseudo run shows no unbracketed interface text in the shell, Beranda, the Rincian or sign-in. User data stays as stored.
- **Tight boxes found:**
  - with padding, "Accounts & Pockets" ellipsizes in the 240 px sidebar;
  - the Scheduled caption wraps at 1440;
  - the hero label wraps at 390.
- **Measured with the real English copy:** the sidebar label fits on one line at every width. The caption and the hero mark were fixed as above.

## Keyboard and screen reader (6.4)

`language-keys.mjs` ran in Brave headless against the English account, and 8 of 9 checks passed:
- **Skip link:** "Skip to content" is the first stop and moves focus to `main`.
- **Tab order:** Breakdown, Open Review · 4, the three Needs action rows, the activity rows, All transactions, then the panel.
- **Breakdown:** it opens with focus inside, and Escape closes it with focus back on its button.
- **→ in the Language group:**
  - saves `id` and refreshes in place;
  - `/settings/tampilan` is unchanged;
  - `<html lang="id">`, and the sidebar reads "Beranda";
  - focus stays on "Bahasa Indonesia".
- **←** switches back to English the same way.
- **The cookie** reads `cft_language=en`.
- **The data:** an md5 over every transaction, category and account row of that database is identical before and after the two switches (`a26d6521…`).
- **One check failed: the cookie's stored lifetime is 180 days, not a year.** `Path=/`, `SameSite=Lax` and not `HttpOnly` are as specified, and `Secure` is absent over plain-HTTP loopback, as specified. Brave caps cookies set by `document.cookie` at 180 days. The written attribute string is `Max-Age=31536000` (unit test), and `Secure` is added on `https:` (unit test). No HTTPS stack or other browser was available locally.

VoiceOver was **not run**: there is no interactive session in this environment. Instead:
- the `lang` evidence above shows the page language and `lang="id"` on the "Bahasa Indonesia" option;
- the layout tests show `<html lang>` and the description for both languages;
- a production-build `curl` of `/auth/login` shows the language for each case:
  - `Accept-Language: id-ID,id;q=0.9,en;q=0.8` gives `lang="id"` with the Indonesian description;
  - `en-US,id;q=0.5`, `ms, fr` and no header give `lang="en"`;
  - the cookie `id` wins over the header `en`;
  - the unknown cookie `fr` is ignored.

## Deviations and decisions (no money or scope change)

- **Stacked branch (task 1.2):** `add-category-system-keys` is in the history through b61ad53, not through a merge into `main`. Update this branch from `main` once #88 merges.
- **Bank hints:** a parser hint matched by name or alias no longer reaches a category with a `system_key`. An English account's movement category is named "Transfer", and myBCA's "account transfer" alias would otherwise file transfers to other people under it. Indonesian accounts are unaffected: no Indonesian system category carries an alias name. Tested.
- **Kakeibo labels:** they follow the language on the translated surfaces and the shared components. Inside untranslated page bodies (Tinjau choices, Transaksi filters, forms) they stay Indonesian with their page, marked `lang="id"`, until each page's phase. Mixing English pillar names into Indonesian sentences would mislabel the language.
- **Stepper:**
  - on Transaksi and Laporan the stepper names the period from its dates in English ("Oct 2026", "Cycle 25 Sept – 24 Oct");
  - Analisis keeps the server's label, because its Ringkasan tab has no dates;
  - `localize-server-text` renders window labels in the account language.
- **Page keys in Bantuan:** the page key rows of Tinjau and Transaksi stay Indonesian, with `lang="id"`, until those pages translate their key groups.
- **Companion status:** a device problem in the companion status (Pengaturan › HP) stays Indonesian with `lang="id"`.
- **Decimal inputs:** investment price and unit inputs keep Indonesian grouping in region US, because their parser reads a comma as the decimal mark. Integer amount inputs follow the region.
- **Month names:** `en-ID` writes "Sept" (CLDR).
- **Large counts:** an Indonesian count of 1,000 or more inside a message is now grouped ("1.234 transaksi" instead of "1234").
- **New Indonesian text:**
  - a FastAPI 422 list now reads "Isian belum valid. Periksa lalu coba lagi." instead of raw JSON;
  - the Tampilan purpose line adds "Bahasa".
- **Dead code:** the obsolete `planDay` helper is removed. `LedgerRowMarks` has no consumers and is unchanged.
- **Seed loader:** `seed_parity.py` still maps V51, so the gate seeded at V51 and migrated to V52 or V53, as V53 intends for existing accounts.

## Not verified

- VoiceOver, as above.
- `Secure` and the one-year lifetime in a real HTTPS browser.
- Production:
  - the post-merge check (Flyway 53 applied, the owner's account `id`/`ID`) is recorded in `PROJECT_STATE.md` for after the merge;
  - release stays with the owner.
- **AI-written descriptions** for English accounts stay Indonesian, as do the purpose, top-up, habit and capture questions and other server-composed text. They come with `localize-server-text`, which ships next, before phase 4.

## Revision

Implementation commit 947f8b0 (`feat(i18n): add English interface with per-account language and region`). On that exact commit: frontend type-check 0 errors, lint clean, 94 files and 900 tests passed, build passed, `openspec validate --all --strict` 85 passed, `git diff --check` clean; the backend suite above ran on the same backend tree (no backend change after it). The archive commit follows on `feat/language-system`.
