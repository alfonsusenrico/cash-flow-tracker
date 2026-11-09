## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Pengaturan (`/settings`) is the last page still built from the pre-redesign cards. It is phase 10 of the phased plan (`cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md`).

The audit (`design-options/audit/AUDIT.md` §Pengaturan) found the structure right but the page too wordy: seven long captions on the first section against two in the original, and seven font weights. Five more issues belong to this phase:
- **The page is Indonesian in an English interface.** Only the Language and Region controls of `add-language-system` and the companion status line are translated. `/settings` is still in `untranslatedPages.ts`, and its Pengaturan dialogs (Hubungkan ponsel, Cabut perangkat) are Indonesian.
- **The rail disappears below about 1237 px.** The settings grid is a container query at 900 px, so with the sidebar open the page switches to the phone index ("Semua pengaturan" back link) on a 1280 px laptop. The prototype keeps the `200 | ≤ 720` grid from 1024 px and puts the rail above the form on a phone.
- **"Keluar" is not in Pengaturan.** The shell kept it as the last row of the phone's Lainnya sheet until this phase (`apply-markas-shell` owner decision 7).
- **Wording.** "Batas belanja operasional bulanan" breaks the Pengeluaran rule (SPEC §12: "Batas pengeluaran bulanan"); the pairing dialog speaks slang ("Aku berhenti ngecek dulu. Lanjutin di aplikasi HP-mu, ya."), which the owner's register rule forbids in fixed interface text.
- **Coloured status fills.** The pairing checklist and the device badges use the tinted `--in-bg`, `--out-bg` and `--warn-bg` fills that phase 1 retired everywhere else.

The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/pages2.js` `PAGES.pengaturan` and `settingsBody`, `js/dialogs.js` `hubungkan-ponsel`, `SPEC.md` §6 to §8, §11 and §13, and the captures `pengaturan-perangkat`, `-kebiasaan`, `-tampilan`, `-profil`, `-gaji`, `-anggaran`, `-pemetaan`, `-pengirim`, `-sesi-uji` and `dlg-hubungkan-ponsel`, each at 1440 and 390 in both themes.

## Expected Outcome

- At 1440 px Pengaturan reads as in the prototype: the title "Pengaturan", then a 200 px rail and a form column of at most 720 px, 48 px apart, inside the 1040 frame. The rail lists PERANGKAT (HP & perangkat, Kebiasaan), TAMPILAN (Tampilan) and AKUN (Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Nama pengirim, Sesi uji), a rule, and "Keluar". The current section carries the 2 × 20 px selection bar.
- Each section starts with its Manrope 20 title and one description line; its parts follow 48 px apart with 12 px uppercase labels, without cards.
- From 768 px the rail stays beside the form whether the sidebar is open or collapsed. Below 768 px the rail sits above the section, as in the prototype; `/settings` shows HP & perangkat under it.
- "Keluar" in the rail signs out exactly as the account menu does. The phone's Lainnya sheet no longer lists it.
- HP & perangkat shows a "Status HP" box with each paired phone's readiness and problems, the companion download with a real QR code in the prototype's 136 px box, the paired phones with "Cabut", "Hubungkan ponsel", and the manual API key as its own section.
- "Hubungkan ponsel" opens a centered 520 px dialog (a bottom sheet on a phone) with the real pairing QR code, three numbered steps, the code to type, the countdown, "Buat kode baru" and the live checklist. Every pairing, polling, revoke and key request is the one sent today.
- Kebiasaan, Tampilan, Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Nama pengirim and Sesi uji keep every control and request they have today, restyled with the shared rows, switches, segments, fields and lists. "Batas pengeluaran bulanan" replaces "Batas belanja operasional bulanan".
- An English account reads the whole page and its dialogs in plain English ("Settings", "Phone & devices", "Habits", "Appearance", "Profile", "Salary & cycle", "Currency & budget", "Notification mapping", "Sender names", "Test session", "Sign out"). An Indonesian account reads neutral, firm Indonesian. "This page is in Indonesian for now." no longer appears on `/settings`.
- Field focus is the phase 1 1 px edge with a 3 px soft ring; no thick or coloured border remains.
- Every saved value, list, count and date equals today's for the same data, in both languages.

## Approval

Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D24, summarized in "Decisions and their defaults"). No decision changes money behaviour: no figure, no budget rule, no currency rule and no request body changes. Six decisions are flagged because they go beyond the phased plan's wording or change an approved requirement:
- **D3 (scope):** the rail stays beside the form from 768 px (a viewport rule, as `styles.css`) and lists every section above the form on a phone, replacing today's container query and the phone index with its "Semua pengaturan" back link (spec "Sectioned Settings Page").
- **D5 (scope, owner decision 7 of phase 2):** "Keluar" joins the rail and leaves the Lainnya sheet. The desktop account menu keeps it, as in the prototype.
- **D7 (scope):** the device readiness and problems move from each device row into the "Status HP" box; the rows keep name, pairing date, last activity, version and "Cabut" (spec "Device Readiness in Settings").
- **D8 (scope):** the manual API key becomes an open section with its rotation confirmation, as in the prototype, instead of a collapsed disclosure (spec "API Key Section in Settings").
- **D10 (scope):** Kebiasaan's two auto-record states stay read-only status rows. The prototype draws them as switches, but the server setting `HABIT_AUTO_RECORD_ENABLED` controls them and no endpoint changes them.
- **D17 (copy):** the pairing dialog's slang and second-person lines become neutral sentences, including the ready line "HP-mu siap! …" required by spec "Live Pairing Checklist".

## Deviations from the prototype

Each is a decision in `design.md` and goes to the owner's end-review list:
1. The Status HP box lists every paired phone (one block each) and its problems from the health report; the prototype draws one phone (D7).
2. The pairing dialog keeps the app's countdown, "Buat kode baru", the live four-step checklist and the collapsed companion download; the prototype draws a QR, three steps and a code (D9).
3. Kebiasaan's auto-record states are status rows, not switches (D10). Habits keep two lists (payments and investment top-ups) with the app's status words instead of one "Kebiasaan dihafal" list with "% cocok" and "Dipakai/Belajar" badges, because the API has no match percentage per habit (D10).
4. "Seberapa sering ditanya" keeps phase 6's content and request (`GET /api/dashboard/question-rate?weeks=8`), restyled (D10).
5. Tampilan starts with "Bahasa" from `add-language-system`, which the prototype predates (D11).
6. Mata uang & anggaran starts with "Wilayah" from `add-language-system`; the currency hint keeps today's rate ("1 USD ≈ Rp …"); the limit field keeps its "leave empty" hint, because it changes a decision (D13).
7. Gaji & siklus keeps the app's source picker with paging inside "Ganti sumber" and "Tandai sebagai gaji" (D12).
8. Nama pengirim keeps the search above eight names and the ambiguity line; Sesi uji keeps the running-session report, labelling, CSV and delete (D14, D15).
9. The prototype shows a "forced theme" caption that the app has no source for; it is not built (D11).

## What Changes

- **Page frame (D1 to D4):** the `.set` grid (`200 | minmax(0, 720px)`, gap 48) from 768 px, the phone stack below 768 px; the shared phase 4 `Rail`, `RailItem` and `RailRule` with group labels and the selection bar; each section's `t-dtitle` title and one `t-meta` line, parts in `.stack` 48 apart with `SectionHeader` labels; `/settings` keeps rendering HP & perangkat.
- **Keluar (D5):** a rail button with the logout icon that calls the workspace `logout`; removed from `MoreSheet`.
- **HP & perangkat (D6 to D9):** "Status HP" box (companion line, then each phone's four checks with check and alert icons and status words); "Unduh aplikasi pendamping" with the 136 px QR box, the APK button and the Android and SHA-256 lines; "Perangkat terhubung" with "Hubungkan ponsel" and one row per phone with "Cabut" (centered 480 confirmation); "Kunci API manual (cadangan)" as a section; the Hubungkan ponsel dialog rebuilt on `ShellDialog` (520).
- **Other sections (D10 to D15):** restyled with `.toggle` rows, the `.switch`, `Segmented`, `.form-grid` fields, `ListRow`-style plain rows, icon buttons and today's inline `ConfirmRemove`.
- **Wording (D16, D17):** "Batas pengeluaran bulanan"; one description per section; hints only where they change a decision; neutral pairing copy.
- **Hidden balances (D18):** amounts in Gaji & siklus, Kebiasaan, the rate line and the limit field follow "Sembunyikan saldo".
- **Language (D19):** `settings.*` namespaces in `messages/en.json` and `id.json`; `/settings` added to `TRANSLATED_ROUTES` and removed from `untranslatedPages.ts`; `lib/deviceHealth.ts` and `lib/habits.ts` texts become message keys; dates and amounts through `lib/format.ts` instead of hard-coded `id-ID`.
- **Removed** (each with its destination in D22): `SettingsCard`, `SettingsSubsection`, `SettingsList`, `SettingsSectionList` and the "Semua pengaturan" back link; the settings rules of `components/accounts/money.module.css` (`.settingsPage` to `.emptyBlock`, `.themePick`, `.swatch`, `.switchRow`, `.drawerFacts`); `ws-card` on settings; the theme swatches; the tinted readiness and checklist fills; the `Modal` use of the pairing and revoke dialogs; Keluar in `MoreSheet`; the `/settings` entry of `untranslatedPages.ts`.
- **BREAKING (spec):** `currency-conversion-and-settings` "Sectioned Settings Page", "Section-Scoped Settings Save", "Consistent Settings Form Layout" and "Help and Phone Status in Pengaturan"; `device-pairing` "Pairing in Web Settings" and "Companion Download in Settings"; `device-health` "Live Pairing Checklist" and "Device Readiness in Settings"; `web-api-key-management` "API Key Section in Settings"; `habit-learning` "Kebiasaan Settings Section"; `theme-presets-switcher` "Tampilan Settings Section"; `executive-sidebar-layout` "Phone Tab Bar" change their presentation. Requests, stored data and rules do not change.

## Non-goals

- No backend, API, schema, migration or data change, and no change to the Android companion. `backend/`, `db/` and `mobile/` stay untouched.
- No new setting and no new control: no habit or top-up switch, no theme sync to the server, no new currency or region.
- No change to pairing codes, their lifetime, the polling rule, revoke, the key shown once, salary candidates and confirmation, currency and rate, budgets, mappings, sender names, test sessions, theme and shortcut storage.
- No Catat or global dialog work beyond the Lainnya sheet's Keluar row (phase 11, `apply-markas-catat`).
- No new npm dependency; `qrcode` 1.5.4 stays.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `currency-conversion-and-settings`: "Sectioned Settings Page", "Section-Scoped Settings Save", "Consistent Settings Form Layout", "Help and Phone Status in Pengaturan"; added "Sign Out in Pengaturan", "Pengaturan in Both Languages" and "Hidden Balances in Pengaturan".
- `device-pairing`: "Pairing in Web Settings", "Companion Download in Settings".
- `device-health`: "Live Pairing Checklist", "Device Readiness in Settings".
- `web-api-key-management`: "API Key Section in Settings".
- `habit-learning`: "Kebiasaan Settings Section".
- `theme-presets-switcher`: "Tampilan Settings Section".
- `executive-sidebar-layout`: "Phone Tab Bar" (Lainnya without Keluar).

`interface-language` and `ui-copy-register` need no delta: moving `/settings` to the translated routes follows their rule that the set of untranslated pages only shrinks.

Standards:
- UI and accessibility (WCAG 2.2 AA): landmarks and headings, rail as a labelled navigation with `aria-current`, switches `role=switch`, segments `role=radiogroup`, focus trap and return in both dialogs, focus not obscured by the top bar or tab bar, 44 px targets on a phone, reflow at 320 px, status in words beside every icon, 3.1.1 and 3.1.2;
- the copy register in both languages (`ui-copy-register`);
- `money`: the limit, the emergency months and every listed amount unchanged; hidden balances respected;
- `personal-data`: no rendered text, key or user data in logs; the new API key is shown once and never logged;
- `api-consumers`: no API change.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Pengaturan:** `app/settings/layout.tsx`, `page.tsx` and the nine section routes; `components/settings/*` (`SettingsNav.tsx` rebuilt on `Rail`, `sections.ts` with message keys and the PERANGKAT/TAMPILAN/AKUN groups, `DevicesSettings`, `HabitsSettings`, `AppearanceSettings`, `ProfileSettings`, `BudgetSettings`, `NotificationMappings`, `TestSessionSection`, `ManualApiKey`, `ConfirmRemove`; `SettingsCard.tsx` replaced by `SettingsSection.tsx`); `components/ui/DevicePairingSection.tsx`, `DevicePairingDialog.tsx`, `DeviceHealthChecklist.tsx`, `CompanionDownload.tsx`, `SalarySettingsSection.tsx`, `SenderAliasesSection.tsx` (moved to `components/settings/` with their tests); phase 6's `components/settings/QuestionRate.tsx`.
- **Shell:** `components/workspace/PhoneNav.tsx` (`MoreSheet` loses Keluar) and its test.
- **Language:** `messages/en.json` and `id.json` (`settings.*`), `lib/i18n/translatedRoutes.ts`, `components/workspace/untranslatedPages.ts`, the copy register roots, `lib/deviceHealth.ts`, `lib/habits.ts`.
- **CSS:** `app/globals.css` gains `.set`, `.toggle`, `.switch`, `.qr`, `.form-grid`, `.form-stack` and `.stack` from `styles.css` where earlier phases did not add them; `components/accounts/money.module.css` loses its settings rules.
- **Tooling (local only):** `scripts/verify/markas/`: the Pengaturan scenarios, `pengaturan-checklist.js`, `pengaturan-keys.mjs`, the parity seed extended with two paired phones (one ready, one silent), habits and top-ups, salary receipts and a candidate, mappings, eleven sender names, a finished test session; the stress and USD seed for Pengaturan.
- **Dependencies:** `localize-server-text`, phases 4 to 9 (`apply-markas-tinjau` for the rail, `apply-markas-analisis` for the moved "Seberapa sering ditanya") merge first (design D23).
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit; no data is involved.
