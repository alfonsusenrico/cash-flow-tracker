# Verification: apply-markas-pengaturan (Markas+ phase 10)

Date: 2026-11-09 · Branch `feat/markas-pengaturan` · Base: `origin/main` `2e891fe` (phases 1 to 9, the language
system, localized server text and the credit flow; PR #97 merged), merged in `5d241c2` · Code under test: `57a003e`
(frontend and tooling) unless a check names another revision.

## Approval, decisions and dependencies (tasks 1.1, 1.2)

- Approval: the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08) and
  "continue all" (2026-11-09). Every flagged default (D3, D5, D7, D8, D10, D17) is taken.
- **Owner override 2026-11-08:** the pairing QR in "Hubungkan ponsel" stays at least 200 px (drawn at 200 css px at
  the device pixel ratio, in a 218 px white box with an 8 px quiet border plus the code's two-module margin), not the
  prototype's 118 px.
- Base: the plan was committed on `0308b7b` (`2f279cd`), phase 7's branch merged (`396b887`, PR #97 not merged then),
  the plan re-based on the merged code (`a066c83`, design "Re-based on the merged code"), then `origin/main` `2e891fe`
  after PR #97 merged (`5d241c2`, no file changed against `a066c83`). D23's "merged into main first" holds: phases
  4 to 9 and `localize-server-text` are ancestors of `2e891fe`.
- `Rail`, `RailItem`, `RailRule`, `RailLabel`, `ListRow` (`layout="stacked"`, `timeIsDate`), `ShellDialog`
  (`initialFocus`, `scrollFocusable`), `RadioSegments`, `KindAmount`/`RowAmount`, `SectionHeader`, `Field`,
  `format.*` and phase 6's `components/settings/QuestionRate.tsx` existed and are reused.

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these
over:
- Phase 10 (`apply-markas-pengaturan`) built 2026-11-09 under the standing instruction; branch `feat/markas-pengaturan`
  pushed, no PR (coordinator's instruction). Flags D3, D5, D7, D8, D10, D17 accepted with their defaults.
- Owner override 2026-11-08 applied: pairing QR ≥ 200 px. D9's fallback applied to the download QR (136 px of code in a
  154 px box), because 118 px gives 2.62 px per module.
- `/settings` is translated (`TRANSLATED_ROUTES`) and out of `untranslatedPages.ts`; only the local `/komponen` sheet
  remains untranslated, so the untranslated-page tests use `/komponen`. The Catat capture form (phase 11) is the
  remaining untranslated surface (`Modal untranslated`, tested by `sharedLanguage.test.tsx`).
- "Keluar" left Lainnya; it ends the Pengaturan rail. The account menu keeps it.
- `Modal`'s remaining users are `CatatDialog` and `QuickCaptureModal` (phase 11).
- `components/accounts/money.module.css` is deleted (Pengaturan was its last user); `.ws-switch` CSS is replaced by
  `.switch` (`components/ui/Switch.tsx`).
- End-review list: the proposal's nine deviations, the deviations below, and a real-phone scan of the pairing QR and of
  the download QR (agents do not operate the owner's phone).
- Known outdated check (phase 12): Beranda's checklist still expects a 112 px status column and dark expense amounts.

## What changed

- **Frame and rail (D1 to D5, D20):** `app/settings/layout.tsx` renders `.set`; `SettingsRail` on `Rail` with three
  lists labelled PERANGKAT / TAMPILAN / AKUN, section links with `aria-current="page"` and the 2 × 20 bar, a rule and
  "Keluar" (workspace `logout`). Focus moves to the section title after a rail choice; on a phone the title scrolls
  under the header. `SettingsSectionList`, the "Semua pengaturan" back link and the 900 px container query are gone.
  `RailItem` gained `href` (a `Link`) and `icon`; `RadioSegments` options may carry their own `lang`.
- **Sections (D4, D6 to D15):** `SettingsSection` (Manrope 20 title, one meta line, `.stack`), `SettingRow` (`.toggle`),
  `Switch`. HP & perangkat: "Status HP" box (`PhoneStatus`: companion line, one block per phone with four checks in
  words), download (`QrCode` at device pixel ratio), "Perangkat terhubung" rows with "Cabut" on a 480 `ShellDialog`,
  the open manual-key section with the inline confirmation and the once-only box. "Hubungkan ponsel" on a 520
  `ShellDialog` with the QR, three steps (countdown in step 3, one announcement at one minute and at expiry), the code
  box, the rule, the collapsed download, the restyled checklist after pairing, "Nanti saja"/"Buat kode baru" or
  "Selesai". Kebiasaan: status rows, two plain lists, "Seberapa sering ditanya" with a `t-fig`. Tampilan: Bahasa and
  Tema segments, two switches and the help row. Profil and Mata uang & anggaran on `.form-grid` with saves at the foot;
  Gaji & siklus with the source box, the picker behind "Ganti sumber" and `KindAmount` incomes; Pemetaan and Nama
  pengirim as plain rows with icon buttons; Sesi uji with the running line, rows with "Selesai", report, CSV and
  "Hapus data".
- **Language (D19):** `settings.*` (nav, sections, common, devices, health, pairing, apiKey, habits, questionRate,
  appearance, profile, salary, budget, mappings, senders, testSession, remove; `language` and `region` kept), 363 new
  messages per catalogue. `lib/deviceHealth.ts` returns codes (`DeviceProblem`, step keys) and `problemText`;
  `lib/habits.ts` returns status codes. The shell's companion line now words a device problem in the interface
  language (no `lang="id"` part). Dates and times through `useFormat` (`yearDateAt`, `time`, new `timeSeconds`),
  amounts through `useMoney`/`MaskedMoney`. The copy register checks `app/settings`, `components/settings`,
  `components/ui/Switch.tsx`, `lib/deviceHealth.ts`, `lib/habits.ts` as translated roots (no literal copy) and the
  `settings.*` messages in both registers.
- **Hidden balances (D18):** habit top-ups, salary amounts, the rate and session amounts are masked; the limit is a
  read-only mask and the save sends the stored limit.
- **Moved:** `CompanionDownload`, `DeviceHealthChecklist`, `DevicePairingDialog`, `DevicePairingSection`,
  `SalarySettingsSection`, `SenderAliasesSection` and their tests from `components/ui/` to `components/settings/`.
- **Removed:** `SettingsCard.tsx` (`SettingsCard`, `SettingsSubsection`, `SettingsField`, `SettingsList`, the class
  constants), `SettingsSectionList`, `money.module.css`, `.ws-switch*`, `.qr-moved`, tinted readiness and checklist
  fills, the `Modal` use of both device dialogs, Keluar in `MoreSheet`, the `/settings` entry in `untranslatedPages.ts`.
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/` (`git diff --stat origin/main -- backend db mobile nginx`
  is empty).

## Deviations and why

Beyond the proposal's nine and design "Re-based on the merged code":
1. **Breakpoints:** `styles.css`' tablet rule wins (184 | 1fr, gap 32 from 768 to 1199 px); 200 | ≤ 720 gap 48 from
   1200 px. Phone rail items are 44 px (SPEC targets), the prototype's are 36.
2. **Download QR:** 154 px box (D9 fallback), see the QR note.
3. **Copy register:** a passed check reads "Baik" / "Good" (`beres` and English `OK` are register slang); the salary
   meta has no "gajian sekitar tanggal {day}" (no day exists: `payday_day` is deprecated, `/salary/source` returns
   none). Further neutral rewrites outside Appendix A: the battery hint ("Agar aplikasi tidak dihentikan diam-diam…"),
   "HP belum dapat mengirim ke server.", "{step} belum berjalan." and the habit rule; validation messages keep their
   Indonesian.
4. **English plain-register rewrites of Appendix A:** "Repeated answers are recorded automatically.", "The display
   name and the names that appear in bank notifications.", "The display name is always included.", "Confirmed by the
   owner" (Appendix A's lines used "you"/"your", which the register forbids).
5. **Pairing footer:** "Nanti saja" and "Buat kode baru" before a phone pairs (today's "Buat kode baru" stays reachable
   while a code is valid), "Selesai" once paired (D9 said "Nanti saja" until ready); the waiting checklist is not shown
   before pairing (D9.4: the checklist replaces the QR once a phone pairs).
6. **Rows that keep today's facts:** Sesi uji rows keep the session window ("24 Okt 19.00 – 20.00") and the
   case-number count instead of "{minutes} menit"; salary review rows show name, date and amount as the confirmed
   rows; a mapping row keeps "→ {account} · {INSTITUTION} · {source}". Sender edit/remove names keep the mask
   ("Ubah nama pengirim {mask}") because names repeat and masks do not.
7. **Hints dropped (D16):** the display-name hint, the emergency months hint and its "{n}× pengeluaran pokok" aside,
   the running session's second line and the salary intro sentence; the rule "Kode hanya dapat dipakai sekali…" stays
   because it explains the countdown.
8. **Revoke focus:** after a successful revoke focus goes to "Hubungkan ponsel" (the row and its "Cabut" are gone).
9. **Download button icon:** the download icon, not the prototype's upload icon (the button downloads).
10. **Search folding:** `SenderAliasesSection` still folds with `toLocaleLowerCase("id-ID")`; it is search matching,
    not display formatting, so `rg "id-ID" components/settings` finds only that line.
11. **Commit 51c9ff0** holds only the file moves and the CSS-module deletion (a chained command stopped before staging
    the rest); `7103e94` completes it. History was not rewritten.

## QR module size (task 3.3)

| Code | Payload | Modules (+ 2 × 2 margin) | Drawn | css px per module |
| --- | --- | --- | --- | --- |
| Pairing | `cashflow://pair?v=1&server=https%3A%2F%2Ffinance.alfonsusenrico.com&code=ABCDE12345` (83 chars, level M) | 37 (41) | 200 css px | 4.88 |
| Download | the latest-release APK URL (99 chars, level M) | 41 (45) | 136 css px (fallback; 118 gave 2.62) | 3.02 |

Both are black on white in both themes (`color: #000000/#ffffff`), drawn at `devicePixelRatio`, inside a white box
with an 8 px padding. Captures measure the rendered pairing canvas at 200 × 200 css px in every cell and theme
(checklist "pairing QR ≥ 200 css px"). A scan with a real phone is on the end-review list.

## Environment

- Local only, synthetic data. PostgreSQL 16 containers `cft-pengaturan-pg` (127.0.0.1:8751) and `cft-pengaturan-pytest`
  (8752) with throwaway passwords; Flyway `flyway/flyway:10`. No production system, paid service or AI run.
- Owners: `cft_markas_set` = `seed_parity.py` (data.js) migrated V51 → V54, then the new `seed_pengaturan.py` (the
  parity phone made ready, "Ponsel Cadangan" silent for 30 hours, sender names up to eleven with one ambiguous);
  `cft_markas_set_stress` = the same with `--stress` (twelve phones, one with a 60-character name, access off and failed
  self-tests among them, sixty sender names with a 60-character one). `_en` twins with the account in English; the
  stress English twin also uses the USD display.
- Backend from this worktree in a `uv` Python 3.12 venv (`requirements-dev.txt`, as CI); jobs, AI and outbound HTTP
  off. Web after (`next start`, 3790) and before (`5d241c2`'s `frontend/` from `git archive`, 3791) behind loopback
  proxies; a failing proxy (3799) answers the device, habit, mapping and sender reads with 500. Headless Chrome over
  CDP, axe-core 4 (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa). Captures and JSON stay in `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/`.

## Checks

| Check | Revision | Result |
| --- | --- | --- |
| `npm run type-check` | `3635257` | pass |
| `npm run lint` | `3635257` | pass |
| `npm test` | `3635257` | 99 files, 1,062 tests passed (base `0308b7b`+phase 7: 1,050) |
| `npm run build` | `3635257` | pass |
| Backend pytest, fresh `postgres:16` V1 → V54 | `57a003e` (backend unchanged since `2e891fe`) | 2197 passed, exit 0 |
| `openspec validate --all --strict` | `3635257` | 85 of 85 |
| `git diff --check origin/main HEAD` | `3635257` | clean |
| `git diff --stat origin/main -- backend db mobile nginx` | `3635257` | empty |

## Captures, checklist and accessibility (tasks 5.1 to 5.3, 5.7)

`pengaturan-checklist.js` runs on every capture: frame at y = 168 at 1440 (the prototype's measured value), rail 200 and
form ≤ 720 with gap 48 from 1200 px, 184 and 32 from 768 to 1199, one column with the rail 24 above below 768, nine
section links and Keluar, three group labels, rail items 36 (44 on a phone), one current item with the square 2 × 20
bar, section title Manrope 600 20/28, description 12/16 with margins 4/32, parts 48 apart (32), no card, toggle rows
≥ 56, switch 36 × 20 with a 44 px phone hit area, segments 30 (40), fields 40, download QR box 154 on white with ≥ 136
px of code, box padding 24 (16), buttons ≥ 44 on a phone, dialogs 520 (pairing) and 480 (revoke) or a bottom sheet,
pairing QR ≥ 200 css px, square, on white with an 8 px quiet border, drawn at the pixel ratio, font sizes and weights
of the scale, ≤ 8 text styles, no text ≥ 33 px, no border ≥ 2 px, no coloured border, no tinted status fill, no
horizontal overflow.

| Set | Build | Cells | Captures | Checklist | axe serious/critical | Console errors | Overflow | Notice or `lang="id"` in English |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Final, Indonesian | `3635257`'s frontend minus the failure-state fix (`57a003e`) | 1440, 1280, 1024, 768, 767, 390, 320 × light, dark | 238 | 5542 / 5542 | 0 | 0 | 0 | — |
| Final, English | same | same | 238 | 5542 / 5542 | 0 | 0 | 0 | 0 |
| HEAD, Indonesian | `3635257` | 1440 light/dark, 390 dark; HP & perangkat and both dialogs | 9 | 151 / 151 | 0 | 0 | 0 | — |
| HEAD, English | `3635257` | same | 9 | 151 / 151 | 0 | 0 | 0 | 0 |
| Failure states, Indonesian | `3635257` | 1440 light, 390 dark; five sections | 10 | 244 / 244 | 0 | only the planted 500 responses | 0 | — |
| Stress, Indonesian | `57a003e` | 1440, 1024, 390, 320 × light, dark; seven sections | 56 | 1380 / 1380 | 0 | 0 | 0 | — |
| Stress + USD, English | `57a003e` | same | 56 | 1380 / 1380 | 0 | 0 | 0 | 0 |

Scenarios: `/settings`, the nine sections, Mata uang & anggaran and Kebiasaan with hidden balances, "Hubungkan ponsel"
(code shown), "Cabut Ponsel Cadangan?", the API key confirmation, Nama pengirim with a search, Sesi uji with a report.
The prototype's nine `pengaturan-*` routes and `dlg-hubungkan-ponsel` were captured at 1440, 1024 and 390 in both
themes for the side-by-side (`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/prototype/`); its values set the checklist (frame y = 168, the tablet rule).
A transient 502 from the loopback proxy in an earlier run (`dlg-cabut-1024-light`) did not recur in the final runs.

Key screenshots:
- 1440 light: `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-id/pengaturan-perangkat-1440-light.png`, `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-en/pengaturan-perangkat-1440-light.png`
- 390 dark: `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-id/pengaturan-tampilan-390-dark.png`, `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-en/pengaturan-perangkat-390-dark.png`
- Hubungkan ponsel (QR 200 × 200 css px): `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-id/dlg-hubungkan-ponsel-1440-light.png`, `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-en/dlg-hubungkan-ponsel-390-dark.png`
- Cabut: `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-id/dlg-cabut-1440-light.png`
- Kebiasaan: `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-id/pengaturan-kebiasaan-1440-light.png`, `/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-pengaturan/shots/final-en/pengaturan-kebiasaan-1440-light.png`

## Keyboard flows and requests (tasks 5.4, 5.5)

`pengaturan-keys.mjs` (nine flows of D24) on `57a003e` at 1440 and 390: Indonesian 37 of 37, English 37 of 37. Rail
Enter opens the section with focus on its title (in view below the phone header); Keluar is the last rail item, sends
one `POST /api/auth/logout` per run (2 per language, counted in the API log) and opens the sign-in page; the language
segment sends `PATCH /api/auth/settings {"language"}` once per arrow and keeps focus on the chosen option; theme and
shortcut controls send nothing; Profil sends `{name, name_aliases}`; 40 months raises the alert and sends nothing, a
valid save sends `{currency, region, emergency_fund_multiplier, monthly_spending_budget}` with the stored 4.800.000;
the mapping trash asks in the row with focus on Batal and Esc returns focus; "Hubungkan ponsel" sends one
`POST /api/devices/pairing-codes`, traps Tab, and Esc returns focus to it; "Cabut" opens its dialog and Esc returns
focus with no `DELETE`.

Request parity: the recorded bodies are those the old components sent (same `api.*` calls, unchanged in the diff), and
the unit tests assert them: profile, budget (including null and the hidden-balance case), language, salary calls,
pairing, revoke, mappings, sender names and test sessions. A recorder on the old interface was not scripted, because
its controls differ; the request functions are untouched.

## Value parity (task 5.6)

`pengaturan-parity.mjs` reads each section's text and fields and checks every value the API holds (device names and
versions, the key prefix, habit labels, categories, counts and top-up amounts, the salary source and receipts, emergency
months and the limit, profile names, mappings, sender names and masks, session counts): before (`5d241c2`) 51 of 51,
after Indonesian 51 of 51, after English 51 of 51. Every figure (digit group) of the eight sections is identical in
English and Indonesian with region Indonesia.

## Copy (task 4.9)

Indonesian strings changed on purpose: every "changed" line of Appendix A; "beres" → "Baik"; the pairing slang
(`HP-mu siap!…`, `Tenang, … benerinnya`, `Aku berhenti ngecek…`, `Kodenya gagal dibuat. Coba lagi, ya.`,
`Belum pernah kirim status…, ya.`, the battery hint, `belum bisa ngirim`, `belum jalan`); "Buat Kunci Baru (Reset)" →
"Buat kunci baru (reset)"; "Batas belanja operasional bulanan (IDR)" → "Batas pengeluaran bulanan"; the section
descriptions of Appendix A; the session line "Sesi berjalan sampai … disimpan, tidak dicatat". `copyRegister.test.ts`
checks the `settings.*` messages in both registers and the settings sources as translated roots; the task's
pattern "ya\.|-mu\b|ngecek|arahin|benerin" finds nothing in `components/settings` sources and, in the `settings`
messages, only standard "-nya." endings (berikutnya, langkah-langkahnya, sebelumnya, statusnya, memperbaikinya), no
"…, ya." tag and no "-mu".

## Not verified

- A real-phone scan of the pairing and download QR codes (end-review list).
- Pixel-level side-by-side composites of prototype and app (captures exist for both; differences are listed above).
- A request recorder on the old interface (see request parity).
- Salary candidates needing review: the seed has none, so "Perlu ditinjau" was captured empty; the rows are covered
  by `SalarySettingsSection.test.tsx`.
