## 1. Approval, dependencies and baseline

- [ ] 1.1 Record the approval and the flagged review:
  - in `proposal.md` (already written: "Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults") and in `PROJECT_STATE.md`, with the date;
  - the outcome of the flagged decisions D3, D5, D7, D8, D10 and D17 (default, or the recorded fallback) in `design.md` and the spec delta before any code.

  Evidence: both files name the date and each flag's outcome; `openspec validate apply-markas-pengaturan --strict` passes after the edits.
- [ ] 1.2 Confirm the dependencies and update the branch (D23). `localize-server-text` and phases 4 to 9 (`apply-markas-tinjau`, `-transaksi`, `-analisis`, `-laporan`, `-rekening`, `-target`) must be merged into `main`; stop and report if one is not. Then update `feat/markas-pengaturan` from `origin/main`. Evidence:
  - `git merge-base --is-ancestor` succeeds for each merge commit;
  - `Rail`, `RailItem`, `RailRule`, `ListRow` `stacked`, `RowAmount` and the moved `components/settings/QuestionRate.tsx` exist; every difference from design Context is written to `verification.md`;
  - a refused settings save (`PATCH /api/auth/settings {language: "fr"}`) returns `detail.code` `invalid_language` on a migrated disposable database;
  - an English `/settings` shows "This page is in Indonesian for now.";
  - `openspec validate --all --strict` passes on the updated branch, and the revision is recorded in `verification.md`.
- [ ] 1.3 Capture the "before" state on that `main` revision, tied to its hash: the nine sections, the pairing dialog (code, expired, paired, ready), the revoke dialog, the API key after rotation, Kebiasaan empty and error, in `en` and `id`, at 1440, 1280, 1024, 768, 767, 390 and 320 px, light and dark, with and without hidden balances. Record every settings request of the D24 flows with the request recorder (`requests-before.json`) and the Indonesian rendered text (`copy-before-id.json`). Evidence: captures and both JSON files in `research/evidence/apply-markas-pengaturan/before/`.
- [ ] 1.4 Extend the seeds (D24), local and disposable only: two paired phones (one ready, one silent for 30 hours), five habits and three top-ups in each status, a salary source with two confirmed receipts and one candidate, four mappings, eleven sender names with one ambiguous, a finished test session with labelled events; the stress and USD seed (twelve phones, 120 habits, 60 sender names, 60-character names). Evidence: `test_seed_parity.py` passes; the seed report lists every addition; `git diff --stat origin/main -- backend db mobile` is empty.
- [ ] 1.5 Add the Pengaturan scenarios to `scenarios-app.json` and `scenarios-prototype.json` (`en` and `id` for the app), `pengaturan-checklist.js`, `pengaturan-keys.mjs` and the request recorder. Evidence: a dry run over the prototype's nine `pengaturan-*` routes and `dlg-hubungkan-ponsel` at 1440, 1024 and 390 in both themes writes captures and a checklist JSON whose expected values equal D1 to D15 and the prototype's computed values; `pengaturan-keys.mjs --dry-run` lists the nine flows of D24.

Expected Result: the dependencies are on the branch, every section, state and dialog can be seeded and captured, and the pre-change state, requests and copy are recorded.

## 2. Frame, rail and Keluar

- [ ] 2.1 Add `.set`, `.toggle`, `.switch`, `.qr`, `.form-grid`, `.form-stack` and `.stack` from `styles.css` with the phase 1 token names where missing, and `Switch` if no shared switch exists (D22). Evidence: the `/komponen` sheet shows a toggle row with a switch on and off, a QR box and a two-column form grid in both themes, and their checklist values pass there; `selectionMarks.test.ts` passes.
- [ ] 2.2 Rebuild `layout.tsx`, `SettingsNav` and `sections.ts` on `Rail` with the three groups, `SettingsSection` and the viewport breakpoints (D1 to D4, D20). Evidence: `app/settings/layout.test.tsx` rewritten for spec "Sectioned Settings Page" (every scenario, including the grouped lists, `aria-current`, focus on the section title, the not-found slug); a test at 767 and 768 px widths shows the stacked rail and the grid; `SettingsSectionList`, the back link and the container query are gone.
- [ ] 2.3 Add "Keluar" to the rail and remove it from `MoreSheet` (D5). Evidence: a rail test asserts one `POST /api/auth/logout` and the navigation to `/auth/login` (spec "Sign Out in Pengaturan", both scenarios); `workspace.test.tsx`'s Lainnya order ends with "Tema" (spec "Phone Tab Bar"); the account menu test still lists "Profil" and "Keluar".

Expected Result: every section opens in the prototype's frame on every width, and Keluar lives in Pengaturan.

## 3. HP & perangkat and its dialogs

- [ ] 3.1 Build the "Status HP" box (D7) from `GET /api/devices` with `readiness` and `deviceProblems` codes, and slim the device rows (D6). Evidence: tests for spec "Device Readiness in Settings" (ready, silent, two phones, never reported) and "Help and Phone Status in Pengaturan" (the companion line first); no tinted fill remains (`rg "in-bg|out-bg|warn-bg" components/settings` empty).
- [ ] 3.2 Restyle the download section with the 136 px QR box (D6). Evidence: `CompanionDownload.test.tsx` passes with the spec "Companion Download in Settings" scenarios (URL, checksum link, Android line, QR failure alert); the canvas draws at device pixel ratio.
- [ ] 3.3 Rebuild "Hubungkan ponsel" on `ShellDialog` 520 and the revoke confirmation on `ShellDialog` 480 (D6, D9), and measure the QR modules. Evidence:
  - `DevicePairingDialog.test.tsx` and `DevicePairingSection.test.tsx` pass with the new markup: polling every 3 s, stop on close, ready and timeout, expiry and "Buat kode baru", the collapsed download, revoke and cancel with focus return;
  - spec "Pairing in Web Settings" scenario "Same pairing requests" against the request recorder;
  - the module-size note in `verification.md`: modules per side and css px per module for the longest pairing payload at 200 px (owner override 2026-11-08) and the download URL at 118 px, each ≥ 3 or the fallback of D9 applied and listed as a deviation.
- [ ] 3.4 Make the manual API key an open section with the inline confirmation and the once-only box (D8). Evidence: `DevicesSettings.test.tsx` covers spec "API Key Section in Settings" (rotate, paired phone kept, section last and open); a test asserts the key is not in `console` calls and is cleared from state after "Tutup".

Expected Result: HP & perangkat matches the prototype with every device, pairing and key request unchanged.

## 4. Other sections, language and clean-up

- [ ] 4.1 Move the copy to `settings.*` in `messages/en.json` and `id.json` (D19, Appendix A), convert `lib/deviceHealth.ts` and `lib/habits.ts` to codes, and route dates and amounts through `lib/format.ts` and `useMoney`. Evidence: the catalogue parity test passes (same trees and argument names); a list of every moved string with its key is in `verification.md`; `rg "id-ID" components/settings` is empty.
- [ ] 4.2 Kebiasaan (D10). Evidence: `HabitsSettings.test.tsx` covers spec "Kebiasaan Settings Section" (both scenarios) and the unchanged "Kebiasaan Status and Empty States" scenarios (load fails, nothing learned, server switch off); the moved "Seberapa sering ditanya" test passes with the same request.
- [ ] 4.3 Tampilan (D11). Evidence: `AppearanceSettings.test.tsx` covers spec "Tampilan Settings Section" (all five scenarios) and the language control's existing cases (both languages with their own `lang`, save, cookie, refresh, focus kept, refusal announced).
- [ ] 4.4 Profil and Gaji & siklus (D12). Evidence: `ProfileSettings.test.tsx` and `SalarySettingsSection.test.tsx` pass with the same request bodies (`{name, name_aliases}`; the ten salary calls) and spec "Section-Scoped Settings Save" scenario "Saving the profile leaves budget settings untouched"; salary amounts render through `RowAmount`.
- [ ] 4.5 Mata uang & anggaran (D13, D18). Evidence: `BudgetSettings.test.tsx` covers spec "Section-Scoped Settings Save" (budget save, clearing the limit sends null, 40 months refused) and spec "Hidden Balances in Pengaturan" "Saving with balances hidden"; the label reads "Batas pengeluaran bulanan".
- [ ] 4.6 Pemetaan notifikasi and Nama pengirim (D14). Evidence: `NotificationMappings.test.tsx` and `SenderAliasesSection.test.tsx` pass for spec "Confirmed Removal and Search of Notification Names" (confirm, cancel with focus return, search at twelve names, none at five) and spec "Consistent Settings Form Layout" "Empty mapping list".
- [ ] 4.7 Sesi uji (D15). Evidence: `TestSessionSection.test.tsx` passes with the same requests, the 409 message, the report labelling and delete confirmation; the shell banner test passes unchanged.
- [ ] 4.8 Language registry (D19). Evidence: `/settings` is in `TRANSLATED_ROUTES` and not in `untranslatedPages.ts`; the copy register's roots include `app/settings` and `components/settings`; `copyRegister.test.ts` passes; spec "Pengaturan in Both Languages" scenarios pass in a test that renders `/settings/anggaran` in `en` and compares figures and dates with `id`; the untranslated-page test example moves as D19 says.
- [ ] 4.9 Copy review. Evidence: `copy-after-id.json` compared with `copy-before-id.json`; every changed Indonesian string is one Appendix A marks "changed", listed in `verification.md`; no slang or "kamu/-mu" remains (`copyRegister.test.ts` and an `rg` for "ya\.|-mu\b|ngecek|arahin|benerin" in `components/settings` and `messages/id.json` settings keys).
- [ ] 4.10 Remove what D22 lists. Evidence: `rg` finds no user of `SettingsCard`, `SettingsSubsection`, `SettingsList`, `SettingsSectionList`, `settingsPage`, `themePick`, `swatch`, `switchRow`, `drawerFacts`, `habitTable` (or each remaining user is named in `verification.md`); `Modal`'s remaining importers are only `CatatDialog` and `QuickCaptureModal`, listed for phase 11.

Expected Result: every section is rebuilt with today's controls and requests, in both languages, and nothing obsolete remains.

## 5. Verification gate

- [ ] 5.1 Side by side, on the parity seed: prototype and app captures of the nine `pengaturan-*` scenarios and `dlg-hubungkan-ponsel` at 1440 × 900, 1024 × 768 and 390 × 844, light and dark, the app in `en` and `id`, plus composed left/right images; overflow widths 1280, 768, 767 and 320. Evidence: images in `research/evidence/apply-markas-pengaturan/after/` tied to the branch revision; every visible difference listed in `verification.md` and fixed, named as a deviation, or added to the owner's end-review list.
- [ ] 5.2 The SPEC measurement checklist (`pengaturan-checklist.js`) and the SPEC §13 global rules, in both languages. Evidence: the checklist JSON with pass and fail counts in `verification.md` and zero fails (or each with its reason): rail 200 and form ≤ 720 with gap 48, rail items 36, selection bar 2 × 20, title Manrope 20/28, description 12/16 with 4 and 32 margins, parts 48 apart, toggle rows 56, switch 36 × 20, segments 30 (40 phone), fields 40 with the 1 px edge and 3 px ring on focus, QR box 136, dialog 520 and 480; first content `y = 192` at 1440; no hero figure; sizes, weights and ≤ 8 text styles; no border ≥ 2 px and no coloured border except the selection bar and focus; no horizontal overflow at every width of 5.1.
- [ ] 5.3 Accessibility. Evidence: axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical findings in both themes and both languages at 1440, 1024 and 390 on every section, both dialogs (each state of the pairing dialog), the API key box, hidden balances, loading, failure and empty states; zero console errors; no `lang="id"` region on an English settings page. Results in `verification.md`.
- [ ] 5.4 Keyboard flows: `pengaturan-keys.mjs` runs the nine flows of D24 in both languages at 1440 and 390, including the focus trap and focus return of "Hubungkan ponsel" and "Cabut". Evidence: every flow passes, recorded with the revision.
- [ ] 5.5 Request parity. Evidence: `requests-after.json` equals `requests-before.json` for every scripted flow (method, path, body keys and values), recorded in `verification.md`; the only differences allowed are none.
- [ ] 5.6 Value parity, en vs id. Evidence: `parity.mjs` reads every saved value, list entry, count, date and amount on the nine sections before and after, and in `en` against `id` (region `ID`), with zero changed values in `verification.md`.
- [ ] 5.7 Stress and USD seed at 1440, 1024, 390 and 320 in both themes and both languages. Evidence: captures with no clipping, overlap or horizontal scroll for twelve phones, 120 habits, 60 sender names and 60-character names; recorded in `verification.md`.
- [ ] 5.8 Frontend and repository checks on the exact revision. Evidence: `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict` pass, each recorded with the commit hash in `verification.md`; `git diff --stat origin/main -- backend db mobile nginx` is empty.
- [ ] 5.9 Complete `verification.md` and present the result. Record the owner's standing instruction (2026-11-06, renewed 2026-11-08) as the acceptance basis, and add the deviations, the flag outcomes, the QR module-size result and the real-phone scan to the end-review list in `PROJECT_STATE.md`. Evidence: `verification.md` names what changed, the gate counts, the deviations, and what was not verified and why.

Expected Result: the phase passes the plan's common gate and the both-language gate on its exact revision, with every deviation and flag recorded.

## Workflow follow-up

- Archive: `openspec archive apply-markas-pengaturan`, then `openspec validate --all --strict` must pass.
- Commit with a subject-only Conventional Commits message, and push `feat/markas-pengaturan`.
- Create the pull request into `main` (the owner's standing instruction of 2026-11-06, renewed 2026-11-08), and report its URL with the pushed revision. Merging stays with the owner.
