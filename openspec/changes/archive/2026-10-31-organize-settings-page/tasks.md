# Tasks

## 1. Settings page and navigation

- [x] 1.1 Add `components/settings` primitives (section registry, `SettingsCard`, `SettingsField`, inline removal confirmation) and the `app/settings` layout with the desktop section list and the phone section list. Evidence: `app/settings/layout.test.tsx` — six links, `aria-current="page"` on the current section, Profil current at `/settings`, phone back link on a section route, axe clean.
- [x] 1.2 Add the six section routes and the Profil and Mata uang & anggaran sections with their own saves. Evidence: `ProfileSettings.test.tsx` and `BudgetSettings.test.tsx` — PATCH bodies contain only the section's keys, the alias trimming and the 3–150 character and 1–36 month refusals send nothing, invalidations match design decision 3, unsaved-changes note, axe clean.
- [x] 1.3 Point the sidebar gear, the collapsed-sidebar menu item and the phone drawer item to `/settings`, remove `SettingsModal` and its `AppLayout` wiring. Evidence: `Sidebar.test.tsx` — links to `/settings` with accessible name "Pengaturan", `aria-current` on `/settings`, drawer closes on activation; `rg SettingsModal frontend/src` returns nothing.

Expected Result: Settings opens as a page with one section per card and a Save that only saves that card.

## 2. Section content

- [x] 2.1 Reorganise `SalarySettingsSection`: current-source block, "Ganti sumber" disclosure with the full-width select, "Lebih baru"/"Lebih lama" in one row, "Jadikan sumber gaji" primary, "Tandai sebagai gaji", "Periksa pemasukan dari sumber ini", review rows with "Ini gaji"/"Bukan gaji", confirmed rows with "Batalkan". Evidence: `SalarySettingsSection.test.tsx` — source PUT, confirm, reject, revoke, detect and paging requests unchanged, accessible names name the income, empty state, conflict alert, axe clean.
- [x] 2.2 Perangkat & akses: devices list with "Cabut", "Hubungkan ponsel" dialog, collapsed "Kunci API manual (cadangan)" after the devices. Evidence: `DevicesSettings.test.tsx` — manual key collapsed and after devices, rotation shows the key once, Escape closes only the pairing dialog and returns focus; `DevicePairingSection.test.tsx` still passes.
- [x] 2.3 Pemetaan notifikasi and Nama pengirim with quiet "Hapus" plus inline confirmation, and sender search above 8 names. Evidence: `NotificationMappings.test.tsx` and `SenderAliasesSection.test.tsx` — confirm sends the delete once, cancel and Escape send nothing and return focus, search filters by mask/name/bank/account with a match count, no search at 8 or fewer, failure and empty states, axe clean.

Expected Result: every existing settings function works from its section with aligned, consistent controls.

## 3. Beranda insight

- [x] 3.1 Rebuild `NarrativeInsightCard` on `GET /api/dashboard/period-comparison` and use it on desktop Beranda and in `MobileHomeView` instead of the overview narrative; remove the verdict badge. Evidence: `NarrativeInsightCard.test.tsx` — fewer than 3 days shows the waiting note and no comparison, no previous data note, enough days shows the amount headline and category changes with no `%`, error state, query uses the selected basis and offset, axe clean; `MobileHomeView.test.tsx` passes.

Expected Result: Beranda never headlines an unequal-window percentage.

## 4. Verification and delivery

- [x] 4.1 Frontend `npm ci`, `npm test`, `npm run lint`, `npm run type-check`, `npm run build` on the final tree. Evidence: exit codes and counts in verification.md.
- [x] 4.2 Headless Brave over CDP against a local backend on a disposable PostgreSQL migrated V1→latest, seeded with synthetic data (many sender names and mappings): `/settings` sections at 1440, 1280, 390 and 320 px in light and dark, keyboard pass with focus visible, no horizontal overflow, zero console errors, screenshots per section, Beranda insight card. Evidence: script output and screenshot paths in verification.md.
- [x] 4.3 `git diff --check`, `openspec validate organize-settings-page --strict`, `openspec validate --all`; final diff review. Evidence: exit codes in verification.md.
- [x] 4.4 Commit, archive the change, re-validate, push `feat/settings-page-layout` and report the compare link. Evidence: pushed branch.

Expected Result: verified change archived on a pushed branch for the owner to review and merge.
