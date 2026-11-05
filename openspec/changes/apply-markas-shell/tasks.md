## 1. Approval, seed and baseline

- [x] 1.1 Record the owner's approval of this change and the answers to the fifteen owner decisions in `design.md`, with the date, in `proposal.md` (replacing "Plan approval pending") and `PROJECT_STATE.md`. Update `design.md` and the spec deltas first where an answer differs from the default. Evidence: both files name the date and every decision's choice; `openspec validate apply-markas-shell --strict` passes after the edits.
- [x] 1.2 Add `scripts/verify/markas/seed_parity.py` (D13) with its pytest guard file. Evidence:
  - the guard tests pass for a non-loopback host, a database name without `cft_markas_`, an existing user without `--reset`, `meta.synthetic` missing, and a schema version other than the mapped one; each exits non-zero and writes nothing;
  - a load into a disposable PostgreSQL 16 migrated by `flyway/flyway:10` to V51 completes in one transaction;
  - `seed-check.json` lists every data.js headline figure beside the app's, with any mismatch named;
  - `data.js`'s SHA-256 and the shift in days are recorded in `verification.md`;
  - `git diff --stat origin/main -- backend db mobile` stays empty.
- [x] 1.3 Add the shell scenarios to `scenarios-app.json` and `scenarios-prototype.json`, and the shell checklist to `measure.js`, generated from the D1 to D9 tables (D16). Evidence: a dry run over the prototype's `#/beranda`, `#/transaksi` and `#/pengaturan/perangkat` at 1440 and 390 writes captures and a shell checklist JSON whose values equal the D1 to D9 tables.
- [x] 1.4 Capture the "before" state of the shell on `feat/markas-foundation` `76d5f8b` with the parity seed and with the audit seed. Cover every route at 1440, 1280, 1024, 1023, 768, 767, 390 and 320 in both themes, plus the figure baseline for parity. Evidence: captures, checklist and `parity-before.json` in `research/evidence/apply-markas-shell/before/`, tied to that revision.

Expected Result: the shell can be measured against the prototype with the prototype's own data, and the pre-change state is recorded.

## 2. Navigation model and icons

- [x] 2.1 Give `NAV_LINKS` its groups (HARIAN: Beranda, Tinjau, Transaksi; RINGKASAN: Analisis, Laporan; SIMPANAN: Rekening & Kantong, Target & Tagihan) and derive the Lainnya order from it (D1, D5). Keep every href. Evidence: `workspace.test.tsx` asserts the groups, the order, unchanged hrefs and `isActivePath` results; `redirects.test.ts` passes unchanged.
- [x] 2.2 Set the `Icon` stroke to 1.5 and replace the shell's drawings with the prototype's `core.js` paths, keeping the existing names (D12, design-tokens "Interface Icons"). Evidence: an `Icon` test asserts `stroke-width="1.5"` and `aria-hidden`; the shell checklist "Shell icons measured" passes at 1440 and 390.

Expected Result: one ordered, grouped page list feeds every navigation surface, drawn with the prototype's icons.

## 3. Sidebar and rail

- [x] 3.1 Rebuild `SideNav` and the user button to D1: brand row, `nav` "Menu utama" with the group labels, 36 px items with the `--s2` current fill and the shared 2 × 20 px selection bar, muted right-aligned counts, the foot with Pengaturan and the user button. Remove the coral pill, the filled count chips and the bordered account card. Evidence:
  - component tests for "Navigating via streamlined sidebar" and "No coral pill" (accessible names with counts in words, `aria-current`);
  - `selectionMarks.test.ts` passes;
  - the shell checklist matches every D1 value at 1440 in both themes, including hover, pressed, current and focus.
- [x] 3.2 Apply the rail and breakpoints of D2:
  - the manual rail from 1024 px, stored under `sidebarCollapsed`, with only the expand button in its brand row;
  - the automatic rail from 768 to 1023 px without the toggle;
  - group dividers and the restyled keyboard-visible tooltips.

  Evidence: component tests for "Toggling sidebar collapse on desktop" and "Rail below 1024 px"; captures at 1440 (collapsed and expanded), 1024 and 1023 match the checklist.

Expected Result: the sidebar matches the prototype's in both widths and both themes.

## 4. Top bar and shell menus

- [x] 4.1 Add the shared `Menu` (D6): `role="menu"`, `menuitem` and `menuitemradio`, ↑/↓/Home/End, Esc and outside click close with focus returned, anchoring with flip, bottom sheet below 768 px. Evidence: `overlays.test.tsx` cases for arrow keys, Esc focus return and the checked radio item; axe has no violations on an open menu in both themes.
- [x] 4.2 Rebuild `TopBar` to D3:
  - sticky 56 px with global actions only: search 320 × 36 with the ⌘K cap on every page, "Sembunyikan saldo" (`aria-pressed`), the Tema menu, the bell with its muted count, and "+ Catat";
  - remove the title, the controls slot and the phone avatar;
  - change the account menu to name and username, Profil and Keluar.

  Evidence: tests for "Search", "Search on Transaksi", "Account menu", "Hiding balances from the top bar" and "Theme menu"; the Tampilan scenarios of `theme-presets-switcher` pass with the new counterparts; the checklist matches D3 at 1440 and 1023.

Expected Result: the top bar holds only the prototype's global actions, and every shell menu works by keyboard.

## 5. Title block and page frame

- [x] 5.1 Render the frame and title block in `AppLayout` (D4):
  - `main#main.page` › `.page-inner` with `--page-w` per route (1040 on `/ledger` and `/settings/*`) and the container moved to `.page-inner`;
  - the title and subtitle from `pageMeta` or `usePageHeader`, with the prototype's subtitles;
  - the skip link "Lewati ke isi";
  - remove `.ws-stage` and `.ws-window`.

  Evidence: tests for "Page-written title" and one `h1` per page; the checklist "First content position" gives `y = 192` on every route with a subtitle at 1440 (168 on Pengaturan) and the frame widths of "Frame width per page" at 1920.
- [x] 5.2 Replace `TopBarControls` with `PageActions`, including the "…" menu, and move the six call sites as in the D4 table: Analisis, Transaksi, Laporan, Rekening & Kantong, Target & Tagihan, and Beranda's header. Each page keeps its handlers. Evidence:
  - tests for "More actions" on Target & Tagihan and Rekening (Rencana and Tambah utang in "…", Tambah target secondary);
  - `rg TopBarControls frontend/src` is empty;
  - each page's own tests pass unchanged apart from the title's location.
- [x] 5.3 Unify the breakpoints (D2): phone below 768 and rail below 1024 in `globals.css` and `PHONE_QUERY`, with the one-token edits in the ledger page, `AccountsView`, `KalenderTab`, `report.module.css` and `komponen.module.css`. Offset the sticky page elements and the scroll padding by `--topbar-h`. Evidence:
  - a test asserts `PHONE_QUERY` is `(max-width: 767px)` and that no `760px`, `761px` or `1180px` rule is left in `frontend/src`;
  - captures at 768 and 767 show the desktop and phone shells with matching page layouts;
  - a keyboard pass on Tinjau, Transaksi and Rekening shows sticky elements below the top bar.

Expected Result: every page sits in the prototype's frame with its title block, and its first content starts at `y = 192`.

## 6. Phone shell

- [x] 6.1 Add the phone header (D5): brand, search link to `/ledger?fokus=cari`, "Sembunyikan saldo" and the bell, 44 × 44 targets. Transaksi consumes `fokus=cari` once. Evidence: tests for "Searching from the phone header" (the field is focused and the parameter is removed from the URL) and the Beranda scenario of `mobile-native-layout`; the checklist matches D5 at 390 in both themes.
- [x] 6.2 Rebuild the tab bar and Lainnya (D5):
  - 64 px plus the safe area, with 20 px icons, the Catat accent block with its label, the 2 px current-tab bar and the muted Tinjau count;
  - the Lainnya sheet in the sidebar order with Notifikasi, Tema and Keluar;
  - scroll padding for the header and the bar.

  Evidence:
  - tests for "Reaching every page on a phone", "Active tab" and "Catat on the tab bar";
  - the keyboard check "Focus not obscured" at 390 and 320;
  - tab label contrast ≥ 4.5:1 in both themes;
  - "Adding a target on a phone" and "Adding a debt on a phone" pass.

Expected Result: the phone shell matches the prototype's header, tab bar and Lainnya sheet.

## 7. Notifications and due plans

- [x] 7.1 Rebuild the notifications popover (400 px; a bottom sheet on the phone) with unread dots and the `notificationsSeen` state (D6). Evidence: tests for "Notifications" (count before and after opening) and "Seen state unavailable" (storage throws); axe has no violations on the open popover and sheet.
- [x] 7.2 Merge `/recurring/pending` into the bell's plan items, add the automatic-link item, and open the shell-owned "Rencana jatuh tempo" dialog from due and overdue items. Remove `DuePlansNotice`, its test and Transaksi's drawer (D7). Evidence:
  - tests for "Due plan from the bell on Transaksi" and "No duplicate plan item";
  - a browser run with the parity seed: SPayLater (due today) links, skips and records from the bell with the same confirmation as before, and the automatic-link undo still works;
  - Beranda › Perlu tindakan still lists the plan;
  - `rg DuePlansNotice frontend/src` is empty.

Expected Result: the bell carries what the banner carried, with the same actions, and Transaksi has no banner.

## 8. Bantuan, test-session notice and toast

- [x] 8.1 Rebuild the help as "Bantuan & pintasan" (D8): the facts, the key table with the open page's keys and the Tampilan line, with no switch and no dotted-figure caption. Evidence: `shortcuts.test.tsx` cases for "Opening Bantuan", "Page keys in the help" and "Turning single-key shortcuts off" (from Tampilan); the copy register test passes.
- [x] 8.2 Turn `TestSessionBanner` into the one-line notice of D9 and move the toast to the D11 positions. Evidence:
  - tests for "Session running" (no warning colour, neutral fill) and "Ending from the notice";
  - captures with a session open at 1440 and 390;
  - the toast's bottom edge at 32 px from the viewport at 1440, and 16 px above the tab bar at 390.

Expected Result: help, the test-session line and toasts match the prototype or the approved deviation.

## 9. Gate and delivery

- [x] 9.1 Run the common gate on the shell of every route and the five shell layers:
  - prototype and app captures with composed side-by-side images at 1440 × 900 and 390 × 844 in light and dark (parity seed);
  - the shell checklist with counts passed and failed;
  - the global rules: families, sizes, weights, borders, radii;
  - no horizontal overflow at 1440, 390 and 320;
  - axe-core (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa) with zero serious or critical in both themes;
  - zero console errors;
  - the keyboard flows of D16, including the skip link and focus order.

  Evidence: counts and the side-by-side images in `verification.md` and `research/evidence/apply-markas-shell/`.
- [x] 9.2 Check figure parity and the real-data shape. Every money figure, count and date read from the DOM on every route must equal the 1.4 baseline with the same seed. The audit seed (17 pockets, long names) must show no clipping, overlap or horizontal scroll in the shell or the title block at 1440, 1024, 390 and 320. Evidence: `parity.mjs` output with zero differences; real-data captures and their checklist in the evidence folder.
- [x] 9.3 Run `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict`, and confirm `git diff --stat origin/main -- backend db mobile nginx` is empty. Review the final diff for unrequested changes, dead CSS left by the old shell (`.ws-stage`, `.ws-window`, `.ws-topbar`, `.ws-title`, `.ws-account-card`, `.ws-plan-notice`) and secrets. Evidence: command results with counts in `verification.md`, with the backend suite recorded as not run because no backend file changed. The parity seed's guard tests run with the backend's virtual environment.
- [ ] 9.4 Present the evidence, the deviations and the end-review items to the owner, and revise until the owner accepts. Evidence: the owner's acceptance with its date in `verification.md` and `PROJECT_STATE.md`.
- [ ] 9.5 After the owner accepts: sync and archive with `openspec archive apply-markas-shell`, commit on `feat/markas-shell`, push it and report the compare link against the branch the owner names (`main` once `feat/markas-foundation` is merged). The owner opens and merges the merge request. Evidence: the archive output, the pushed branch and the compare link.

Expected Result: the shell is verified against the prototype and handed to the owner for review.
