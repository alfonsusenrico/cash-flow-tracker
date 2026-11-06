## Why

On 2026-11-05 the owner chose design option 6 "Markas+" and expects the app to match it exactly:
- "I want 3, with combinated stylings (especially fonts) from 4 & 5, but the layout totally 3"
- "for any borders that colored and thick like border on add new transaction make it simple and clean"
- on the resulting prototype: "ok accepted"

Phase 1 (`apply-markas-foundation`, fonts, tokens, scale, borders, focus and shared controls) was presented the same day and the owner answered "accepted, continue". It is pushed on `feat/markas-foundation` and awaits the owner's merge; this change is based on it.

Every page sits inside the shell, so the shell comes before the pages. Today the shell still has the pre-redesign structure: one flat list with a coral active pill, the page title inside the top bar, page controls squeezed beside the global actions, a phone header with the date and an avatar, a dark Catat button, and a due-plan banner on Transaksi. Until the shell matches the prototype, no page can be measured against it, because every page's first content must start at the prototype's `y = 192`.

The phased plan is in the prototypes repository at `research/2026-11-05-markas-implementation-plan.md` (this is phase 2 of twelve). The fidelity source is `design-options/option-6/` (`styles.css`, `js/shell.js`, `SPEC.md` §6 to §8 and §11); where `SPEC.md` and `styles.css` differ, `styles.css` wins.

## Expected Outcome

- At 1440 px the owner sees the prototype's frame on every page:
  - a 240 px sidebar with the CashFlow mark and the groups HARIAN, RINGKASAN and SIMPANAN in small uppercase labels;
  - the current page marked by a grey fill and a thin straight coral bar, with muted counts at the right;
  - a quiet 56 px top bar with search (⌘K), Sembunyikan saldo, Tema, Notifikasi with a muted count, and **+ Catat**.
- Each page starts with its own title and one-line subtitle under the top bar, with at most two actions and a "…" menu at the right. The page's first content starts 192 px from the top, and the content frame is 1200 px wide (1040 px on Transaksi and Pengaturan), aligned left.
- The sidebar collapses to a 64 px icon rail by its toggle, and turns into the rail by itself below 1024 px.
- On a phone (below 768 px):
  - a 56 px header with the brand, search, Sembunyikan saldo and the bell;
  - a 64 px tab bar with Beranda, Tinjau, a coral **Catat** block, Transaksi and Lainnya, the current tab marked by a 2 px coral bar along its top edge;
  - Lainnya opens a bottom sheet with the other pages, Notifikasi, Tema and (until phase 10) Keluar.
- Transaksi no longer shows the due-plan banner. The bell lists due plans and opens the same link, skip and record view the banner opened. Beranda › Perlu tindakan keeps its plan rows.
- `?` opens "Bantuan & pintasan" in the prototype's layout.
- While a test session runs, one neutral line under the top bar says until when and offers "Akhiri sesi".
- Inside the frame every page's body looks and behaves as before. Routes, links, shortcuts, counts, figures and money rules are unchanged.

## Approval

On 2026-11-05 the owner approved this change. Asked to approve the phase 2 plan with "defaults ok", the owner answered "merged, continue". The plan-level decisions accepted on 2026-11-05 apply (all seven defaults, among them the one-line test-session notice, foundation-only styling for login and onboarding, and the parity seed with the development-only `/komponen` sheet).

The fifteen owner decisions in `design.md` all take their recommended defaults, so `design.md` and the spec deltas stand as written:

1. Collapsed brand row: only the expand button in the manual rail.
2. Rail labels: the keyboard-visible tooltip, restyled.
3. Rail counts: hidden in the rail, still in each icon's accessible name.
4. Title-block actions: today's header actions fitted into "two + …" in the prototype's order.
5. Frame width: 1040 on Transaksi and Pengaturan, 1200 elsewhere.
6. Subtitles: the prototype's wording for Transaksi, Rekening & Kantong, Target & Tagihan and Pengaturan.
7. Keluar on the phone: the last row of Lainnya until phase 10 (a temporary deviation).
8. Notification read state: per browser, marked when the bell opens.
9. Due plans in the bell: due and overdue items open "Rencana jatuh tempo"; later items keep `/goals`.
10. Bantuan keys: global keys plus the open page's keys, the switch only in Tampilan, no dotted-figure caption.
11. Test-session notice: one neutral, non-sticky 40 px line with "Lihat sesi" and "Akhiri sesi".
12. Masked figures' accessible name: added with each page's figure components (phases 3 to 9).
13. Icons: a 1.5 px stroke for all icons and the prototype's drawings for the shell's icons now.
14. Shell layers: notifikasi, tema, akun, lainnya and bantuan built and gated in this phase.
15. Parity seed dates: shifted to the capture day.

## What Changes

- **Sidebar** (prototype `.sb`):
  - 240 px, or 64 px as an icon rail by a manual toggle (remembered) and automatically below 1024 px;
  - brand row 56 px with a 24 px coral mark and "CashFlow" in Manrope;
  - groups HARIAN (Beranda, Tinjau, Transaksi), RINGKASAN (Analisis, Laporan) and SIMPANAN (Rekening & Kantong, Target & Tagihan) under 12 px uppercase labels;
  - 36 px nav items; the current one has a `--s2` fill and the straight 2 × 20 px `--accent` bar, replacing the coral pill;
  - the Tinjau and Laporan counts in 12 px muted text at the right;
  - the foot holds Pengaturan and the user button (avatar, name, username), which opens Profil and Keluar.
- **Top bar** (56 px, sticky, `--s0` with a 1 px line):
  - search 320 × 36 with a ⌘K key cap, on every page;
  - Sembunyikan saldo as a pressed-state icon button, moved there from the account menu;
  - the Tema menu (Terang, Gelap, Ikuti sistem) and Notifikasi with a muted "N baru" count;
  - **+ Catat** as the only primary button.
  - The page title and the page controls leave the top bar.
- **Title block and page frame:**
  - a shared title block (48 px top and 32 px bottom padding, Manrope 24/32 title, 14/20 subtitle, actions on the title's bottom edge) holding each page's existing controls;
  - at most two visible actions plus a "…" menu, following the prototype's choice and order for actions that exist today;
  - page side margins 48/32/16 px, a 1200 px frame aligned left (1040 px on Transaksi and Pengaturan) and a 64 px bottom padding;
  - subtitles take the prototype's wording, and the skip link reads "Lewati ke isi".
- **Phone shell:**
  - 56 px header with the brand, search (opens Transaksi with its search focused), Sembunyikan saldo and the bell;
  - 64 px tab bar plus the safe area, with 20 px icons, the 44 × 32 coral Catat block with its visible label, the 2 px current-tab bar and the muted Tinjau count;
  - the Lainnya sheet in the sidebar's page order, with Notifikasi and Tema rows;
  - shell popovers become bottom sheets.
- **Layers owned by the shell:**
  - the notifications popover (400 px), whose items carry an unread dot;
  - the theme and account menus as real menus (`role="menu"`, ↑/↓ keys);
  - the "Bantuan & pintasan" dialog (560 px: three short facts, a key table, and a line saying where single-key shortcuts are switched off);
  - the toast placement (bottom centre 32 px; on the phone 16 px above the tab bar).
- **Due plans:** the Transaksi due-plan banner is removed. Its items join the bell, and activating a due plan there opens the existing "Rencana jatuh tempo" view (link, skip, record).
- **Test session:** the full banner becomes a one-line neutral notice under the top bar (owner decision 4).
- **Breakpoints:** the phone layout switches below 768 px and the rail below 1024 px everywhere, replacing 760/761 and 1180. Sticky page elements and scroll padding are offset by the sticky top bar.
- **Icons:** the shell's icons take the prototype's 24-grid, 1.5 px stroke drawings (D12).
- **Parity seed:** a local-only script loads the prototype's `shared/data.js` into a disposable database, shifted to the capture day, so shell and page captures show the prototype's names and amounts.
- **BREAKING (spec):** the sidebar gains group headings and loses the coral pill. The floating window is gone. The help dialog no longer holds the "Pintasan satu tombol" switch, which stays in Pengaturan › Tampilan. The account menu loses "Sembunyikan saldo" and "Pengaturan" (they are in the top bar and the sidebar foot).

## Non-goals

- No page content or layout inside the pages beyond the frame: hero figures, KPI strips, cards, grids, rows, page toolbars and page dialogs stay as they are until their page phase (3 to 11). Actions that live in a page body today (for example Tinjau's "Cari saran dari riwayat", Transaksi's merge and filter, Rekening's "Atur urutan") stay there.
- No new page action that the app lacks today. "Ubah target dana darurat" in the Target "…" menu, for example, arrives in phase 9.
- No change to Catat or the transaction dialogs (phase 11), the login and onboarding pages (foundation styling only), or the Pengaturan page body (phase 10).
- No backend, API, schema, data or companion change, and no new npm dependency. The parity seed writes only to a disposable local database.
- Local verification only. Release and production stay with the owner through the approved pipeline after acceptance.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `executive-sidebar-layout`:
  - grouped sidebar with selection bar and counts;
  - the 240/64 rail and the 1024/768 breakpoints;
  - the page frame replacing the floating window;
  - the top bar contents, notifications, the phone tab bar and the Lainnya sheet;
  - shortcuts and the Bantuan dialog.
  - New requirements: the title block, the phone header and the test-session notice.
- `tactile-pulse-ui`: the information architecture gains the three sidebar groups and the 768 px phone breakpoint.
- `mobile-native-layout`:
  - the phone Beranda header becomes the shell's phone header;
  - the phone page actions follow the title block's "two actions and …" rule.
- `theme-presets-switcher`: the Tampilan controls stay in sync with the top-bar theme menu and Sembunyikan saldo button, no longer with the help dialog's switch and the account menu.
- `design-tokens`: shell icons use the prototype's 24-grid, 1.5 px stroke set at 16 and 20 px.

Standards: UI and accessibility (WCAG 2.2 AA: landmarks, skip link, focus visible and not obscured, keyboard menus, target size, reflow at 320 px, reduced motion) and the copy register. No capability in `AGENTS.md` changes.

## Impact

- **Frontend shell:**
  - `components/layout/AppLayout.tsx`;
  - `components/workspace/` (`SideNav`, `TopBar`, `PhoneNav`, `AccountMenu`, `Disclosure`, `navigation.ts`, `context.tsx`, `shortcuts.tsx`, `overlays.tsx`, `layers.ts`, and a new shared menu and title block);
  - `components/settings/TestSessionBanner.tsx`, `components/ui/Icon.tsx`;
  - the shell rules and breakpoints in `app/globals.css`.
- **Page call sites** (header only):
  - the six `TopBarControls` users move to the title block's actions slot;
  - Beranda's `usePageHeader` writes the title block;
  - `app/ledger/page.tsx` loses `DuePlansNotice` and reads `fokus=cari`;
  - one-token breakpoint edits in `app/ledger/page.tsx`, `AccountsView.tsx`, `KalenderTab.tsx`, `report.module.css` and `komponen.module.css`;
  - sticky offsets in `globals.css` and `money.module.css`.
- **Tests:** `AppLayout.test.tsx`, `workspace.test.tsx`, `shortcuts.test.tsx`, `overlays.test.tsx`, `companion.test.tsx`, `DuePlansNotice.test.tsx` (removed or moved with the notice) and the page tests that assert the top-bar title.
- **Tooling:** `scripts/verify/markas/` gains the parity seed loader with its guard tests and the shell scenarios. It is not part of any image, CI job or deploy script.
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit.
