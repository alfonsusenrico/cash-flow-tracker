## Context

See proposal.md › Why. The measurement source is `cash-flow-tracker-prototypes/design-options/option-6/`: `styles.css` (Shell, Page frame, Tablet and Phone blocks), `js/shell.js` (structure, labels, counts, keyboard) and `js/dialogs.js` (notifikasi, tema, akun, lainnya, bantuan). Where `SPEC.md` differs, `styles.css` and `js/*.js` win. Two such differences matter here:
- SPEC §8 lists HARIAN as "Beranda, Tinjau"; `shell.js` and every capture put Transaksi in HARIAN too.
- SPEC §12 says content starts "at 176 px"; §6, §13 and `styles.css` give `y = 192`.

The app's shell today (on `feat/markas-foundation`, phase 1 tokens and primitives applied):

| Part | Today | Source |
| --- | --- | --- |
| Frame | `.ws-stage` › `.ws-window` grid (sidebar \| `main#main.ws-main`), max 1680 px, padding 22/28/36/18 | `AppLayout.tsx`, `globals.css` |
| Sidebar | flat list of seven links, coral pill on the current link (`--r-panel`), counts as filled chips, 28 px layers mark, account card with a border | `SideNav.tsx`, `AccountMenu.tsx` |
| Rail | chosen above 1180 px, forced from 761 to 1180 px, small filled counters and hover/focus tooltips | `globals.css` |
| Top bar | inside `main`, not sticky: page title + subtitle, a controls slot (period steppers, page buttons) filled by `TopBarControls` portals, search (hidden on `/ledger`), theme, bell with a pip, + Catat, phone avatar | `TopBar.tsx`, `context.tsx` |
| Phone (≤ 760) | the same top bar sticky with a 20 px title, tab bar with a dark Catat button (label hidden) and a `menu` icon on Lainnya; Lainnya lists Rekening, Target, Analisis, Laporan, Pengaturan, the notifications inline and Keluar | `PhoneNav.tsx` |
| Menus | `Disclosure` (`role="region"` panel, `aria-pressed` items), no arrow keys | `Disclosure.tsx` |
| Help | "Pintasan keyboard" dialog with a key list, the open page's keys, the "Pintasan satu tombol" switch and a caption about dotted figures | `shortcuts.tsx` |
| Bell | report-ready item plus active expense plans due within two days (`/recurring`), all counted as new | `AppLayout.tsx` `planNotifications` |
| Due plans | Transaksi shows `DuePlansNotice` (`/recurring/pending`) whose "Lihat rencana" opens a drawer with `PendingScheduledBanner` (link, skip, record, undo auto-link). Beranda already lists due plans in Perlu tindakan and opens the same drawer. | `app/ledger/page.tsx`, `BerandaView.tsx` |
| Test session | warning banner with "Lihat sesi" and "Akhiri sesi" at the top of `main` | `TestSessionBanner.tsx` |
| Companion status | already only in Pengaturan · HP & perangkat | `CompanionStatus.tsx` |

Pages that put controls in the top bar today: Analisis (period stepper), Transaksi (period stepper), Laporan (basis segment and stepper), Rekening (Perbarui harga, Rencana, Tambah rekening) and Target (Rencana, Tambah utang, Tambah target as the coral primary). Beranda writes its own title and subtitle through `usePageHeader`.

## Goals / Non-Goals

**Goals:**
- The shell of every route measures like the prototype at 1440 × 900 and 390 × 844 in both themes: sidebar, top bar, title block, frame, phone header, tab bar and the shell's layers.
- Every page's first content starts at `y = 192` at 1440 (with a subtitle and no test session).
- Behaviour that users rely on stays: routes, shortcuts, counts, notification sources, theme, hide balances, focus handling.

**Non-Goals:**
- Page bodies, page toolbars and page dialogs (phases 3 to 11).
- Copy outside the shell, except the subtitles the title block shows.

## Decisions

### D1. Sidebar: three groups, straight selection bar, muted counts

Structure (from `shell.js`):
- Brand row: mark, "CashFlow", collapse button.
- `nav aria-label="Menu utama"` with three groups:
  - HARIAN: Beranda `/`, Tinjau `/review` · attention count, Transaksi `/ledger?period=cycle`;
  - RINGKASAN: Analisis `/insights`, Laporan `/laporan` · ready count;
  - SIMPANAN: Rekening & Kantong `/accounts`, Target & Tagihan `/goals`.
- Foot: Pengaturan `/settings` and the user button.

`navigation.ts` keeps one ordered list and gains a `group` per link. The phone tab bar, Lainnya and the rail read the same list.

Geometry (`styles.css` `.sb` to `.user-btn`):

| Part | Values |
| --- | --- |
| Sidebar | 240 px, sticky, `height: 100vh`, `--s1`, 1 px `--line` right edge, `overflow: hidden`; nav scrolls (`padding: 8 0 16`) |
| Brand row | 56 px, gap 12, padding 0 16 0 24, 1 px `--line` bottom; mark 24 × 24 radius 6 `--accent` with the trend icon 16 in `--on-accent`; name Manrope 14/20 600, flex 1 |
| Group | padding-top 16; label `.t-label` 12/16 500 uppercase +0.06em `--muted`, padding 8 24 |
| Nav item | h 36, margin 0 8, padding 0 16, gap 12, radius 6, icon 16, 14/20 500 `--muted`; hover `--s2` + `--ink`; pressed `--line`; current `--s2` + `--ink` + `::before` 2 × 20 px `--accent` at left −8, top 8 |
| Count | 12/16 400 `--muted`, tabular, right-aligned by the label's `flex: 1`; "99+" above 99 |
| Foot | 1 px `--line` top, padding 8 0 |
| User button | width 100 % − 16, margin 0 8, padding 8 16 8 12, gap 12, radius 6, hover `--s2`; avatar 32 × 32 radius 6 `--s2` 12/600 initials; name 14/20 and username 12/16 `--muted`, ellipsis |
| Focus | 2 px `--focus` outline, offset −2 on nav items |

Counts keep their sources and their words in the link's accessible name ("Tinjau, 6 perlu ditinjau", "Laporan, laporan baru siap").

Rejected alternatives:
- Keeping the flat list: the owner asked for option 3's layout "totally", and the groups are part of it.
- Keeping filled count chips: the prototype uses muted text so only the current item carries colour.
- SPEC §8's HARIAN without Transaksi: `shell.js` and the captures are authoritative.

### D2. Rail and breakpoints: 240/64, rail below 1024, phone below 768

| Width | Shell |
| --- | --- |
| ≥ 1024 | sidebar 240, or the 64 px rail when the owner collapsed it (stored under the existing `sidebarCollapsed` key); page and top bar side padding 48 |
| 768–1023 | always the 64 px rail, collapse button hidden; side padding 32 |
| < 768 | phone header and tab bar, no sidebar and no top bar; side padding 16, bottom padding 64 + 48 |

In the rail, labels, counts, the brand name and the user's name hide. Nav items centre their icon (padding 0, the 2 × 20 bar stays at the rail's left edge). Each group label becomes a 1 px `--line` divider (`margin: 0 16 8`).

Two refinements of the prototype need the owner's confirmation (see Owner decisions 1 and 2):
- **Collapsed brand row.** In the prototype's manual rail, the mark, a 12 px gap and the 36 px button do not fit the 32 px content box and `overflow: hidden` clips the button. The app shows only the expand button, centred, in the manual rail. Below 1024 the button is hidden and the mark is centred, as in `styles.css`.
- **Rail labels.** The prototype names rail icons only through `title`. The app keeps its tooltip, shown on hover and on keyboard focus, so sighted keyboard users can read the label. It is restyled as a popover (`--dialog`, 1 px `--line`, radius 6, 12/16).

Every 760/761 px and 1180 px rule moves:
- the shell rules in `globals.css`;
- `PHONE_QUERY` in `layers.ts`, which Drawer and the pages read;
- one-token edits so phone pages switch together with the phone shell: `PHONE_LIST_WIDTH` in the ledger page, `PHONE_WIDTH` in `AccountsView`, `KalenderTab`, `report.module.css` and `komponen.module.css`.

Container queries on `main` keep their values; the container becomes the page frame (D4).

Rejected alternatives:
- Keeping 760/1180 until the page phases: the phone shell and the phone page layouts would disagree from 761 to 767 px, and the rail would appear at a width the prototype shows the full sidebar.
- Changing every page's grid breakpoint (for example the prototype's 1200 one-column rule) now: that is page layout, so it belongs to phases 3 to 11.

### D3. Top bar: global actions only

`<header class="topbar">` inside the main column: sticky at top 0, z 20, h 56, flex gap 8, padding 0 48 (0 32 below 1024), `--s0` with a 1 px `--line` bottom. From left to right:

1. **Search** (`role="search"`), 320 px wide and 36 high:
   - field: padding 0 44 0 36, 1 px `--line`, radius 6, `--s1` fill, 14 px, placeholder "Cari transaksi…" in `--muted`; on focus a `--field-focus` edge and a 3 px `--ring`;
   - icon 16 at left 12, top 10;
   - ⌘K key cap at right 8, top 8.
   - Enter opens `/ledger?q=…` as today.
   - Shown on every page, Transaksi included; on Transaksi ⌘K focuses the page's own search first, as `shell.js` does.
2. Spacer.
3. **Sembunyikan saldo:** icon button 36, eye or eye-off, `aria-pressed`, constant label "Sembunyikan saldo".
4. **Tema:** icon button, sun or moon for the resolved theme, label "Tema", opens the theme menu (D6).
5. **Notifikasi:** the bell as an auto-width icon button (padding 0 8, gap 4) with the unread count in 12/16 `--muted`. Label "Notifikasi, N baru", or "Notifikasi, tidak ada yang baru".
6. **+ Catat:** primary button (h 36, plus icon 16), `aria-keyshortcuts="N"`.

The page title, the controls slot and the phone avatar leave the top bar.

Rejected alternatives:
- Hiding search on Transaksi, as today: the prototype shows it on every page, so the top bar never changes shape between routes.
- Keeping hide balances in the account menu only: the prototype puts it beside the theme, one press away. Tampilan keeps its switch (D10).

### D4. Title block and page frame, rendered by the shell

`AppLayout` renders `main#main.page` (`tabIndex=-1`) › `.page-inner` › title block, then the route's children.

**Frame:**
- `.page`: padding 0 48 64; 0 32 64 below 1024; 0 16 (64 + 48 + safe area) on the phone.
- `.page-inner`: max-width `--page-w` with `margin-left: max(0px, calc((100% − 1200px) / 2))`.
- `--page-w` per route: 1040 for `/ledger` and `/settings/*`, 1200 elsewhere (`shell.js` `out.w`).
- `container-type: inline-size; container-name: main` moves to `.page-inner`, so the pages' container queries keep working.

**Title block:**
- flex, `align-items: flex-end`, gap 16, padding 48 0 32;
- text: `h1` `.t-title` (Manrope 24/32 600 −0.015em) and the subtitle `p` 14/20 `--muted` 4 px below;
- actions: flex, gap 8, on the title's bottom edge.
- On the phone: padding 24 0 24, wrapping, actions full width unless there is only one.
- The title and subtitle come from `pageMeta(pathname)`, or from the page through `usePageHeader` (Beranda's date and cycle line).
- Subtitles take the prototype's wording:
  - Transaksi "Semua uang masuk, pengeluaran, dan pindah saldo";
  - Rekening & Kantong "Saldo rekening, kantong, investasi dan utang";
  - Target & Tagihan "Dana darurat, tagihan, utang dan target tabungan";
  - Pengaturan has no subtitle;
  - the others are unchanged.

**Actions slot:**
- `TopBarControls` becomes `PageActions`: the same portal mechanism, targeting the title block's actions element.
- It takes the page's controls as children plus an optional `more` list. Each `more` item has a label, an icon and an `onSelect` or `href`, and is rendered as the "…" icon button (`aria-label="Tindakan lain"`, `aria-haspopup="menu"`) opening a 260 px menu (D6).
- The page keeps every handler. Only where the button renders changes.

Existing actions placed as in the prototype:

| Page | Visible | In "…" | Unchanged until the page phase |
| --- | --- | --- | --- |
| Beranda | — | — | — |
| Tinjau | — | — | "Cari saran dari riwayat" and Bantuan (phase 4) |
| Transaksi | period stepper | — | Gabungkan dua transaksi, Filter (phase 5) |
| Analisis | period stepper | — | basis segment and tabs (phase 6) |
| Laporan | basis segment, period stepper | — | — |
| Rekening & Kantong | Perbarui harga (secondary; in "…" on the phone), Tambah rekening (secondary) | Rencana | Atur urutan (phase 8) |
| Target & Tagihan | Rencana, Tambah target (secondary) | Tambah utang | Ubah target dana darurat (phase 9) |
| Pengaturan | — | — | — |

"Tambah target" becomes a secondary button, because "+ Catat" is the only primary in the frame.

At 1440 with a subtitle, the first child of the page starts at 56 + 48 + 32 + 4 + 20 + 32 = `y = 192`. Without a subtitle (Pengaturan) it starts at `y = 168`, as in the prototype.

Rejected alternatives:
- Each page rendering its own title block: the same markup in nine pages, and the frame could drift between pages.
- Keeping the controls in the top bar: the prototype's top bar holds only global actions, and `y = 192` needs the title block.
- Moving body actions (merge, filter, Atur urutan) into "…" now: their behaviour and dialogs change with their page phase.

### D5. Phone header, tab bar and Lainnya

**Phone header** (< 768): sticky top 0, z 20, h 56, flex gap 4, padding 0 8 0 16, `--s0` with a 1 px `--line` bottom. It holds:
- the brand link to `/` (mark 24 and "CashFlow" in Manrope 600, flex 1);
- search: an icon link to `/ledger?fokus=cari`;
- Sembunyikan saldo;
- the bell.

The icon buttons are 44 × 44 with 20 px icons. Transaksi reads `fokus=cari` once: it focuses its search field and replaces the URL without the parameter. ⌘K on the phone goes the same way.

**Tab bar** (`nav aria-label="Navigasi ponsel"`): fixed at the bottom, z 30, five equal columns, h 64 + `env(safe-area-inset-bottom)`, `--s1` with a 1 px `--line` top. Each tab is a column (gap 4, 20 px icon, label 12/16 500 `--muted`, min-h 44):
- Beranda `home`, Tinjau `inbox`, Catat, Transaksi `list`, Lainnya `more`;
- the current tab is `--ink` with a 2 px `--accent` bar from 25 % to 75 % at the top edge (top −1);
- the Tinjau count sits at top 8, left 50 % + 12 in 12/16 400 `--muted`;
- Catat is a 44 × 32 radius 6 `--accent` block with a 20 px plus in `--on-accent`, above the visible label "Catat" in `--ink`.

`html` scroll padding becomes 56 + 16 at the top and 64 + 16 + safe area at the bottom, so focused content is never under the header or the bar.

**Lainnya** is a 480 px dialog, shown as a bottom sheet on the phone, titled "Lainnya". Its rows (min-h 56, icon 20, label 14/500, trailing meta, chevron 16) are:
- Analisis, Laporan ("1 baru" while ready), Rekening & Kantong, Target & Tagihan and Pengaturan, in the sidebar's order, with `aria-current` on the current page;
- Notifikasi ("N baru"), which opens the notifications sheet;
- Tema (the current choice), which opens the theme menu sheet;
- Keluar, kept as the last row (Owner decision 7).

Rejected alternatives:
- Keeping the dark Catat button with a hidden label: the prototype's Catat is the accent block with its word.
- Keeping the phone avatar: the prototype's phone header has none. The account's actions are Profil (Pengaturan) and Keluar.

### D6. Shell layers: one menu primitive, popovers that become sheets

A shared `Menu` replaces `Disclosure` for the theme and account menus and the title block's "…":
- the button carries `aria-haspopup="menu"` and `aria-expanded`;
- the panel is `role="menu"` with `menuitem`, or `menuitemradio` with `aria-checked`;
- ↑/↓ move between items, Home/End jump, Esc closes and focus returns to the button;
- an outside click closes it.

Popovers follow `.popover`: `--dialog`, 1 px `--line`, radius 12, layer shadow, padding 8 0, max-h 480, the 200 ms rise. They anchor under their button, right-aligned, and flip above when there is no room (the account menu in the sidebar foot). Items are h 36, padding 0 16, gap 12, hover and focus `--s2`; a checked item ends in a muted ✓. Below 768 a popover becomes a bottom sheet: full width, radius 16 16 0 0, max-h 80vh.

| Layer | Width | Content |
| --- | --- | --- |
| Tema | 220 | label TEMA; Terang (sun), Gelap (moon), Ikuti sistem (monitor) as `menuitemradio` |
| Akun | 240 | name 14 and username 12 `--muted` (padding 8 16), separator, Profil (settings icon) → `/settings/profil`, Keluar (logout) |
| Notifikasi | 400 | `role="dialog"`, label NOTIFIKASI; rows (padding 12 16, gap 12): an 8 px `--accent-text` dot when unread (an 8 px space when read; "(baru)" for screen readers), title 14/20, meta 12/16 "detail · day time"; empty state "Belum ada yang baru." |
| "…" | 260 | the page's `more` items with their icons |

**Unread state.** The bell counts items whose id this browser has not seen. Opening the bell marks the listed ids as seen, stored under `notificationsSeen` (at most 50 ids; storage errors leave every item unread for the visit). Notification ids already include the report end date and the plan date, so a new occurrence is new again.

Rejected alternatives:
- Server-side read state: it needs an API and a table for a per-browser convenience.
- Counting every item as new, as today: the prototype distinguishes unread and read, and a count that never falls trains the owner to ignore it.

### D7. The bell absorbs the Transaksi due-plan banner

- The bell's plan items come from `/recurring/pending` (the banner's source: due and overdue occurrences) merged with the existing two-day window from `/recurring`. They are de-duplicated by rule and date, ordered by date, and worded as today ("… hari ini", "… lewat jatuh tempo", "besok", "lusa").
- Recent automatic links appear as one item, "N rencana ditautkan otomatis".
- Activating a due or overdue plan item, or the automatic-link item, opens the shell's "Rencana jatuh tempo" dialog. This is the existing `PendingScheduledBanner` in the existing Drawer, with link, skip, record and undoing an automatic link, now owned by `AppLayout`. Items due later keep their `/goals` link, and the report item keeps its report URL.
- `DuePlansNotice` and Transaksi's own drawer are removed. Beranda keeps its plan rows in Perlu tindakan and its drawer.

Rejected alternatives:
- Bell items only linking to `/goals`: Target & Tagihan has no link, skip or record view, so the banner's one-step action would be lost outside Beranda.
- Keeping the banner until phase 5: the plan removes it with the shell, and the bell is its replacement.

### D8. Bantuan & pintasan

A 560 px dialog (sheet on the phone), titled "Bantuan & pintasan", opened by `?` (single-key shortcuts on) and from page menus that list it later. It contains:
- an ordered list of the prototype's three facts (14/20);
- a table `.tbl` ("Tombol" | "Fungsi", caption "Pintasan keyboard" for screen readers) with key caps:
  - global rows in the prototype's wording: ⌘K / Ctrl K "Cari transaksi", N "Catat transaksi", ? "Bantuan ini", Esc "Tutup lapisan teratas";
  - then the open page's registered keys, each worded "… di <halaman>";
- the meta line "Pintasan satu tombol bisa dimatikan di Pengaturan › Tampilan."

The switch leaves the dialog and stays in Tampilan on the same stored setting. The caption about dotted figures is dropped; phases 3 to 9 remove those underlines.

Rejected alternatives:
- The prototype's full static key table on every page: it would list keys that do nothing on the open page, which today's requirement forbids (Owner decision 10 offers it).
- Keeping the switch in the dialog: the prototype has one place for it, and Tampilan already holds it.

### D9. Test-session notice

While a session is open, a one-line `role="region"` "Sesi uji" sits between the top bar (or the phone header) and the title block. It is not sticky:
- min-h 40, padding 8 with the page side margin, `--s1` fill and a 1 px `--line` bottom;
- clock icon 16 `--muted`;
- text 14/20 `--ink`: "Sesi uji berjalan sampai 18.30 · notifikasi disimpan, tidak dicatat", with the time tabular;
- at the right, small quiet buttons (h 28): the link "Lihat sesi" and the button "Akhiri sesi".

It uses neutral tones only, because the owner chose "simple and clean". The first content then starts 40 px lower. Gate captures run without a session, plus one capture with a session for the owner.

Rejected alternatives:
- Bell and Pengaturan only, as the prototype: owner decision 4 kept a visible line, because notifications are not recorded during a session.
- A sticky line: it would take 40 px from every scroll position and could cover focused rows.

### D10. Sembunyikan saldo

The toggle moves to the top bar and the phone header. Tampilan's switch stays on the same `hideBalances` setting. The mask string stays "Rp ••••••" (`$ ••••••` for USD), as today.

The prototype also names each masked figure `aria-label="Disembunyikan"` and spaces the dots by 0.08em. That needs the figure components that the page phases build (phase 3 onward). It is recorded as each page phase's task, not done here (Owner decision 12).

Rejected alternative: changing every `bal()` call site now, which reaches into every page body.

### D11. Toast placement

The toast stays the phase 1 primitive. Its position becomes bottom centre 32 px above the viewport edge, and on the phone `calc(64px + 16px + env(safe-area-inset-bottom))`, so it sits 16 px above the tab bar.

### D12. Shell icons from the prototype

The prototype draws its own 24-grid icons with a 1.5 px stroke at 16 px (controls, rows, nav) and 20 px (tab bar, phone header). The app's `Icon` uses a 1.8 px stroke and other drawings.

This phase sets the `Icon` stroke to 1.5. It replaces the drawings the shell uses (home, inbox, list, chart, file, wallet, target, settings, search, eye, eye-off, sun, moon, monitor, bell, plus, panel, more, trend, logout, x, chevron-right, help, clock) with the paths from `core.js`, keeping the app's names as aliases. Page icons keep their drawings at the new stroke until their phase.

Rejected alternatives:
- Keeping the app's icons: a visible mismatch in every shell capture.
- Replacing every icon now: page icons change shape with their pages.

### D13. Parity seed from `shared/data.js`, local verification only

The script is `scripts/verify/markas/seed_parity.py`. It runs with the backend's virtual environment against a disposable PostgreSQL that Flyway has migrated to the head version (V51):

```
MARKAS_PASSWORD=… backend/.venv/bin/python scripts/verify/markas/seed_parity.py \
  --data ../cash-flow-tracker-prototypes/design-options/shared/data.js \
  --database-url postgresql://…@127.0.0.1:<port>/cft_markas_parity [--today YYYY-MM-DD]
```

**Guards.** The script refuses, exits non-zero and writes nothing when:
- the host is not loopback;
- the database name does not start with `cft_markas_`;
- `flyway_schema_history` is not at the version the script maps;
- a user row already exists and `--reset` is not given (and `--reset` truncates only a `cft_markas_` database);
- `data.js` does not declare `meta.synthetic: true`.

It reads no `.env` file and prints no credentials. The owner password comes from `MARKAS_PASSWORD`, the variable the capture scripts already use.

**Load.**
- `data.js` is read from the prototypes repository and not vendored. Its SHA-256 is recorded in `verification.md`. It is parsed by stripping `window.CFT_DATA =` and the trailing `;` and reading the rest as JSON.
- Every date and time is shifted by whole days from `meta.today` (2026-11-05) to `--today` (default: today in Asia/Jakarta). The server's "today", cycle day and due plans then match the prototype's relative state (hari ke-14, 20 days to payday, SPayLater due today).
- Everything is written in one transaction:
  - user `dimas_contoh` (name, payday day, currency IDR, settings);
  - the 18 categories with pillars and budgets;
  - the 17 accounts with parent pockets, roles and balances;
  - the 5 products as investment instruments (lots, units, average price, price);
  - the credit account;
  - the 156 transactions with merchant, notes and source as provenance;
  - salary receipts for the cycle start;
  - the 8 recurring rules with their next dates and states;
  - the 3 obligations, and the 4 goals with `goal_accounts`;
  - review items as notification events in the state each folder needs;
  - habits, sender aliases, notification mappings, one device and one test session (ended).
- Reports, notifications, net worth and Ketahanan Dana are not written; the app computes them.

**Check.** After loading, the script requests the app's own figures through the API (net worth, cycle summary, review attention). It writes `seed-check.json` beside the captures with each data.js figure, the app's figure and whether they agree. A disagreement is evidence for the owner, never a reason to change money logic in a UI phase. Fields the schema cannot hold (for example data.js's decorative device health text) are listed there too.

**Isolation.** The script lives under `scripts/`, which neither Docker image copies (the frontend image copies `frontend/` only, the backend image `backend/app` and `backend/evaluation`). No npm script, CI workflow or deploy script calls it. A pytest file beside it covers the guards (non-loopback host, wrong database name, existing users, non-synthetic data, schema version mismatch) without a database.

Rejected alternatives:
- Loading through the public API: past dates, review-queue states and parent pockets cannot be created that way without new endpoints.
- Freezing the clock to 2026-11-05: the backend's "today" cannot be frozen without changing it. The shift keeps relative state, and only weekday and date words differ from the prototype captures.
- Vendoring `data.js` into this repository: a second copy that could drift from the prototype.

### D14. Routes stay the same

`NAV_LINKS` hrefs, `SETTINGS_HOME`, `LEDGER_NAV_HREF` (`/ledger?period=cycle`), the `/dashboard` and `/tanya` redirects, the report-ready URL, every query parameter and the local-only `/komponen` stay as they are. `isActivePath` is unchanged.

The only additions are the optional `fokus=cari` on `/ledger` (consumed and removed), and no route is added or renamed. `AppShell` keeps `/auth/*` outside the shell, so login keeps its foundation-only styling. `redirects.test.ts` and the route tests run unchanged.

### D15. The shell's own layers move into this phase

The plan listed notifikasi, tema, akun menu, lainnya and bantuan under phase 11. They open from the shell and are measured with it, so this phase builds and gates them. Phase 11 keeps Catat and re-captures these five only as part of its global sweep.

Rejected alternative: building them in phase 11, which would leave the shell gate with old layers on new buttons.

### D16. Verification of the shell

- `scenarios-app.json` and `scenarios-prototype.json` gain shell-only scenarios for every route (`beranda`, `tinjau`, `transaksi`, `analisis`, `analisis-kalender`, `analisis-peta-aliran`, `laporan`, `rekening`, `target`, `pengaturan`) and the layers (`dlg-notifikasi`, `dlg-tema`, `dlg-akun`, `dlg-lainnya`, `dlg-bantuan`), at 1440 × 900 and 390 × 844 in both themes.
- Captures run on the parity seed. The side-by-side image crops the shell regions: sidebar, top bar, title block, phone header and tab bar.
- `measure.js` gains a shell checklist generated from the D1 to D9 tables: positions and sizes, paddings, radii, borders, fonts and colours per state (rest, hover, pressed, current, focus), `y` of the first content, and the rail at 1023 and 1024.
- Keyboard flows: skip link → main; ⌘K; N; ?; menus with ↑/↓ and Esc and focus return; Lainnya focus trap; the last row of a long list never under the tab bar.

## Owner decisions (recommended default in bold)

1. **Collapsed brand row:** **only the expand button in the manual rail (the prototype clips it)**; or a stacked mark and button.
2. **Rail labels:** **keep the keyboard-visible tooltip, restyled**; or the prototype's `title` only.
3. **Rail counts:** **hidden in the rail as in the prototype, still in each icon's accessible name**; or keep small counters (a deviation).
4. **Title-block actions now:** **fit today's header actions into "two + …" in the prototype's order (Tambah target secondary, Rencana and Tambah utang into "…")**; or move them as they are and trim in each page phase.
5. **Frame width per route now:** **1040 on Transaksi and Pengaturan, 1200 elsewhere**; or 1200 everywhere until the page phases.
6. **Subtitles:** **the prototype's wording now for Transaksi, Rekening & Kantong, Target & Tagihan and Pengaturan**; or keep today's.
7. **Keluar on the phone:** **keep it as the last row of Lainnya until phase 10 adds it to the Pengaturan rail (a temporary deviation)**; or add it to Pengaturan now.
8. **Notification read state:** **per browser, marked when the bell opens**; or count every item as new, as today.
9. **Due plans in the bell:** **due and overdue items open the "Rencana jatuh tempo" view (link, skip, record); later items keep `/goals`**; or keep `/goals` for all.
10. **Bantuan keys:** **global keys plus the open page's keys, the switch moved to Tampilan only, the dotted-figure caption dropped**; or the prototype's full static table.
11. **Test-session notice:** **one neutral, non-sticky 40 px line with "Lihat sesi" and "Akhiri sesi"**; or sticky.
12. **Masked figures' accessible name "Disembunyikan":** **added with each page's figure components (phases 3 to 9)**; or now, through every `bal()` call site.
13. **Icons:** **1.5 px stroke for all icons and the prototype's drawings for the shell's icons now**; or keep the app's icons until phase 12.
14. **Shell layers:** **notifikasi, tema, akun, lainnya and bantuan built and gated in this phase**; or left to phase 11.
15. **Parity seed dates:** **shift data.js to the capture day, so weekday and date words differ from the prototype captures while amounts and relative state match**; or seed as of 2026-11-05 (cycle and due states would then be wrong on any other day).

## Risks / Trade-offs

- [The frame narrows page bodies (1104 px content at 1440 instead of about 1146, 1040 on Transaksi), and their container queries shift.] → Captures of every route at 1440, 1280, 1024, 768, 390 and 320 before and after. Overflow and clipping are fixed in this change only by the frame; any page grid problem is listed for its page phase.
- [The sticky top bar can cover page sticky elements (`top: 16px` in Tinjau, Rekening and Pengaturan, the Transaksi day headings).] → Offset them by `var(--topbar-h)`, and set the scroll padding (D5). A keyboard check covers the last row and the sticky headings.
- [Moving the phone breakpoint touches page files.] → One-token edits only, guarded by a test that `PHONE_QUERY` is the single source; captures at 767 and 768.
- [Removing the Transaksi banner hides due plans from someone who never opens the bell.] → The bell's count is visible on every page, and Beranda's Perlu tindakan still lists them first-class.
- [Interim look: Markas+ shell around old page bodies, in production once merged (plan decision 3).] → Accepted by the owner on 2026-11-05; the end-review list in `PROJECT_STATE.md` records it.
- [The parity seed maps a synthetic dataset onto a schema with stricter rules.] → Guarded, transactional and checked against the app's own figures (D13); mismatches are evidence, not fixes.

## Migration and rollback

- No data or API changes.
- Browser storage gains `notificationsSeen`. `sidebarCollapsed`, `hideBalances`, `theme` and `shortcuts` keep their keys and meanings, so a rollback leaves them valid.
- Rollback is a revert of the merge commit. The parity seed touches only disposable `cft_markas_*` databases.

## Open Questions

None that changes the specs or tasks. The owner decisions above are needed before implementation.
