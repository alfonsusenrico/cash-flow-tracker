# Verification: apply-markas-shell (Markas+ phase 2)

Date: 2026-11-05 · Branch `feat/markas-shell` from `origin/feat/markas-foundation` `76d5f8b` · Working tree as committed in
the change's commit (`feat(ui): apply Markas shell and navigation`).

Owner approval (2026-11-05): "merged, continue" in answer to the request to approve the phase 2 plan with "defaults
ok"; all fifteen owner decisions take their defaults (see `proposal.md` › Approval).

`origin/main` now contains phase 1 (PR #84, merge `eff4f17`) and PR #85 (`c1139da`, a backend test). This branch is
still based on `76d5f8b`; it needs a rebase onto `origin/main` before its merge request.

## Environment

- Local only. PostgreSQL 16 in a new disposable container `cft-shell-pg` on `127.0.0.1:55450` with two databases:
  - `cft_markas_parity`: migrated by `flyway/flyway:10` to V51 and loaded by the parity seed (below);
  - `cft_markas_audit`: the synthetic audit seed of phase 1 (`raka_contoh`, 17 pockets, long names), at V51.
- Backend from this worktree in its project-local `.venv` (Python 3.12, the pinned `backend/requirements.txt` and
  `requirements-test.txt`); background jobs, AI calls and outbound HTTP off.
- "After": this worktree's production build (`next build`, with `NEXT_PUBLIC_COMPONENT_SHEET=1` so `/komponen` exists)
  behind a loopback `/api` proxy on 3491. "Before": `76d5f8b`'s `frontend/` exported with `git archive` into the
  scratchpad (no extra worktree), built the same way and served on 3493 against the same API and database.
- Prototype: `cash-flow-tracker-prototypes/design-options/` served statically on 3485 (`option-6/index.html`).
- Browser: headless Brave over CDP (`scripts/verify/markas/cdp.mjs`), device scale 1; axe-core 4 with wcag2a,
  wcag2aa, wcag21a, wcag21aa, wcag22aa.
- Raw captures stay in the session scratchpad because of their size; `research/evidence/apply-markas-shell/` keeps
  the checklists and a reviewed selection of images (JPEG, synthetic data only).

## Parity seed (task 1.2, D13)

`scripts/verify/markas/seed_parity.py` with `test_seed_parity.py`.

- `data.js`: `cash-flow-tracker-prototypes/design-options/shared/data.js`, SHA-256
  `c1d9a98f743416cfaf8d40106819cbdfd1781e8e2b33b29103e514437832f089`, unchanged after the runs; `meta.today`
  2026-11-05, capture day 2026-11-05 (Asia/Jakarta), **shift 0 days**.
- Guard tests (`backend/.venv/bin/python -m pytest scripts/verify/markas/test_seed_parity.py`): **8 passed**: the five
  required refusals (non-loopback host, database name without `cft_markas_`, an existing user without `--reset`,
  `meta.synthetic` missing, a schema version other than V51) plus `MARKAS_PASSWORD` missing, `--reset` outside a
  `cft_markas_` database, and loopback targets accepted. A fake connection rejects every statement that is not a
  SELECT, so each refusal is shown to write nothing; each exits 2. Against the loaded database a run without
  `--reset` exited 2 ("the database already has 1 user(s)").
- Load: one transaction (reset and load together; it checks every cash and credit balance against data.js before
  commit). Rows: users 1, categories 20, accounts 22 (17 accounts and 5 products), transactions 188, investment
  top-ups 2, salary receipts 3, obligations 3, recurring rules 8, goals 4 with 3 goal accounts, notification events
  12, one device, one ended test session, sender and account aliases 2 each, classification feedback 5.
- Rows the seed adds, each listed in `seed-check.json` with its reason: the app's two "Internal Movement"
  categories; one salary receipt on 2026-08-25 (the expected-salary rule needs three; the opening balance of
  BCA · Utama is lowered by the same amount); a notification text for the "Terlihat dua kali" item; seven
  test-session notifications (data.js states only the count).
- `seed-check.json`: **87 of 110 data.js figures agree** with the app's own API, among them net worth 45,828,280,
  every account, product and credit figure, the cycle's income 9,518,400, expense 3,502,990 and the pillars, the
  October report and the Tinjau rows. The 23 that differ come from the app's rules, not from the load: Ketahanan
  Dana's core cost (the app has rows only from 25 Sep, so 8.1 instead of 5.4 months), the next payday (the app takes
  the last receipt's day 23, data.js the payday day 25), saved this cycle (the app's saving rule leaves the dana
  investasi role out), scheduled and safe to spend (the app adds the overdue plan and the goals' monthly
  set-aside), the Tinjau badge (6 items in the prototype, 4 rows in the app), three Tinjau folders and two habits.
  These are evidence for the owner; no money logic changed.
- Fields the schema cannot hold are listed in the same file (initials, institution, statement day, units, recurring
  mode, goal contributions, review options and others).
- Isolation: the script lives under `scripts/`, which no image copies; no npm script, CI job or deploy script calls
  it. `git diff --stat 76d5f8b -- backend db mobile nginx` is empty.

## Before state (task 1.4)

`76d5f8b` with the parity seed: the ten shell routes at 1440, 1280, 1024, 1023, 768, 767, 390 and 320
(light; 1440, 390 and 320 also dark), **110 captures**, checklist `before/checklist.json`, figure baseline
`parity.json`. The audit seed baseline is part of the real-data check (9.2).

On the old shell the D1 to D9 checks do not apply (no `.title-block`, no grouped sidebar); the captures serve the
comparison images, axe, overflow and the "content inside the frame" check. Results: axe serious or critical on 6
captures, all Analisis › Kalender at 390, 768, 767 and 320 (`color-contrast` on the calendar markers, a page-body
finding phase 1 already recorded); 0 console errors; 0 horizontal overflow. Images: `before/`.

## Shell checklist and gate (tasks 1.3, 3–8, 9.1)

`shell-checklist.js` (generated from the D1 to D9 tables) runs unchanged on the prototype and on the app, because
the app's shell uses the prototype's class names. Dry run on the prototype (`#/beranda`, `#/transaksi`,
`#/pengaturan/perangkat` at 1440 and 390): **235 of 235 checks pass**; over all 56 prototype shell captures it
gives **2,244 of 2,244**, which confirms the checklist encodes the prototype's values.

All on the after build with the parity seed. Images in `compare/` (prototype left, app right) and `after/`.

| Set | Captures | Shell checklist | axe serious or critical | Console errors | Horizontal overflow |
| --- | --- | --- | --- | --- | --- |
| Ten routes and the five layers at 1440 × 900 and 390 × 844, light and dark (`after/checklist.json`) | 56 | **2,324 of 2,326** | 2 (Kalender at 390, before too) | 0 | 0 |
| The routes at 1920, 1280, 1024, 1023, 768, 767 and 320, plus the phone recapture, the manual rail and the test-session line (`after/checklist-widths.json`) | 120 | **4,610 of 4,633** | 7 (Kalender at 1024 and below; before too, except 1024) | 0 | 0 |
| Prototype, same scenarios (`prototype/`) | 58 | 2,340 of 2,342 | 0 | 0 | 2 (its own Peta Aliran at 390) |

What the checklist measured, and the result:
- Sidebar (D1): 240 px on `--s1` with the 1 px line; brand row 56 with the 24 × 24 accent mark (radius 6) and
  "CashFlow" in Manrope 14/20 600; group labels HARIAN, RINGKASAN, SIMPANAN in Inter 12/16 500 uppercase 0.72 px,
  `--muted`, padding 8 24; eight 36 px items, radius 6, margin 8, padding 16, Inter 14/20 500 `--muted`, 16 px icons;
  current item `--s2` fill, `--ink` text, no border, and the 2 × 20 px `--accent` bar at −8; counts Inter 12/16 400
  `--muted`; foot line; avatar 32 × 32 radius 6. All pass on every route in both themes.
- Rail (D2): 64 px at 1023 and 768 without the toggle (dividers for labels, the bar at the rail's edge, tooltips on
  hover and focus); the manual rail at 1440 by `sidebarCollapsed` with only the expand button in the brand row
  (owner decision 1). Side margins 48, 32 and 16 px. The prototype's own manual rail fails its brand-mark checks
  (the mark is clipped, as design.md said), which is why the app hides it.
- Top bar (D3): 56 px, sticky, `--s0` with the 1 px line, padding 48 (32 below 1024); search 320 × 36 on `--s1`
  with the 1 px line and the ⌘K cap on every page, Transaksi included; then Sembunyikan saldo, Tema, Notifikasi and
  "+ Catat" (36 px, `--accent`) as the only primary in the shell and the title block; no heading; 16 px icons.
- Title block and frame (D4): padding 48/32 (24/24 on the phone), Manrope 24/32 600 −0.36 px, subtitle Inter 14/20
  `--muted` 4 px below; **first content at y = 192** on every route with a subtitle at 1440, 168 on Pengaturan,
  232 with a test session (192 + 40); frame 1200 px, 1040 on Transaksi and Pengaturan; titles start at the same x
  at 1920; one `h1` per page; Target & Tagihan shows Rencana and Tambah target (secondary) and "Tindakan lain" with
  Tambah utang; Rekening shows Perbarui harga and Tambah rekening with Rencana in "…", Perbarui harga moving into
  "…" on the phone.
- Phone (D5): header 56 px, sticky, `--s0`, the search link, Sembunyikan saldo and the bell, every target at least
  44 × 44; tab bar 64 px on `--s1` with the 1 px line, Beranda · Tinjau · Catat · Transaksi · Lainnya, the current
  tab `--ink` with the 2 px accent bar over its middle half, the 44 × 32 accent Catat block, labels Inter 12/16 500,
  20 px icons; every shell icon has the 1.5 px stroke.
- Layers (D6, D8): notifications 400 px with unread dots, a bottom sheet with a close button on the phone; Tema
  220 px and Akun 240 px as `role="menu"`; "…" 260 px; Lainnya 480 and Bantuan 560 as dialogs, bottom sheets on the
  phone.
- Test-session line (D9, task 8.2): 40 px at y = 56 on `--s1` with a 1 px `--line` bottom, static, text "Sesi uji
  berjalan sampai 01.09 · notifikasi disimpan, tidak dicatat"; "Akhiri sesi" ended the session and the line went away
  (captures `after/sesi-uji-*.jpg`).

Two of the failures are the test-session captures, where the first content starts at 232 by design (192 + the
40 px line). The others are page bodies and fail the same way on `76d5f8b`: the Peta Aliran map's four labels past
the frame at every desktop width, and at 320 px two Beranda figures, one Laporan line, four Rekening gain lines and
29 elements of the Target goal tiles. One is new: at 1024 px Beranda's Kekayaan bersih KPI figure reaches 24 px
past the frame (not cut), because the full sidebar now starts at 1024 (below).

## Figure parity (task 9.2)

`parity.mjs` on the parity seed, before (`76d5f8b`) against after, both read on 2026-11-05 within the same
minutes: every money figure, count and date in `main`, in document order, on **18 routes**. **17 routes are
identical** (Beranda 68 figures, Tinjau 15, Analisis 72, Laporan 105, Rekening 62, Target 34, the eleven Pengaturan
sections). **Transaksi** differs only by the two figures of the removed due-plan banner ("Rp 584.000", "2 Nov",
D7); with them removed, the compared figures are equal. Beranda keeps its plan rows in Perlu tindakan.

## Real-data shape (task 9.2)

The audit seed (`raka_contoh`) on both builds at 1440, 1024, 390 and 320 (`audit/`): 32 captures each; after
**1,318 of 1,322** shell checks, 0 axe serious or critical, 0 console errors, 0 horizontal overflow. The four that
fail are the same page-body items as above (Beranda at 1024 and 320, Rekening and Target at 320), and three of them
fail identically before. Long names stay inside the sidebar, the title block and the phone header (ellipsis on the
user button, the nav labels and the Lainnya rows).

Figure parity on the audit seed (`audit/parity.json`): **17 of 18 routes identical**; Transaksi again differs only
by the removed banner's two figures ("Rp 65.000", "3 Nov").

## Due plans from the bell (task 7.2)

Browser runs on the parity seed (`dueplans/dueplans.json`, `dueplans2.json`), on 2026-11-06 Asia/Jakarta, so
SPayLater (due 5 Nov) reads "lewat jatuh tempo" rather than "hari ini":
- Transaksi shows no due-plan banner; the bell lists the report, Kelas Yoga (overdue), SPayLater, Token Listrik PLN
  (today) and "4 rencana ditautkan otomatis", each with "(baru)" for screen readers.
- Activating SPayLater in the bell opens "Rencana jatuh tempo · Tautkan, lewati, atau catat manual", the unchanged
  `PendingScheduledBanner`, with every link, "Lewati kali ini", "Catat manual" and "Batalkan tautan" action.
- From that dialog: "Lewati kali ini" skipped the first due plan (Kelas Yoga); "Kaitkan … ke Tagihan SPayLater"
  linked SPayLater; "Catat manual (tidak tertangkap)" recorded Token Listrik PLN as the transaction
  "Rutin: Token Listrik PLN" Rp 200.000; "Batalkan tautan Langganan Musik jatuh tempo 3 Nov" undid an automatic
  link, which brought Langganan Musik back as due. Each removed the plan from `/recurring/pending` as before; the
  view asks no extra confirmation, as before (the component is unchanged).
- Beranda › Perlu tindakan still lists the plan ("Tagihan SPayLater Rp 434.000 lewat jatuh tempo") and the
  automatic links.
- Unit tests: "Due plan from the bell" (focus returns to the bell, the view opens), "No duplicate plan item", the
  date wording and order, and the automatic-link item (`workspace.test.tsx`); `DuePlansNotice` and its test are
  removed and `rg DuePlansNotice frontend/src` is empty.

## Keyboard flows (D16)

`shell-keys.mjs` on the after build: **46 of 46 pass** (`keys/shell-keys.json`).
- 1440, light and dark: Tab's first stop is "Lewati ke isi" and Enter moves focus to `main`; the next Tab starts
  inside `main`; ⌘K focuses the top-bar search on Beranda and Transaksi's own search on Transaksi; N opens Catat;
  ? opens "Bantuan & pintasan" and Esc closes it; the Tema menu opens on its checked item, ↓ moves and Esc returns to
  Tema; the account menu opens on Profil, ↓ reaches Keluar and Esc returns to the user button; the bell puts focus
  inside the popover and Esc returns to the bell; the sticky Rekening summary, Tinjau list and Pengaturan rail stay
  below the top bar after scrolling (192 and 168 px from the top).
- 390 and 320, light and dark: Lainnya keeps 14 Tabs inside and Esc returns to Lainnya; tabbing through the last
  twelve controls of Transaksi, Target & Tagihan and Rekening never leaves a control under the phone header or
  the tab bar; the phone header's search opens Transaksi with its field focused and the address without `fokus`.
- Toast (D11): its bottom edge sits 32 px above the viewport at 1440 and 80 px (64 + 16) at 390.

## Frontend checks (task 9.3)

On the final working tree:
- `npm test`: **82 files, 763 tests passed**. New or rewritten: `workspace.test.tsx` (51: navigation model, sidebar
  groups, bar, counts, rail, account menu keys, top bar contents and order, search, ⌘K on Transaksi and on the
  phone, hide balances, Tema keys, Home/End and outside press, notifications and seen state, due plans from the bell,
  `planNotifications` de-duplication and wording, title block, `PageActions` with "…" and phone-only items, phone
  header, tab bar, Lainnya with Notifikasi and Tema sheets, frame wiring, collapse memory, storage unavailable),
  `shortcuts.test.tsx` (Opening Bantuan, page keys in the help, single keys off through the shared setting),
  `AppLayout.test.tsx` (Session running, Ending from the notice), `Icon.test.tsx` (1.5 stroke, prototype drawings,
  aliases, dots), `breakpoints.test.ts` (`PHONE_QUERY` is `(max-width: 767px)`, no 760/761/1180 media rule or phone
  constant), `planDay.test.ts`; `ledger/page.test.tsx` (no banner, `fokus=cari` focuses once and is removed) and
  `review/page.test.tsx` (help rows) updated.
- `npm run type-check`: passed. `npm run lint`: passed. `npm run build` (production, no component sheet): passed.
- Parity seed guard tests: 8 passed.
- `git diff --check`: clean. `openspec validate --all --strict`: **84 passed, 0 failed**.
- `git diff --stat 76d5f8b -- backend db mobile nginx`: empty. The backend suite was not run, because no backend
  file changed.
- `rg TopBarControls frontend/src` and `rg DuePlansNotice frontend/src`: empty. The old shell's CSS (`.ws-stage`,
  `.ws-window`, `.ws-topbar`, `.ws-title`, `.ws-account-card`, `.ws-plan-notice`, and the rail counters, tooltip,
  tab and sheet rules) is gone; an earlier broken selector list (`.ws-tip` merged into `.ws-main`) went with it.
- Clean-up: every server, browser and the `cft-shell-pg` container started for this run were stopped and the
  container removed; `lsof` shows no listener on the ports used and `ps` no remaining process. The other running
  container (`pintar-pintar-local-postgres-1`) was not touched.

## Deviations, findings and items for the page phases

**Choices made where the sources disagree (for the owner to confirm):**
1. Phone header icons are 20 px for search, Sembunyikan saldo and the bell, as the approved spec says; the
   prototype's `shell.js` reuses the top bar's 16 px eye and bell there. With its count the bell is 47.8 px wide
   (the prototype squeezes it into 44).
2. Bantuan's third fact keeps the prototype's "…aman dipakai sampai gajian." `copyRegister.test.ts` lists it beside the
   hero name "Aman dipakai sampai gajian", which the owner approved; the alternative is neutral wording ("sampai
   gaji berikutnya").
3. The test-session line uses 4 px vertical padding, so it is exactly 40 px with its 28 px buttons (D9's "padding 8"
   would make it 44); on the phone its buttons are 44 px like every phone button.
4. On the phone a title block's single stepper or group takes the full width under the title; only a single button
   stays beside it (the prototype's rule assumed one button, and Analisis now has only its stepper there).
5. Notification rows show the item's detail ("Rencana · jatuh tempo 5 Nov") without the prototype's extra time,
   because the app's items have no time.
6. The bell and Tema keep phase 1's pressed fill while their layer is open (`aria-expanded`); the prototype shows none.

**Page-body edits beyond the one-token breakpoint edits** (each needed so the new frame does not cut content):
- `.ws-tx-tbl td:first-child { width: 100%; max-width: 0 }`: in the 1040 frame the Transaksi table was 1,143 px and
  the Jumlah column was cut; the Transaksi column now ellipsizes (table 996 px).
- `.ws-tx-toolbar` wraps in a frame of 1,000 px or less: with the full sidebar at 1024 px, Filter was 93 px past
  the frame.
- `.ws-triage` lost its 4 px top margin so Tinjau's first content starts at 192.
- `money.module.css` `.toolbar` rules removed (no user left once Rekening and Target moved to `PageActions`); sticky
  panels offset by `--topbar-h`.
- The page shortcut groups lost their `caption` field (Tinjau, Transaksi), which only the old help switch used.

**For the page phases (end-review list):**
- The full sidebar from 1024 px narrows page bodies between 1024 and about 1100 px: Transaksi shows its phone feed
  there (its list measures 688 px at 1024, and its phone switch is an element width of 767 px), Beranda's Kekayaan
  bersih figure overflows its KPI card by 24 px, and Kalender's marker contrast issue now also shows at 1024.
  Phases 3, 5 and 6 rebuild these.
- Pre-existing page items, unchanged: Kalender marker contrast below 1024; Peta Aliran labels past the frame;
  Beranda, Laporan, Rekening and Target content past the frame at 320 px.
- Parity seed disagreements (23 figures, above): Ketahanan Dana, payday, saving rule, scheduled amount, Tinjau
  counting.
- The captures after midnight (from 2026-11-06 00:00 Asia/Jakarta) see the seed one day later than the prototype
  (SPayLater overdue, "kemarin" on 5 Nov). Figure parity ran before midnight, both builds within the same minutes.

**Not done here:** task 9.4 (owner review) and 9.5 (archive, push, compare link) wait for the owner.
