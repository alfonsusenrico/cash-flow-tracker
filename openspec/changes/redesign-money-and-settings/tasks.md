# Tasks

This plan waits for the owner's approval (proposal.md, "Approval"). Implementation starts only after:
- that approval;
- the merge of phases 3, 4 and 6 (design D1).

Evidence for every task goes into `verification.md`: commands, counts, skips with reasons, screenshots and measured values. Use synthetic data only, for example "Bank Utama", "Dompet Digital", "RDN Contoh", "Bibit Contoh · Reksa Dana A", "Sekuritas Contoh · Saham AAAA", "Dana Darurat", "Liburan Lombok", "Cicilan Laptop" and "Kedai Awan".

Backend tests run as `.github/workflows/checks.yml` does: on a disposable PostgreSQL 16 migrated by Flyway (V1 to the latest version on `main`), with `REQUIRE_TEST_DATABASE=1`.

The browser matrix means 1440, 1280, 1024, 768 and 390 px, each in light and dark, in a headless browser on a local production build.

The owner will not test before every phase is done ("I'm not going to test anything until all phases done implemented properly and approved", 2026-11-03). The agent's evidence is therefore the only runtime proof, and no check may be skipped because the owner might catch it later.

## 1. Rebase, refresh and baseline

- [x] 1.1 Rebase `feat/money-and-settings` onto `main` after phases 3, 4 and 6 have merged (D1).
  - Refresh every MODIFIED block in `specs/` from the then-current main specs, keeping every scenario name, and re-apply this change's edits.
  - Re-run the alias usage measurement (`var()` uses and legacy Tailwind keys per file, and the six legacy utility classes). Update design.md "Tokens and styles" with the new counts.
  - Evidence: `openspec validate redesign-money-and-settings --strict` passes, plus the measurement table.
- [x] 1.2 Record the baseline on the rebased branch before any change:
  - `npm test`, `npm run type-check`, `npm run lint` and `npm run build`;
  - first-load JS for `/accounts`, `/goals` and every `/settings/*` route, and the built CSS size;
  - the full backend suite with its skips;
  - the medians of 20 local `GET /api/dashboard/net-worth` and `GET /api/accounts` calls;
  - axe, overflow and console results;
  - screenshots of every route (`/`, `/review`, `/ledger`, `/insights` and its tabs, `/laporan`, `/accounts`, `/goals`, every `/settings/*`, `/tanya`, `/auth/login`) at 1280 and 390 px in both themes, kept for the pixel comparison in 7.4.

  Evidence: the baseline table in verification.md.
- [x] 1.3 Build the unchanged-writes harness (D17) and run it on a production build of `main` against a fresh copy of the seeded database.
  - It covers every flow D17 lists.
  - It records normalised request bodies and normalised snapshots of the touched tables, including `transaction_obligation_allocations`.
  - The market price provider is stubbed, and no external request is made.

  Evidence: the flow list with request and row counts per flow, stored under the scratchpad.

Expected Result: the plan matches the merged main specs, and every gate has a measured baseline.

## 2. Backend

- [x] 2.1 Make `cycle_summary._net_worth` the public `net_worth_breakdown`, and compute `GET /dashboard/net-worth` from it (D3, OD57).
  - The endpoint keeps every existing field and adds `liquid`, `investment`, `reconciliation_pending` and `lines`.
  - Tests:
    - `net_worth` equals `cycle-summary` `net_worth.total` with a parent and two pockets, an amount-tracked product, a unit-tracked position, a pocket at −Rp 50.000 waiting for reconciliation and one active debt;
    - the pocket is not counted twice;
    - an archived and a paid-off debt are left out;
    - owner isolation;
    - the response keys are a superset of today's (a fixture of today's keys);
    - the statement count is constant.

  Evidence: pytest counts.
- [x] 2.2 Add `backend/app/services/habit_list.py` and `backend/app/routers/habits.py` (`GET /api/habits`, D7), registered in `main.py`. Tests for every scenario of the habit-learning requirements "Habit List API", "Habit List Entries" and "Learned Top-up Entries":
  - ready, learning and conflicting;
  - corrections weighing 2;
  - an undone label left out;
  - two bands of one merchant;
  - "Tanpa nama toko";
  - the signature never in the response;
  - a learned top-up, a rejected one and a corrected one;
  - the 100-entry cut with `truncated`;
  - another owner's labels absent;
  - 401 without authentication;
  - API key access;
  - no row changed (table checksums before and after);
  - no model call (provider mocked and asserted unused);
  - a caplog check that no merchant name or amount is logged;
  - a constant statement count for 10 and 1000 labels;
  - the median time with the D7 dataset under 300 ms.

  Evidence: pytest counts and the timing.
- [x] 2.3 Capture undo reverts its label (D10, OD56). `text_capture.undo_capture` calls `review_undo.revert_feedback` for the capture's live `capture` rows in the same transaction. Tests:
  - the label count of a key before and after;
  - the next payment of the key is asked with the guess instead of recorded;
  - a forced failure after the insert rolls the `undo` row back with the rest;
  - a replayed undo leaves one `undo` row;
  - the V29 trigger still rejects an update;
  - every existing capture undo test passes.

  Evidence: pytest counts.
- [x] 2.4 Run the full backend suite on a freshly migrated database. Evidence: passed, failed and skipped counts tied to the commit.

Expected Result: one net worth helper, a read-only owner-scoped habit list, and honest habit evidence after a capture undo.

## 3. Rekening & Kantong

- [x] 3.1 Compose `/accounts` from the preview (D2, OD54):
  - the toolbar with `?tampil=` through `lib/urlState.ts`;
  - the "Uang siap pakai" table with pocket rows, collapse buttons and reconciliation labels;
  - the "Investasi" table grouped by platform with "Cara hitung";
  - the Total aset card with `MoneyBar`, facts and two `ExplainableFigure`s from the server's `lines`;
  - the awaiting-outflow count from `GET /review/queue`, with no amount, and the caption with links to `/review?folder=pindah` (OD59).

  Move the page's in-file forms unchanged into `components/accounts/*Dialog.tsx`. Remove the `0` and client-sum fallbacks and the `opacity-75` captions.

  Tests:
  - the table structure (caption, scope, row groups);
  - the address switch;
  - no fallback: net worth failing shows "Kekayaan bersih belum dapat dihitung" with retry and no Rp 0;
  - hidden balances mask every amount;
  - colour is never the only carrier.

  Evidence: Vitest counts.
- [x] 3.2 Add the row drawer (side or sheet) with the details and the ordered actions from D2. Each opens the existing modal, and focus returns to the drawer and then to the row. Add `?akun=<id>` to open a drawer from a link (D13).

  Tests:
  - the actions offered per kind (liquid parent, pocket, amount-tracked product, unit-tracked position, archived parent hidden);
  - "Sesuaikan Saldo" only where eligible;
  - the drawer opens from `?akun=`;
  - Escape and focus return.

  Evidence: Vitest counts.
- [x] 3.3 Add the "Ubah urutan" mode with drag handles and move buttons for accounts and for sibling pockets. Disable boundary moves, return focus on "Selesai", and keep the same `POST /accounts/reorder` bodies as today.

  Tests:
  - the move-button order and request body for accounts and for pockets;
  - drag of a pocket not dragging its parent;
  - focus staying on the moved control.

  Evidence: Vitest counts.
- [x] 3.4 Phone composition (`mobile-native-layout` "Phone Rekening & Kantong Composition"): the summary first, lists without columns, sheets, and targets of 44 px. Evidence: Vitest layout test and the 390 px screenshots in 7.2.

Expected Result: Rekening & Kantong matches the preview, shows the server's figures only, and keeps every action reachable by keyboard, pointer and touch.

## 4. Target & Tagihan

- [x] 4.1 Compose `/goals` from the preview (D4, OD55):
  - Target tabungan rows with status pills in words, labelled bars, pace and "Dicatat manual" or linked accounts;
  - "Tagihan sampai gaji berikutnya" from `cycle-summary` `scheduled.lines` (`kind = plan`), titled by `window_basis`;
  - Ketahanan Dana from the runway fields, with the unknown state;
  - Utang rows with paid against total, remaining amount, minimum payment, due day and the archived disclosure.

  Tests:
  - the goal statuses for on track, behind, reached and no date (no invented pace);
  - the bills list equals the plan lines and their total equals the server's;
  - the unknown runway text;
  - a debt at payoff in the archived list;
  - no aggregate goal banner.

  Evidence: Vitest counts.
- [x] 4.2 Add the goal and debt drawers with the existing modals:
  - "Tambah progres" only for standalone goals, saying no account transaction is made;
  - "Kaitkan pembayaran" and "Catat pembayaran";
  - the allocation editor where it applies;
  - delete confirm-first;
  - `?target=` and `?utang=` deep links.

  Tests: the actions per goal and debt kind, the confirmation text, and focus return. Evidence: Vitest counts.
- [x] 4.3 Phone composition (Ketahanan Dana first, sheets, the "Tambah target" target above the tab bar). Evidence: Vitest layout test and the 390 px screenshots in 7.2.

Expected Result: Target & Tagihan shows goals, bills, Ketahanan Dana and debts with the same figures as Beranda, and every existing action through a drawer.

## 5. Pengaturan

- [x] 5.1 Restructure the sections (D6, OD53):
  - the `sections.ts` order and groups;
  - the grouped `SettingsNav` with `aria-current`;
  - the new `kebiasaan` and `tampilan` routes;
  - wide `/settings` opening HP & perangkat;
  - "Perangkat & akses" renamed "HP & perangkat";
  - every old address still working;
  - an unknown address showing the not-found page.

  Tests: the scenarios of the modified "Sectioned Settings Page". Evidence: Vitest counts.
- [x] 5.2 Restyle HP & perangkat, Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi and Nama pengirim on `Card` and the restyled `FormField`, keeping each section's save scope. Tests: the existing section tests unchanged in request bodies, plus the copy register. Evidence: Vitest counts.
- [x] 5.2a Name the owner the same way everywhere (carried over from phase 6 acceptance, 2026-11-03): Beranda's written lede greets with the account's display name, as Tanya does, and falls back to the username only when no name is set. Tests: Beranda lede with a name, without a name, and Tanya and Beranda using the same name. Evidence: Vitest counts and a Beranda screenshot at 1280.
- [x] 5.3 Add Kebiasaan (D8). Tests:
  - the habit and top-up rows and status words;
  - the server switch facts, including off;
  - hidden balances masking the top-up amounts;
  - the empty, loading and error states with retry;
  - no edit or forget control.

  Evidence: Vitest counts.
- [x] 5.3a Add the "Kebiasaan dihafal" card to Analisis Ringkasan (phase 4 OD42, deferred here), reading `GET /api/habits`: the count of `ready` habits and the most recent ones, a link to Pengaturan · Kebiasaan, an error state with retry (never 0 when the request fails), and an empty state when nothing is learned yet. Copy follows the register. If phase 4's Ringkasan spec delta names the card, MODIFY that requirement here; otherwise ADD it to `observed-insights`. Tests: loaded, empty and failed states. Evidence: Vitest counts and a Ringkasan screenshot at 1280 and 390.
- [x] 5.4 Add Tampilan (D9). The `ShortcutProvider` exposes the switch state. Tests:
  - the theme choice syncs with the account menu and persists;
  - the shortcut switch syncs with the help dialog, and ⌘K still works when off;
  - "Sembunyikan saldo" syncs with the account menu;
  - throwing storage leaves the defaults without an error.

  Evidence: Vitest counts.

Expected Result: Pengaturan has eight sections in the approved order, two new ones, and unchanged saves.

## 6. Tokens, aliases and copy

- [x] 6.1 Write the alias-to-token mapping table from the alias block, line by line (D11, step 1), into design.md. Decide each raw-valued contrast alias: renamed if a consumer remains, removed if none. Evidence: the table.
- [x] 6.2 Add the semantic Tailwind keys (`in`, `out`, `tr`, `inv`, `warn`, `ok` with `fg`, `bg` and `solid`) on workspace tokens. Evidence: `tokens.test.ts` contrast pairs for each key in both themes.
- [x] 6.3 Replace every `var(--alias)` in `frontend/src` (D11, step 2) with a scripted replacement, reviewed per file. Evidence: the measurement shows 0 `var()` alias uses, and the diff is reviewed.
- [x] 6.4 Replace every legacy Tailwind key and palette-remapped class (D11, step 3). Evidence: the measurement shows 0 legacy keys and 0 default palette classes.
- [x] 6.5 Replace the six legacy utility classes at their call sites and delete them and their banner from `globals.css` (D11, step 4). Evidence: `rg` finds none.
- [x] 6.6 Delete the alias block from `tokens.css`, including the `.dark` overrides, and the legacy keys and remaps from `tailwind.config.js`. Remove every component that `rg` shows without an importer, with its tests (D11, steps 5–6). Evidence: the `rg` output per removed component.
- [x] 6.7 Extend `tokens.test.ts` for the `design-tokens` requirements:
  - the alias list as a constant, neither declared nor used;
  - no palette remap and no default palette class;
  - no legacy utility class;
  - every used token declared in both themes;
  - the contrast pairs;
  - no opacity-faded text tokens in the page sources.

  Show that the test fails on a planted `var(--text-secondary)`, `text-emerald-600` and `btn-primary`. Evidence: the failing and then passing runs.
- [x] 6.8 Apply `ui-copy-register` to every string on `/accounts`, `/goals` and `/settings/*` and their modals, and to `/auth/login` (D16). Extend `copyRegister.test.ts` with these files. Evidence: the rename list (old → new) in verification.md, and the guard test passing and failing on a planted "Anda" label.
- [x] 6.9 Docs: AGENTS.md (styling notes without the alias wording; Rekening, Target & Tagihan and Pengaturan in the frontend layout) and `docs/NEW_USER_GUIDE.md` (Rekening & Kantong, Target & Tagihan, Kebiasaan and Tampilan). Evidence: the diff.

- [x] 6.10 Restyle the observed-insight and report cards that phase 4 left on their own markup (phase 4 design, "Implementation notes") on the workspace primitives and semantic tokens, with unchanged content, links and states. Tests: their existing Vitest suites unchanged in behaviour, plus the token test. Evidence: Vitest counts and Ringkasan and Laporan screenshots at 1280 and 390 in both themes.

Expected Result: only workspace tokens remain, a test keeps it that way, and every label on these pages follows the register.

## 7. Integration verification

- [x] 7.1 Run the checks and tie every count to the commit:
  - in `frontend`: `npm ci`, `npm test`, `npm run type-check`, `npm run lint` (0 warnings) and `npm run build`;
  - the backend suite on a disposable Flyway-migrated PostgreSQL 16;
  - `openspec validate --all --strict`;
  - `git diff --check`.

  Evidence: pass, fail and skip counts.
- [x] 7.2 Browser matrix on `/accounts`, `/goals` and every `/settings/*` section, plus each drawer and each modal open at 1280 and 390 px, with a seeded synthetic owner. Check:
  - axe with zero serious or critical violations;
  - measured contrast of captions, pills, amounts and table headers;
  - no horizontal overflow and no console errors;
  - one screenshot per cell;
  - a keyboard-only walk: every row, every drawer action, reorder mode, every modal opened and closed with focus return;
  - 200% and 400% zoom at 1280 px;
  - reduced motion.

  Read every screenshot and fix what looks off. Evidence: the matrix table with counts and the screenshot paths.
- [x] 7.3 Run the unchanged-writes harness from 1.3 on the phase 5 build against a fresh copy of the same seeded database, and compare per flow. Evidence: an equal result for every flow, or each difference with its cause and fix.
- [x] 7.4 Pixel comparison of every route against 1.2's baseline screenshots at 1280 and 390 px in both themes. Every difference outside `/accounts`, `/goals` and `/settings/*` must be explained (antialiasing) or fixed. Also run axe, overflow and console checks on every route. Evidence: the diff counts per route and the regression table.
- [x] 7.5 Gates (D18):
  - first-load JS for `/accounts`, `/goals` and the new settings routes;
  - the built CSS size;
  - the `/dashboard/net-worth` median;
  - `GET /api/habits` timing and statement count.

  Evidence: the numbers before and after.
- [x] 7.6 Cross-page consistency in the browser, with values recorded before and after:
  - Rekening's Kekayaan bersih equals Beranda's KPI;
  - Target & Tagihan's bills total equals the plan part of Beranda's Terjadwal;
  - Ketahanan Dana equals Beranda's card;
  - after a top-up from Rekening, both net worth figures still agree, and the product's value rose by the amount;
  - after a capture and its undo, the Kebiasaan count of that key is unchanged.

  Evidence: the recorded values.

- [x] 7.7 Fix OD70 (owner, 2026-11-03: "fix the bug first"): the goal form omits `current_amount` for account-backed goals, so editing their target and switching a standalone goal to linked accounts are accepted. Evidence: the two Vitest cases failed before the fix with `current_amount` in the payload and pass after; the backend contract test passes on Flyway V44; backend 1710 passed, Vitest 78 files and 711 tests with no unhandled errors.

Expected Result: phase 5 is proven locally. Every write is unchanged against `main`, every route keeps its look, and the three pages pass WCAG 2.2 AA checks at five widths in both themes.


## 8. Acceptance

- [ ] 8.1 Present to the owner:
  - verification.md;
  - the key screenshots;
  - the unchanged-writes comparison;
  - the alias measurement at 0;
  - OD50–OD59 as applied.

  The owner accepts on this evidence. Record the owner's words and date in proposal.md, and in PROJECT_STATE.md if the owner keeps one this session. Evidence: the owner's words quoted with the date.

## Workflow follow-up

- After acceptance:
  - archive with `openspec archive redesign-money-and-settings` on `feat/money-and-settings`, before the owner merges;
  - `openspec validate --all --strict` exits 0;
  - push the branch and report the compare link.
- The owner opens and merges the pull request, and the release runs only through `deploy.yml`.
- After the deploy, check read-only and record the result in PROJECT_STATE.md:
  - `GET /api/habits` answers 200 for the owner;
  - `/dashboard/net-worth` `net_worth` equals Beranda's figure;
  - the three pages load.
- Delete the branch locally and on the remote after the merge.
