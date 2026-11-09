# Verification: apply-markas-cleanup (Markas+ phase 12)

Date: 2026-11-09 · Branch `chore/markas-cleanup` · Base: `origin/main` `170e182` (PR #99 merged; the same tree as
`origin/feat/markas-catat` `10cd7e5`), merged in `2bcc36a` · Code under test: `606ff5d` (frontend and backend; the
later commits change only this change's documents) unless a check names another revision.

Scratchpad of the gate (synthetic data only):
`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/cleanup/gate/`
(below: `gate/`). An earlier capture attempt in `scratchpad/cleanup/shots/` was interrupted and not reused: its
database was gone and its "after" set predated `39df848`.

## Approval, decisions and dependencies (tasks 1.1, 1.2)

- Plan committed in `52c4ee8`. Approval 2026-11-09 through the coordinator: every default as written (D1 to D5, D8 to
  D17, D19), item 11 dropped with only a regression test, D9 accepted as the owner's one-basis rule. The owner chose
  F1 option A ("Keep the date") on 2026-11-09; it was built last (task 8.1).
- `origin/main` was `5401a03` with PR #99 open when the work started; PR #99 merged during the work (`170e182`) and
  was merged into this branch in `2bcc36a` ("chore: merge main into chore/markas-cleanup"), which changed no file.
  `git fetch` before the gate and again before the push: `origin/main` still `170e182`.
- D19: `renderWithIntl` keeps `id` as its default language (no product change; about 2,000 assertions rely on it).
- Not in this change, as approved: public sign-up, the recurring-charge basis for Tabungan-pillar charges, a full
  catalogue-key prune (listed in `end-review.md`).

## Environment (task 1.3)

- Disposable `postgres:16` `cft-cleanup-pg` (127.0.0.1:9830) on network `cft-cleanup-net`. `cft_markas_cl` migrated
  V1 → V51 with `flyway/flyway:10` (`gate/logs/flyway-v51.log`), `seed_parity.py --today 2026-11-09` (188
  transactions), → V54 (`gate/logs/flyway-v54.log`), then `seed_pengaturan.py` (3 added) and, through a loopback API,
  `seed_transaksi.py` (70), `seed_analisis.py` (7), `seed_rekening.py` (20), `seed_laporan.py` (21), `seed_target.py`
  (8 added, 6 present, 1 skipped), `seed_catat.py` (3 added, 2 already present). Reports: `gate/logs/seed-*.json`.
  `cft_markas_cl_en` is a copy whose owner switched to English through `PATCH /auth/settings`.
- Each capture side had its own copy (`_bid`, `_aid`, `_ben`, `_aen`), so nothing one side wrote reached the other;
  `_kid`/`_ken` for keyboard flows that write, `_d9b`/`_d9a` with a later-today credit purchase, `_perf` for timing.
- Backends: this worktree (after) and a temporary `git worktree` of `170e182` (before), both run from one
  `uv venv -p 3.12` with `requirements-dev.txt`; `NOTIFICATION_AI_ENABLED=false`, no API key, jobs off, outbound HTTP
  pointed at a closed port. APIs on 8771–8779; production builds (`npm run build`, webpack) on 3830 (after) and 3831
  (before) behind loopback proxies 3832–3840. Headless Brave over CDP 9832–9850; axe-core from the session's `axe/`
  folder (wcag2a, wcag2aa, wcag21a, wcag21aa, wcag22aa). No production system, paid service or AI provider was used.

## Checks

| Check | Revision | Result |
| --- | --- | --- |
| `npm run type-check` | `606ff5d` | pass |
| `npm run lint` | `606ff5d` | pass, no warnings |
| `npm test` (vitest) | `606ff5d` | 97 files, 1073 tests passed |
| `npm run build` (after; no `/komponen` in the route list) | `b0ffe6f` (`606ff5d` changes a test file only) | pass |
| `npm run build` (before) | `170e182` | pass (lists `/komponen`) |
| Lockfile | — | `frontend/package.json` unchanged since `170e182`; lockfile untouched |
| Backend pytest, fresh `postgres:16` V1 → V54 (`flyway/flyway:10`), `uv venv -p 3.12` | `606ff5d` | 2203 passed, 0 skipped, exit 0 (second full run; see below) |
| `openspec validate --all --strict` | `606ff5d` | 85 passed, 0 failed |
| `git diff --check origin/main` | `606ff5d` | clean (`606ff5d` removed a blank line at the end of `utils.test.ts` that `70bf135` left) |
| `git diff --stat origin/main -- db mobile nginx` | `606ff5d` | empty (no migration) |
| `/komponen` on the built apps | `606ff5d` / `170e182` | 404 / 404 (the base hid the sheet without `NEXT_PUBLIC_COMPONENT_SHEET`; now the route is gone) |

Pytest note: the first full run (under the load of eight capture runs) had 1 failure,
`test_balance_check.py::test_saldo_cocok_starts_the_next_window` (`assert [] == ['tx:…']`). Cause: the module fixes
`NOW = datetime.now(timezone.utc)` at import and adds its "recent" row at `NOW + 1 minute`, while the balance match it
compares against is recorded at the real time; when more than a minute passes between collection and this test, the
row falls before the match. The file is unchanged since `3ad2d1f`; alone it passes (11 passed) and the full re-run on
a fresh database passed 2203/2203. Recorded as a follow-up, not changed here.

## Captures and pixel diff (tasks 1.4, 6.4, D16)

Every app scenario of `scenarios-app.json` (the two `komponen` scenarios are gone) through `capture.mjs`, on the before
build (`170e182`) and the after build (`606ff5d`) at the same time against identical database copies, compared with
`pixel-diff.mjs` (tolerance 0).

| Set | Cells | Captures per side | Compared / identical / different |
| --- | --- | --- | --- |
| Indonesian | 1440 light, dark | 413 | 413 / 366 / 47 |
| Indonesian | 390 light, dark | 404 | 404 / 357 / 47 |
| English | 1440 light, dark | 413 | 413 / 341 / 72 |
| English | 390 light, dark | 404 | 404 / 326 / 78 |
| Indonesian, changed pages | 1024, 320 light, dark | 336 | 336 / 276 / 60 |
| English, changed pages | 1024, 320 light, dark | 336 | 336 / 231 / 105 |
| Signed out (Masuk, Daftar), id and en | 1440, 390 light, dark | 8 + 8 | 16 / 16 / 0 |
| Re-capture of the unexplained pairs, id and en | 1440, 390 light, dark | 107 + 107 | see groups 6 and 7 |

In all 3,268 + 1,344 + 32 captures per run set. Folders: `gate/shots/{before,after}-{id,en}-{1440,390}`,
`gate/shots/x-{before,after}-{id,en}` (1024/320), `gate/shots/out-*`, `gate/shots/re-*`; diff images and
`pixel-diff.json` in `gate/diff/<set>/`. Changed pages at 1024 and 320: Beranda, the Transaksi provider view, Laporan,
Analisis and their dialogs, Catat, Pengaturan, and the Perpindahan sheet (85 scenarios, `gate/extra-regex.txt`).

Every differing pair falls in one of these groups; nothing else differs:

1. **Laporan "Pengeluaran dari tabungan" (D8, expected).** `laporan`, `laporan-rincian`, `laporan-tabel`,
   `laporan-sembunyi`, `laporan-siklus`, `laporan-november`, `laporan-catatan-baru`, `dlg-laporan-tak-terduga` grow by
   one row (56 px at 1440, 64 px at 390) in both languages, every width and theme; periods with no spending from
   savings (`laporan-juli`, `laporan-sebagian`) are identical. Side-by-side (the pair differs in size, so there is no
   diff image): `gate/diff/side-by-side/laporan-1440-light.png`, `laporan-november-1440-light.png`,
   `laporan-390-dark.png`.
2. **Kredit header (D9, expected).** `transaksi-kredit-penyedia`: "Pengeluaran pakai kredit" reads Rp 581.500 (the
   hero's figure) instead of Rp 1.031.500; the seed's "Sepatu lari" Rp 450.000 at 19.15 today is after "now" and not in
   the list. At 320 in Indonesian the fixed phone tab bar covers that row of the full-page capture, so that pair is
   identical. Diff: `gate/diff/id-1440/transaksi-kredit-penyedia-1440-light.png`.
3. **Catat default "Ke" (D13, expected).** `catat-perpindahan`, `catat-panjang-perpindahan`, `dlg-catat-perpindahan`:
   Ke is "Dana Darurat" instead of "Utama" (the pocket BCA's money resolves to). Diff:
   `gate/diff/id-1440/catat-perpindahan-1440-light.png`.
4. **English "Sep" (D11, expected, English only).** `analisis-lalu`, `analisis-siklus-lalu`, `glob-notifikasi`,
   `shell-dlg-notifikasi`, `dlg-notifikasi-kredit`, `pengaturan`, `pengaturan-perangkat`, `pengaturan-kunci-konfirmasi`,
   `pengaturan-gaji`, `dlg-cabut`, `dlg-produk-nominal`, `dlg-rekening-kredit-tagihan`, `laporan-catatan`: "29 Sept",
   "Aug–Sept", "CYCLE 29 SEPT – 27 OCT" became "Sep" (crops: `gate/crops/en-montage.png`; diffs in `gate/diff/en-1440/`).
5. **Clock and random values (not a product difference).** Catat's time field and the top-up dialog's date-time field
   show the capture's own seconds (`catat-pemasukan`, `catat-manual-detail`, `catat-galat-sama`, `catat-dari-bel`,
   `catat-perpindahan-kredit`, `catat-perpindahan-tanpa-tagihan`, `catat-tulis-kredit`, `catat-galat-nominal`,
   `catat-sembunyi`, `catat-panjang`, `catat-manual`, `dlg-catat-manual`, `dlg-topup`; 6 to 290 px inside a 17 × 11 px
   box, crop `gate/crops/catat-pemasukan-1440-light.png`); English Pengaturan's test-session times
   (`pengaturan-sesi-uji`, `pengaturan-sesi-laporan`); the pairing code is new on every open (`dlg-hubungkan-ponsel`).
6. **Run artefacts, gone on re-capture (not a product difference).** `shell-target`, `shell-laporan`, `shell-dlg-tema`,
   `dlg-ganti-nama`, `dlg-saran-riwayat`, `tinjau-sembunyi`, `tinjau-perlu-jawaban`, `transaksi-gabung`,
   `transaksi-cari`, `transaksi-filter-aktif`, `transaksi-rekening`, `transaksi-sembunyi`, `rekening-grafik`,
   `pengaturan-profil` (320 dark): the 1440 and 390 runs of one side shared a database, so the 390 run's test-session
   banner ("Sesi uji berjalan …") showed in a 1440 capture of one side only, and lists or transitions were caught at
   another moment. All are identical in the re-capture (`gate/diff/re-{id,en}/`, `gate/diff/re2-id/`).
7. **Rendering noise of the same build.** `analisis-peta-aliran` and `analisis-bulan` (the Peta/Tabel segment and a
   2 px corner, 1 to 3,334 px), `dlg-beli`, `dlg-jual`, `dlg-kantong-tambah`, `dlg-target-tambah`, `dlg-ubah-kategori`,
   `dlg-saran-riwayat` at 390 (1 to 6 px at a field's edge): the before build compared with itself across two runs
   differs in the same pairs by the same amounts (`gate/diff/noise-{id,en}/`, 23 and 19 pairs).

### Checklists, axe, console, overflow (after; the before sets give the same counts)

| Set | Captures | Beranda | Transaksi | Analisis | Laporan | Catat | Pengaturan | Other checklists | axe serious/critical | Console errors | Overflow | Notice / other `lang` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id 1440 | 413 | 1416/1416 | 1498/1498 | 1062/1062 | 1390/1390 | 854/854 | 838/838 | target 1636/1636, tinjau 674/674, rekening 861/878, shell 1819/1828 | 0 | 2 | 0 | 0 / 0 |
| id 390 | 404 | 1254/1254 | 1002/1002 | 1008/1008 | 1278/1278 | 818/818 | 780/780 | target 1480/1480, tinjau 430/430, rekening 775/792, shell 762/786 | 4 | 2 | 0 | 0 / 0 |
| en 1440 | 413 | 1416/1416 | 1498/1498 | 1062/1062 | 1390/1390 | 852/852 | 838/838 | as id 1440 | 0 | 2 | 0 | 0 / 0 |
| en 390 | 404 | 1254/1254 | 1002/1002 | 1008/1008 | 1278/1278 | 818/818 | 780/780 | as id 390, shell 758/786 | 4 | 2 | 0 | 0 / 0 |
| id 1024, 320 | 336 | 2224/2224 | 248/248 | 2006/2006 | 2616/2616 | 1588/1588 | 1558/1558 | target 98/98 | 0 | 4 | 0 | 0 / 0 |
| en 1024, 320 | 336 | 2224/2224 | 248/248 | 2006/2006 | 2616/2616 | 1588/1588 | 1558/1558 | target 98/98 | 0 | 4 | 0 | 0 / 0 |

- **Beranda checklist (task 5.1):** with the 136 px status column and `--out-fg` expense amounts it passes in full,
  4,894/4,894 per language (1440, 390, 1024 and 320, both themes), where it gave 440 of 446 before the update.
- The checklists of every page this change touched pass in full in both languages and at every width.
- Console errors: only the expected 400 of `POST /api/movements` in `catat-galat-sama` (the scenario picks a refused
  pair on purpose), and the signed-out 401 of `/api/auth/me` on Masuk and Daftar.
- axe: the 4 serious findings per 390 set are `target-size (1) .ws-linkbtn` in `dlg-target-manual` and
  `dlg-target-progres`: the flagged control is Target's "SPayLater · kredit" link behind the open modal sheet (inert);
  the same 4 appear on the base build, so this change did not add them. Listed in `end-review.md`.
- Failures outside the changed pages, identical on the base build: Rekening's "groups 48 apart" measures 16 px on this
  combined seed (17 per width); the phase 2 shell checklist's "content inside the frame" and "visible actions" on some
  pages, and "first content y" where a test-session banner is open. Steps "NOT FOUND" in older scenarios
  (`dlg-catat-*`, `dlg-target-detail`, `dlg-penjelasan-angka`, `tinjau-topup`, `tinjau-ambigu`, and `catat-panjang*`
  without the stress seed) are the same on both sides.
- The `state-*` scenarios ran without the fault proxies, so they captured the normal pages on both sides.

## Target size (task 6.5, D12)

Phase 9's scenario (`dlg-perpindahan-kredit`: Perpindahan for Kartu Kredit Contoh's bill over Beranda) at 390 and 320,
light and dark, in Indonesian and English, after: axe `target-size` 0 in all 8 captures (and 0 at 1024). The phase 9
finding does not return.

## Figure parity (task 6.6)

`gate/figures.mjs` reads every amount, percentage, time, day-month date and count (`parity-languages.mjs`'s extractor,
months normalised) from 37 scenarios at 1440 light: Beranda, Rincian (closed and open), Beranda with credit, Transaksi
(cycle, month, all time, Pengeluaran, Tabungan, Kredit, the SPayLater provider view, one account), Analisis
(Ringkasan, month, last month, last cycle, table, Kalender, Peta aliran and its table, Rincian pengeluaran), Laporan
(default, July, partial, November, cycle, Rincian, table), Rekening (both views, Rincian kekayaan, a credit account),
Target (page, all bills, paid off, Rincian ketahanan, a debt). Results: `gate/parity/`.

| Pair | Same | Different | Differences |
| --- | --- | --- | --- |
| before vs after, Indonesian | 31 | 6 | provider view: Rp 1.031.500 → Rp 581.500 (D9); Laporan default, Rincian, table and cycle gain "−Rp 360.000", November "−Rp 141.000" (D8) |
| before vs after, English | 31 | 6 | the same six with the same values |
| English vs Indonesian, after | 22 | 15 | wording only, and the same 15 on the before build (`before-id--before-en.json`): the extractor keeps the full stop of Indonesian sentences ending "Rp 2.203.830."; English writes the cycle's end date ("27 Oct – 9 Nov") where Indonesian writes "sejak 27 Okt · berlanjut"; counts whose English unit word the extractor does not know. No amount differs |

`parity-languages.mjs` (Beranda, Rincian, Rincian with every line open), English vs Indonesian: 3/3 scenes SAME on the
after build (45, 12 and 52 figures, money byte-identical) and on the before build.

The parts of "Rincian tabungan" add up in every period with savings (`GET /api/reports/monthly`): October Rp 1.600.000
+ Rp 2.600.000 − Rp 360.000 = Rp 3.840.000; November Rp 300.000 + Rp 283.000 − Rp 141.000 = Rp 442.000.

## D9 and D13 against the base build

- D9 (copies `_d9b`/`_d9a` with a Rp 77.000 SPayLater purchase at 23.00 today): before, hero Pengeluaran Rp 581.500
  and header "Pengeluaran pakai kredit" Rp 1.108.500 (both later-today purchases counted); after, both Rp 581.500.
  Screenshots `gate/d9/before.png`, `gate/d9/after.png`.
- D13 (`gate/d13-flow.mjs`, default pair kept, Rp 25.000): before, BCA (Default: Utama) → Utama, refused "Rekening
  sumber dan tujuan harus berbeda."; after, BCA → Dana Darurat, saved with no error. Screenshots in `gate/d13/`.

## Keyboard flows (task 6.6)

`gate/flows.mjs` on fresh copies (`_kid` in Indonesian, `_ken` in English), 1440 and 390: 32/32 in Indonesian and
32/32 in English (`gate/flows/{id,en}/flows.json`):
- A, Catat Perpindahan defaults: N opens Catat, "Isi manual" by Tab and Enter, Shift+Tab to the mode radio, → to
  Perpindahan, Ke preselected as Dana Darurat (not Dari's pocket), Tab to the amount, Enter saves with no error.
- B, provider view: SPayLater reached by Tab (34 stops at 1440, 20 at 390), Enter opens the view with focus kept on
  the pressed chip; hero and header both Rp 581.500; "Siklus sebelumnya" by Tab and Enter (`offset=-1`), both
  Rp 434.000.
- C, Laporan's Rincian tabungan: three rows adding up to Rp 3.840.000; "Pengeluaran dari tabungan −Rp 360.000" holds
  no focusable element; the saving-pillar row is a Tab stop and the next stop is outside the list; Enter on it opens
  `/ledger?kakeibo=saving&type=expense&from=…&to=…`.

Phase 11's `catat-keys.mjs` on the same copies: 108/108 in Indonesian and 108/108 in English.

## Ledger list timing (task 4.5, D10)

`_perf` (the parity owner widened to 19,506 rows by copying its own expenses and incomes 430 to 3,130 days back),
`GET /api/transactions?limit=50&logical_movements=true`, 30 runs after 3 warm-ups, through the loopback proxy, ms:

| Variant | Before (`170e182`, JIT on) p50 / p95 | After (JIT off for the request) p50 / p95 |
| --- | --- | --- |
| all rows, `daily_totals,type_counts` | 635.4 / 670.6 | 110.5 / 116.3 |
| all rows, `daily_totals` | 76.7 / 82.3 | 56.1 / 60.4 |
| `period=cycle`, both | 526.6 / 574.9 | 45.3 / 53.9 |
| `type=movement`, both | 670.4 / 719.9 | 105.2 / 109.9 |

The widened history raises the planner's cost, and so JIT's compile time, above the design's 20,116-row table; the
direction is the same.

## Prototype side-by-side (task 6.7, D17)

`compare.mjs`, prototype against the after build, Indonesian, light: 68 pairs at 1440 and 67 at 390, 135 images in
`gate/prototype-side-by-side/`. The prototype side is the 146 captures in `scratchpad/cleanup/shots/proto/` (taken
13.19–13.34 today; no prototype file and no `scenarios-prototype.json` change since). Without an app capture of the
same name: `dlg-catat` (the app's is `catat-tulis`), `state-tinjau-galat`, `state-tinjau-kosong`, `state-tinjau-memuat`,
`glob-lainnya` at 1440, `glob-akun` and `glob-tema` at 390 (phone and desktop variants are named differently).

## Removals re-proved (tasks 3.1, 3.5)

`rg -w` in `frontend/src` at `606ff5d` finds no use of: `Button`, `CardHeader`, `ChoiceGroup`, `DeltaChip`,
`Disclosure`, `FormErrorSummary`, `INDONESIAN_ROW_LABELS`, `Meter`, `MovementFormValues`, `PayrollItemPayload`,
`ReconciliationFlag`, `ReviewSummary`, `TRANSLATED_ROUTES`, `UntranslatedNotice`, `UntranslatedRegion`, `clampNumber`,
`classifierLabel`, `currentMonthYM`, `deleteMovement`, `fromDatetimeLocal`, `getCurrentTestSession`, `greetingName`,
`habitBasisText`, `habitRecordLabel`, `hasRowFilters`, `isTranslatedRoute`, `isUnpaid`, `kakeiboGroups`,
`kakeiboLabel`, `kakeiboPillarInfo`, `kindInfo`, `needsOwnerReview`, `parseClampedNumber`, `pillarPct`, `pillarSpent`,
`pressOnKey`, `reviewStatusLabel`, `sourceLabel`, `untranslatedPageTitle`, `updateTransaction`, `useInterfaceLang`,
`useRegion`, `useUntranslated`. The remaining matches are other things with the same word: `Card` (a catalogue
value), `Input` (a test's error text), `PageHeader` (the workspace context type), `useWidthAtMost` (the breakpoints
guard's pattern). Removed files: `types/domain.ts`, `hooks/useWidthAtMost.ts`, `lib/untranslatedRowLabels.ts`,
`lib/i18n/translatedRoutes.ts`, `ui/Button.tsx` and its test, `ui/Input.tsx`, `ui/PageHeader.tsx`,
`ui/ReconciliationFlag.tsx`, `workspace/Disclosure.tsx`, `workspace/untranslated.tsx`, `workspace/untranslatedPages.ts`,
`app/komponen/` (4 files).

Catalogue keys removed from both catalogues, absent from `en.json` and `id.json` and named in no source file:
`notice.untranslated`, `nav.components`, `home.viewBreakdownLabel`, `home.closeExplanation`, `home.whyUnavailable`,
`home.itemCount` (a parity test still uses the name as sample data), `common.closeDetails`.

## Diff review (task 6.8)

`git diff origin/main` reviewed: the backend adds the three optional parameters, the additive field, the JIT statement
and F1's setter rule; the web part is the removals, the savings row, the provider window, the "Sep" formatter and the
default Ke. No secrets, `.env` content, debug output or commented-out code; nothing outside the plan except `606ff5d`
(a trailing blank line). No migration.

## Not verified

- A real phone and a screen reader; phone widths use the browser's device mode. Zoom to 200 % and 400 % (the 320 px
  captures cover reflow).
- State scenarios with the fault proxies (loading, empty, failure): not run in this gate; their code did not change.
- The stress seeds (long names, USD display): not loaded; `catat-panjang*` were captured without them on both sides.
- No AI provider was called; the backend ran with AI off and no key.

## Clean-up (task 7.2)

Stopped by PID after the gate: the eight APIs (8771–8778), both Next servers (3830, 3831), the eight proxies
(3832–3839); the timing API (8779) and its proxy (3840) were stopped right after the measurement, and every capture
browser closed with its run. Removed containers `cft-cleanup-pg` and `cft-cleanup-pytest`, network `cft-cleanup-net`,
and the base worktree (`git worktree remove`, then `git worktree prune`; `git worktree list` shows only the owner's
checkout and this worktree). A listener check of 8771–8779, 3830–3849 and 9830–9849 found none, and no capture, proxy,
uvicorn or Next process of this gate remains.

## Final revision

Archived on 2026-11-09 as `2026-11-09-apply-markas-cleanup`; the specs of `clean-core-ledger`, `credit-accounts`,
`design-tokens`, `explainable-figures`, `interface-language`, `monthly-navigation-stepper`, `monthly-report`,
`tactile-pulse-ui`, `theme-presets-switcher` and `transaction-ledger-management` took the deltas. The first
`openspec validate --all --strict` after archiving failed two merged specs on the "requirement text over 500
characters" rule: "Credit Account Overview" (the listing sentences moved to a new requirement "Credit Bills Across
Accounts" with their scenario "Bills due soon across accounts") and "Four Kakeibo questions in a fixed order" (shorter
wording, same obligations). Then: 84 passed, 0 failed (84 items).

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these
over:
- Phase 12 (`apply-markas-cleanup`) built and gated 2026-11-09, archived and pushed on `chore/markas-cleanup` without a
  PR (coordinator's instruction). The Markas+ redesign is complete; the owner's review list is
  `openspec/changes/archive/2026-11-09-apply-markas-cleanup/end-review.md`.
- Gate on `606ff5d`: vitest 1073/1073, type-check and lint pass, build pass, pytest 2203/2203 (fresh V1 → V54),
  openspec 85/85; before/after captures with a pixel diff in which every difference is D8, D9, D11, D13 or explained
  noise; figure parity before/after differs only by D8 and D9.
- No untranslated-page path and no `/komponen` sheet remain; requirement "Untranslated Pages Inside a Translated
  Shell" is removed. `ProgressBar` is the one progress bar (`Meter` is gone). `tokens.css` holds only used tokens and
  `tokens.test.ts` fails on an unused token or Tailwind colour key.
- Additive server changes: `GET /api/reports/monthly` `savings.paid_from_savings`; `GET /api/credit/accounts/{id}`
  accepts `period`/`period_offset` or `from_date`/`to_date` (422 `period_with_dates` for two forms,
  `credit_period_invalid` for half a range). `GET /api/transactions` runs with JIT off for its transaction.
- Behaviour changes the owner sees: Laporan's "Pengeluaran dari tabungan" row; the Kredit header now equals the listed
  rows (it no longer counts a purchase timed after "now" or before the cycle's salary instant); English "Sep";
  Catat's default "Ke" never equals "Dari"; a count-only terms edit keeps a system-kept due date (F1).
- Follow-ups: public sign-up decision, Tabungan-pillar recurring charges, a catalogue-key prune; the real-phone QR
  scans and the other owner checks in `end-review.md`; `test_balance_check.py`'s import-time `NOW` fails when the suite
  runs slowly; axe `target-size` on Target's "SPayLater · kredit" link behind the goal sheet at 390 (base build too).
- Disk: `scratchpad/cleanup/` holds about 6 GB of this phase's captures and earlier attempts; deleting them is the
  owner's call.
