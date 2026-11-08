# Verification: apply-markas-tinjau (Markas+ phase 4)

Date: 2026-11-08 · Branch `feat/markas-tinjau`, created from `f1ee604` · Code under test: `f689171` (frontend and
tooling; `tinjau-checklist.js`'s two expectation corrections and this record are committed on top).

## Approval, dependencies and the flagged decision (tasks 1.1, 1.2)

- Approval: the owner's standing instruction "complete all until finished" (2026-11-06), renewed 2026-11-08 ("continue
  all"). Every decision takes its recommended default.
- **D14 reviewed 2026-11-08: default taken.** "Semua sudah ditinjau" shows no cycle figure, and Tinjau no longer
  requests `/dashboard/cycle-summary`. Recorded in `proposal.md` › Approval, `design.md` D14 and the decisions table;
  the `review-queue` delta "Semua Beres" already states it.
- `PROJECT_STATE.md` lives untracked in the owner's checkout, which this run does not touch (as in phase 3), so these
  are recorded here for the owner to carry over:
  - phase 4 approved 2026-11-06 under the standing instruction, D14 default taken 2026-11-08;
  - **open item:** after phase 8 merges, delete `fix/selection-bars-and-tinjau` locally and on the remote and remove the
    `cash-flow-tracker-ui-refine` worktree, after checking its parts are on `main` (design D21);
  - end-review list: the deviations and the kinds without a prototype screen below.
- Dependencies: the language system and server text came first through the stacked base
  `origin/feat/localize-server-text` `af8f95e` (PR #90, on main `0308b7b`), merged as `c6c1b1a`
  ("chore: merge localize server text into markas tinjau"). PR #90 then merged into main (`17d3f4a`), and the branch
  was updated from `origin/main` `9a2f954` (which also holds PR #91, credit spending) as `138005b`. `git merge-base
  --is-ancestor` holds for `0308b7b`, `af8f95e` and `9a2f954`. `messages/en.json`, `lib/format.ts` and
  `lib/i18n/translatedRoutes.ts` exist; on the base build an English Tinjau showed "This page is in Indonesian for
  now." (workspace test, now moved to Transaksi).

## Environment

- Local only. PostgreSQL 16 container `cft-tinjau-pg` on `127.0.0.1:55480` with throwaway credentials; Flyway
  `flyway/flyway:10`.
  - `cft_markas_parity`: migrated to V51, loaded by `seed_parity.py` (`dimas_contoh`, data.js shifted to today,
    188 transactions), migrated to V54, then `seed_tinjau.py --kinds`; `cft_markas_parity_en` is its copy with the
    account in English.
  - `cft_markas_audit`: the phase 1 audit seed (`raka_contoh`) migrated to V54, then `seed_tinjau.py --kinds
    --stress`; `cft_markas_audit_en` its English copy; both later switched to USD.
- Backend from this worktree in a `uv` Python 3.12 environment; background jobs, AI and outbound HTTP off.
- "After": this worktree's production build (`NEXT_PUBLIC_COMPONENT_SHEET=1`) behind loopback proxies (3621 id, 3622
  en, 3623 holding the queue for the loading state, 3624 failing it, 3625/3626 the audit seed). "Before": `c6c1b1a`'s
  `frontend/` exported with `git archive` and built the same way (3610). Prototype `design-options/` on 3681.
- Headless browser over CDP (`scripts/verify/markas/cdp.mjs`), device scale 1; axe-core 4 (wcag2a, wcag2aa, wcag21a,
  wcag21aa, wcag22aa).
- Captures, checklists, composed images and JSON stay in the session scratchpad
  (`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/markas-tinjau/`),
  not in `research/evidence/`, as in phases 2 and 3.

## Seeds (task 1.4)

`scripts/verify/markas/seed_tinjau.py` adds, on top of a seeded owner and in one transaction (loopback `cft_markas_*`
only, guards tested in `test_seed_parity.py`, 11 passed):

- parity seed: automatic group: Kopi Pojok Contoh, 2 auto rows; ambiguous leg: Rp 750.000 into Dana Darurat, sends from GoPay and ShopeePay; awaiting debit of Rp 750.000 on Utama, beside its learned top-ups; history-suggestion group: Kafe Senja Contoh, 3 pending suggestions; seen twice: a Jago report of Rp 500.000, kept leg on GoPay; skipped by a link: GoPay also reported by Jago;
- audit seed: automatic group: Kopi Pojok Contoh, 2 auto rows; ambiguous leg: Rp 750.000 into ATM, sends from Gopay and Cash; history-suggestion group: Kafe Senja Contoh, 3 pending suggestions; seen twice: a Jago report of Rp 500.000, kept leg on Gopay; skipped by a link: Gopay also reported by Jago; stress: 250 rows of 'Toko Peralatan Rumah Tangga dan Dapur Sejahtera Makmur Abadi' (60 characters), 1050 more waiting rows, and one spending of Rp 1.234.567.890; skipped: awaiting debit with a learned top-up: the owner has no converted top-up to learn from.

`GET /api/review/queue` on the parity database then returns every D9 kind: 1 question, groups in "Perlu jawaban"
and "Dicatat otomatis", 2 awaiting debits (one with a `topup_suggestion`), 1 ambiguous credit with 3 candidates,
2 seen twice with a kept leg, 1 skipped by a link, and 3 pending history suggestions. On the stress seed it returns
`truncated: true` (hero 1.303). No skip remains for the parity seed; the audit seed has no converted top-up, so its
learned top-up is a recorded skip. `git diff --stat origin/main -- backend db mobile nginx` is empty.

## What changed

- **Page:** the title block from the catalogue (`nav.review`, `review.page.subtitle`) with one "…" (`review.page.more`)
  holding "Cari saran dari riwayat" and "Bantuan & pintasan"; the hero `HeroFigure` without a mark (`attention`, the
  `unreviewed` line, the truncated line); the `.tj` region of rail, list and item at 232 · 336 · 1fr, the item below
  from 1199 px, chips and a full-height sheet below 768 px.
- **Rail** (`components/workspace/rail.tsx`, new shared piece): labelled `nav`, 36 px items with the straight bar
  (`.ws-select-bar`), counts with hidden words, the rule before the optional folders, empty folders hidden, chips below
  768 px. Optional folders show their caption above the list.
- **List:** `ListRow` gained `layout="stacked"`, `current`, `rowKey` and a plain variant with `actions`; muted icons
  (help, alert, check, archive), "kind · date time" in Semua, signed and coloured amounts.
- **Item:** `ItemBody` (kind label with "N dari M" and ‹ ›, the question as the focused `h2`, one meta line, warning,
  note and reasons lines, the status line, answers, rows, the notification text box) and one parts builder per kind in
  `components/review/triage/items/`; numbered answers in `Answers.tsx` (`AnswerList`, `NestedAnswers`, `Answer`,
  `KeyCap`, `pressChoice`, `pressMarked`).
- **Dialogs:** `CategoryDialog` (category, name and spending modes, 560), `AnswerDialog` (520) and
  `HistorySuggestionsDialog` (520, estimate before start, polls only while open), all on `ShellDialog`, which gained
  `fullHeight` for the phone sheet.
- **Language:** the `review` namespace (288 messages in each catalogue), the seven Tinjau error codes in `errors`, every
  date, amount and count through `lib/format.ts` (`dateRangeAt`, `yearDate`, `yearDateAt` added); `lib/triage.ts`,
  `lib/review.ts`, `lib/reviewUndo.ts` and `lib/historySuggestions.ts` hold no copy (codes and key descriptors);
  `/review` is in `TRANSLATED_ROUTES` and out of `untranslatedPages.ts`; the title block reads Tinjau's fixed title
  from the catalogue; the help dialog's Tinjau keys carry no `lang="id"`; `useTransactionUndo` renders its undo messages
  from `review.undo`.
- **Removed:** `TriageInbox`'s two-card layout and `TWO_PANE_MIN`/`useWidthAtMost` use, `TriageRows`, `Avatar`,
  `ItemHead`, `Choice`/`Picker`, `PurposeQuestionCard`, `ReviewGroupCard`, `MovementReviewCards`, `SeenTwiceCard`,
  `SuggestionGroupCard`, `HistorySuggestionsSection` (and their tests), `RUN_PANEL_ID`, `foldOpen`, `ChipGroup`, the
  cycle-summary request and figures of the done state, and the CSS `.ws-triage`, `.ws-tri-*`, `.ws-choice*`,
  `.ws-picker`, `.ws-facts`, `.ws-suggest`, `.ws-form-row`, `.ws-alert`, `.ws-btn-row`, `.ws-grow`. Kept with users:
  `.ws-field` (Transaksi), `.ws-rows` (Analisis), `.ws-notice` (Transaksi, Catat), `.ws-kbd`, `.ws-chip(s)`, `.ws-ava`,
  `useWidthAtMost`, `Pill`. `rg` finds none of the removed names in `frontend/src`.
- No change under `backend/`, `db/`, `mobile/` or `nginx/`, and no request, figure or count changed.

## Side by side (task 5.1)

40 composed images in `…/markas-tinjau/compare/` (prototype left, app in Indonesian right): Tinjau, "Perlu jawaban",
"Terlihat dua kali", "Dilewati otomatis", the "…" menu and "Cari saran dari riwayat" at 1440 × 900, 1024 × 768 and
390 × 844 in both themes, and "Ubah kategori" at 1440 and 1024. App captures of every D22 scenario in both languages
are in `shots/final-id` and `shots/final-en` (1440, 1024, 390 and 320 in both themes, 124 each); the widths 1280, 1199,
768 and 767 in `shots/final-widths`.

Every visible difference, and why:
1. **Data:** the app shows its own server data for the parity seed and the Tinjau kinds (for example the hero 6 counts
   rows, the prototype's 6 counts items; the prototype's "Perlu jawaban" holds a top-up debit, the app's top-up waits in
   "Pindah saldo belum lengkap" as the app files it).
2. **Hero line** without an amount (deviation 8) and folder membership (deviations 1 to 3): "Dicatat otomatis" under
   the rule, "Saran" and "Terlihat dua kali" there too.
3. **Amounts** carry their sign and colour (SPEC rows; spending red by the owner's rule), where the prototype's list
   writes "Rp 70.500" unsigned.
4. **Questions** are the D9 wording ("Kategori untuk Pembayaran QRIS?"), not the prototype's sample sentences, and the
   meta line reads kind detail · amount · date · account.
5. **Group rows** are whole-row links with a chevron, where the prototype writes "Ubah" at the end; their meta adds the
   category, pillar and review state.
6. **Category dialog** has the D10 fields per mode (category and pillar; name and notes in "Ganti nama") and no
   "remember" checkbox (deviation 5); its pillar control wraps to two lines.
7. **History dialog** adds the estimate step's lines and the last run, and "Batal" sits at the left (D12).
8. On the phone the app's first chip line ends where the prototype's does; no row is current until an item opens.

## Measurement checklist and global rules (task 5.2)

`tinjau-checklist.js` (D22: `.tj` columns and gap, top edges, hero at y = 192, one 44 px figure, rail item, bar, rule,
chips, rows, answers, key caps, question, raw box, sheet height, dialogs centred or bottom sheets, no other text
≥ 33 px, no border ≥ 2 px, no horizontal overflow) with `measure.js` (SPEC §13) on every capture:

| Set | Captures | Tinjau checklist | axe serious/critical | Console | Overflow |
| --- | --- | --- | --- | --- | --- |
| Prototype, 1440/1024/390 × 2 themes (`final-prototype`) | 62 | 862 of 862 | 0 | 1 (its missing favicon) | 0 |
| App, Indonesian, every Tinjau scenario at 1440/1024/390/320 × 2 themes (`final-id`) | 124 | **2,176 of 2,176** | 0 | 0 | 0 |
| App, English, the same (`final-en`) | 124 | **2,176 of 2,176** | 0 | 0 | 0 |
| App, 1280, 1199, 768, 767 × 2 themes (`final-widths`) | 8 | 176 of 176 | 0 | 0 | 0 |
| App, loading (queue held) and failure (queue 500), 1440 × 2 and 390 | 3 + 3 | 24 of 24, 9 of 9 | 0 | 3 (the induced 500, logged by the browser) | 0 |
| Before (`c6c1b1a`), 10 widths (`final-before`) | 10 | 30 of 30 (the checks that apply to the old page) | 0 | 0 | 0 |

- Global rules on every app capture: Inter and Manrope only; sizes ⊆ {12, 14, 16, 20, 24, 32, 44}; weights ⊆ {400,
  500, 600}; 5 to 8 text styles in `main`; one 44 px figure (32/40 below 360 px) and no other text ≥ 33 px; no border
  ≥ 2 px; no coloured border (the bars are pseudo-elements, not borders); no off-scale radius; every amount tabular.
- Geometry measured: 232 · 336 · ≥ 488 with 24 px gaps at 1440 and top edges within 1 px; 184 + item at full width
  below from 1199 to 768 px; one column below 768 px; hero at y = 192; rail items 36 px, padding 12, radius 6, 14/500,
  counts 12/400, the 2 × 20 bar at −8/8, the 1 px rule; chips 44 px, radius 6, 1 px line, no bar, scrolling inside
  their line; list rows ≥ 64 px with padding 12 and the 40 px icon column; answers ≥ 56 px with padding 8 12 and 20 px
  key caps; the question 16/24 400; the notification box padding 12 16 radius 6; the phone sheet as tall as the
  viewport with radius 0; dialogs centred at 1440 and 1024 and bottom sheets at full width on the phone.
- English rail labels at 184 and 390 px: "Transfer not complete", "Recorded automatically" and "Skipped automatically" ellipsize in the 184 px
  rail at 1024 (allowed by D5; full names in the accessible names) and fit the chips.
- The checklist's own expectations were corrected twice during the gate (phone rows keep padding 12 as the prototype's
  `.tj-list .row` does; the hero is 32/40 below 360 px); the counts above are with the corrected checklist.

## Accessibility and keyboard (tasks 5.3, 5.4)

- axe-core: **0 serious or critical** on all 124 + 124 app captures (both languages, both themes, 1440, 1024, 390,
  320), which include an item of each kind, every dialog open ("Ubah kategori", "Ganti nama", "Jawab sendiri", "Cari
  saran dari riwayat"), the "…" menu open, the phone sheet open, hidden balances, and on the loading, failure,
  stress and USD sets; also 0 in the unit tests with dialogs and nested lists open. Console errors: 0.
- Language marks: an English Tinjau has no `lang="id"` element and no untranslated notice on any capture; the help
  dialog's Tinjau keys carry no `lang`.
- `tinjau-keys.mjs` (11 flows) at 1440 and 390: **Indonesian 20 of 20**; **English 20 of 20** in two runs, 19 of 20
  in a first run, where the deep-link flow (9) found the item selected and shown but focus not on its heading when
  read 400 ms after load (`…/keys-en/tinjau-keys.json`); recorded as a residual risk. Flows: Tab order skip link →
  sidebar → top bar → "…" → rail → list → item; no focused element under the top bar; the menu with Enter, ↓ and Esc
  back on "…"; J from the list to the next row and K from the item; 1 on a group with the toast and Z restoring it with
  focus; T converting the learned top-up and Z bringing it back; E without a suggested answer naming it; 2 opening the
  category dialog with focus inside, Esc returning focus to answer 2, saving, and Z; "1 kopi" typed into "Jawab
  sendiri" pressing nothing; a deep link selecting its item; J inert with single keys off; at 390 a row opening the
  full-height sheet with the heading focused, 1 inside it answering and showing the next item, and back closing it with
  focus on a row. Every answer was undone.

## Figure parity (task 5.5)

`scripts/verify/markas/tinjau-parity.mjs` on the parity seed, before (`c6c1b1a` build) and after (`f689171` build) in
the same minutes, opening every queue item by its deep link and reading the item pane's amounts, dates, times and
counts (`…/markas-tinjau/parity/tinjau-parity.json`), then the folder counts and the hero:

| Item (folder) | Before → after values | Changed values | Explanation |
| --- | --- | --- | --- |
| question (apa) | 4 → 4 | none | |
| group (jawab) | 12 → 10 | none | the latest row's date and time no longer repeat in the head (they stay on the row) |
| automatic group (otomatis) | 12 → 10 | none | same |
| awaiting with top-up, awaiting (pindah) | 4 → 4, 3 → 3 | none | |
| ambiguous (pindah) | 12 → 13 | "3" added | the candidate count in the meta line ("3 kemungkinan pasangan"), the number of candidates listed |
| seen twice (dua), skipped (lewat) | 6 → 9 | kept leg "Rp 500.000" → "+Rp 500.000" | same amount, now signed by direction on the kept-leg row (SPEC row signs); the report's amount also shows in the meta line and its row |
| suggestion (saran) | 12 → 11 | "08.05" removed | the head's latest time; the range and rows keep every date |

- Every money amount, date and count is unchanged in value: **0 changed values**. Folder counts before (chips and
  folds) and after (rail): Semua 6, Belum dikenali 1, Perlu jawaban 2, Pindah saldo belum lengkap 3, Dicatat otomatis 2,
  Saran (was "Saran dari riwayat") 3, Terlihat dua kali 2, Dilewati otomatis 1; hero 6 before and after, equal to the
  badge. Removed figures: the done state's Pengeluaran, Aman dipakai and Belum diklasifikasi (D14; on Beranda).
- **English against Indonesian** (region ID, same data): every item's figures are byte-identical except one extraction
  artefact (the Indonesian question "untuk 3 transaksi lama" matches the count pattern, the English "3 older
  transactions" does not); hero 6 and the eight folder counts are identical.

## Stress and USD (task 5.6)

The audit seed with `seed_tinjau.py --kinds --stress` (`raka_contoh`: a 250-row group of a 60-character merchant, 1,050
more waiting rows, Rp 1.234.567.890), then the same with the USD display, each in both languages:

| Set | Captures | Tinjau checklist | axe | Console | Overflow |
| --- | --- | --- | --- | --- | --- |
| Stress, Indonesian / English (1440, 1024, 390, 320 × 2 themes) | 16 / 16 | 328 / 328 each | 0 | 0 | 0 |
| USD, Indonesian / English | 16 / 16 | 328 / 328 each | 0 | 0 | 0 |

- The queue is truncated: the hero reads 1.303 with "Hanya 1.000 transaksi terbaru yang ditampilkan…"; long merchants
  and Rp 1.234.567.890 ellipsize their titles without clipping or overlap; nothing scrolls sideways at 320 px.
- Through the page, "Terima semua (250)" sent **2** `/review/bulk` requests (200 + 50) and showed "… 250 transaksi
  ditandai sudah ditinjau"; Z sent 2 `/review/undo` requests and restored the group ("Dibatalkan. … kembali ke Perlu
  jawaban.").
- Composed images of the stress and USD sets are not made; the captures are in `shots/final-stress-*` and
  `shots/final-usd-*`.

## Copy (tasks 3.1 to 3.4)

- Catalogues: `review` with 288 messages in each language and the seven Tinjau `errors` codes; the parity test passes
  (same keys and arguments; English counts with `one`/`other`). `npm run type-check` fails on a planted
  `t("review.states.emptyFolder.missing")` (TS2345 naming the key) and passes once it is removed.
- Copy register: `components/review`, `app/review` and the four Tinjau libs are translated roots with no literal copy;
  the spending-term rule now covers `review.*` in both languages, and the planted fixtures fail as required (Indonesian
  "Belanja" label, English "Expenses" label, English "your"). 14 of 14 register tests pass.
- Indonesian byte check: each of the 295 Indonesian messages (288 `review` + 7 `errors`) was searched, with its
  arguments as wildcards, in the Tinjau sources at `f1ee604` (components, libs, `untranslatedPages.ts`, `provenance.ts`).
  248 are found as they were. The 47 others are:
  - **intentional new or changed strings:** "Tindakan lain untuk Tinjau", "Bantuan & pintasan" (shell term in the menu),
    the hero line "{unreviewed} dicatat otomatis, opsional" (was "· opsional"), the failure detail, "Cek yang dicatat
    otomatis" (was "Cek yang otomatis"), "Pilih folder lain.", "Kembali ke daftar", the Dilewati caption with "30 hari
    terakhir." joined, the short kinds "Pindah belum lengkap" and "Dua kali", "Detail", "Sebelumnya"/"Berikutnya" (were
    "Item sebelumnya/berikutnya"), "Teks notifikasi:" (was "Tertulis di notifikasi"), "Top up investasi", "Kemungkinan
    tercatat dua kali", the D9 questions (group, automatic, suggestion, seen twice, skipped), "Ya, top up {product}"
    (was "Iya"), "Transaksi · {n}", "Sudah tercatat · {place}", "Catat sebagai pengeluaran", "Diingat untuk transaksi
    berikutnya dari {label}." (was "Pilihan ini juga diingat …"), "… dengan teks yang sama.", "Daftar kategori gagal
    dimuat.", and "Saran" in the run messages (was "Saran dari riwayat"); the done state's figures and "Kembali ke
    Beranda" are gone (D14), and the dialogs' buttons read "Simpan jawaban" (were "Simpan untuk {n}" and "Simpan sebagai
    pengeluaran");
  - **the same text composed differently** (the old code joined fragments with template strings, so the search cannot
    match them): the toasts ("Tersimpan: … Berikutnya: …", the group and suggestion outcomes, partial failures),
    "produk ini", the unit-fund note, the row name and the last-run line with its rejected count.
- English was checked against Appendix B: "Review", "Needs review", "Transfer not complete", "Spending" only, no
  "Expense", "Spent", "Outflow", "ledger" or "you" (catalogue register, and a page test on the English page).

## Frontend and repository checks (task 5.7)

On `f689171` (the frontend equals `64be549`'s; `138005b` brought main's credit work):
- `npm test`: **94 files, 929 tests passed** (new or rewritten: `app/review/page.test.tsx` 61 cases covering every
  scenario of the deltas, `Answers.test.tsx` 5, `HistorySuggestionsDialog.test.tsx` 5, `primitives.test.tsx` +6, `Icon`,
  `format`, `reviewUndo`, `review`, `historySuggestions`, `copyRegister`, `selectionMarks`, `workspace` updates).
- `npm run type-check` passed; `npm run lint` passed; `npm run build` passed (production, with and without the
  component sheet flag).
- Backend `pytest` on a fresh postgres:16 migrated V1 to V54 by flyway/flyway:10: **2163 passed** (also 2097 passed on
  the pre-merge revision at V53). `test_seed_parity.py`: 11 passed.
- `openspec validate --all --strict`: **85 passed, 0 failed**. `git diff --check origin/main HEAD`: clean.
  `git diff --stat origin/main -- backend db mobile nginx`: empty.
- No package file changed, so no lockfile step was needed.

## Deviations and notes for the owner's end review

The proposal's nine recorded deviations hold (folder membership, "Dicatat otomatis" kept, skipped = link skips, the
top-up detail line without a checkbox, one request per dialog, the question as heading, two-step history run, no amount
in the hero line, "Saran" in the rail and "Saran dari riwayat" as the kind). Further, within the design's latitude or
by the owner's rules:
1. **Expense amounts are red** (`--out-fg`, owner 2026-11-06) in the list, meta and rows, where D6 says ink; the shared
   red amount component arrives in phase 5.
2. **T on a learned top-up** presses "Ya, top up …" (D9 table: E, T). Before, T only opened "Ini top up investasi" and
   did nothing while a suggestion was shown; it now sends the same conversion request as 1 and E.
3. "Ubah di Transaksi" stays as a quiet link under the answers of awaiting and ambiguous legs (not listed in D9 or
   D20); without it those legs would have no path to the transaction besides the failure line.
4. The top-up suggestion's own prompt ("Sepertinya top up ke …") is not shown when a product is learned (D9 fixes the
   question); its explanation is the answer's detail. When the server cannot pick a product, its prompt is the question
   (the fixed question while balances are hidden) and the explanation a note line.
5. The awaiting leg's stored notes stay as a muted note line under the warning.
6. Toasts after a "stay" outcome now end with a period ("Tersimpan: bukan top up. Pilih rekening pasangannya.").
7. The seven Tinjau error codes already existed in `errors` from localize-server-text with other wording; their
   Indonesian is now Tinjau's displayed text from `lib/review.ts` (byte for byte) and the English follows it.
8. English: `notice.linkDone` is "The linked item is already done." (Appendix A's "The item you opened…" breaks the
   no-"you" register); `answer.unitFunds` reads "Still tracked by units: {funds}. …".
9. The category dialog's pillar control has six options and wraps to two lines at 560 px (the prototype's has five).
10. On the phone, no row is marked current until an item opens, as in the prototype.
11. Server text on the seeds stays as stored: the parity seed's question prompt is Indonesian in the English account.
12. The rail keeps "Dicatat otomatis" hidden when it has no group of only automatic rows, even if mixed groups hold
    automatic rows (the folder-hiding rule of D5).

Kinds without a prototype screen, for the end review: "Belum dikenali" income, "Dicatat otomatis", "Saran", ambiguous
legs, the awaiting leg without a suggestion, the dialogs "Ganti nama", "Catat sebagai pengeluaran" and "Jawab sendiri",
the loading and failure states (the prototype's generic ones).

## Not verified

- Zoom to 400 % and a real screen reader (VoiceOver, NVDA); the 320 px captures cover the reflow, and the live region
  of the status line is checked in jsdom only.
- The `/komponen` sheet's new Tinjau section (rail, chips, stacked rows, answers) is built and covered by the component
  tests and axe there; it was not captured in this gate.
- A real device: the phone captures are the browser's device mode at 390 × 844 and 320 × 844.
- The prototype has no screen for several kinds (listed above) and only its generic loading, failure and empty blocks;
  those were checked against SPEC §7 and the design.

## Owner acceptance

Accepted under the owner's standing instruction of 2026-11-06, renewed 2026-11-08: "complete all until finished" and
"continue all". No separate review of this phase was requested before archiving; the deviations above stay on the
end-review list.
