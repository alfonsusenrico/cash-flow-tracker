## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. On Tinjau the owner said: "tinjau page also still not organized properly, not aligned, feels scrambled".

The audit (`design-options/audit/AUDIT.md` §Tinjau) measured why:
- four rounded folder chips wrap over three lines, and four optional `<details>` folds sit under the list;
- a "Cari saran dari riwayat" button and a permanent key hint strip crowd the list's footer;
- 12 font sizes and 8 weights on a page that shows one item, a 24 px title and a 32 px amount competing in the item pane;
- the list and the item pane start and end at different heights;
- the selected row used a pink fill (since phase 1, a straight bar on `--surface-2`).

Phases 1 to 3 (foundation, shell, Beranda) are merged at `f1ee604`. `add-language-system` and `localize-server-text` land before this phase, so English is the default interface language and every page rebuilt from phase 4 on ships English and Indonesian.

The phased plan is `cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md` (phase 4 of twelve). The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/pages.js` `PAGES.tinjau`, `js/dialogs.js` (`ubah-kategori`, `saran-riwayat`, the `tinjau` menu), `SPEC.md` §6 to §8, §11 and §13, and the captures `tinjau-*`, `state-tinjau-*`, `dlg-ubah-kategori-*` and `dlg-saran-riwayat-*`.

## Expected Outcome

- At 1440 px Tinjau reads top to bottom as in the prototype:
  - the title "Tinjau" (English "Review") with its subtitle and one "…" button that holds "Cari saran dari riwayat" and "Bantuan & pintasan";
  - one hero "Perlu ditinjau" with the count in 44 px (the same number as the sidebar badge) and one muted line with how many rows were recorded automatically and are optional;
  - three aligned panes, 232 · 336 · 1fr with 24 px gaps, that start on the same top edge:
    - a folder rail under a "FOLDER" label: Semua and the needed folders with right-aligned muted counts, then a rule and the optional folders; folders without items are hidden;
    - the item list: whole-row items, a 40 px icon column, title, a short kind and time line, and the amount;
    - the item: the kind label with "1 dari 6" and ‹ ›, the question in 16 px, one meta line, numbered 56 px answer rows with key caps 1–9, the rows each kind needs, and the notification text in a quiet box.
- "Ubah kategori", "Ganti nama", "Jawab sendiri", "Bukan pindah saldo, ini pengeluaran" and "Cari saran dari riwayat" open centered dialogs (bottom sheets on a phone). The answers send exactly the requests they send today.
- Between 768 and 1199 px the folder rail and the list sit side by side and the item follows below them at full width.
- On a phone, the folders are one sideways-scrolling line of chips over the list, and an item opens in a full-height sheet. The browser's back, Esc and the close button return to the list, with focus on that item's row.
- J, K, E, T, 1–9 and Z, auto-advance, Urungkan, deep links and every review action work exactly as today. The keys are listed in "Bantuan & pintasan", and only numbered answers show key caps.
- When nothing waits, the page shows "Semua sudah ditinjau" with one line and, when automatic rows exist, "Cek yang dicatat otomatis".
- An English account sees Tinjau in plain English ("Review", "Needs review", "Spending", "Transfer not complete"). An Indonesian account sees Indonesian, with neutral firm labels and "Pengeluaran" for spending. Tinjau no longer shows "This page is in Indonesian for now."
- With "Sembunyikan saldo" every Tinjau amount reads "Rp ••••••" and is announced as hidden.
- Every count, amount and date on Tinjau equals today's value for the same data, in both languages.

## Approval

Approved 2026-11-06 under the owner's standing instruction "complete all until finished"; decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D23, summarized in "Decisions and their defaults"). No default changes money behaviour: every figure is a server field shown as today, and every action sends today's request. One decision is flagged and is reviewed before implementation starts (task 1.1):
- **D14 (scope beyond the approved behaviour):** "Semua sudah ditinjau" drops the three cycle figures (Pengeluaran, Aman dipakai sampai gajian, Belum diklasifikasi) that the "Semua Beres" requirement shows today. They stay on Beranda, one click away. Fallback if rejected: keep them as one quiet line of three small figures under the empty state.

One rejected alternative would have changed money presentation and is recorded so it is not reopened by accident: the prototype's "Rp 190.500 menunggu jawaban" in the hero line (D3). The approved `redesign-review-triage` design rejected it on 2026-11-03, because an all-time count cannot be paired with the cycle-scoped waiting total, and the "Triage Folders and Counts" requirement keeps "no amount".

## Deviations from the prototype

Each is a decision in `design.md` and is captured for the owner's end review:
1. Folder membership stays the app's (D4). "Saran" and "Terlihat dua kali" are optional folders under the rule, as today, and are not counted in "Semua", so the hero, "Semua" and the sidebar badge keep reading `attention`. The prototype counts them in "Semua".
2. "Dicatat otomatis" stays as an optional folder (D4); the prototype has none.
3. "Dilewati otomatis" lists the reports an account link skipped, as today (D4). The prototype lists promotions and codes.
4. The top-up answer keeps "Diingat untuk debit seperti ini" as its detail line, without the prototype's checkbox, because rejecting a suggestion is always remembered (D9).
5. "Ubah kategori" and "Ganti nama" are two modes of one dialog that send one request each, without the prototype's "Ingat untuk transaksi berikutnya" checkbox (D10).
6. The question is the item's heading and focus target; the kind label above it is not a heading (D7).
7. "Cari saran dari riwayat" keeps its two steps, "Hitung perkiraan" then "Mulai buat saran", because a run must be estimated first (D12).
8. The hero line shows no amount (D3). It reads "{unreviewed} dicatat otomatis, opsional", today's figure, where the prototype reads "4 transaksi · Rp 190.500 menunggu jawaban · 2 dilewati otomatis, opsional".
9. The rail says "Saran" as in the prototype; the item's kind label says "Saran dari riwayat" (D5).

## What Changes

- **Title block:** "Tinjau" with its subtitle and one "…" menu button, `role="menu"`: "Cari saran dari riwayat" and "Bantuan & pintasan" (D2).
- **Hero:** "Perlu ditinjau", `attention` in Manrope 44, and the line "{unreviewed} dicatat otomatis, opsional" (D3). It is hidden in the loading and failure states and when nothing at all waits.
- **Panes:** `232px 336px minmax(0,1fr)`, gap 24, `align-items: start`. The list has a 1 px right line, and the item is at most 880 px wide. From 768 to 1199 px: `184px minmax(0,1fr)` with the item below. Below 768 px: chips, the list, and a sheet (D1, D5, D13).
- **Folder rail** (SPEC "Rail item"): 36 px items, the count right, and the straight 2 × 20 px bar 8 px left of the current fill. "Semua", "Belum dikenali", "Perlu jawaban" and "Pindah saldo belum lengkap", then a rule, then "Dicatat otomatis", "Saran", "Terlihat dua kali" and "Dilewati otomatis". Empty folders are hidden, except "Semua" and the current folder. Phone chips: 44 px, radius 6, 1 px line; the current chip has the `--surface-2` fill and the strong line, with no bar (D5).
- **Item list:** the shared list row in a stacked variant: min-h 64, padding 12, the icon column 40, title, meta (in "Semua", short kind · date and time; elsewhere date and time), and the amount with SPEC signs and colours. The selected row has the `--surface-2` fill and the 2 px bar (D6).
- **Item pane:** the kind label (12/500 uppercase), "N dari M" with ‹ › icon buttons, the question (16/400, `h2`, focus target), the meta line (sub · amount · date and time · account), an optional warning line, the reasons as one muted line, numbered answers, per-kind rows (a group's transactions, the seen-twice pair) and the notification text box (D7 to D9).
- **Dialogs** on the shared shell dialog:
  - "Ubah kategori" / "Ganti nama" / "Catat sebagai pengeluaran", 560 px (D10);
  - "Jawab sendiri", 520 px (D11);
  - "Cari saran dari riwayat", 520 px, with the period, estimate, progress and last run (D12).
- **Phone sheet:** the item in a full-height sheet titled by its kind, with a history entry; keys work inside it (D13, D16).
- **States:** skeleton rows (`aria-busy`), the SPEC error box with "Coba lagi", "Folder ini sudah kosong." in an empty folder, and the page empty state "Semua sudah ditinjau" (D14, D15).
- **Language:**
  - a `review` namespace in `messages/en.json` and `id.json` (copy in design Appendix A);
  - Tinjau's error codes in `errors`;
  - dates, amounts and counts through `lib/format.ts`;
  - `/review` added to the translated routes, and Tinjau's roots added to the copy register's translated roots (D18).
- **Shared pieces extended or built here for later phases:**
  - the list row's stacked variant;
  - `HeroFigure` without a colour mark;
  - the rail and rail item (Transaksi's JENIS/PERIODE in phase 5, Pengaturan in phase 10);
  - the phone chip line;
  - the numbered answer list and key cap;
  - the full-height sheet variant of the shell dialog;
  - the `archive` icon (D19).
- **Removed** (each with its destination in D20):
  - the folder chips and `ChipGroup`, whose only user was Tinjau;
  - the four `<details>` folds;
  - the list footer with the "Cari saran dari riwayat" button and the key hint strip;
  - the in-pane run panel (`RUN_PANEL_ID`);
  - the pane bar with "Daftar" and the K/J key caps;
  - the 720 px main-width one-pane rule (`TWO_PANE_MIN`, Tinjau's `useWidthAtMost` use);
  - `ItemHead` with its kind pill, 24 px title, 32 px amount, provenance line and reason pills;
  - the bordered two-column choice cards and `.ws-picker` boxes;
  - the inline category, rename and text-answer forms;
  - the round avatars;
  - the three cycle figures and the "Kembali ke Beranda" link of the done state;
  - the `ws-tri-*`, `ws-choice*`, `ws-picker`, `ws-facts` and `ws-tri-fold` rules.
- **BREAKING (spec):** `review-queue` "Tinjau Sections by Need", "Triage Folders and Counts", "One Item at a Time", "Semua Beres", "Phone List and Item", "Seen-Twice Section", "Skipped by a Linked Account" and "Question Row Direction" change their presentation. `history-suggestions` "Saran Perbaikan Riwayat Section" and its period and progress requirements move from a pane section to a dialog opened from "…".

## Non-goals

- No change to any API, request, response, figure, count, money rule, schema, data or the Android companion. `backend/`, `db/` and `mobile/` are untouched. Server text (purpose and top-up prompts, suggestion reasons, error messages) is shown as the server sends it in the account's language (`localize-server-text`).
- No change to the review semantics: folder membership, `attention` and `unreviewed`, auto-advance, Urungkan, deep links (`?item=&folder=` and the legacy hashes) and the keys stay as today.
- No other page body. The transaction dialog that group rows open is Transaksi's (phase 5), and the Bantuan dialog is the shell's.
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Held branch `fix/selection-bars-and-tinjau` (`64909ee`)

This phase absorbs the branch's Tinjau parts. Its other parts went to phase 1 or wait for phase 8 (design D21):
- **Taken:**
  - one vertical folder list instead of chips and folds, with the optional folders in the same list;
  - each optional folder's caption shown when it is chosen;
  - the deep-link and undo paths without fold state;
  - the reworked `review-queue` wording for "Tinjau Sections by Need" and "Triage Folders and Counts";
  - its folder tests, adapted.
- **Superseded:**
  - the "Opsional" label (the prototype's rule instead);
  - equal-height inner-scrolling panes (prototype panes are top-aligned and the page scrolls);
  - "Cari saran dari riwayat" as the list's last row (now in "…");
  - the one-line key hints (removed; key caps on numbered answers and Bantuan);
  - the 720 px main-width phone folder line (now the 768 px breakpoint).
- **Not here:** the straight selection bar and its guard are already on `main` from phase 1. Tagihan off Rekening and the Grafik labels and rails are phase 8.
- The branch is never merged. It and its worktree `cash-flow-tracker-ui-refine` are deleted after phases 4 and 8 have merged. The deletion is recorded in `PROJECT_STATE.md` as an open item for after phase 8.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `review-queue`:
  - the page in the interface language;
  - folders as a rail with a rule;
  - three panes and the 768/1200 px breakpoints;
  - the item pane and numbered answers;
  - the phone sheet;
  - the empty, loading and failure states;
  - the question row's meta line;
  - the optional seen-twice and skipped folders.
  - Added: the hero, the page's "…" actions, the category and name dialog, the text-answer dialog, and the language behaviour of Tinjau.
- `history-suggestions`: the run controls move from a pane section into the "Cari saran dari riwayat" dialog opened from "…", and the folder is labelled "Saran".

`interface-language` and `ui-copy-register` (from `add-language-system`) need no delta: adding `/review` to the translated routes follows their existing rule that the set of untranslated pages only shrinks.

Standards:
- UI and accessibility (WCAG 2.2 AA): landmarks and headings, focus order and focus return through dialogs and the sheet, focus not obscured by the sticky top bar, target size on the phone chips and answers, reflow at 320 px, non-colour cues on amounts and folders, reduced motion, 3.1.1 and 3.1.2 language of page and parts;
- the copy register in both languages (`ui-copy-register`);
- `money`: figures identical before and after, and in English and Indonesian;
- `personal-data`: no rendered text or user data in logs.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Tinjau:**
  - `components/review/triage/TriageInbox.tsx` (split into page, panes and detail), `TriageList.tsx` (rail and list), `Choice.tsx` (answer rows);
  - `ItemHead.tsx` (replaced by the item head);
  - `components/review/PurposeQuestionCard.tsx`, `ReviewGroupCard.tsx`, `MovementReviewCards.tsx`, `SeenTwiceCard.tsx`, `SuggestionGroupCard.tsx` and `HistorySuggestionsSection.tsx` (into the dialog);
  - `lib/triage.ts` (data only, labels move to the catalogue), `lib/review.ts` (reason and kind labels to keys, `dateRangeLabel` to the formatter), `lib/reviewUndo.ts` and `lib/historySuggestions.ts` (messages to keys);
  - `app/review/page.tsx` and `app/review/page.test.tsx`;
  - `components/review/*.test.tsx`.
- **Shared:**
  - `components/workspace/lists.tsx` (stacked row), `figures.tsx` (optional mark), `ShellDialog.tsx` (full-height variant), `primitives.tsx` (`ChipGroup` removed);
  - `components/ui/Icon.tsx` (`archive`);
  - `app/globals.css`: rail, chips, answers, raw box and the `.tj` grid added; the Tinjau-only `ws-*` rules removed.
- **Language:** `messages/en.json` and `id.json` (`review`, `errors` codes), `lib/i18n/translatedRoutes.ts` (`/review`), the copy register roots, and the untranslated-page test of `add-language-system`, which moves its example from Tinjau to Transaksi.
- **Tooling (local only):** `scripts/verify/markas/`:
  - the Tinjau scenarios in both scenario files;
  - a Tinjau checklist for `measure.js`;
  - the keyboard flow `tinjau-keys.mjs`;
  - the parity seed extended with the review kinds it skips today, where this needs no double booking;
  - a stress and USD seed for Tinjau.
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit.
