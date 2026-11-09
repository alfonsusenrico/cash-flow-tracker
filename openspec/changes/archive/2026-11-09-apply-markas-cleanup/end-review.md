# Markas+ end-review list

The owner's review list for the whole redesign (phases 4 to 11, with the phase 1 to 3 items those phases marked for
this review). Compiled on 2026-11-09 from the archived changes in `openspec/changes/archive/` at `10cd7e5`; every line
names its source ([proposal dev. N], [proposal flag Dn], [design …], [verification dev. N], [verification …]).
Phases 8 and 9 record their acceptance as "pending the owner's end review"; phases 10 and 11 have no acceptance
section. Phase 12 (`apply-markas-cleanup`) adds its own deviations to the last section after its gate.

Count notes: phase 6's proposal says "four decisions are flagged" but lists five (the fifth is D31, the owner's "one
basis" decision of 2026-11-08); phase 8's verification does not restate its proposal's thirteen deviations, so they
are listed from the proposal. Every other stated count matches.

## Checks that need the owner

- Scan both QR codes with a real phone: the pairing QR (Pengaturan › Perangkat, 200 css px, 4.88 px per module) and
  the download QR (136 css px of code in a 154 px box, 3.02 px per module, the D9 fallback). [phase 10 verification]
- Use Catat on a real phone; every phone check so far ran in the browser's device mode. [phase 11 verification]
- Not verified in any phase: 200 % and 400 % browser zoom, and a real screen reader (VoiceOver, NVDA). The 320 px
  captures cover reflow. [phases 4 to 11, "Not verified"]

## Changed by phase 12

- Phase 5 further note 1 (Kredit hero vs provider header): the header now reads the list's own window, so both
  agree for a purchase timed later today (`apply-markas-cleanup` D9).
- Phase 6 "Not verified" ("Sept" vs "Sep"): English short months read "Sep" everywhere (D11).
- Phase 7 follow-up ("Rincian tabungan" cannot add up): Laporan lists "Pengeluaran dari tabungan" (D8).
- Phase 11 remaining risk (default Dari/Ke refused as `same_account`): the default Ke can no longer be Dari's account
  (D13).
- Phase 11 remaining risk (`credit_terms` count 1 stored by Catat): the server stores no row for it, the same as a
  notification purchase; no figure differs (D14, regression test added).
- Phase 9 axe `target-size` behind the phone sheet: re-verified after phase 11's modal Catat, 0 at 390 and 320 in
  both languages (D12).
- The `/komponen` sheet and the "This page is in Indonesian for now." path are gone (D1, D2), so the phase 4, 7 and
  11 notes about sheet sections not captured or not extended no longer apply.
- Phase 5 further note 2 (count-only edit re-derives a kept first due date): the owner chose option A on 2026-11-09;
  a count-only edit now sends the kept date back and it stays set by the system (F1).

## Phase 4 — Tinjau (2026-11-08-apply-markas-tinjau)

Flags and recorded alternatives:
- "Semua sudah ditinjau" drops the three cycle figures and Tinjau no longer requests `/dashboard/cycle-summary`; fallback (one quiet line of three figures) not built. [proposal flag D14]
- Prototype's "Rp 190.500 menunggu jawaban" in the hero line stays rejected (2026-11-03 decision), recorded so it is not reopened. [proposal Approval, D3]

Deviations from the prototype:
1. Folder membership stays the app's: "Saran" and "Terlihat dua kali" are optional and not counted in "Semua"; hero, "Semua" and badge read `attention`. [proposal dev. 1]
2. "Dicatat otomatis" stays as an optional folder; the prototype has none. [proposal dev. 2]
3. "Dilewati otomatis" lists reports an account link skipped, not promotions and codes. [proposal dev. 3]
4. Top-up answer keeps "Diingat untuk debit seperti ini" as detail line, without the prototype's checkbox. [proposal dev. 4]
5. "Ubah kategori" and "Ganti nama" are two modes of one dialog, one request each, without "Ingat untuk transaksi berikutnya". [proposal dev. 5]
6. The question is the item heading and focus target; the kind label above is not a heading. [proposal dev. 6]
7. "Cari saran dari riwayat" keeps two steps, "Hitung perkiraan" then "Mulai buat saran". [proposal dev. 7]
8. Hero line shows no amount: "{unreviewed} dicatat otomatis, opsional". [proposal dev. 8]
9. Rail says "Saran"; the item's kind label says "Saran dari riwayat". [proposal dev. 9]

Further notes from verification:
1. Expense amounts are red (`--out-fg`, owner 2026-11-06) where D6 says ink. [verification dev. 1]
2. T on a learned top-up now presses "Ya, top up …" and sends the same conversion request as 1 and E. [verification dev. 2]
3. "Ubah di Transaksi" stays as a quiet link under awaiting and ambiguous legs (not in D9 or D20). [verification dev. 3]
4. Top-up suggestion prompt "Sepertinya top up ke …" hidden when a product is learned; shown as the question when the server cannot pick one. [verification dev. 4]
5. Awaiting leg's stored notes stay as a muted note line under the warning. [verification dev. 5]
6. "Stay" outcome toasts now end with a period. [verification dev. 6]
7. The seven Tinjau error codes show Tinjau's displayed text (from `lib/review.ts`), not localize-server-text's wording. [verification dev. 7]
8. English `notice.linkDone` is "The linked item is already done."; `answer.unitFunds` reads "Still tracked by units: {funds}. …". [verification dev. 8]
9. Category dialog pillar control has six options and wraps to two lines at 560 px (prototype: five). [verification dev. 9]
10. On the phone no row is marked current until an item opens, as in the prototype. [verification dev. 10]
11. Server text on seeds stays as stored: parity seed's question prompt is Indonesian in the English account. [verification dev. 11]
12. Rail hides "Dicatat otomatis" when no group holds only automatic rows, even if mixed groups do. [verification dev. 12]
13. Kinds without a prototype screen: "Belum dikenali" income, "Dicatat otomatis", "Saran", ambiguous legs, awaiting leg without suggestion, three dialogs, loading and failure states. [verification, kinds note]

Not verified there:
- Zoom to 400 % and a real screen reader (VoiceOver, NVDA); status-line live region checked in jsdom only.
- `/komponen` Tinjau section built and unit-tested, not captured.
- A real device (browser device mode 390 × 844 and 320 × 844 only).
- Kinds without a prototype screen checked against SPEC §7 and the design only.

## Phase 5 — Transaksi (2026-11-08-apply-markas-transaksi)

Flags (ten items in seven decisions, decided 2026-11-06, all defaults; D27 labels tightened):
- Rows gain read-only `saving_flow` from the shared Kakeibo predicates (backend addition). [proposal flag D27]
- Saving labels only where Beranda's "Ditabung & investasi" changes: into `dana_investasi` and `tabungan`-to-product top-ups show "—"; "Bayar utang" stays informational. [proposal flag D27]
- `GET /transactions` gains `type=saving`, `type_counts.saving` and `saving_net` (backend addition). [proposal flag D28]
- "Tabungan & investasi" filter lists all four parts of Beranda's figure, so rows add up. [proposal flag D28]
- "Rutin" leaves Transaksi's "…" menu; plans reachable from Target & Tagihan, bell, Beranda. [proposal flag D2]
- Four single-select chips become independent Status, Sumber and "Menunggu jawaban" controls in Filter. [proposal flag D8]
- Filter dialog keeps "Periode" at every width, though the plan listed it as removed. [proposal flag D8]
- No "Kemungkinan duplikat" row badge (no row flag exists); `dup=1` stays a chip. [proposal flag D11]
- Delete has no "Urungkan" undo, correcting the plan; restoring a row is a new money operation. [proposal flag D14]
- Prototype's hero arithmetic (all expenses, income less that) stays rejected; server `spending` and `spending_net` kept. [proposal flag D4]

Deviations from the prototype:
1. From 1024 to 1199 px the list is full width with a type control instead of the 200 px rail. [proposal dev. 1]
2. Hero keeps one muted qualifier line, such as "sudah dikurangi penggantian Rp 250.000". [proposal dev. 2]
3. Loading state shows skeleton figures, since figures arrive with the rows. [proposal dev. 3]
4. Period stepper keeps its "Siklus ini" / "Bulan ini" reset. [proposal dev. 4]
5. Filter dialog uses one select each for Rekening, Kategori and Pilar, not checkboxes. [proposal dev. 5]
6. Merge bar stays in view under the top bar while selecting. [proposal dev. 6]
7. Pillar column keeps "Uang masuk" for income and names saving direction for transfers and top-ups, not "Tidak dihitung". [proposal dev. 7]
8. Delete confirmation keeps the list of effects on balances, goals and debts. [proposal dev. 8]
9. Source labels keep "Notifikasi bank", "Diketik di ponsel", not "Ditangkap dari HP". [proposal dev. 9]
10. Movement "Status pasangan" says "Lengkap" or "Pasangan belum ditemukan", not the prototype's money claim. [proposal dev. 10]
11. Top-up title is "Top up {product}" (Transaksi and Beranda), "Top up Investasi" when unknown. [proposal dev. 11]
12. Phone rows keep long-press to start a merge, besides "…" › "Gabungkan dua transaksi". [proposal dev. 12]
13. Transaction dialog is 560 px as in `dialogs.js`, not the plan's 480 px. [proposal dev. 13]
14. Owner-directed: expense amounts red (`--out-fg`) on Transaksi, the dialog and Beranda. [proposal dev. 14]
15. Owner-directed: a transfer is titled by its note, with the route as meta. [proposal dev. 15]
16. Owner-directed: sixth type "Tabungan & investasi" with hero "Ditabung & investasi". [proposal dev. 16]
17. Pillar column 136 px instead of 112 on Transaksi and Beranda. [proposal dev. 17]

Further notes from verification:
1. Credit (H5): credit purchases are red, not H5's ink; "Kredit" hero (ledger summary) can differ from the provider header's "Pengeluaran pakai kredit". [verification dev. 1]
2. Edit keeps an owner-set first due date; a system-kept date is not shown, so a count-only change still sends `first_due_date: null`. [verification dev. 2]
3. `lib/movementFormSchema.ts` kept Indonesian messages for Catat; the movement dialog maps issues to keys. [verification dev. 3]
4. Filter button and "…" item use the `sliders` icon (no `filter` icon). [verification dev. 4]
5. A scrolling dialog body without controls is a tab stop (`ShellDialog scrollFocusable`). [verification dev. 5]
6. Dead Analisis rules `.cmpName`, `.bench` read `--k-none` instead of undeclared `--kc`. [verification dev. 6]
7. English rail labels such as "Savings & investments" ellipsize in the 200 px rail at 1440. [verification dev. 7]
8. `TodoSection` plan amounts stay plain; only "−Rp" spending is red. [verification dev. 8]
9. Shared-part changes on other pages: stepper on Analisis and Laporan, red `RowAmount` on Beranda and Rekening detail, transaction dialog on Beranda, Kalender, Peta Aliran. [verification For PROJECT_STATE.md]
10. Branch first stacked on phase 4 (`feat/markas-tinjau`), then updated from `origin/main`. [proposal H5, D26 amended]

Not verified there:
- Zoom to 400 % and a real screen reader.
- Composed side-by-side images (captures exist separately).
- "Belum ada transaksi", "Nominal tidak sama" and the top-up conversion: unit tests only.
- Pixel compare of Beranda before and after (page tests only).
- A real device (browser device mode only).

## Phase 6 — Analisis (2026-11-09-apply-markas-analisis)

Count note: the proposal says "Four decisions are flagged" but lists five bullets; the fifth, D31, is the owner decision of 2026-11-08 added during implementation.

Flags and owner decision:
- `GET /reports/monthly/comparison` gains read-only `progress.baseline_share`. [proposal flag D5]
- While running, the last completed period's per-pillar comparison leaves the first screen; shown when stepping back. [proposal flag D6]
- Flow-map fold "Tampilkan uang masuk, saldo dan Masih ada" removed; opening and "Masih ada" links never drawn on the map. [proposal flag D15]
- Owner decision "one basis": every "Pengeluaran" on Analisis uses new read-only Pengeluaran-basis fields; Belanja-basis fields unchanged. [proposal D31]
- "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" not shown on Analisis (no period field). [proposal flag D29]
- Untranslated-page test example moved to Pengaturan (`/settings`), not Laporan (coordinator 2026-11-09). [verification Approval]

Deviations from the prototype:
1. Stepper keeps phase 5's label and reset ("Siklus 23 Okt – 24 Nov", "Siklus ini"). [proposal dev. 1]
2. Tabs stay an ARIA tab list with `?tab=`, not hash-route links. [proposal dev. 2]
3. "Baca Laporan Bulanan" opens the latest closed report, not a fixed month. [proposal dev. 3]
4. Hero keeps one "Rincian" for the server's Pengeluaran lines. [proposal dev. 4]
5. Completed period's comparison rows read as changes with ▲ or ▼. [proposal dev. 5]
6. Kakeibo rows are whole-row ledger links; "Belum diklasifikasi" keeps "Perlu dipilih". [proposal dev. 6]
7. Daily chart drawn at real width, no "0" tick, no overlapping dates, legend swatch for today's hatch. [proposal dev. 7]
8. "Tanpa kategori" and archived categories with spending stay in the category list. [proposal dev. 8]
9. "Atur anggaran" opens the shared budget dialog, not Pengaturan. [proposal dev. 9]
10. "Kelola kategori" edits and adds inside one dialog and lists archived ones with "Pulihkan". [proposal dev. 10]
11. Bills panel's second button reads "Lihat semua" (one "Rincian" per page). [proposal dev. 11]
12. Calendar strip opacity from the server's heat level 1–5; legend names lowest and highest thresholds. [proposal dev. 12]
13. Phone shows the whole calendar grid instead of the week strip. [proposal dev. 13]
14. Flow map keeps link patterns for transfers, investments, waiting links and adds a legend. [proposal dev. 14]
15. Flow table keeps its "Transaksi" column from 768 px. [proposal dev. 15]
16. Phone segments and small buttons are 40 px instead of 26 or 28 px. [proposal dev. 16]

Further notes from verification:
1. Running cycle reads "Siklus sejak 27 Okt" / "Cycle since 27 Oct" (no end date in the periods list). [verification dev. 1]
2. Prototype `--m-*` mark tokens are the app's `--*-solid` tokens. [verification dev. 2]
3. Laporan frame styles moved to `laporan.module.css`; history bars used Indonesian compact units until phase 7. [verification dev. 3]
4. `ChangeFigure` words from `common.change`; "Keluar" and "Keluar ke" allowed in the copy guard. [verification dev. 4]
5. `shopping-cart` icon option is "Keranjang", not "Belanja". [verification dev. 5]
6. Calendar: today in ink without 500 weight, phone amount 12/400; below 360 px cells have 2 px side padding. [verification dev. 6]
7. Waiting marker drawn with a `currentColor` border (no coloured box-shadow allowed). [verification dev. 7]
8. Flow map's 24 px slots let the drawing grow beyond 320 px. [verification dev. 8]
9. Phase 8 `ProgressBar` and phase 6 `Meter` both draw `.prog`, now `display: block`. [verification dev. 9]
10. Categories with only saving-pillar expenses show Rp 0 behind "Tampilkan … kategori tanpa pengeluaran". [verification dev. 10]
11. "Seberapa sering ditanya" moved to Pengaturan › Kebiasaan (old styling until phase 10). [verification For PROJECT_STATE.md]

Not verified there:
- Full "before" capture set (no before screenshots or before axe run kept).
- Composed side-by-side images.
- 200 % and 400 % zoom and a real screen reader.
- Empty Ringkasan and empty Peta Aliran: unit tests only.
- English cycle labels read "Sept" (formatter `en-ID`) while server labels read "Sep"; unchanged.
- A real device (browser device mode only).

## Phase 7 — Laporan (2026-11-09-apply-markas-laporan)

Flags and owner decision:
- "Pengeluaran per bulan" becomes one series of bars; the per-month pillar split leaves the chart. [proposal flag D11]
- First note writable from a quiet "Tulis catatan untuk {period}" button; never an empty form on load. [proposal flag D13]
- No credit figures ("Pengeluaran pakai kredit", "Tagihan kredit dibayar") on Laporan. [proposal flag D26]
- One spending basis: report gains additive read-only Pengeluaran-basis fields (top merchants, budget, reimbursement offset, top-merchant fact). [proposal D29]
- Stacked on phase 6's branch and later on phase 9 (`8312696`) by coordinator instruction; stepper gained `monthStyle="long"`. [design Re-based; verification]

Deviations from the prototype:
1. Stepper keeps phase 5's reset "Bulan ini" / "Siklus ini". [proposal dev. 1]
2. Owner rule: hero line compares with both the previous period and the 3-month average. [proposal dev. 2]
3. Running hero line gives shares against previous period and average, with a bar. [proposal dev. 3]
4. Income sources, merchants, budget rows, facts and table rows are whole-row ledger links. [proposal dev. 4]
5. "Rincian tabungan" keeps today's two rows, not one row per destination. [proposal dev. 5]
6. Table keeps "belum lengkap" and the per-pillar shares while running. [proposal dev. 6]
7. "Biaya hidup pokok · saat ini" keeps the method sentence and drops emergency-fund months. [proposal dev. 7]
8. Budget row reads amounts ("Rp 620.000 dari Rp 500.000 · lebih Rp 120.000") besides the bar. [proposal dev. 8]
9. "Atur anggaran" opens phase 6's shared budget dialog. [proposal dev. 9]
10. Month bars proportional in a 112 px area, with legend and table. [proposal dev. 10]
11. Facts are the server's deterministic facts, not three client sentences. [proposal dev. 11]
12. Previous period's note line appears only when that note exists. [proposal dev. 12]
13. "Selisih" shown for completed periods only. [proposal dev. 13]
14. Classification dialog "Lengkapi klasifikasi" uses SPEC dialog parts at 560 px. [proposal dev. 14]

Further notes from verification:
1. Pillar marks are phase 6's 8 px square `PillarDot` (prototype: round dots). [verification dev. 1]
2. No sticky first column at 320 px; two columns fit. [verification dev. 2]
3. Budget rows sorted by what is left on the Pengeluaran basis, most over first. [verification dev. 3]
4. Previous note line uses the server's period label ("Catatan September 2026: …"). [verification dev. 4]
5. Note form label reads 14/500 (prototype's 12/500 would be a ninth text style). [verification dev. 5]
6. Empty-ledger state unit-tested only. [verification dev. 6]
7. Category budgets with all spending in the Tabungan pillar read Rp 0. [verification dev. 7]
8. Beranda checklist passes 440 of 446: its 112 px status column and ink expense checks predate phase 5; not changed. [verification Stacked on phase 9]

Not verified there:
- Full "before" capture set.
- Composed side-by-side images; `/komponen` does not show a question block yet.
- The copy-key list file of task 2.1.
- 200 % and 400 % zoom and a real screen reader (VoiceOver).
- A real device (browser device mode only).

## Phase 8 — Rekening (2026-11-08-apply-markas-rekening)

Count note: the verification does not restate the proposal's thirteen deviations; it lists "the deviations below and the seven flags above" for the end review. All thirteen are listed here from the proposal.

Flags (seven, all defaults):
- Utang lists credit accounts only plus one "Tagihan" row; Utang total becomes `credit_owed` (was `total_liabilities`). [proposal flag D5]
- Per rekening shows composition as words in its header; stacked composition bar dropped. [proposal flag D5]
- Grafik keeps the owner's ring of uses instead of the prototype's per-use bars. [proposal flag D11]
- Drag and Alt + arrows only in "Atur urutan" mode. [proposal flag D12]
- "Bayar tagihan" opens Perpindahan prefilled with `left_to_pay` via new `openMovementCapture`. [proposal flag D8]
- Gains read "▲ +Rp 396.000 · +8,0%" with one decimal everywhere. [proposal flag D6]
- Five summary figures leave the header for group totals and the Rincian; "Saldo tersedia" kept ("Dana likuid" rejected). [proposal flag D3]

Deviations from the prototype:
1. Owner-directed: Utang credit-only plus "Tagihan" row; no "Utang lain" row. [proposal dev. 1]
2. Owner-directed: Grafik per-use view is the ring with legend, not bars. [proposal dev. 2]
3. Credit tile follows H8: owed, "Sisa limit" or "Melewati limit", next bill. [proposal dev. 3]
4. Credit detail has no "Minimal bayar"; "Tagihan terbit tiap tgl"; adds H8 bill list. [proposal dev. 4]
5. A tile may carry one quiet note line ("Melewati limit", "Perlu dicocokkan", "N pindah saldo belum lengkap"). [proposal dev. 5]
6. Institution mark uses the account's colour and initials (no institution field). [proposal dev. 6]
7. Account form keeps nine colours, investment model fields and default pocket. [proposal dev. 7]
8. Beli and Jual have no "Biaya" field (request has no fee). [proposal dev. 8]
9. "…" item "Rencana" opens the Rencana dialog in place, not a link to Target & Tagihan. [proposal dev. 9]
10. On a phone reorder mode uses move buttons, not touch drag. [proposal dev. 10]
11. Detail keeps "Juga dilaporkan", "Buka {induk}" and an investment platform's group gain. [proposal dev. 11]
12. Owner rule: expense amounts in "Transaksi terakhir" are red via `RowAmount`. [proposal dev. 12]
13. Cocokkan saldo keeps the server's causes with links and "Periksa lagi". [proposal dev. 13]

Further notes from verification and design:
1. Stacked on phase 5 before it merged (coordinator). [verification dev. 1]
2. "Saldo tersedia" kept in the Rincian (coordinator 2026-11-08). [verification dev. 2]
3. Credit bill items show kind and date, not a purchase name (API has none). [verification dev. 3]
4. "Melewati limit" is in the credit tile's meta and is its one note; no second note line. [verification dev. 4]
5. `.hbar` fourth column 176 px (168 below 1024) instead of 136 px. [verification dev. 5]
6. Detail header has no institution mark beside the title. [verification dev. 6]
7. Beli/Jual have no date field; in-dialog direction toggle gone. [verification dev. 7]
8. "Sama dengan top up terakhir ke produk ini." is in the catalogue but never shown. [verification dev. 8]
9. Dialog words follow Appendix A ("Beli"/"Jual", "Ubah rekening" with name as meta). [verification dev. 9]
10. Editing a credit account to another type also clears `statement_day`. [verification dev. 10]
11. Ring dims non-chosen uses (opacity) besides the legend bar. [verification dev. 11]
12. Per-use subtotals, counts and type line are new words; values are sums of server balances. [verification dev. 12]
13. Two tile columns at 1024 px, not three. [verification dev. 13]
14. Shared fixes: dialog menus render inside their dialog; `dialog.shell-dialog` rise animation fills backwards only. [verification dev. 14]
15. `format.percent(value, fractionDigits)` added; `ExplanationDialog` footer link made optional. [design Deviations recorded at implementation]
16. Untranslated test example moved from `/accounts` to `/settings`, not `/goals`. [design Deviations recorded at implementation]
17. Pushed without a PR; held branch deleted by the owner after merge. [design Deviations recorded at implementation]

Not verified there:
- Prototype side-by-side composites (checklist against `styles.css` instead).
- Before captures of the old dialogs.
- English stress and USD captures (seed is Indonesian only).
- Pointer drag and drop in the browser.
- Trade and top-up dialogs reopened from Transaksi and Tinjau in the browser.
- Gate ran on the stacked revision (phase 5 not yet merged then).

## Phase 9 — Target & Tagihan (2026-11-09-apply-markas-target)

Flags (seven, all defaults):
- Coverage bar reads server `coverage_pct` instead of browser division. [proposal flag D3]
- A single flagged emergency goal leaves the goal list, edited from "…". [proposal flag D7]
- Goal form drops "Warna Label"; stored colours kept. [proposal flag D8]
- "Sudah lunas · N" reads `GET /obligations?include_archived=true` (fixes a dead toggle). [proposal flag D9]
- Credit bills sit beside plans but never in the section total or Terjadwal; plan shows `plan_amount` and `counted_elsewhere`. [proposal flag D14]
- Due credit bill in "Perlu tindakan" or bell opens Perpindahan prefilled via `openMovementCapture`. [proposal flag D16]
- "Rencana & Pengingat" rebuilt as 880 px list and detail; app filters and requests unchanged. [proposal flag D20]

Deviations from the prototype:
1. Debts have no lender or paid-off date: meta reads due date and "N% terbayar"; paid-off reads "lunas". [proposal dev. 1]
2. No "Minimal bayar" on credit row or dialog. [proposal dev. 2]
3. "Rencana & Pengingat" keeps "Semua · Cicilan · Rutin", not "Semua · Terjadwal · Terlewat · Dibayar". [proposal dev. 3]
4. Goal dialog keeps account picker, "Total saldo terhubung" and source-change confirmation. [proposal dev. 4]
5. "Tambah progres" stays its own small dialog; no "Riwayat setoran" list. [proposal dev. 5]
6. Debt dialog shows "Kaitkan atau catat pembayaran", not a list of linked payments. [proposal dev. 6]
7. Debt dialog footer action is "Arsipkan", not "Hapus". [proposal dev. 7]
8. Hero line names the basis, including "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas". [proposal dev. 8]
9. Dana darurat caption "{n}% dari target"; "· 2 sumber" not shown. [proposal dev. 9]
10. Tagihan rows include detected recurring charges marked "perkiraan". [proposal dev. 10]
11. Credit bill row and credit row in Utang & kredit come from H9. [proposal dev. 11]
12. Ketahanan Dana "Rincian" keeps the app's basis lines. [proposal dev. 12]

Further notes from verification and design:
1. Credit bills under Tagihan read from today; an overdue bill appears in Perlu tindakan, bell and its credit row, not Tagihan. [verification dev. 1]
2. Perpindahan for a credit bill fills "Ke" and amount; Catat preselects its default "Dari" (D16 said empty; Catat is phase 11's). [verification dev. 2]
3. Credit lines in Pengeluaran Rincian name accounts in the note, not a nested disclosure. [verification dev. 3]
4. Debt and credit amounts use phase 8's Utang style (red, minus sign). [verification dev. 4]
5. Fund caption can exceed 100 % ("150% dari target") while the bar is capped. [verification dev. 5]
6. "Belum dapat dihitung" uses the shared 20 px words style, not 24 px. [verification dev. 6]
7. "Sumber progres": Manual clears chosen accounts, Otomatis restores them; save still confirms. [verification dev. 7]
8. Debt dialog intro line only when adding a debt. [verification dev. 8]
9. Bill rows take their schedule word from `GET /recurring`. [verification dev. 9]
10. Same-day plans and bills in Perlu tindakan and the bell ordered by name. [verification dev. 10]
11. Ketahanan Dana Rincian links each period by its label, not its amount. [verification dev. 11]
12. Coverage total and month and day counts stay visible while balances are hidden. [verification dev. 12]
13. At 320 px the title block's "…" wraps under "Rencana" and "Tambah target". [verification dev. 13]
14. "Rencana & Pengingat" details: activate, deactivate, delete in detail only; one form; "Nominal" without "(IDR)"; `initialFocus` reaches a named plan only when cached. [verification dev. 14]
15. A credit-bill plan whose date differs from the bill's due date shows both rows (listed for end review). [design Risks]
16. `ListRow timeIsDate` (48 px date column) added for Tagihan after dates wrapped. [verification Bill dates on one line]
17. Plan types gained optional credit fields; bell items gained `onSelect`; evidence kept in the session scratchpad; pushed without a PR. [design Deviations recorded at implementation]

Not verified there:
- Prototype side-by-side composites; the prototype scenarios added were not run.
- English stress and USD captures (seed is Indonesian only).
- Pointer-only interactions beyond clicks.

## Phase 10 — Pengaturan (2026-11-09-apply-markas-pengaturan)

Flags (six, all defaults) and owner override:
- Rail beside the form from 768 px; phone lists every section above the form; no "Semua pengaturan" index. [proposal flag D3]
- "Keluar" joins the rail and leaves Lainnya; account menu keeps it. [proposal flag D5]
- Device readiness and problems move into the "Status HP" box. [proposal flag D7]
- Manual API key is an open section with rotation confirmation, not a collapsed disclosure. [proposal flag D8]
- Kebiasaan auto-record states stay read-only status rows, not switches (`HABIT_AUTO_RECORD_ENABLED`). [proposal flag D10]
- Pairing slang and second-person lines become neutral, including "HP-mu siap! …". [proposal flag D17]
- Owner override 2026-11-08: pairing QR at least 200 css px in a 218 px box, not the prototype's 118 px. [verification Approval]

Deviations from the prototype:
1. Status HP lists every paired phone and its problems; prototype draws one. [proposal dev. 1]
2. Pairing dialog keeps countdown, "Buat kode baru", four-step checklist and collapsed download. [proposal dev. 2]
3. Auto-record states are status rows; habits keep two lists without "% cocok" or badges. [proposal dev. 3]
4. "Seberapa sering ditanya" keeps phase 6's content and request, restyled. [proposal dev. 4]
5. Tampilan starts with "Bahasa". [proposal dev. 5]
6. Mata uang & anggaran starts with "Wilayah"; keeps rate hint and "leave empty" limit hint. [proposal dev. 6]
7. Gaji & siklus keeps the source picker with paging inside "Ganti sumber" and "Tandai sebagai gaji". [proposal dev. 7]
8. Nama pengirim keeps search above eight names; Sesi uji keeps report, labelling, CSV and delete. [proposal dev. 8]
9. Prototype's "forced theme" caption not built (no source). [proposal dev. 9]

Further notes from verification and design:
1. Breakpoints: `styles.css` tablet rule wins (184 | 1fr, gap 32 from 768 to 1199 px); phone rail items 44 px (prototype 36). [verification dev. 1]
2. Download QR 136 px of code in a 154 px box (D9 fallback; 118 px gave 2.62 px per module). [verification dev. 2]
3. Passed check reads "Baik" / "Good"; salary meta drops "gajian sekitar tanggal {day}"; further neutral rewrites outside Appendix A. [verification dev. 3]
4. English rewrites of Appendix A lines that used "you"/"your". [verification dev. 4]
5. Pairing footer: "Nanti saja" and "Buat kode baru" before pairing, "Selesai" once paired; no waiting checklist before pairing. [verification dev. 5]
6. Rows keep today's facts: session window, mapping route, masked sender names. [verification dev. 6]
7. Hints dropped: display name, emergency months, running session's second line, salary intro. [verification dev. 7]
8. After revoke, focus goes to "Hubungkan ponsel". [verification dev. 8]
9. Download button uses the download icon, not the prototype's upload icon. [verification dev. 9]
10. `SenderAliasesSection` search still folds with `toLocaleLowerCase("id-ID")`. [verification dev. 10]
11. Commit `51c9ff0` holds only file moves; `7103e94` completes it; history not rewritten. [verification dev. 11]
12. Settings box is `.set-box` (padding 24) because `.box` is Transaksi's; untranslated test example moved to `/komponen`. [design Re-based]
13. Twelve rail rows push the section below the fold at 390 px ("Captured for the owner"). [design Risks]

Not verified there:
- Real-phone scan of the pairing and download QR codes (end-review list).
- Pixel-level side-by-side composites.
- A request recorder on the old interface.
- Salary candidates needing review (seed has none; unit tests only).

## Phase 11 — Catat (2026-11-09-apply-markas-catat)

Flags (seven, all defaults, with reconciliation adjustments):
- Credit expense always sends `credit_terms {installment_count, first_due_date?}`; date only when edited. [proposal flag D9]
- First due date and instalment amount previewed in the browser from the credit spec rules; never sent unless edited. [proposal flag D10]
- Credit purchase labelled "dibayar nanti · {account} · jatuh tempo {date}"; no figure change. [proposal flag D11]
- "Bayar sisa tagihan {amount}" fills `left_to_pay`; never records itself. [proposal flag D12]
- "Rencana jatuh tempo" moves from `Drawer` to `ShellDialog` 560 (`DuePlansDialog`). [proposal flag D20]
- Dialog untranslated path removed; page-level registry stays for phase 12. [proposal flag D21]
- Removed: `Modal`, `Drawer`, `InfoHelp`, `FormSection`, dialog `untranslated` prop, `AccountOptions`, `explain.tsx` popover and `ExplainProvider`. [proposal flag D22]

Deviations from the prototype:
1. "Bayar pakai kredit" and "Bayar sisa tagihan" come from H11; prototype draws neither. [proposal dev. 1]
2. Expense amounts red (owner feedback, phase 5 D29). [proposal dev. 2]
3. "Contoh" chips are fixed sentences per language without account names. [proposal dev. 3]
4. Below 480 px "Dari" and "Ke" stack with a down arrow. [proposal dev. 4]
5. Pemasukan keeps "Bukti transaksi" and "Mengurangi pengeluaran"; Pengeluaran keeps the debt split. [proposal dev. 5]
6. Prototype's "+ Tambah tagihan" link and prefilled note "Bayar tagihan SPayLater" not built. [proposal dev. 6]
7. Save confirmation stays "✓ Tersimpan!" with the 450 ms close, not an instant close with toast. [proposal dev. 7]
8. Sentence view keeps the server's inline follow-up questions. [proposal dev. 8]

Further notes from verification and design:
1. Goal hint is phase 5's "Kaitan ini tidak menambah progres target…"; debt editor keeps its hint; spec goal scenario corrected. [verification dev. 1]
2. English instalment choices read "3x · 6x · 12x". [verification dev. 2]
3. Credit block has a "Cara bayar" label over its segments. [verification dev. 3]
4. "Dari" starts on the first liquid account, as the former form. [verification dev. 4]
5. Parity seed bell shows SPayLater's bill as its plan; bell → Catat verified with "Kartu Kredit Contoh". [verification dev. 5]
6. Bantuan lists global keys plus the page's keys, not all eleven everywhere. [verification dev. 6]
7. Toast stays 6.5 s (`TOAST_MS`), prototype 6 s. [verification dev. 7]
8. Focus moves to each follow-up question's first answer. [verification dev. 8]
9. `/komponen` not extended with money input and chips; the sheet is phase 12's. [verification dev. 9]
10. Date field format follows the browser locale (native `type="date"`). [verification dev. 10]
11. No `MovementEditFields`: Catat writes its own Dari/Ke pair with phase 5's parts. [design Reconciliation 1]
12. `FormField` descriptions render as hint lines so `InfoHelp` could go; `explain.tsx` types moved to `components/workspace/explanation.ts`. [design Reconciliation 5, 6]
13. Target & Tagihan lists credit bills without a pay action, so it is not an `openMovementCapture` caller. [design Reconciliation 9]
14. Pushed without a PR, though the plan's follow-up said to open one. [design Reconciliation 12]
15. Remaining risk: Perpindahan default pair can be one account to the server ("BCA (Default: Utama)" and "Utama"), refused as `same_account`; left for the owner. [verification Remaining risks]
16. Catat-recorded credit purchases store `installment_count 1`; notification-captured ones store none (both paid in full). [verification Remaining risks]

Not verified there:
- A real phone and a screen reader (device mode only).
- Receipt upload by keyboard in the browser (unit tests only).
- "Before" captures at all seven widths.
- Zoom to 400 %.
- No AI provider called (`NOTIFICATION_AI_ENABLED=false`; deterministic parser only).
- Browser due-date parity covers three bill/payment-day pairs, not every pair (remaining risk).


## Phases 1 to 3 — interim items marked for the end review

These were interim states for later phases; the phase 12 gate captures every page, so any that remained would show
there.

Phase 1 — Foundation (2026-11-05-apply-markas-foundation), "Interim look, listed for the owner's end review":
1. Coral active sidebar pill and old top bar until phase 2. [verification Interim look]
2. Rekening cards keep a 6 px coloured top strip until phase 8 removes the card. [verification Interim look]
3. Coral is the only primary fill, so some pages show more than one coral button. [verification Interim look]
4. Raw page and dialog inputs keep 44 px; primitives are 40 and 56 px until each page phase. [verification Interim look]
5. Analisis and Rekening tabs take the segmented look; Analisis becomes underline tabs in phase 6. [verification Interim look]
6. Table heads keep sentence case until each page's table gets the uppercase label. [verification Interim look]
7. Catat movement validation alert in English ("Source and target accounts must be different") until phase 11. [verification Interim look]

Phase 2 — Shell (2026-11-06-apply-markas-shell), "For the page phases (end-review list)":
1. Full sidebar from 1024 px narrowed bodies 1024–1100 px (Transaksi phone feed, Beranda KPI overflow, Kalender contrast) for phases 3, 5, 6. [verification end-review list]
2. Pre-existing: Kalender marker contrast, Peta Aliran labels past frame, several pages past the frame at 320 px. [verification end-review list]
3. Parity seed disagreements (23 figures): Ketahanan Dana, payday, saving rule, scheduled amount, Tinjau counting. [verification end-review list]
4. Captures after midnight see the seed a day later than the prototype (SPayLater overdue, "kemarin"). [verification end-review list]
5. Interim look: Markas+ shell around old page bodies, accepted 2026-11-05. [design Risks]

Phase 3 — Beranda (2026-11-06-apply-markas-beranda):
1. Interim inconsistency: Beranda expenses in ink, Transaksi in coral until phase 5. [design Risks; verification]

## Follow-ups outside the redesign

- Recurring charges keep their detection basis: a charge in the Tabungan pillar is still listed as a repeating bill
  on Analisis and Laporan. [phase 7 verification follow-ups]
- Public sign-up stays closed (invite code); the language plan said "until phase 12", but opening it is an access
  decision for the owner, not cleanup. [add-language-system proposal]
- A full catalogue-key prune (keys are reached dynamically, such as server error codes). [apply-markas-cleanup D3]
- `test_balance_check.py` fixes `NOW` at import, so one test fails when the suite runs slowly. [apply-markas-cleanup
  verification]

## Phase 12 — Cleanup (apply-markas-cleanup)

Gate on `606ff5d` against the base `170e182`; images in the session scratchpad
`/private/tmp/claude-501/-Users-enrico-enrico-project-enrico-cash-flow-tracker/148d8a0e-cf25-4ec5-9de4-a0f135ad4203/scratchpad/cleanup/gate/`
(below: `gate/`). [verification "Captures and pixel diff"]

Visible differences the before/after pixel diff found (every other pair is identical, or explained in the
verification as clock values, random codes or run noise):
1. Laporan lists "Pengeluaran dari tabungan" / "Spending from savings" with "tetap dihitung sebagai pengeluaran"
   under Rincian tabungan when not zero (October −Rp 360.000, November −Rp 141.000); the page grows by one row.
   [D8; `gate/diff/side-by-side/laporan-1440-light.png`, `laporan-390-dark.png`]
2. The Kredit provider header's "Pengeluaran pakai kredit" equals the hero (Rp 581.500; it read Rp 1.031.500 with a
   purchase timed later today). [D9; `gate/diff/id-1440/transaksi-kredit-penyedia-1440-light.png`, `gate/d9/`]
3. Catat Perpindahan preselects "Ke" Dana Darurat instead of Utama, the pocket BCA's money resolves to, so the default
   pair saves. [D13; `gate/diff/id-1440/catat-perpindahan-1440-light.png`, `gate/d13/`]
4. English short months read "Sep" (dates, ranges, cycle labels, notifications, Pengaturan). [D11;
   `gate/crops/en-montage.png`, `gate/diff/en-1440/`]

Deviations and notes:
1. `/komponen` already answered 404 on the base production build (the sheet needed `NEXT_PUBLIC_COMPONENT_SHEET`);
   now the route is gone from the build. [verification Checks]
2. axe `target-size` on Target's "SPayLater · kredit" link behind the goal sheet (`dlg-target-manual`,
   `dlg-target-progres`) at 390, on the base build too; the control is inert behind the modal sheet. Phase 9's
   Perpindahan finding (D12) is 0 at 390 and 320. [verification "Checklists, axe"; "Target size"]
3. Checklist failures carried from earlier phases, the same on the base build: Rekening's "groups 48 apart" measures
   16 px on the combined seed; phase 2's shell checklist flags "content inside the frame" on several pages.
   [verification "Checklists, axe"]
4. `test_balance_check.py::test_saldo_cocok_starts_the_next_window` fails when more than a minute passes between test
   collection and the test (import-time `NOW`); it passed in the full re-run. [verification Checks]
5. The ledger list with type counts on a 19,506-row owner: p95 670.6 ms → 116.3 ms with JIT off. [D10; verification
   "Ledger list timing"]

Prototype side-by-side images (D17): 135 (68 at 1440, 67 at 390, Indonesian, light) in
`gate/prototype-side-by-side/`.
