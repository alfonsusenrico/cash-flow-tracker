# Verification: apply-markas-catat (Markas+ phase 11)

Date: 2026-11-09 · Branch `feat/markas-catat` · Base: `origin/main` `5401a03` (phases 1 to 10, the language system,
localized server text and the credit flow up to V54; PR #98 merged), merged in `ff4f000` after phase 10's branch
(`33a5e89`) · Code under test: `0c846a1` (frontend; `f243669` changes tooling only) unless a check names another revision.

## Approval, decisions and dependencies (tasks 1.1, 1.2)

- Approval: the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08) and
  "continue all" (2026-11-09). Every flagged decision (D9, D10, D11, D12, D20, D21, D22) takes its default, with the
  reconciliation in design.md "Reconciliation with phases 4 to 10" (committed `50f7f13` before any code).
- D23 holds: phases 4 to 10, `localize-server-text` and `add-credit-spending-flow` are ancestors of `5401a03`.
- Probes on the migrated disposable database (V54, parity seed): `POST /api/movements` with the same account answers
  400 `same_account` (and so does Jago with its own default pocket "Kantong Utama", the pair the browser cannot see);
  `GET /api/accounts` credit rows carry `limit_left`, `statement_day`, `payment_day` and `next_due`; `POST
  /api/transactions` with `credit_terms {installment_count: 3}` succeeds on SPayLater and answers 422
  `credit_terms_not_credit_purchase` on a bank account.
- `openMovementCapture`, `RowAmount`/`KindAmount`, `DuePlanActions`, `ShellDialog` (`initialFocus`, `scrollFocusable`)
  exist. `MovementEditFields` does not (reconciliation item 1). `Modal`'s importers were only `CatatDialog` and
  `QuickCaptureModal`; `MoreSheet` has no Keluar. `DebtAllocationEditor` is in `transactions.debts` (item 3).

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these
over:
- Phase 11 (`apply-markas-catat`) built 2026-11-09 under the standing instruction; branch `feat/markas-catat` pushed,
  no PR (coordinator's instruction; the plan's follow-up said to open one). Flags D9 to D12 and D20 to D22 accepted.
- Catat is one dialog (`components/capture/CaptureDialog.tsx`) with the sentence and the manual view; `CatatDialog`,
  `QuickCaptureModal`, `Modal`, `Drawer`, `InfoHelp`, `AccountOptions`, the `explain.tsx` popover and
  `ExplainProvider`, `FormSection`, `PendingSubmitButton`, `openMovement` and the dialog `untranslated` path are gone.
  The page-level untranslated registry stays for `/komponen` (phase 12, D21).
- H11 is built: an expense on a credit account always sends `credit_terms {installment_count}` and `first_due_date`
  only when the owner edits the date; "Bayar sisa tagihan" fills the amount, never records. Browser due dates equal the
  server's for 120 purchases on three bill/payment-day pairs.
- `movementFormSchema` returns codes; Catat words them from `capture.errors`, "Ubah pindah saldo" from
  `transactions.movement.errors`.
- End-review list: the proposal's eight deviations, the deviations below, and Catat on a real phone (device mode only
  here).
- Known outdated check (phase 12): Beranda's checklist still expects a 112 px status column and dark expense amounts
  (not run in this phase).

## What changed

- **One dialog (D1, D2, D15, D16):** `CaptureDialog` on `ShellDialog` 560 (sheet below 768) with the sentence view
  and the manual view swapped in place; each keeps its state while the dialog is open; the amount read from the
  sentence fills the manual amount on its first visit; a polite line announces the new view; "Isi manual" / "Tulis
  kalimat" as the footer's quiet button (hidden while a question waits). Entry points in `AppLayout`: "+ Catat", N
  and the phone tab open the sentence (N does nothing while open); `openQuickAdd(type)` opens the manual mode;
  `openMovementCapture` opens Perpindahan prefilled. Focus: the field from 768 px, the title below (`ShellDialog
  focusTitle`); `useReturnFocus` gives it back to the opener, or to `main` when the opener is gone. Lazy load after
  idle kept.
- **Sentence view (D3 to D5):** 56 px `t-lead` field with a visually hidden label, "Contoh" chips from
  `capture.examples`, the "Terbaca" `.box` with the amount by meaning (−red / +green / transfer), Jenis, Rekening
  ("· dibayar nanti" for a credit account), Kategori, Pilar and Tanggal ("Hari ini, 11.13"), the refusals under the
  field, the preview-failure line, inline follow-up questions (focus moves to each question's first answer), toast with
  Urungkan. Requests unchanged.
- **Manual view (D6 to D8, D13, D14, D17):** `RadioSegments` modes; `.input-money` 56 px with inline arithmetic
  (`amount.ts`, "= Rp 40.000" only while an operator is typed); "Tambah cepat" chips; Pengeluaran with "Bayar dari",
  the credit block, categories grouped by pillar, the pillar segment and its hint, and "Detail tambahan" (time, goal +
  hint, debt split, note, receipt + hint); Pemasukan with "Mengurangi pengeluaran"; Perpindahan with Dari → Ke
  (stacked below 480 px), the bill line, "Bayar sisa tagihan", the fee. Errors sit under their field
  (`aria-invalid`, `aria-describedby`, focus); request failures in one alert; `same_account` and `credit_terms_*`
  under their fields. Hidden balances: options without balances, every shown amount masked.
- **H11 (D9 to D12):** `creditTerms.ts` (first due date, instalments, Jakarta dates), `CreditTermsFields` ("Bayar
  pakai kredit", Sisa limit / Melewati limit, Sekali bayar · 3x · 6x · 12x · Lainnya (2 to 36), "Jatuh tempo
  pertama" with "Pakai tanggal tagihan", the "dibayar nanti" line); the bill meta, "Bayar sisa tagihan" chip and fee.
- **Global surfaces (D19, D20):** measured against the prototype at 1440 and 390 (notifications, theme, account menu,
  Lainnya, Bantuan, toast): no measured difference needed a change. Fixed: a credit bill chosen in the bell returns
  focus to the bell before Catat opens. "Rencana jatuh tempo" is `DuePlansDialog` (`ShellDialog` 560) for the bell and
  Beranda.
- **Language (D18, D21):** `capture` namespace (124 keys, listed below); copy-register roots gain
  `components/capture`, `lib/capture.ts`, `lib/movementFormSchema.ts`; the spending-term check covers `capture.*`.
  `useUntranslated()` lost its argument; `ShellDialog` lost `untranslated`; `FormField` descriptions are hint lines.
- **CSS:** `.input-money`, `.quick-chips`, `.capture-*` (read box, segments, details summary, direction, credit block),
  select chevrons inside Catat; removed `.ws-catat*`, `.ws-seg-on`, `.ws-explain`, `.ws-xn`, `.ws-x-*`, `.ws-xtable`,
  `.ws-sheet-scrim`, `.ws-close`, the drawer's `data-mode` rules and `ws-center-in`. `.ws-drawer`/`.ws-drawer-h`/
  `.ws-grab` stay for `/komponen`'s static sample.
- **Tooling (D24):** `seed_catat.py` (SPayLater bill day 1, Kredivo Contoh without a payment day, Kartu Kredit Contoh
  owing Rp 300.000 due today without a plan, a third debt, the reimbursement flag; `--stress`: 17 pockets with
  60-character names and Rp 1.234.567.890), `catat-scenarios.py` (26 app and 10 prototype scenarios),
  `catat-checklist.js`, `catat-keys.mjs` (eleven flows), `catat-requests.mjs` (recorder and comparison),
  `catat-credit-parity.mjs`.

## Deviations and notes for the owner's end review

The proposal's eight deviations hold. Further:
1. **Hints the server contradicts (reconciliation item 4):** the goal hint is phase 5's "Kaitan ini tidak menambah
   progres target. Target yang terhubung ke rekening mengikuti saldonya." and the debt editor keeps its own hint; the
   spec delta's goal scenario no longer claims that the goal's saved amount grows (it never did).
2. English instalment choices read "3x · 6x · 12x" like phase 5's "Ubah transaksi", not "3 · 6 · 12".
3. The credit block has a "Cara bayar" label over its segments, so the radio group has a name; the legend carries
   "Bayar pakai kredit" and the limit line.
4. "Dari" starts on the first liquid account, as the former form (phase 9's "default account"); no rule is added.
5. In the parity seed the bell shows SPayLater's bill as its plan "Tagihan SPayLater" (phase 9 lists a bill once, under
   its plan), which opens Rencana jatuh tempo; the bell → Catat path is verified with "Kartu Kredit Contoh".
6. Bantuan lists the global keys on Beranda and adds a page's keys on that page (phase 2); the prototype lists all
   eleven everywhere. Unchanged.
7. Toast stays 6.5 s (phase 2's `TOAST_MS`); the prototype's is 6 s.
8. Focus moves to each follow-up question's first answer (the former dialog left focus on the body when the field
   was replaced).
9. `/komponen` was not extended with the money input and chips (task 2.1); the sheet is phase 12's.
10. The date field's display format follows the browser's locale (native `type="date"`).

## Checks

| Check | Revision | Result |
| --- | --- | --- |
| Frontend tests (`npm test`) | `f243669` | 99 files, 1069 tests passed |
| `npm run type-check` | `f243669` | passed |
| `npm run lint` | `f243669` | passed, no warnings |
| `npm run build` (webpack) | `0c846a1` | passed |
| Backend pytest, fresh `postgres:16` migrated V1 → V54 with `flyway/flyway:10` (`uv venv -p 3.12`) | `ff4f000` (backend unchanged since) | 2197 passed |
| `openspec validate --all --strict` | `f243669`, and after archiving | 85 passed before archiving; see "Final revision" |
| `git diff --check` | every commit | clean |
| `git diff --stat 5401a03 -- backend db mobile nginx` | final | empty |
| Example sentences through `POST /api/capture/parse` | seed | id 3/3 and en 3/3 read one amount, no problem |
| Credit preview parity (task 4.3) | `e1a965f` | 120 purchases on bill/payment days 1/9, 25/5 and 31/31: due dates 120/120, instalments 120/120; Pengeluaran on the day grew by the amount 12/12 |
| Request parity before (`5401a03`) vs after, Indonesian (task 5.4) | `e1a965f` | 9 flows, identical except server ids and `credit_terms` on the two credit purchases |
| Request parity Indonesian vs English (task 6.5) | `e1a965f` | identical except the sentence's own text and the server's follow-up question |
| Keyboard flows (`catat-keys.mjs`), 1440 and 390 | `e1a965f` | id 108/108, en 108/108 |
| Captures, axe, console, overflow, checklist | `e1a965f`, 320 px re-run on `0c846a1` | 192 + 192 captures; checklist 3024/3024 and 3020/3020; axe serious/critical 0; console errors 0; overflow 0 |

### Request parity (field by field)

Recorded with `catat-requests.mjs` on the same disposable owner (reset between runs): sentence capture, expense with a
category, expense with a pillar override and a goal, expense split across three debts, income with a reimbursement,
movement into credit with a fee, credit purchase paid in full, credit purchase in 3 with an edited date, bill from the
bell. Before vs after differ only in:
- sentence capture: the capture id in the answer path and its `question_id` (server-generated);
- credit purchase paid in full: `credit_terms` absent → `{"installment_count": 1}`;
- credit purchase in 3 with an edited date: `credit_terms` absent → `{"installment_count": 3, "first_due_date": "2026-12-20"}`.
Every other method, path and body field is identical. `client_capture_id`, `device_time` and `reply_id` are excluded
by design. Indonesian vs English differ only in the sentence typed ("parkir 5rb" / "parking 5k") and the question the
server asked about it.

### Keyboard

`catat-keys.mjs` on fresh copies of the parity owner, Indonesian (3812) and English (3813) accounts, 1440 × 900 and
390 × 844 (device mode): 108 checks each, all passed. Per flow: (1) N opens with focus in the sentence (title on a
phone), Tab ×40 stays inside, an example chip by Enter, Catat by Enter, each follow-up question takes focus on its
first answer and Enter answers it, the toast's Urungkan by Enter undoes; (2) "Isi manual" keeps the amount read and
focuses it, "Tulis kalimat" brings the sentence back, Esc returns focus to the page; (3) → chooses the next pillar,
Enter opens "Detail tambahan", Tab trapped; (4) three debt rows by keyboard, Tab ×60 trapped; (5) "Mengurangi
pengeluaran" reachable by Tab; (6) "Bayar sisa tagihan" by Enter fills the amount left, Enter in the fee saves;
(7) → chooses 3x, the first due date shows the bill date, Enter in the amount saves; (8) Rekening › SPayLater ›
"Bayar tagihan" opens Perpindahan with Ke and the amount, Tab trapped, Esc returns focus; (9) the bell's "Kartu
Kredit Contoh" bill opens Perpindahan prefilled, Esc returns focus to the bell (fixed in `e1a965f`; before it, focus
fell to the body); (10) Tema by keyboard (Lainnya › Tema on a phone), ↓ moves, Esc returns focus; (11) ? opens
Bantuan, Esc returns focus to the opener; a due plan opens Rencana jatuh tempo, Tab trapped, Esc returns focus.

### Captures

`catat-scenarios.py` scenarios through `capture.mjs` with `catat-checklist.js` and axe-core (wcag2a, wcag2aa, wcag21a,
wcag21aa, wcag22aa) at 1440, 1024, 390 and 320 in light and dark, on fresh Indonesian and English copies of the parity
owner (Tema and the account menu at 1440 and 1024; Lainnya and its Tema at 390 and 320):

| Run | Captures | Checklist | axe serious/critical | Console errors | Overflow | Notice / `lang="id"` |
| --- | --- | --- | --- | --- | --- | --- |
| Indonesian | 192 | 3024/3024 | 0 | 0 | 0 | 0 / 0 |
| English | 192 | 3020/3020 | 0 | 0 | 0 | 0 / 0 |
| Stress, Indonesian (17 pockets, 60-character names, Rp 1.234.567.890) | 40 + 12 + 6 | 808/808, 238/238, 114/114 | 0 | 0 | 0 | 0 / 0 |
| Stress, English with the USD display | 40 + 12 + 6 | 808/808, 238/238, 114/114 | 0 | 0 | 0 | 0 / 0 |

Notes: the bell scenario first failed its focused-field checks (16 per language) because a script click does not give
the headless page focus; with a key press first it passed 156/156 per language, and the totals above count that run.
The "same_account" scenario logs the browser's network line for the expected 400 of `POST /api/movements` (8 per
language); there is no script error. At 320 px the three modes were clipped inside their segment before `0c846a1`
(no page overflow); they now fit (re-captured: 114/114 per language). With the USD display the amount is still typed
in IDR ("Amount (IDR)"), as planned. Long pocket names end with an ellipsis inside a closed select and read in full in
its list; below 480 px Dari and Ke are stacked at full width.

Prototype comparison (`shots/proto` against `shots/g1`, composed in `shots/cmp`): notifications 400 px with the 8 px
dot, theme 220, account menu 240, Lainnya rows 56 with counts and the theme state, Bantuan 560, toast frame: no
difference that needed a change (Bantuan's key list: deviation 6). The manual view at 390 matches the prototype's
order and sizes; the sentence view keeps the app's examples and preview (deviations 3 and 8 of the proposal).

Key captures (scratchpad `markas-catat/shots`): `final-id/catat-tulis-terbaca-1440-light.png`,
`final-en/catat-tulis-terbaca-1440-light.png`, `final-id/catat-manual-1440-light.png`,
`final-en/catat-manual-1440-light.png`, `final-id/catat-tulis-terbaca-390-dark.png`,
`final-id/catat-kredit-1440-light.png`, `final-en/catat-kredit-1440-light.png`,
`final-id/catat-perpindahan-kredit-1440-light.png`, `final-id/catat-galat-sama-1440-dark.png`,
`bel-id/catat-dari-bel-1440-light.png`, `final-id/glob-notifikasi-1440-light.png`, `w320-id/catat-panjang-perpindahan-320-dark.png`.

## Copy (tasks 3.1, 3.9)

Changed Indonesian strings, each as Appendix A marks or as the reconciliation explains: "Catat transaksi" (sentence
title) → "Tulis transaksi"; the visible label "Tulis transaksi" → visually hidden "Kalimat transaksi"; placeholder
"Contoh: parkir 5rb jam 2" → "makan siang 35rb pakai gopay"; "Pratinjau belum tersedia. Teks tetap dapat dicatat." →
"Belum bisa membaca sekarang. Teks tetap bisa dicatat."; the "Yang terbaca" chips ("Rekening: ditentukan saat
dicatat", "Kategori: dikenali saat dicatat", "Jam: ditanyakan setelah dicatat", "Waktu: sekarang, 12.37") → the
"Terbaca" box ("ditentukan saat dicatat", "dikenali saat dicatat", "ditanyakan saat dicatat", "Hari ini, 12.37");
"Simpan Transaksi" / "Pindahkan Saldo" / "Coba Unggah Lagi" → "Simpan transaksi" / "Pindahkan saldo" / "Coba unggah
lagi"; "Waktu Transaksi" / "Waktu Pemindahan" → "Waktu transaksi" / "Waktu pemindahan"; "Pilih Kategori…" → "Pilih
kategori…"; goal "Tidak ada" → "Tidak dikaitkan"; the section titles "Rincian transaksi", "Detail tambahan", "Arah
perpindahan", "Waktu dan catatan" → the one disclosure "Detail tambahan · waktu, target, tagihan, bukti" and the
"Arah perpindahan" label; "Kaitan transaksi" and its info text removed; receipt "JPG, PNG, WebP, atau PDF. Transaksi
tetap tersimpan jika unggahan gagal." → "JPG, PNG, WebP atau PDF, paling besar 5 MB. Transaksi tetap tersimpan jika
unggahan gagal."; note placeholder "Catatan transaksi (opsional, misal: Makan siang, kopi)" → "Misalnya makan siang,
kopi"; a missing amount "Masukkan nominal yang valid" → "Isi nominalnya dulu." ("Masukkan nominal yang valid" stays for
an expression that reads 0). Every other Indonesian string is unchanged.

The `capture` namespace (Indonesian · English):

| Key | Indonesian | English |
| --- | --- | --- |
| `close` | Tutup catat transaksi | Close the transaction form |
| `sentence.title` | Tulis transaksi | Write a transaction |
| `sentence.meta` | Satu kalimat dengan jumlahnya. Waktu dan rekening boleh ditulis. | One sentence with the amount. The time and account may be added. |
| `sentence.label` | Kalimat transaksi | Transaction sentence |
| `sentence.placeholder` | makan siang 35rb pakai gopay | lunch 35k |
| `sentence.examples` | Contoh | Examples |
| `sentence.record` | Catat | Record |
| `sentence.recording` | Mencatat… | Recording… |
| `sentence.manual` | Isi manual | Fill in manually |
| `sentence.noAmount` | Jumlah belum ada. Tulis jumlahnya, misalnya 5rb. | No amount yet. Write the amount, for example 5k. |
| `sentence.severalAmounts` | Ada lebih dari satu jumlah. Tulis satu jumlah saja. | There is more than one amount. Write one amount only. |
| `sentence.previewFailed` | Belum bisa membaca sekarang. Teks tetap bisa dicatat. | Cannot read it right now. The text can still be recorded. |
| `sentence.answer` | Jawaban | Answer |
| `sentence.send` | Kirim | Send |
| `sentence.failed` | Belum dapat dicatat. Coba lagi. | Could not record it. Try again. |
| `sentence.answerFailed` | Jawaban belum tersimpan. Coba lagi. | The answer was not saved. Try again. |
| `examples.first` | makan siang 35rb | lunch 35k |
| `examples.second` | parkir 5rb jam 2 | parking 5k yesterday |
| `examples.third` | gaji masuk 8jt | coffee 18k |
| `preview.empty` | Hasil terbaca muncul di sini sebelum disimpan. | What is read appears here before it is saved. |
| `preview.label` | Terbaca | Read as |
| `preview.kind` | Jenis | Type |
| `preview.account` | Rekening | Account |
| `preview.category` | Kategori | Category |
| `preview.pillar` | Pilar | Pillar |
| `preview.date` | Tanggal | Date |
| `preview.accountLater` | ditentukan saat dicatat | chosen when recorded |
| `preview.categoryLater` | dikenali saat dicatat | recognized when recorded |
| `preview.payLater` | {account} · dibayar nanti | {account} · pay later |
| `preview.today` | Hari ini, {time} | Today, {time} |
| `preview.yesterday` | Kemarin, {time} | Yesterday, {time} |
| `preview.otherDay` | {date}, {time} | {date}, {time} |
| `preview.timeLater` | ditanyakan saat dicatat | asked when recorded |
| `toast.recorded` | Dicatat: {label}{when, select, none {} other {, {when}}}. | Recorded: {label}{when, select, none {} other {, {when}}}. |
| `toast.undone` | Catatan dibatalkan. | Recording undone. |
| `toast.undoFailed` | Gagal membatalkan. | Could not undo. |
| `manual.title` | Catat transaksi | Record a transaction |
| `manual.kindLabel` | Jenis catatan | Entry type |
| `manual.sentence` | Tulis kalimat | Write a sentence |
| `manual.save` | Simpan transaksi | Save transaction |
| `manual.saving` | Menyimpan… | Saving… |
| `manual.move` | Pindahkan saldo | Move balance |
| `manual.moving` | Memindahkan… | Moving… |
| `manual.retryUpload` | Coba unggah lagi | Try the upload again |
| `modes.expense` | Pengeluaran | Spending |
| `modes.income` | Pemasukan | Income |
| `modes.transfer` | Perpindahan | Transfer |
| `fields.amount` | Nominal (IDR) | Amount (IDR) |
| `fields.amountTotal` | Total bayar (IDR) | Total paid (IDR) |
| `fields.equals` | = {amount} | = {amount} |
| `fields.quickAdd` | Tambah cepat | Quick add |
| `fields.payFrom` | Bayar dari | Pay from |
| `fields.incomeTo` | Masuk ke | Paid into |
| `fields.category` | Kategori | Category |
| `fields.categoryNone` | Pilih kategori… | Choose a category… |
| `fields.incomeCategory` | Kategori pemasukan | Income category |
| `fields.pillar` | Pilar Kakeibo | Kakeibo pillar |
| `fields.offset` | Mengurangi pengeluaran | Reduces spending in |
| `fields.offsetNone` | Belum dipilih | Not chosen |
| `fields.details` | Detail tambahan · waktu, target, tagihan, bukti | More details · time, goal, bill, receipt |
| `fields.time` | Waktu transaksi | Time |
| `fields.transferTime` | Waktu pemindahan | Transfer time |
| `fields.goal` | Kaitkan ke target (opsional) | Link to a goal (optional) |
| `fields.goalNone` | Tidak dikaitkan | Not linked |
| `fields.note` | Catatan (opsional) | Note (optional) |
| `fields.notePlaceholder` | Misalnya makan siang, kopi | For example lunch, coffee |
| `fields.receipt` | Bukti transaksi (opsional) | Receipt (optional) |
| `fields.direction` | Arah perpindahan | Direction |
| `fields.from` | Dari rekening | From account |
| `fields.to` | Ke rekening | To account |
| `fields.chooseAccount` | Pilih rekening | Choose an account |
| `fields.fee` | Biaya / bunga (opsional) | Fee / interest (optional) |
| `hints.goal` | Kaitan ini tidak menambah progres target. Target yang terhubung ke rekening mengikuti saldonya. | This link does not add to the goal's progress. A goal linked to an account follows its balance. |
| `hints.fee` | Bagian dari nominal yang menjadi biaya atau bunga. Dicatat sebagai pengeluaran Biaya kredit. | The part of the amount that is a fee or interest. Recorded as Credit fee spending. |
| `hints.receipt` | JPG, PNG, WebP atau PDF, paling besar 5 MB. Transaksi tetap tersimpan jika unggahan gagal. | JPG, PNG, WebP or PDF, up to 5 MB. The transaction is kept if the upload fails. |
| `hints.offset` | Uang ganti ini bukan pemasukan. Pilih kategori pengeluaran yang dibayar kembali. | This refund is not income. Choose the spending category it pays back. |
| `hints.needAnotherAccount` | Tambahkan rekening likuid lain untuk memindahkan saldo. | Add another cash account to move money. |
| `credit.legend` | Bayar pakai kredit | Pay with credit |
| `credit.limitLeft` | Sisa limit <amount></amount> | Limit left <amount></amount> |
| `credit.overLimitBy` | Melewati limit <amount></amount> | Over limit by <amount></amount> |
| `credit.overLimit` | Melewati limit | Over limit |
| `credit.inFull` | Sekali bayar | Pay in full |
| `credit.times` | {times}x | {times}x |
| `credit.other` | Lainnya | Other |
| `credit.planLabel` | Cara bayar | How it is paid |
| `credit.count` | Jumlah cicilan | Number of installments |
| `credit.firstDue` | Jatuh tempo pertama | First due date |
| `credit.useBillDate` | Pakai tanggal tagihan | Use the bill date |
| `credit.noPaymentDay` | Tanggal bayar rekening ini belum diatur. Isi tanggalnya, atau atur di Rekening & Kantong. | This account has no payment day. Enter the date, or set it in Accounts & Pockets. |
| `credit.lineFull` | dibayar nanti · {account} · jatuh tempo {date} | pay later · {account} · due {date} |
| `credit.lineInstallments` | dibayar nanti · {count} cicilan mulai {date} · <first></first> per cicilan · tanpa bunga | pay later · {count, plural, one {# installment} other {# installments}} from {date} · <first></first> each · no interest |
| `credit.lineInstallmentsRemainder` | dibayar nanti · {count} cicilan mulai {date} · <first></first> lalu <rest></rest> per cicilan · tanpa bunga | pay later · {count, plural, one {# installment} other {# installments}} from {date} · <first></first>, then <rest></rest> each · no interest |
| `credit.lineNoDate` | dibayar nanti · {account} · jatuh tempo belum diatur | pay later · {account} · no due date yet |
| `credit.overLimitToast` | Melewati limit {account} | Over the {account} limit |
| `credit.creditAccount` | rekening kredit | credit account |
| `bill.meta` | Tagihan {account} <amount></amount> jatuh tempo {when} | {account} bill <amount></amount> due {when} |
| `bill.overdue` | Tagihan {account} <amount></amount> terlewat {date} | {account} bill <amount></amount> overdue since {date} |
| `bill.none` | Tidak ada tagihan berjalan untuk {account}. | No open bill for {account}. |
| `bill.payLeft` | Bayar sisa tagihan <amount></amount> | Pay the amount left <amount></amount> |
| `bill.today` | hari ini | today |
| `bill.tomorrow` | besok | tomorrow |
| `errors.amountRequired` | Isi nominalnya dulu. | Enter the amount first. |
| `errors.amountInvalid` | Masukkan nominal yang valid | Enter a valid amount |
| `errors.accountRequired` | Pilih rekening / dompet | Choose an account or wallet |
| `errors.sourceRequired` | Pilih rekening sumber | Choose the source account |
| `errors.targetRequired` | Pilih rekening tujuan | Choose the target account |
| `errors.sourceInactive` | Pilih rekening sumber yang masih aktif. | Choose a source account that is still active. |
| `errors.targetInactive` | Pilih rekening tujuan yang masih aktif. | Choose a target account that is still active. |
| `errors.sameAccount` | Rekening sumber dan tujuan tidak boleh sama | The source and target accounts must differ |
| `errors.timeRequired` | Pilih waktu transaksi | Choose the time |
| `errors.transferTimeRequired` | Pilih waktu pemindahan | Choose the transfer time |
| `errors.noteTooLong` | Catatan maksimal 500 karakter | A note can have at most 500 characters |
| `errors.goalOrDebt` | Pilih target tabungan atau tagihan, bukan keduanya | Choose a savings goal or a bill, not both |
| `errors.distinctDebts` | Pilih tagihan yang berbeda untuk setiap baris. | Choose a different bill on each row. |
| `errors.feeOverAmount` | Biaya tidak boleh melebihi nominal. | The fee cannot be more than the amount. |
| `errors.bothCredit` | Salah satu sisi harus rekening bank, e-wallet, atau tunai. | One side must be a bank, e-wallet or cash account. |
| `errors.countRange` | Isi 2 sampai 36 cicilan. | Enter 2 to 36 installments. |
| `errors.dueBeforePurchase` | Jatuh tempo pertama tidak boleh sebelum tanggal transaksi. | The first due date cannot be before the transaction date. |
| `errors.saveFailed` | Gagal menyimpan transaksi | The transaction could not be saved |
| `errors.moveFailed` | Gagal memproses pemindahan saldo | The transfer could not be processed |
| `errors.receiptFailed` | Transaksi tersimpan, tetapi bukti belum terunggah: {reason} | The transaction is saved, but the receipt was not uploaded: {reason} |
| `errors.receiptReason` | Bukti gagal diunggah | The receipt upload failed |
| `saved.transaction` | ✓ Tersimpan! | ✓ Saved |
| `saved.transfer` | ✓ Berhasil dipindahkan | ✓ Moved |

## Not verified

- A real phone and a screen reader; phone captures use the browser's device mode.
- The receipt upload by keyboard in the browser flows (a file chooser cannot be driven headless); the upload, its
  failure and "Coba unggah lagi" are covered by unit tests.
- "Before" captures at all seven widths (task 1.3): the before build ran for the request recorder; its look is the
  phase 10 captures.
- Zoom to 400 %; the 320 px captures cover reflow.
- No AI provider was called: the backend ran with `NOTIFICATION_AI_ENABLED=false`, no API key and outbound HTTP pointed
  at a closed port, so `/capture/text` used the deterministic parser and asked its follow-up questions; unit tests
  mock the capture endpoints.

## Remaining risks

- The Perpindahan defaults are the former form's: "Dari" is the first liquid account and "Ke" the first other one. When
  the first account is a parent whose default pocket comes next (the parity seed's "BCA (Default: Utama)" and "Utama"),
  the default pair is one account to the server, which refuses it as `same_account`; Catat now says so under "Ke".
  Changing the defaults would change requests, so it is left for the owner.
- The browser's first due date is a preview; the 120-purchase parity covers bill days 1, 25 and 31, payment days 9, 5
  and 31 and Jakarta day boundaries, not every pair.
- `credit_terms` is always sent for a credit purchase, so a purchase recorded in Catat stores `installment_count 1`
  where a notification-captured purchase stores none; both mean paid in full (design Risks).

## Final revision

Archived on 2026-11-09 as `2026-11-09-apply-markas-catat` after `d6ffc47`; the specs of `text-capture`,
`tactile-pulse-ui`, `accessible-financial-input-forms`, `mobile-native-layout`, `kazz-ui-and-kakeibo` and
`credit-accounts` took the deltas. `openspec validate --all --strict` after archiving: 84 passed, 0 failed (84 items).
