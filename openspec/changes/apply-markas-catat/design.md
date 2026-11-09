## Context

See proposal.md for the motivation. Code facts were read on `origin/main` `0308b7b` (after #89 `add-language-system`); the credit facts on `feat/credit-spending-flow` `23ccd85` (archived `add-credit-spending-flow`, not merged yet); the error code on `feat/localize-server-text` (`localize-server-text`, in progress); the other phases' facts in their planned changes (`markas-tinjau`, `-transaksi`, `-analisis`, `-laporan`, `-rekening`, `-target`, `-pengaturan` worktrees).

**Today's capture** (`components/layout/AppLayout.tsx`):
- `openCapture` (top bar "+ Catat", N, phone tab) opens `CatatDialog` (`components/capture/CatatDialog.tsx`, a `Modal` marked `untranslated`). It previews through `POST /api/capture/parse` after 300 ms, records through `POST /api/capture/text` with a `client_capture_id` that is resent on retry, answers follow-up questions through the capture's answer endpoint, and toasts "Dicatat: …" with "Urungkan" (`/capture/{id}/undo`) while `undo_until` is open. "Isi manual" closes it and opens the form with the previewed amount.
- `QuickCaptureModal` (`components/ui/QuickCaptureModal.tsx`, 981 lines, `Modal`, `untranslated`) is the form. Modes Pengeluaran · Pemasukan · Perpindahan (`aria-pressed` buttons). Amount: a boxed 30 px input with inline arithmetic (`+ − * /`, no `eval`), "= Rp …", five "+10k … +500k" chips and "Reset". Expense: "Bayar dari" (liquid and credit accounts, `AccountSelectOptions` with balances), "Kategori", "Pilar Kakeibo" (five buttons and the pillar help, with an info icon), "Waktu Transaksi", "Kaitan transaksi" (info icon) with "Kaitkan ke target" (🎯 options) and `DebtAllocationEditor` (info icon; hidden for a credit account), "Catatan", "Bukti transaksi". Income: "Masuk ke", income category, "Mengurangi pengeluaran" for a reimbursement category, time, note, receipt. Movement: "Dari rekening" and "Ke rekening" (credit allowed on one side only), "Biaya / bunga" when Ke is credit, time, note.
- Requests: `POST /api/transactions {type, amount, account_id, category_id, kakeibo_type, goal_id, obligation_id | obligation_allocations, notes, offset_category_id?, date}` then `POST /api/transactions/{id}/receipt` when a file is chosen; `POST /api/movements {source_account_id, target_account_id, amount, notes, date, fee?}`. Success: "✓ Tersimpan!" / "✓ Berhasil dipindahkan" then close after 450 ms; a receipt failure keeps the form open with "Coba Unggah Lagi"; an over-limit purchase toasts "Melewati limit {account}".
- Movement defaults: `handleOpenMovement(source?, target?)` (no caller); phase 8 adds `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` to the workspace context, opening today's form; phase 9 calls it for due credit bills. `openQuickAdd(type)` is used by Laporan's data-quality panel.
- **Errors:** `ApiError.message` renders `errors.<code>` when the code is known (`add-language-system` D8). The movement endpoint answers a same-account pair with the plain string "Source and target accounts must be different" today; `localize-server-text` makes it `api_error(400, ErrorCode.SAME_ACCOUNT)` with `errors.same_account` "Rekening sumber dan tujuan harus berbeda." / "Source and target accounts must be different." in both web catalogues. The browser already blocks the same pair (`movementFormSchema`, Indonesian messages), so the server message appears only for a pair the browser cannot see (a stale list, a parent and its own pocket resolved to one account).

**Credit API** (`add-credit-spending-flow`):
- `GET /api/accounts` credit rows: `credit_limit`, `credit_owed`, `limit_left` (may be negative), `over_limit`, `statement_day`, `payment_day`, `next_due {due_date, amount, left_to_pay, status}` or null.
- `POST /api/transactions` accepts `credit_terms {installment_count (1–36, default 1), first_due_date?}` only for an expense on a credit account; refusals `credit_terms_not_credit_purchase`, `credit_terms_count_range`, `credit_terms_due_before_purchase` (422, `{code, params, message}`, in the web `errors` namespace). `PUT`/`DELETE /api/transactions/{id}/credit-terms` change or reset them later (phase 5's "Ubah transaksi").
- Due dates: bill day `S = statement_day` (1 when empty); the close is the first date after the purchase's local date whose day is `S`; the first due date is the first `payment_day` on or after the close (days clamped to the month); no `payment_day` means no due date. An owner `first_due_date` replaces the derived one and must not be before the purchase. Instalment amounts are `floor(amount / n)` with the remainder on the first. Spending counts in full on the purchase date (credit D3).

**The prototype** (`dialogs.js` `catat`, `manual`; `styles.css` `.field`, `.input`, `.input-money`, `.quick-chips`, `.seg`, `.box`, `.dl`, `.dlg-*`):
- "Tulis transaksi" (560): sub "Satu kalimat dengan jumlahnya. Waktu dan rekening boleh ditulis."; `.input.t-lead` 56 px; "Contoh" (`t-meta`) and `.quick-chips`; the preview `.box` "Terbaca" with a `t-fig` amount and a `.dl`; footer quiet "Isi manual", grow, "Batal", primary "Catat" (disabled without a reading).
- "Catat transaksi" (560): full-width `.seg` of the three modes; `.form-stack` 24 apart: "Nominal (IDR)" `.input-money`, "Tambah cepat" chips; expense: "Bayar dari", "Kategori" (grouped by pillar), "Pilar Kakeibo" segment + hint, `details` "Detail tambahan · waktu, target, tagihan, bukti" with time, goal + hint, debt + hint, note, receipt + hint; income: "Masuk ke", "Kategori pemasukan", "Waktu", note; movement: "Arah perpindahan" (`1fr 24px 1fr` with an arrow), "Biaya / bunga (opsional)" + hint when Ke is credit, "Waktu pemindahan", note; footer quiet "Tulis kalimat", "Batal", "Simpan Transaksi" / "Pindahkan Saldo". A missing amount shows "Isi nominalnya dulu." under the field and focuses it.

**Shared pieces this phase reuses:** `ShellDialog` (phase 2), `Segmented` (phase 1), `MaskedMoney`, `useMoney`, `lib/format.ts`, phase 5's `RowAmount` colours (`c-expense` red, `--income`, `--transfer`) and `MovementEditFields` (Dari/Ke pickers with the credit rule), phase 5's translated `DebtAllocationEditor` if phase 5 translated it, phase 8's `openMovementCapture` and `credit.ts` (`creditFacts`, the `terms` keys `limitLeft`, `overLimit`, `leftToPay`, `billStatus.*`), phase 9's `DuePlanActions`, phase 10's `Switch`-free form parts (`.form-grid`, `.form-stack`).

**Dialog ownership** (read from the planned changes on 2026-11-08; this phase takes everything no earlier phase takes):

| Phase | Dialogs it rebuilds or removes |
| --- | --- |
| 3 Beranda (merged) | `ExplanationDialog` ("Rincian") |
| 4 Tinjau | category, "Jawab sendiri" and history-suggestion dialogs, the phone item sheet |
| 5 Transaksi | transaction detail and "Ubah transaksi" (`TransactionDrawer` → `TransactionDialog`, `TransactionEditDialog`), filter, "Nominal tidak sama", "Jadikan Top up Investasi"; removes `InternalMovementModal` |
| 6 Analisis | category manager, budget dialog, Peta Aliran node and link details |
| 7 Laporan | "Lengkapi klasifikasi" |
| 8 Rekening | account detail and forms, "Cocokkan saldo", investment trade, top-up and amount-tracking dialogs; adds `openMovementCapture` |
| 9 Target | goal and debt dialogs, payment, "Rencana & Pengingat"; extracts `DuePlanActions` |
| 10 Pengaturan | "Hubungkan ponsel", "Cabut", API key confirmation, inline removals; Keluar out of Lainnya |
| **11 Catat (this)** | Catat (sentence and manual), notification popover, theme menu, account menu, Lainnya, Bantuan, toast frame, "Rencana jatuh tempo"; removes `Modal`, `Drawer`, `InfoHelp`, `AccountOptions`, the `explain.tsx` popover and the dialog `untranslated` path |

## Goals / Non-Goals

**Goals:** one capture dialog that matches the prototype; H11 complete; every request unchanged except `credit_terms`; English and Indonesian; the unowned global surfaces measured and the leftover dialog primitives removed.

**Non-Goals:** parsing, timing, linking or receipt rules; the transaction edit dialog; page bodies; the token sweep (phase 12).

## Decisions

### D1. One dialog with two views

`CaptureDialog` is one `ShellDialog` (560, `aria-labelledby` its title) whose body is either the sentence view ("Tulis transaksi") or the manual view ("Catat transaksi"). "Isi manual" and "Tulis kalimat" swap the view in place: the title changes, focus moves to the new view's first field, and a polite announcement says the view's title. State survives a swap while the dialog is open: the sentence and its reading, and the manual form's values. The amount read from the sentence fills the manual amount when that is still empty (today's rule).

Rejected:
- Two dialogs as today: a second layer, two focus returns, two loading paths.
- Tabs for the two views: the prototype uses footer quiet buttons; a tab list would add a third level of choice above the mode segment.

### D2. Entry points

| Entry | Opens |
| --- | --- |
| "+ Catat", N (single-key shortcuts on), phone tab | sentence view |
| `openQuickAdd(type)` (Laporan data quality, Pemasukan shortcuts) | manual view in that mode |
| `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` (Rekening "Bayar tagihan", Target & Tagihan, Perlu tindakan, bell) | manual Perpindahan with the given Dari, Ke and amount |

`openMovementCapture` keeps phase 8's signature in the workspace context; only its target changes from `QuickCaptureModal` to `CaptureDialog`. It never posts. `openMovement` on the app context (no caller) is removed. The dialog is still loaded lazily after idle, as `CatatDialog` is today.

Rejected: opening the sentence view for `openMovementCapture` (the caller knows the movement; typing it again is a step back).

### D3. Sentence view

- Field: `input.input.t-lead` 56 px, label "Kalimat transaksi" (visually hidden, as the prototype) with the description line as the dialog's meta; `maxLength` 200 (`CAPTURE_TEXT_LIMIT`); placeholder "makan siang 35rb pakai gopay" / "lunch 35k".
- Preview: unchanged request and timing (300 ms after typing stops, aborted on change; a 429 keeps the last reading). The `.box` (D5) sits in a polite live region. Without text it reads "Hasil terbaca muncul di sini sebelum disimpan."
- Refusals, unchanged rules: no amount → "Jumlah belum ada. Tulis jumlahnya, misalnya 5rb."; several amounts → "Ada lebih dari satu jumlah. Tulis satu jumlah saja."; "Catat" disabled. Preview failure → "Belum bisa membaca sekarang. Teks tetap bisa dicatat." (spec text "Belum bisa baca sekarang" and the code's "Pratinjau belum tersedia…" reconciled into one neutral sentence).
- "Catat" sends `POST /api/capture/text` with the same `client_capture_id` on retry. A follow-up question replaces the field and the preview inside the same view: the server's question text (account language, `localize-server-text`), its options as secondary buttons, the free-text answer field when allowed. The footer then shows "Batal" only.
- Recorded: the dialog closes and the toast reads "Dicatat: {description} {amount}, {time}." with "Urungkan" while the undo window is open; undo toasts "Catatan dibatalkan.". Unchanged.

Rejected: recording from the preview without the server (the server decides; the preview writes nothing).

### D4. "Contoh" chips

Three example sentences per interface language from the `capture.examples` catalogue, each a `.quick-chips` secondary button (32 px, 44 px on a phone). Choosing one fills the field, focuses it, and the normal preview runs. Examples name no account, so they never point at an account the owner lacks.

| Indonesian | English |
| --- | --- |
| makan siang 35rb | lunch 35k |
| parkir 5rb jam 2 | parking 5k yesterday |
| gaji masuk 8jt | coffee 18k |

Amounts avoid a decimal comma, which a parser can read as a group separator ("8,5jt"). Task 3.2 runs each example through `POST /api/capture/parse` on a seeded account and asserts an amount and no `several_amounts`. An English sentence the parser cannot read is replaced before merge, never shipped.

Rejected: the prototype's sentences (they name its accounts); examples built from the owner's account names (a typed sentence with real names would appear in captures of the page).

### D5. "Terbaca" box and amount colours

`.box` with `t-label` "Terbaca", the amount in `t-fig` (Inter 20/600 tabular), then a `.dl`:
- Jenis: Pengeluaran / Masuk / Pindah saldo (`terms.*`);
- Rekening: the account hint, plus "· dibayar nanti" when it is a credit account (D11), or "ditentukan saat dicatat" in `--warning` text;
- Kategori and Pilar: the category hint, or "dikenali saat dicatat" in `--warning` text;
- Tanggal: the time reading ("Hari ini, 12.37", "Kemarin, 14.00").

Amount colour by meaning, with the sign always in the text: expense "−Rp 35.000" `c-expense` (red, owner feedback; phase 5 D29), income "+Rp" `--income`, movement "Rp" `--transfer`. Hidden balances render "Rp ••••••" (D17).

Rejected: ink expense amounts (the prototype; the owner asked for red); chips per fact (today; the prototype's list reads faster and has labels).

### D6. Manual view

- **Mode:** `Segmented` full width, `role="radiogroup"` labelled "Jenis catatan", options Pengeluaran · Pemasukan · Perpindahan (`terms.spending`, `terms.income` as the kind "Pemasukan", `terms.transfer` as "Perpindahan"). Arrow keys move and select. Changing mode keeps the amount and clears mode-only fields, as today.
- **Amount:** "Nominal (IDR)" label; `input.input.input-money` (56, Inter 20/600, −0.01em, tabular), `inputmode="numeric"`, digits grouped with dots; an expression (`+ − * /`) is kept as typed and "= Rp 40.000" shows as the field's hint only while an operator is present (D tactile "Inline Arithmetic in the Amount"). Read-only while a debt split sets the total, as today. Missing amount: "Isi nominalnya dulu." in `.err` under the field, `aria-invalid`, focus.
- **Tambah cepat:** `.quick-chips` "+10k +20k +50k +100k +500k" in both languages (the prototype's labels), labelled "Tambah cepat"; hidden while a debt split sets the amount. "Reset" is removed: the field clears with the keyboard.
- **Pengeluaran:** "Bayar dari" (liquid and credit accounts), the credit block when it is credit (D9), "Kategori" (expense categories grouped by pillar in `optgroup`s, as the prototype), "Pilar Kakeibo" segment (five pillars; follows the category until the owner picks one, as today) with its hint line, then "Detail tambahan" (D7).
- **Pemasukan:** "Masuk ke" (liquid accounts), "Kategori pemasukan", "Mengurangi pengeluaran" with its hint when the category is a reimbursement (kept), "Waktu", "Catatan (opsional)", "Bukti transaksi (opsional)" with its hint (kept).
- **Perpindahan:** phase 5's `MovementEditFields` for Dari → Ke (`1fr 24px 1fr` with the arrow; below 480 px stacked with a down arrow, deviation 4), the credit bill line and "Bayar sisa tagihan" (D12), "Biaya / bunga (opsional)" with its hint when Ke is credit, "Waktu pemindahan", "Catatan (opsional)". The credit rules (one side credit at most) stay in the shared fields.
- **Account options:** `AccountSelectOptions` with the pocket hierarchy as `optgroup`s ("Jago · Kantong Utama · Rp 1.234.000 · bawaan"), balances masked or omitted with hidden balances (D17); credit accounts in their "Kredit" group.
- **Footer:** quiet "Tulis kalimat" at the left, then "Batal" and the primary "Simpan transaksi" / "Pindahkan saldo" ("Menyimpan…" / "Memindahkan…" while pending); disabled without an amount.

Rejected: category icon chips (spec `mobile-native-layout` asks for them, the app never had them, the prototype uses a grouped select); keeping "Reset" (no prototype, redundant with the keyboard).

### D7. "Detail tambahan"

A native `details` with the quiet summary "Detail tambahan · waktu, target, tagihan, bukti" holding, for Pengeluaran: "Waktu transaksi", "Kaitkan ke target (opsional)" + hint, "Bayar tagihan / utang (opsional)" (`DebtAllocationEditor`, split rows kept) + hint, "Catatan (opsional)", "Bukti transaksi (opsional)" + hint. It opens by itself when one of its fields has a value from outside (for example a goal passed by a caller), holds an error, or the owner opened it earlier in the same dialog session. The goal options lose the 🎯 emoji. Goal and debt stay exclusive, as today ("Pilih target tabungan atau tagihan, bukan keduanya"). The debt editor stays hidden for a credit account (a credit purchase is not a debt payment, `add-credit-accounts` D7).

Rejected: the prototype's "+ Tambah tagihan" link (a dialog over a dialog; debts are added in Target & Tagihan); all fields always visible (today; the owner's capture is usually amount, account and category).

### D8. Hints replace info icons

The three `InfoHelp` controls go. One `.hint` line each, associated with `aria-describedby`:
- Pilar: the pillar's own description (`kakeibo.*`, as today's help text), changing with the pillar;
- Kaitkan ke target: "Pengeluaran yang dikaitkan mengurangi progres target.";
- Bayar tagihan / utang: "Nominal mengurangi sisa utang; kelebihan dicatat sebagai pengeluaran biasa." (the split rows keep their totals line);
- Biaya / bunga: "Bagian dari nominal yang menjadi biaya atau bunga. Dicatat sebagai pengeluaran Biaya kredit.";
- Bukti: "JPG, PNG, WebP atau PDF, paling besar 5 MB. Transaksi tetap tersimpan jika unggahan gagal.".

This changes spec "Concise Labels and Accessible Contextual Help", which asked for information controls.

### D9. Paying with credit (H11) — flagged: scope

When "Bayar dari" is a credit account, a `fieldset` follows it:
- legend "Bayar pakai kredit" with "Sisa limit Rp 1.566.000" (`limit_left`) on the same line; "Melewati limit Rp 120.000" in `--warning` text when the amount exceeds `limit_left`, and "Melewati limit" when `limit_left` is already negative. The purchase is still saved, as `add-credit-accounts` D5 says; the existing over-limit toast after saving stays.
- `Segmented` radiogroup "Sekali bayar · 3x · 6x · 12x · Lainnya". "Lainnya" shows "Jumlah cicilan" (numeric, 2 to 36, `.err` "Isi 2 sampai 36 cicilan." outside the range).
- "Jatuh tempo pertama" date field: its value is the derived first due date (D10) until the owner edits it; "Pakai tanggal tagihan" (quiet) appears after an edit and restores the derived date. Without a `payment_day` the field starts empty with the hint "Tanggal bayar rekening ini belum diatur. Isi tanggalnya, atau atur di Rekening & Kantong."; an empty date is allowed.
- One hint line (D11) under the date.

Request: today's `POST /api/transactions` body plus `credit_terms: { installment_count }` for every expense on a credit account (1 for "Sekali bayar"), and `first_due_date` (local `YYYY-MM-DD`) only when the owner has set or edited the date. A date before the purchase's local date is refused in the form with "Jatuh tempo pertama tidak boleh sebelum tanggal transaksi." (the same rule as `credit_terms_due_before_purchase`, whose server message also renders through `errors.<code>`). Switching "Bayar dari" to a non-credit account removes the block and the field from the request.

This is H11 verbatim (credit design D12):
- *Requirement: Paying With Credit.* When an expense's account is a credit account, Catat SHALL show "Bayar pakai kredit" ("Pay with credit") with "Sisa limit", a choice of "Sekali bayar · 3x · 6x · 12x · Lainnya" ("Pay in full · 3 · 6 · 12 · Other"), and the first due date. It SHALL send `credit_terms`.
- *Requirement: Paying a Credit Bill.* Perpindahan into a credit account SHALL offer "Bayar sisa tagihan {amount}" ("Pay the amount left {amount}") for the next bill, and SHALL keep the "Biaya / bunga" field.

Rejected:
- Sending `credit_terms` only when it differs from the default: H11 says "SHALL send `credit_terms`"; a count-1 row with no date is the server's paid-in-full default, so the two are equal in every figure, and always sending keeps the purchase's terms explicit.
- Sending the derived date as `first_due_date`: it would be stored as the owner's date, which does not follow a later payment-day change (credit D2), so a preview would silently become a fixed date.
- A 1–36 slider or a free number only: the prototype's language is segments; "Lainnya" covers the rest.

### D10. Due date and instalment preview in the browser — flagged: money presentation

`components/capture/creditTerms.ts` implements, as pure functions on local dates:
- `derivedFirstDueDate(purchaseLocalDate, statementDay, paymentDay)`: the credit spec's steps 1–3 (bill day `S` or 1; the first `S` after the purchase date, clamped; the first `paymentDay` on or after it, clamped; `null` without a payment day);
- `instalmentAmounts(amount, n)`: `floor(amount / n)` each, the remainder on the first.

The purchase date is the form's "Waktu transaksi" in Asia/Jakarta, as the server reads it. The values are shown, never sent unless the owner edits the date (D9). Tests run the spec's worked examples: S=1, P=11 (5 Nov → 11 Dec; 1 Dec → 11 Jan), S=25, P=5 (20 Nov → 5 Dec; 25 Nov → 5 Jan), month-end clamping (S=31 in February), Rp 1.000.000 in 3 → 333.334, 333.333, 333.333; and a parity test compares the helper with the server's `next_due_date` for 40 seeded purchases (task 4.3).

Rejected:
- No date before saving ("jatuh tempo mengikuti tagihan"): the owner's concept is that the dialog says when the money is due.
- A preview endpoint: a backend change in a phase whose gate requires an untouched backend.

### D11. "Dibayar nanti" — flagged: money presentation

The credit block's hint line says what happens to cash:
- Sekali bayar: "dibayar nanti · {account} · jatuh tempo {date}" ("pay later · {account} · due {date}");
- n cicilan: "dibayar nanti · {n} cicilan mulai {date} · {first} per cicilan · tanpa bunga" ("pay later · {n} installments from {date} · {first} each · no interest"); when the first instalment carries a remainder it reads "{first} lalu {rest} per cicilan" ("{first}, then {rest} each");
- no due date: "dibayar nanti · {account} · jatuh tempo belum diatur".

The sentence preview's Rekening row adds "· dibayar nanti" for a credit account. Nothing else changes: the purchase is Pengeluaran on its date (credit D3, approved by the owner on 2026-11-05 and 2026-11-06), and the real money out is the later bill payment.

Rejected: "Pengeluaran ditunda" or moving the purchase out of Pengeluaran (contradicts credit D3); no wording (the owner asked that the UI make clear no cash left).

### D12. Paying a credit bill (H11) — flagged: scope

When Ke is a credit account:
- the dialog meta reads "Tagihan {account} {left_to_pay} jatuh tempo {date}" from `next_due` ("hari ini", "besok", or the date; "terlewat {date}" in `--warning` text when overdue); without `next_due` it reads "Tidak ada tagihan berjalan untuk {account}.";
- under the amount, a secondary chip "Bayar sisa tagihan {left_to_pay}" fills the amount with `next_due.left_to_pay`; it is hidden when `left_to_pay` is 0 or no bill exists;
- "Biaya / bunga (opsional)" stays, with today's rule (not more than the amount) and request field `fee`.

`openMovementCapture` with an `amount` fills the amount on opening; the owner still chooses "Pindahkan saldo". The prototype's prefilled note "Bayar tagihan SPayLater" is not written: notes are the owner's words, and the ledger already shows the payment as "bayar tagihan" (phase 5, H5).

Rejected: filling the bill amount automatically whenever Ke becomes credit (the prototype; an owner paying part of a bill would have to clear it); recording the payment from the chip.

### D13. Validation and errors

- Client checks render message keys (`capture.errors.*`): amount, account, time, note length, goal-or-debt, distinct debts, split total, fee over amount, instalment range, due date before purchase, same accounts. `movementFormSchema` returns codes (`source_required`, `target_required`, `amount_invalid`, `date_required`, `notes_too_long`, `same_account`), so the shared movement fields of phase 5 render the same keys.
- API errors render through `ApiError.message` (`errors.<code>`). `same_account` from `POST /api/movements` (after `localize-server-text`) is shown under "Ke" and focuses it; other movement codes show in the form's alert; `credit_terms_*` codes show under their credit field. A code the catalogue lacks shows the server's `message`; the phase adds no code.
- Errors sit under their field (`.err`, `aria-invalid`, `aria-describedby`) or in one `role="alert"` line above the footer for request failures, never in a hint.

### D14. Saving, confirmation and toasts

Unchanged rules from spec "Quick Capture Save Confirmation": the primary button shows "✓ Tersimpan!" / "✓ Berhasil dipindahkan" ("✓ Saved" / "✓ Moved") and the dialog closes after about 450 ms; a receipt upload failure keeps it open with "Transaksi tersimpan, tetapi bukti belum terunggah: {reason}" and "Coba unggah lagi"; the over-limit toast; the query invalidations. The check glyph is text, without the zoom animation (phase 1 removed decorative motion).

### D15. Phone, focus and motion

Below 768 px the dialog is the bottom sheet (`ShellDialog`), max-height 92vh, footer buttons sharing the width. On open:
- from 768 px the sentence field or the amount field takes focus (`data-autofocus`), as today;
- below 768 px focus goes to the dialog title, so the soft keyboard does not cover the context (spec "Mobile Bottom-Sheet Quick Capture" item 6); the field is one tap away.

The view swap and the mode change do not animate; `prefers-reduced-motion` keeps the sheet's entry still.

### D16. Keyboard

Enter in the sentence field records (when allowed); Enter in a manual field submits the form; Escape closes the dialog and returns focus to the opener (the top bar button, the tab, the page action, or the bell item); Tab is trapped. The segments use arrow keys; "Contoh" and "Tambah cepat" chips are buttons. N does nothing while the dialog is open.

### D17. Hidden balances

With "Sembunyikan saldo" on: the account options omit balances (as the prototype), the "Terbaca" amount, "Sisa limit", the bill line, "Bayar sisa tagihan" and the instalment line read "Rp ••••••" with the hidden name. Typed amounts stay visible in the field the owner types into.

### D18. Language

- Namespace `capture`: `sentence`, `examples`, `preview`, `manual`, `modes`, `fields`, `hints`, `credit`, `bill`, `errors`, `saved`, `toast` (Appendix A).
- Shared components used here that no earlier phase translated (`AccountSelectOptions` empty options, `DebtAllocationEditor`) move their strings to `common.accounts` and `transactions.debtSplit` (the namespace phase 5 created, or a new `debtSplit` if phase 5 left it untranslated); task 1.2 records which.
- Figure and concept names come from `terms` (Pengeluaran, Pemasukan, Pindah saldo, Sisa limit, Melewati limit, Sisa tagihan, Jatuh tempo, Cicilan, Sekali bayar, dibayar nanti), as the credit glossary (credit D14).
- The server's follow-up questions arrive in the account's language (`localize-server-text` D5); user data (account, category, goal and debt names) is shown as stored.

### D19. Remaining global surfaces

No page owns these; phase 2 built them and `add-language-system` translated them. This phase measures each against the prototype and fixes only the differences found, recording each:
- **Notifikasi** popover (400): `t-label` header, `.notif` rows with the 8 px unread dot (`--accent-text`) and "(baru)" for screen readers, title 14/400, meta 12 "{detail} · {day} {time}", anchored under the bell, right-aligned; phone: from Lainnya as a sheet.
- **Tema** menu (220): `menuitemradio` items with icons sun, moon, monitor and the check; ↑/↓.
- **Akun** menu (240): name 14 and username 12 muted, separator, "Profil" (→ `/settings/profil`), "Keluar".
- **Lainnya** sheet (480): rows 56 px with 20 px icons and chevrons, counts as `t-meta` ("1 baru"), Notifikasi and Tema rows with their state; no Keluar (phase 10).
- **Bantuan & pintasan** (560): three numbered lines, the key table with `.kbd` caps, the Tampilan note. Once every page is translated, the page shortcut groups lose their `lang="id"`.
- **Toast:** min-h 48, padding 8 8 8 16, radius 12, `--dialog`, bottom centre 32 px (phone: above the tab bar + 16), one at a time, 6.5 s (phase 2's `TOAST_MS`; the prototype's 6 s is recorded as a deviation if it differs), `role="status"`, Z = Urungkan.

### D20. "Rencana jatuh tempo" on `ShellDialog` — flagged: scope

The due-plans view opened from the bell and from Beranda moves from the old `Drawer` to `ShellDialog` (560, sheet below 768) titled `shell.duePlans.title` with its subtitle as meta, holding phase 9's `DuePlanActions` per due plan and the link to "Rencana & Pengingat" (880, phase 9). Every request (link, skip, record) is unchanged. With its two callers moved, `Drawer` has no user and is removed (D22).

Rejected: leaving the `Drawer` (the last user of a pre-redesign primitive with its own focus code).

### D21. The untranslated registry

- The `untranslated` prop leaves `ShellDialog`; `useUntranslated(explicit)` loses its argument; `Modal` and `Drawer` go with their `untranslated` handling. No dialog carries `lang="id"` or the notice any more.
- `untranslatedPages.ts` drops any route that is already in `TRANSLATED_ROUTES` (for example `/review` or `/ledger` if phases 4 and 5 left their entries), so the registry lists only routes still untranslated.
- The page-level machinery (`UntranslatedRegion`, `UntranslatedNotice`, `untranslatedPages.ts`, `TRANSLATED_ROUTES`) stays while `/komponen` uses it; phase 12 removes it with the development sheet's title in the catalogue.

Rejected: removing the page machinery here (it still has a user and an approved requirement, "Untranslated Pages Inside a Translated Shell", that phase 12 retires).

### D22. Shared pieces and removed elements — flagged: scope

**New:** `components/capture/CaptureDialog.tsx`, `SentenceView.tsx`, `ManualView.tsx`, `CreditTermsFields.tsx`, `creditTerms.ts`, `examples.ts`; CSS `.input-money`, `.quick-chips`, `.field .hint`, `.field .err`, the `details` summary as a quiet button, from `styles.css` where earlier phases did not add them.

**Reused:** see Context.

| Removed | Destination |
| --- | --- |
| `CatatDialog`, `QuickCaptureModal` and their tests | `CaptureDialog` and its tests (the old cases rewritten, none dropped) |
| `components/ui/Modal.tsx` (last users: the two capture dialogs, after phase 10) | `ShellDialog` |
| `Drawer` in `components/workspace/overlays.tsx` (last users: the due-plans views) | `ShellDialog` (D20); the toast stays in `overlays.tsx` |
| `components/ui/InfoHelp.tsx` (last users: Catat and `DebtAllocationEditor`) | hints (D8) |
| `FormSection` (and `PendingSubmitButton` if unused) in `FormField.tsx` | `.form-stack`, the dialog footer button |
| `components/ui/AccountOptions.tsx` (no importer on `main`) | — |
| `components/workspace/explain.tsx` popover and `ExplainProvider`, when phases 6 and 8 have removed `ExplainableFigure`'s last users | `ExplanationDialog` (phase 3) |
| `openMovement` on the app context | `openMovementCapture` |
| The boxed amount, "Reset" chip, 🎯, the check zoom animation, the grey `FormSection` boxes | D6, D7, D14 |
| `.ws-catat`, `.ws-catat-*`, `.ws-tx-error`, `.ws-tx-actions`, `.ws-seg-on` and `ws-btn-accent`/`ws-btn-ink` where no user remains | the prototype's classes |
| The `untranslated` prop and its dialog paths | D21 |

Each removal is checked with `rg` first (task 5.6). A component that still has a user is kept and the user is named in `verification.md`; nothing is deleted on a plan's word.

### D23. Dependencies and order

Merged into `main` before this phase starts: `localize-server-text` (`same_account` and the other codes), `add-credit-spending-flow` (the credit API and `errors` codes), phases 4 to 9 (shared rows, `RowAmount`, `MovementEditFields`, `openMovementCapture`, `DuePlanActions`), and phase 10 `apply-markas-pengaturan` (the pairing dialogs off `Modal`; Keluar out of `MoreSheet`). A planned contract that differs when this phase starts is adapted to and recorded; a missing one stops the phase (task 1.2).

### D24. Verification tooling

`scripts/verify/markas/`, local only:
- **Scenarios** (prototype and app, same seed): `catat` (empty, typed example, refusal, question), `catat-manual` (expense; with details open), `catat-pemasukan` (with reimbursement), `catat-perpindahan`, `catat-perpindahan-kredit` (bill line, chip, fee), `catat-kredit` (expense on credit: Sekali bayar, 3x, Lainnya 18, no payment day, over limit; app only), the phone sheet, hidden balances, loading of accounts, a request failure; `notifikasi`, `tema`, `akun`, `lainnya`, `bantuan`, `rencana-jatuh-tempo`, a toast.
- **`catat-checklist.js`:** dialog 560, header 24 24 16, body 0 24 24 with 24 gaps, footer 16 24 with a 1 px line; sentence field 56 and `t-lead`; chips 32 (44 phone); segment items 30 (40 phone); money input 56 and Inter 20/600; fields 40 with the 1 px `--field-focus` edge and 3 px `--ring` on focus and no outline; `.hint` and `.err` 12/16; `.box` radius 12 padding 24; `.dl` 160 column; the global surfaces' values of D19.
- **`catat-keys.mjs`:** eleven flows: N → sentence → example → Catat → question answered → toast → Urungkan; sentence → "Isi manual" (amount kept) → "Tulis kalimat"; expense with pillar override and details (goal); expense with a three-debt split; income with reimbursement and receipt; movement with fee into credit; credit purchase 3x with an edited date; `openMovementCapture` from Rekening and from the bell (prefill, focus, Escape returns focus to the opener); Lainnya → Tema; Bantuan via `?`; due plans from the bell. Each checks the focus trap and the focus return.
- **Request recorder:** method, path and body of every request the flows send, on `main` (with `QuickCaptureModal`) and on the branch, compared field by field.

### D25. Spec reconciliation

| Spec | Requirement | Change |
| --- | --- | --- |
| `text-capture` | Web Catat Dialog | MODIFIED: one dialog with two views, examples, preview box, refusal and unavailable lines, both languages |
| `tactile-pulse-ui` | 3-Second Rapid Transaction Capture Pad | MODIFIED: grouped category select, amount focus, reconciled with the app |
| | Quick Capture Linking to Goals and Obligations | MODIFIED: inside "Detail tambahan", hints, debt hidden for credit |
| | Inline Arithmetic in the Amount | ADDED (moved from the keypad requirement) |
| `accessible-financial-input-forms` | Concise Labels and Accessible Contextual Help | MODIFIED: hints, no information controls |
| `mobile-native-layout` | Mobile Bottom-Sheet Quick Capture | MODIFIED: the app's grouped select, focus rule, one dialog |
| `kazz-ui-and-kakeibo` | Tactile Calculator Keypad | REMOVED (the app has no keypad; `web-api-key-management` "Quick Capture Without Calculator Keypad" already forbids it) |
| `credit-accounts` | Paying With Credit, Paying a Credit Bill | ADDED, H11 verbatim |
| | Credit Terms From Catat, Credit Purchase Says No Cash Left | ADDED (D9 to D11) |

"Quick Capture Save Confirmation", "Capture Parse Preview", "Undo Window", the time requirements and `internal-movement-api` keep their behaviour and wording.

### D26. Change name

The change is `apply-markas-catat`, as the owner's brief names it; the phased plan and the credit hand-off call it `apply-markas-catat-dialogs`. Both mean phase 11. H11's text is unchanged.

## Decisions and their defaults

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08). Every decision takes its recommended default. **Flag** marks a decision that touches money presentation or goes beyond the phased plan's scope; it is reviewed before implementation (task 1.1).

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Structure | One `ShellDialog` 560, two views swapped in place | Two dialogs; tabs | — |
| D2 | Entry points | Sentence for Catat/N/tab; manual for `openQuickAdd` and `openMovementCapture` | Sentence for every entry | — |
| D3 | Sentence view | 56 px field, live preview, inline questions, same requests | Local-only preview | — |
| D4 | Examples | Three catalogue sentences per language, parser-checked | Prototype's sentences; owner's names | — (deviation 3) |
| D5 | Preview and colours | `.box` + `.dl`; expense red with "−" | Ink expense; fact chips | — (deviation 2) |
| D6 | Manual view | Segment, 56 px money input, chips, mode fields, grouped categories, stacked Dari/Ke < 480 | Icon chips; keep "Reset" | — (deviations 4, 5) |
| D7 | Details | `details` that opens itself when needed; no "+ Tambah tagihan" | Always open; nested dialog | — (deviation 6) |
| D8 | Hints | One hint line per field; no info icons | Keep `InfoHelp` | — |
| D9 | Pay with credit (H11) | Block under Bayar dari; `credit_terms` always for credit, date only when edited | Send only when not default; send derived date | **Flag: scope** |
| D10 | Due date preview | Browser helper with the spec's rules, tested against the server | No date; preview endpoint | **Flag: money presentation** |
| D11 | "Dibayar nanti" | One hint line; preview row suffix; no figure change | Defer spending; no wording | **Flag: money presentation** |
| D12 | Pay a credit bill (H11) | Meta line, "Bayar sisa tagihan" chip, fee kept, prefill from callers | Auto-fill; auto-note | **Flag: scope** |
| D13 | Errors | Keys for client checks; `errors.<code>`; `same_account` under Ke | Server strings as is | — |
| D14 | Confirmation | Unchanged button state and timing; no zoom | Prototype's instant close | — (deviation 7) |
| D15 | Phone and focus | Sheet; focus the title below 768 | Autofocus everywhere | — |
| D16 | Keyboard | Enter, Escape, trap, return, arrows | — | — |
| D17 | Hidden balances | Options without balances; masked figures | — | — |
| D18 | Language | `capture`; shared strings to their namespaces | Strings in components | — |
| D19 | Global surfaces | Measure and fix differences only | Rebuild | — |
| D20 | Due plans | `ShellDialog` 560 with `DuePlanActions` | Keep `Drawer` | **Flag: scope** |
| D21 | Untranslated registry | Dialog path removed; page machinery to phase 12 | Remove all now | **Flag: scope** |
| D22 | Removed elements | As the table, each checked with `rg` | Keep unused primitives | **Flag: scope** |
| D23 | Dependencies | Server text, credit, phases 4–10 merged first | Stacked branches | — |
| D24 | Tooling | Scenarios, checklist, eleven key flows, request recorder | Existing seed only | — |
| D25 | Specs | As listed | — | — |
| D26 | Name | `apply-markas-catat` | `apply-markas-catat-dialogs` | — |

No default changes how a figure is computed, a capture parsed or timed, a movement posted or a debt or goal linked. D10 and D11 are flagged because they decide how credit money is presented before saving; D9 and D12 because H11 adds a request field and a fill action; D20 to D22 because they go beyond the plan's wording.

## Risks / Trade-offs

- [Browser due date differs from the server's] Clamping, time zone or a later rule change could make the preview differ. → One helper with the spec's examples, the 40-purchase parity test (task 4.3), Asia/Jakarta dates, and the server's value shown everywhere after saving (phase 5's rows and dialog). A mismatch found later is a bug in the helper, never a stored value, because the derived date is not sent.
- [Credit change or server text not merged] → Task 1.2 stops; no credit field is faked.
- [Always sending `credit_terms`] A purchase recorded here gets a terms row; one from a notification has none. → Both mean paid in full (credit D1); phase 5's dialog shows either the same way; recorded in `verification.md`.
- [One dialog holds two flows] A bug in the swap could lose typed input. → D1's state rule with tests for both directions and for Escape mid-question.
- [Removing shared primitives] Another phase may still use `Modal`, `Drawer`, `InfoHelp` or `explain.tsx`. → `rg` before each removal; keep and name the user if one remains.
- [English examples] The parser is Indonesian-first. → Task 3.2's parse check; replacement before merge.
- [Copy volume] About 180 strings move to keys. → A rendered-text snapshot of the Indonesian dialog before and after (task 3.9).

## Migration and rollback

No schema, API or stored-data change. Rollback is a revert of the merge commit. Terms rows written through `credit_terms` stay valid for the server and for the previous release (which ignores them in Catat and shows them in phase 5's dialog).

## Open Questions

None. Every flagged item takes its default under the standing instruction; task 1.1 records the review outcome.

## Appendix A. Copy as message keys (Indonesian · English)

Arguments arrive formatted. `{count}` selects plurals (English `one`/`other`, Indonesian `other`). Existing Indonesian text is kept byte for byte unless marked "changed". Strings not listed (the split editor's totals, account option fallbacks, receipt failure states) move under the named namespaces with their Indonesian unchanged and English written with Appendix B; task 3.1 lists them in full.

### `capture.sentence`, `capture.preview`

| Key | Indonesian | English |
| --- | --- | --- |
| `sentence.title` (changed) | Tulis transaksi | Write a transaction |
| `sentence.meta` | Satu kalimat dengan jumlahnya. Waktu dan rekening boleh ditulis. | One sentence with the amount. You may add the time and account. |
| `sentence.label` | Kalimat transaksi | Transaction sentence |
| `sentence.placeholder` (changed) | makan siang 35rb pakai gopay | lunch 35k |
| `sentence.examples` | Contoh | Examples |
| `sentence.record` · `recording` | Catat · Mencatat… | Record · Recording… |
| `sentence.manual` | Isi manual | Fill in manually |
| `sentence.noAmount` | Jumlah belum ada. Tulis jumlahnya, misalnya 5rb. | No amount yet. Write the amount, for example 5k. |
| `sentence.severalAmounts` | Ada lebih dari satu jumlah. Tulis satu jumlah saja. | There is more than one amount. Write one amount only. |
| `sentence.previewFailed` (changed) | Belum bisa membaca sekarang. Teks tetap bisa dicatat. | Cannot read it right now. You can still record the text. |
| `sentence.answer` · `send` | Jawaban · Kirim | Answer · Send |
| `sentence.failed` | Belum dapat dicatat. Coba lagi. | Could not record it. Try again. |
| `sentence.answerFailed` | Jawaban belum tersimpan. Coba lagi. | The answer was not saved. Try again. |
| `preview.empty` (changed) | Hasil terbaca muncul di sini sebelum disimpan. | What is read appears here before it is saved. |
| `preview.label` | Terbaca | Read as |
| `preview.kind` · `account` · `category` · `pillar` · `date` | Jenis · Rekening · Kategori · Pilar · Tanggal | Type · Account · Category · Pillar · Date |
| `preview.accountLater` | ditentukan saat dicatat | chosen when recorded |
| `preview.categoryLater` | dikenali saat dicatat | recognized when recorded |
| `preview.payLater` | dibayar nanti | pay later |
| `toast.recorded` | Dicatat: {label}{when, select, none {} other {, {when}}}. | Recorded: {label}{when, select, none {} other {, {when}}}. |
| `toast.undone` · `undoFailed` | Catatan dibatalkan. · Gagal membatalkan. | Recording undone. · Could not undo. |

### `capture.manual`, `capture.modes`, `capture.fields`, `capture.hints`

| Key | Indonesian | English |
| --- | --- | --- |
| `manual.title` | Catat transaksi | Record a transaction |
| `manual.kindLabel` | Jenis catatan | Entry type |
| `modes.expense` · `income` · `transfer` | Pengeluaran · Pemasukan · Perpindahan | Spending · Income · Transfer |
| `fields.amount` | Nominal (IDR) | Amount (IDR) |
| `fields.amountTotal` | Total bayar (IDR) | Total paid (IDR) |
| `fields.equals` | = {amount} | = {amount} |
| `fields.quickAdd` | Tambah cepat · +10k · +20k · +50k · +100k · +500k | Quick add · +10k · +20k · +50k · +100k · +500k |
| `fields.payFrom` · `incomeTo` | Bayar dari · Masuk ke | Pay from · Paid into |
| `fields.category` · `incomeCategory` | Kategori · Kategori pemasukan | Category · Income category |
| `fields.pillar` | Pilar Kakeibo | Kakeibo pillar |
| `fields.offset` · `offsetNone` | Mengurangi pengeluaran · Belum dipilih | Reduces spending in · Not chosen |
| `fields.details` (changed) | Detail tambahan · waktu, target, tagihan, bukti | More details · time, goal, bill, receipt |
| `fields.time` · `transferTime` (changed) | Waktu transaksi · Waktu pemindahan | Time · Transfer time |
| `fields.goal` · `goalNone` (changed) | Kaitkan ke target (opsional) · Tidak dikaitkan | Link to a goal (optional) · Not linked |
| `fields.debt` | Bayar tagihan / utang (opsional) | Pay a bill or debt (optional) |
| `fields.note` | Catatan (opsional) | Note (optional) |
| `fields.receipt` | Bukti transaksi (opsional) | Receipt (optional) |
| `fields.direction` · `from` · `to` | Arah perpindahan · Dari rekening · Ke rekening | Direction · From account · To account |
| `fields.fee` | Biaya / bunga (opsional) | Fee / interest (optional) |
| `hints.goal` (changed) | Pengeluaran yang dikaitkan mengurangi progres target. | Linked spending reduces the goal's progress. |
| `hints.debt` (changed) | Nominal mengurangi sisa utang; kelebihan dicatat sebagai pengeluaran biasa. | The amount reduces what is left on the debt; any extra is recorded as ordinary spending. |
| `hints.fee` | Bagian dari nominal yang menjadi biaya atau bunga. Dicatat sebagai pengeluaran Biaya kredit. | The part of the amount that is a fee or interest. Recorded as Credit fee spending. |
| `hints.receipt` (changed) | JPG, PNG, WebP atau PDF, paling besar 5 MB. Transaksi tetap tersimpan jika unggahan gagal. | JPG, PNG, WebP or PDF, up to 5 MB. The transaction is kept if the upload fails. |
| `hints.offset` | Uang ganti ini bukan pemasukan. Pilih kategori pengeluaran yang dibayar kembali. | This refund is not income. Choose the spending category it pays back. |
| `manual.sentence` | Tulis kalimat | Write a sentence |
| `manual.save` · `saving` (changed) | Simpan transaksi · Menyimpan… | Save transaction · Saving… |
| `manual.move` · `moving` (changed) | Pindahkan saldo · Memindahkan… | Move balance · Moving… |
| `manual.retryUpload` (changed) | Coba unggah lagi | Try the upload again |
| `saved.transaction` · `transfer` | ✓ Tersimpan! · ✓ Berhasil dipindahkan | ✓ Saved · ✓ Moved |

### `capture.credit`, `capture.bill`

| Key | Indonesian | English |
| --- | --- | --- |
| `credit.legend` | Bayar pakai kredit | Pay with credit |
| `credit.limitLeft` | `terms.limitLeft` (Sisa limit) {amount} | Limit left {amount} |
| `credit.overLimit` | Melewati limit {amount} | Over limit by {amount} |
| `credit.plan` | Sekali bayar · 3x · 6x · 12x · Lainnya | Pay in full · 3 · 6 · 12 · Other |
| `credit.count` · `countRange` | Jumlah cicilan · Isi 2 sampai 36 cicilan. | Number of installments · Enter 2 to 36 installments. |
| `credit.firstDue` | Jatuh tempo pertama | First due date |
| `credit.useBillDate` | Pakai tanggal tagihan | Use the bill date |
| `credit.noPaymentDay` | Tanggal bayar rekening ini belum diatur. Isi tanggalnya, atau atur di Rekening & Kantong. | This account has no payment day. Enter the date, or set it in Accounts & Pockets. |
| `credit.dueBeforePurchase` | Jatuh tempo pertama tidak boleh sebelum tanggal transaksi. | The first due date cannot be before the transaction date. |
| `credit.lineFull` | dibayar nanti · {account} · jatuh tempo {date} | pay later · {account} · due {date} |
| `credit.lineInstallments` | dibayar nanti · {count} cicilan mulai {date} · {first} per cicilan · tanpa bunga | pay later · {count} installments from {date} · {first} each · no interest |
| `credit.lineInstallmentsRemainder` | dibayar nanti · {count} cicilan mulai {date} · {first} lalu {rest} per cicilan · tanpa bunga | pay later · {count} installments from {date} · {first}, then {rest} each · no interest |
| `credit.lineNoDate` | dibayar nanti · {account} · jatuh tempo belum diatur | pay later · {account} · no due date yet |
| `credit.overLimitToast` | Melewati limit {account} | Over the {account} limit |
| `bill.meta` | Tagihan {account} {amount} jatuh tempo {when} | {account} bill {amount} due {when} |
| `bill.overdue` | Tagihan {account} {amount} terlewat {date} | {account} bill {amount} overdue since {date} |
| `bill.none` | Tidak ada tagihan berjalan untuk {account}. | No open bill for {account}. |
| `bill.payLeft` | Bayar sisa tagihan {amount} | Pay the amount left {amount} |

### `capture.errors`

| Key | Indonesian | English |
| --- | --- | --- |
| `amountRequired` (changed) | Isi nominalnya dulu. | Enter the amount first. |
| `amountInvalid` | Masukkan nominal yang valid | Enter a valid amount |
| `accountRequired` | Pilih rekening / dompet | Choose an account or wallet |
| `sourceRequired` · `targetRequired` | Pilih rekening sumber · Pilih rekening tujuan | Choose the source account · Choose the target account |
| `sameAccount` | Rekening sumber dan tujuan tidak boleh sama | The source and target accounts must differ |
| `timeRequired` | Pilih waktu transaksi | Choose the time |
| `noteTooLong` | Catatan maksimal 500 karakter | A note can have at most 500 characters |
| `goalOrDebt` | Pilih target tabungan atau tagihan, bukan keduanya | Choose a savings goal or a bill, not both |
| `distinctDebts` | Pilih tagihan yang berbeda untuk setiap baris. | Choose a different bill on each row. |
| `feeOverAmount` | Biaya tidak boleh melebihi nominal. | The fee cannot be more than the amount. |
| `bothCredit` | Salah satu sisi harus rekening bank, e-wallet, atau tunai. | One side must be a bank, e-wallet or cash account. |
| `needAnotherAccount` | Tambahkan rekening likuid lain untuk memindahkan saldo. | Add another cash account to move money. |
| `saveFailed` | Gagal menyimpan transaksi | The transaction could not be saved |

Server codes (`same_account`, `credit_terms_*`, `insufficient_funds` and the rest) render from the existing `errors` namespace; this phase adds none.

### Global surfaces

Their copy lives in `shell`, `nav` and `common` and is not changed by this phase; a measured difference that needs a new string is listed in task 6.1 with both languages.

## Appendix B. Glossary (Indonesian → English)

Consistent with `add-language-system` Appendix A, the credit change's D14 glossary and phases 4 to 10.

| Indonesian | English | Note |
| --- | --- | --- |
| Catat · Catat transaksi | Record · Record a transaction | the action; never "Log", "Add expense" |
| Tulis transaksi · Tulis kalimat | Write a transaction · Write a sentence | the sentence view |
| Isi manual | Fill in manually | |
| Terbaca | Read as | the preview's label |
| Pengeluaran | Spending | `terms.spending`; never "Expense" |
| Pemasukan · Masuk | Income | `terms.income` |
| Perpindahan · Pindah saldo | Transfer | `terms.transfer`; between own accounts |
| Nominal | Amount | |
| Bayar dari · Masuk ke | Pay from · Paid into | |
| Pilar Kakeibo | Kakeibo pillar | pillar names from `terms` |
| Kaitkan ke target | Link to a goal | |
| Tagihan · Utang | Bill · Debt | |
| Biaya / bunga | Fee / interest | |
| Bukti transaksi | Receipt | |
| Bayar pakai kredit | Pay with credit | credit D14 |
| Sisa limit · Melewati limit | Limit left · Over limit | `terms` |
| Sekali bayar · Cicilan | Pay in full · Installment | US spelling |
| Jatuh tempo (pertama) | (First) due date | |
| dibayar nanti | pay later | row and form status |
| tanpa bunga | no interest | 0% installments |
| Tagihan {akun} | {account} bill | |
| Bayar sisa tagihan | Pay the amount left | |
| Sisa tagihan | Left to pay | |
| Urungkan | Undo | `common.undo` |
| Simpan transaksi · Pindahkan saldo | Save transaction · Move balance | |
| Tambah cepat | Quick add | |
| Contoh | Examples | |
