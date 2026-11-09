## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Catat is the app's most used action and the last surface still built from the pre-redesign form. It is phase 11 of the phased plan (`cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md`, "Catat and the remaining global dialogs"; the plan calls the change `apply-markas-catat-dialogs`, the owner's brief names it `apply-markas-catat`).

What is wrong today:
- **Two dialogs for one action.** "+ Catat", N and the phone tab open `CatatDialog` (the sentence); "Isi manual" closes it and opens `QuickCaptureModal` (the form), a second layer with its own focus return. Both use the old `Modal` and `ws-*` buttons.
- **The form is the old one.** A boxed 30 px amount whose input has `outline-none` and no field focus, so the focused amount shows no indicator at all; three info icons (Pilar Kakeibo, Kaitan transaksi, Bayar tagihan) instead of one-line hints; grey `FormSection` boxes; a "Reset" chip; a 🎯 emoji in the goal options; a check-mark animation.
- **English accounts read Indonesian.** Both dialogs pass `untranslated` and show "This page is in Indonesian for now." in their header; the movement endpoint's refusal reaches the owner as the English sentence "Source and target accounts must be different", whatever the language.
- **Credit has no capture UI.** `add-credit-spending-flow` (archived on `feat/credit-spending-flow`) built terms, due dates and bills in the API and handed block **H11** to this phase: "Bayar pakai kredit" with the limit left, the instalment choice and the first due date, and "Bayar sisa tagihan" when paying a credit bill. Phases 8 and 9 add `openMovementCapture({ targetAccountId, amount })`, which this dialog must honour.
- **Global surfaces nobody owns.** The notification popover, theme menu, account menu, Lainnya sheet and Bantuan were rebuilt in phase 2, but no phase checks them against the prototype after the pages changed; the "Rencana jatuh tempo" view still uses the old `Drawer`; `Modal`, `Drawer`, `InfoHelp`, the dialog-level `untranslated` flag and two unused components are left with no other owner.

The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/dialogs.js` (`catat`, `manual`, `notifikasi`, `tema`, `akun`, `bantuan`, `lainnya`), `SPEC.md` §7, §11 and §13, and the captures `dlg-catat`, `dlg-catat-manual`, `dlg-catat-pemasukan`, `dlg-catat-perpindahan`, `dlg-catat-perpindahan-kredit`, `dlg-notifikasi`, `dlg-tema`, `dlg-akun`, `dlg-lainnya` and `dlg-bantuan`, each at 1440 and 390 in both themes.

## Expected Outcome

- "+ Catat", N and the phone tab open one centered 560 px dialog (a bottom sheet below 768 px) on "Tulis transaksi": a 56 px sentence field, "Contoh" chips, and a "Terbaca" box that shows the server's reading as it is typed. "Catat" records it through the capture path, asks follow-up questions inline and offers "Urungkan".
- "Isi manual" turns the same dialog into "Catat transaksi" with Pengeluaran · Pemasukan · Perpindahan, the 56 px amount (the typed amount carried over), "Tambah cepat" chips and the fields of each mode; "Tulis kalimat" turns it back. No second layer opens.
- Field focus is a 1 px edge with a 3 px soft ring on every field, the amount included. No thick or coloured border, no info icon: Pilar, Kaitkan ke target, Bayar tagihan / utang and Biaya / bunga each carry one hint line.
- Expense amounts in the "Terbaca" preview are red with "−"; income is green with "+"; movements are the transfer colour.
- **Paying with credit:** when "Bayar dari" is a credit account, Catat shows "Bayar pakai kredit · Sisa limit Rp 1.566.000", the choice "Sekali bayar · 3x · 6x · 12x · Lainnya" (2 to 36), the first due date ("jatuh tempo 11 Des", changeable) and the line "dibayar nanti · SPayLater · jatuh tempo 11 Des". Saving sends today's request plus `credit_terms`. The purchase counts in Pengeluaran on its day, as the credit change decided; the dialog says no cash left yet.
- **Paying a credit bill:** Perpindahan into a credit account shows the next bill in the subtitle, offers "Bayar sisa tagihan Rp 434.000" and keeps "Biaya / bunga". A due bill opened from Rekening, Target & Tagihan, Perlu tindakan or the bell arrives with Ke and the amount filled; nothing is recorded until "Pindahkan saldo".
- An English account reads Catat in plain English ("Write a transaction", "Record a transaction", "Spending · Income · Transfer", "Pay with credit", "Pay the amount left"). The movement refusal reads "Source and target accounts must be different." in English and "Rekening sumber dan tujuan harus berbeda." in Indonesian, under the "Ke" field.
- The notification popover, theme menu, account menu, Lainnya sheet, Bantuan and toasts match the prototype's measurements; "Rencana jatuh tempo" opens as a centered dialog.
- Every request equals today's for the same input, except the credit fields H11 adds.

## Approval

Approved under the owner's standing instruction 'complete all until finished' (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D26, summarized in "Decisions and their defaults"). Seven decisions are flagged:
- **D9 (scope, H11):** an expense on a credit account sends `credit_terms {installment_count, first_due_date?}` on `POST /api/transactions`. It is always sent for a credit expense (count 1 for "Sekali bayar"), and `first_due_date` only when the owner changes the date. No other request changes.
- **D10 (money presentation):** the first due date and the per-instalment amount shown before saving are computed in the browser with the credit change's published rules (spec "Credit Due Dates": bill day, payment day, clamping; whole rupiah with the remainder on the first instalment). The server stays the authority: the browser value is a preview, it is never sent unless the owner edits the date, and a test runs the spec's worked examples against the helper.
- **D11 (money presentation):** a credit purchase is labelled "dibayar nanti · {account} · jatuh tempo {date}" in the form and "dibayar nanti" in the sentence preview. It changes no figure: the purchase stays Pengeluaran on its date (credit change D3).
- **D12 (scope, H11):** "Bayar sisa tagihan {amount}" fills the amount with the next bill's `left_to_pay`; it never records by itself. An amount passed by `openMovementCapture` is filled on opening.
- **D20 (scope):** "Rencana jatuh tempo" moves from the old `Drawer` to `ShellDialog` (560) with phase 9's `DuePlanActions`; its requests are unchanged.
- **D22 (scope):** removed with no remaining user: `Modal`, `Drawer`, `InfoHelp`, `FormSection`, the dialog-level `untranslated` prop, `components/ui/AccountOptions.tsx` (no importer today) and the `explain.tsx` popover with `ExplainProvider` once phases 6 and 8 have removed their last users. Each is checked with `rg` first.
- **D21 (scope):** this phase removes every dialog from the untranslated path; the page-level registry (`untranslatedPages.ts`, `UntranslatedRegion`, the notice) stays for phase 12, because `/komponen` still uses it.

No decision changes how a figure is computed, how a capture is parsed or timed, how idempotency, the movement contract, goal and debt linking or receipts work.

## Deviations from the prototype

Each is a decision in `design.md` and goes to the owner's end-review list:
1. "Bayar pakai kredit" (instalments, first due date, limit left) and "Bayar sisa tagihan" come from H11; the prototype draws neither (D9, D12).
2. Expense amounts are red, by the owner's feedback (phase 5 D29); the prototype draws them in ink (D5).
3. The "Contoh" chips are fixed example sentences per interface language without account names, because the owner's accounts differ from the prototype's; English examples use only forms the parser reads (D4).
4. On a phone (below 480 px) "Dari" and "Ke" stack with a down arrow instead of two truncated selects side by side, so long pocket names stay readable (D6).
5. Pemasukan keeps "Bukti transaksi" and the "Mengurangi pengeluaran" choice for reimbursements; Pengeluaran keeps the split across several debts; the prototype omits them (D6, D7).
6. The prototype's "+ Tambah tagihan" link inside the form and its prefilled note "Bayar tagihan SPayLater" are not built: the first would stack a dialog on a dialog, the second would write a note the owner did not type (D7, D12).
7. The save confirmation stays the approved "✓ Tersimpan!" button state with the 450 ms close; the prototype closes at once with a toast (D14).
8. The sentence view keeps the server's inline follow-up questions, which the prototype does not draw (D3).

## What Changes

- **One capture dialog (D1, D2):** `components/capture/CaptureDialog.tsx` on `ShellDialog` (560) with two views, replacing `CatatDialog` and `QuickCaptureModal`. Entry points: `openCapture` (sentence), `openQuickAdd(type)` (manual in a mode), `openMovementCapture({ sourceAccountId?, targetAccountId?, amount? })` (manual Perpindahan, prefilled).
- **Sentence view (D3 to D5):** `.t-lead` 56 px field, "Contoh" chips (`.quick-chips`), the "Terbaca" `.box` with the amount figure and a `.dl` of Jenis, Rekening, Kategori, Pilar and Tanggal; the refusals and the unavailable-preview line; inline questions; footer "Isi manual" · "Batal" · "Catat".
- **Manual view (D6 to D8):** `Segmented` Pengeluaran · Pemasukan · Perpindahan; `.input-money` (56, Inter 20/600) with inline arithmetic and "= Rp …" only while an expression is typed; "Tambah cepat" +10k … +500k; per-mode fields; "Detail tambahan · waktu, target, tagihan, bukti" disclosure; one-line hints; footer "Tulis kalimat" · "Batal" · "Simpan transaksi" / "Pindahkan saldo".
- **H11 (D9 to D12):** the credit block under "Bayar dari"; the bill line, "Bayar sisa tagihan" and the fee under Perpindahan into credit.
- **Errors (D13):** API errors render `errors.<code>` (`same_account` under "Ke"); client checks become message keys; `movementFormSchema` returns codes.
- **Global surfaces (D19, D20):** measured against the prototype and corrected where they differ; "Rencana jatuh tempo" on `ShellDialog`.
- **Language (D18, D21):** a `capture` namespace in `messages/en.json` and `id.json`; the shared account options, debt split and pillar hints translated where earlier phases did not; every dialog leaves the untranslated path.
- **Removed** (each with its destination in D22): `CatatDialog`, `QuickCaptureModal` and their tests (rewritten for `CaptureDialog`), `Modal`, `Drawer`, `InfoHelp`, `FormSection`, `PendingSubmitButton` if unused, the `untranslated` prop on `ShellDialog`, `Modal` and `Drawer` and `useUntranslated(explicit)`, `AccountOptions.tsx`, the `explain.tsx` popover and `ExplainProvider` when unused, the "Reset" chip, the 🎯 emoji, the boxed amount, `.ws-catat*`, `.ws-tx-error`, `.ws-tx-actions` and `.ws-seg-on` where no user remains, `openMovement` on the app context (no caller).
- **BREAKING (spec):** `text-capture` "Web Catat Dialog", `tactile-pulse-ui` "3-Second Rapid Transaction Capture Pad" and "Quick Capture Linking to Goals and Obligations", `accessible-financial-input-forms` "Concise Labels and Accessible Contextual Help", `mobile-native-layout` "Mobile Bottom-Sheet Quick Capture", `kazz-ui-and-kakeibo` "Tactile Calculator Keypad" (removed; the app has no keypad) change their presentation. `credit-accounts` gains the two H11 requirements verbatim and two that state the request and the "dibayar nanti" wording.

## Non-goals

- No backend, API, schema, migration or data change, and no change to the Android companion. `credit_terms`, `limit_left`, `next_due`, `statement_day` and the `same_account` code exist once their changes merge. `backend/`, `db/` and `mobile/` stay untouched.
- No change to capture parsing, time rules, the model fallback, idempotency (`client_capture_id`), the undo window, the movement atomic contract, the fee booking, goal and debt linking, debt splits, receipts and their failure path, the over-limit notice, shortcut N, or the save confirmation.
- No interest model: instalments are 0% by default, as the credit change decided; provider fees are recorded with "Biaya / bunga" when paying the bill.
- No new capture kind, no category icons, no keypad.
- No page body; no transaction edit dialog (phase 5 owns `PUT/DELETE /api/transactions/{id}/credit-terms` in "Ubah transaksi").
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `text-capture`: "Web Catat Dialog" (one dialog, two views, examples, preview box, both languages).
- `tactile-pulse-ui`: "3-Second Rapid Transaction Capture Pad" and "Quick Capture Linking to Goals and Obligations"; added "Inline Arithmetic in the Amount".
- `accessible-financial-input-forms`: "Concise Labels and Accessible Contextual Help" (hints instead of information controls).
- `mobile-native-layout`: "Mobile Bottom-Sheet Quick Capture".
- `kazz-ui-and-kakeibo`: "Tactile Calculator Keypad" removed.
- `credit-accounts`: added "Paying With Credit" and "Paying a Credit Bill" (H11, verbatim), "Credit Terms From Catat" and "Credit Purchase Says No Cash Left".

`interface-language` and `ui-copy-register` need no delta: the dialogs follow their existing rules.

Standards:
- UI and accessibility (WCAG 2.2 AA): one dialog with `aria-modal`, focus trap and return to the opener, Escape, labelled segment (radiogroup), fields with labels, hints and errors associated, focus visible on every field, live regions for the preview and errors, 44 px targets on a phone, reflow at 320 px, the sign beside every coloured amount, 3.1.1 and 3.1.2;
- the copy register in both languages (`ui-copy-register`);
- `money`: requests identical except `credit_terms`; amounts, signs and colours by meaning; credit purchases stay Pengeluaran on their date; hidden balances respected in the account options and the preview;
- `api-consumers`: no API change; the credit fields are the archived contract;
- `personal-data`: no typed sentence, amount or note in logs (the capture endpoints already log codes only).

No capability in `AGENTS.md` changes.

## Impact

- **Frontend capture:** new `components/capture/CaptureDialog.tsx`, `SentenceView.tsx`, `ManualView.tsx`, `CreditTermsFields.tsx`, `creditTerms.ts` (the preview helper), `examples.ts`; removed `components/capture/CatatDialog.tsx`, `components/ui/QuickCaptureModal.tsx` and their tests; `lib/movementFormSchema.ts` (codes); `components/ui/DebtAllocationEditor.tsx` and `AccountSelectOptions.tsx` (hints, keys, hidden balances); `components/layout/AppLayout.tsx` (one dialog, `openMovementCapture`, `openQuickAdd`, due plans dialog); `components/workspace/context.tsx`.
- **Global surfaces:** `components/workspace/Notifications.tsx`, `TopBar.tsx`, `AccountMenu.tsx`, `PhoneNav.tsx`, `shortcuts.tsx` (`HelpDialog`), `overlays.tsx` (toast kept, `Drawer` removed), `ShellDialog.tsx` (`untranslated` removed), `beranda/BerandaView.tsx` (due plans dialog).
- **Removed files:** `components/ui/Modal.tsx`, `InfoHelp.tsx`, `AccountOptions.tsx`, `FormField.tsx`'s `FormSection` (and the file if nothing else uses it), `components/workspace/explain.tsx` when unused.
- **Language:** `messages/en.json` and `id.json` (`capture`, additions to `errors` only if a code is missing), the copy register roots (`components/capture`).
- **CSS:** `app/globals.css` gains `.input-money`, `.quick-chips`, `.field .hint`, `.field .err` and the `details` summary style from `styles.css` where missing; loses `.ws-catat*`, `.ws-tx-error`, `.ws-tx-actions` and other rules left without a user.
- **Tooling (local only):** `scripts/verify/markas/`: the Catat and global-dialog scenarios, `catat-checklist.js`, `catat-keys.mjs`, the request recorder over the scripted capture flows, the parity seed extended with a credit account with `statement_day` 1, `payment_day` 11, a limit and an unpaid bill, a second credit account without a payment day, goals, debts and a reimbursement category.
- **Dependencies:** `localize-server-text`, `add-credit-spending-flow` and phases 4 to 10 merge first (design D23).
- **No change** to `backend/`, `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit; credit terms written by this phase stay valid data for the server.
