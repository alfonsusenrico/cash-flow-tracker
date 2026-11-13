## Why

On 2026-11-13 the owner tried to record an 11 November SPayLater purchase and hit two problems.

1. "tgl 11 kemarin, beli pedigree spaylater kredit 47619" in Tulis kalimat was refused with "Tanggal tidak valid. Pilih hari lain.". The parser read "tgl 11" as 11 November and "kemarin" as yesterday, saw two dates and refused (`several_dates`). In everyday Indonesian a past word right after a date qualifies it: "tgl 11 kemarin" is "this past 11th", as "sabtu kemarin" already is for weekdays.
2. In Catat's manual form nothing says "credit" until a credit account is chosen, and credit accounts sit in the last group of the long "Bayar dari" list. The owner found SPayLater only after asking, and said it is not easily found.

Approved by the owner on 2026-11-13: "add both for the new release".

## Expected Outcome

- "tgl 11 kemarin", "tanggal 11 kmrn" and "11 november kemarin" are read as the most recent 11th (or 11 November) and the capture proceeds.
- When the owner has a credit account, an expense in Catat shows a two-way choice "Bayar sekarang · Bayar nanti (kredit)" next to "Bayar dari". "Bayar nanti" lists only credit accounts, picks the first and shows the credit payment fields at once; "Bayar sekarang" lists only cash accounts.

## What Changes

- `capture_time`: `DAY_DATE` and `MONTH_DATE` take an optional "kemarin / kmrn / kemaren / kmarin / yesterday / lalu", with or without "yang"/"yg", right after the date as part of that date.
- `ManualView`: the "Kapan bayar" choice for expenses, derived from the chosen account; `AccountSelectOptions` gains a `creditOnly` mode.
- `en` and `id` messages for the choice.

## Non-goals

- No change to "kemarin" elsewhere: "kemarin tgl 11" and "tgl 11 beli kemarin" still name two dates and are still refused.
- The `several_dates` message is not changed.
- No change to the edit dialog, Perpindahan, credit terms, totals or the API. A credit purchase stays an ordinary expense.
- No choice is shown to an owner without a credit account.

## Capabilities

- Modified: `text-capture` (a past word after a date), `credit-accounts` (paying later in Catat).
- No new project capability.

## Impact

- `backend/app/services/capture_time.py` and its tests.
- `frontend/src/components/capture/ManualView.tsx`, `frontend/src/components/ui/AccountSelectOptions.tsx`, their tests, `frontend/src/messages/{en,id}.json`.
- No migration, no API change.
