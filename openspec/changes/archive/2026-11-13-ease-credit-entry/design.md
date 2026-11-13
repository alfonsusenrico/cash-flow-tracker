## Context

`parse_time` collects day pieces from explicit dates, weekdays, "N hari lalu" and relative days, and raises `several_dates` when they name more than one day. `WEEKDAY` already absorbs a following "kemarin"/"lalu" as a qualifier; `DAY_DATE` and `MONTH_DATE` do not, so `RELATIVE_DAY` later reads the same "kemarin" as yesterday. "tanggal 11 yang lalu" already works only because "lalu" alone is not a relative day.

`ManualView` shows `CreditTermsFields` when the chosen expense account is a credit account. `AccountSelectOptions` with `includeCredit` appends credit accounts in a final "Kredit" group.

## Decisions

1. **Absorb a past word into the date it follows.** An optional `\s+(yang\s+|yg\s+)?(kemarin|kemaren|kmrn|kmarin|yesterday|lalu)` closes `DAY_DATE` and `MONTH_DATE`, so the span covers it and `RELATIVE_DAY` skips it. A day number keeps resolving to the most recent such day not after today (`latest_day`); an explicit month date keeps its own rules, including future dates being refused. Rejected: dropping `several_dates` when a relative day disagrees with an explicit date. "kemarin tgl 11" on the 13th really is contradictory and should still be asked about.
2. **The choice is derived, not stored.** "Bayar nanti" is on exactly when the chosen account is a credit account. Switching to it selects the first credit account and clears debt rows (a credit purchase keeps no debt link, as today); switching back selects the first cash account. Rejected: a separate boolean, which could disagree with the account.
3. **A two-option segment, not a checkbox.** `RadioSegments` is the form's existing control for one-of-few choices and is keyboard and screen-reader ready. Labels: "Kapan bayar": "Bayar sekarang" / "Bayar nanti (kredit)"; English "When to pay": "Pay now" / "Pay later (credit)".
4. **Each choice lists only its accounts.** `AccountSelectOptions` gets `creditOnly`, which renders only the credit accounts as plain options. "Bayar sekarang" uses the existing liquid list without credit. Rejected: keeping credit accounts under "Bayar sekarang", which would let the two controls disagree.
5. **Shown only when it can apply.** No active credit account means no choice and today's form unchanged.

## Failure behaviour

Unchanged: the server still validates the account and terms. A stale credit account that disappears falls back through the existing first-choice effect.

## Risks

"tgl 11 lalu" written to mean something else is not a realistic phrase. The prefix "yang" is optional, so "tgl 11 yang kemarin" is also read as the past 11th.

## Rollback

Revert the commits. No data or API change.
