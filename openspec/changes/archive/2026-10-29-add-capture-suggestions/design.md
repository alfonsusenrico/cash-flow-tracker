## Context

The capture backend (`add-ai-text-capture`, archived on this branch) stores `text_captures` and `merchant_memory` keys. Owner-entered rows have `source` set to `manual` or `capture`.

## Decisions

### D1. Candidates

Candidates are recorded or undone captures, plus transactions with `source` in (`manual`, `capture`) and `review_status = reviewed`, from the last 90 days. Movement legs and fee rows are excluded.

Candidates are grouped by merchant key: the capture match key, or the normalized `merchant`, or the normalized `notes`.

Each group carries:
- the most common amount (the mode, falling back to the latest);
- the most common category and account;
- a prefill text. The latest capture text with that amount is preferred; otherwise the text is built as "<label> <amount shorthand>", for example "parkir ptc 10rb".

### D2. Score

`score = 3·same_hour_window(±2h) + 2·same_weekday_type(weekday/weekend) + log2(1+count) + recency(1 if within 7 days, 0.5 if within 30)`

Groups whose last entry was undone are skipped. Ties break on the latest use. The top 6 are returned.

`reason` is the strongest factor: "biasanya jam segini" (time), "sering" (count ≥ 3) or "terakhir" (recency).

### D3. Privacy and bounds

The endpoint is owner-scoped. `device_time` must carry a timezone offset; a bad value gives 422. The text is at most 200 characters, as stored. No logs of text or amounts.

*Rejected:* a model-based suggestion. It would add cost and latency on every open, and counting is enough at this data volume.

## Risks / Trade-offs

- [A one-off item appears because it is recent] → It only gets a recency score, so frequent items outrank it.

## Implementation notes (2026-10-29)

These record how the decisions above were implemented where the text left room. None changes a money invariant; the endpoint only reads.

- **N1. One entry per recorded capture (D1).** A recorded capture and its transaction are one entry. The amount, category and account come from the current transaction, so owner edits count; the capture adds its text and match key. A capture whose transaction was deleted drops out. An undone capture counts only for the "last entry undone" rule, never for the count, the amounts or the score.
- **N2. Exclusions (D1).** Besides movement legs (`movement_id`) and fee rows (`fee_for_movement_id`), rows in "Internal Movement" and "Investasi" are left out, as the capture never records into them. Only `expense` and `income` rows take part.
- **N3. Clock (D2).** The reference is the request's `device_time`. Hours and weekdays are read in `settings.tz` (Asia/Jakarta), as the capture parser and the feedback features do. An entry's moment is its ledger date; for an undone capture, the date it would have recorded. The 90-day window and the recency are measured back from `device_time`.
- **N4. Time and weekday factors (D2).** `same_hour_window` and `same_weekday_type` are the share (0 to 1) of the group's entries that fit, so one evening entry does not make an evening habit a morning one. The hour window is ±120 minutes on a 24-hour circle.
- **N5. Reason (D2).** The candidates are the time contribution (when any entry fits the window), the count contribution `log2(1+count)` (when count ≥ 3) and the recency. The largest wins; a tie prefers time, then "sering". "terakhir" is the fallback when nothing else applies.
- **N6. Prefill text (D1).** A capture text is reused only when its own parsed amount and its row's current amount both equal the suggested amount and it has no date words, so "kemarin parkir 10rb" or a text whose amount was answered separately is never prefilled. A built text is the label folded to lower case, with its money and date expressions removed, then the shorthand. When the word table would read another kind, "bayar" or "terima" is put first. The label part is cut so the whole text stays within 200 characters.
- **N7. Shorthand (D1).** Whole millions and tens of thousands within millions use "jt" ("1,5jt", "1,25jt"); multiples of 10 use "rb" ("10rb", "12,5rb", "1234rb"); other amounts from 1.000 use dot groups ("1.005"); amounts from 100 to 999 stay bare; below 100 they get "rp". Every form parses back to the same amount through the capture parser.
- **N8. Label and mode.** The label is the latest entry's merchant, else its notes. The amount, category and account are each the most frequent value; a tie goes to the value used most recently. The final order is score, then the latest use, then the key.
- **N9. Device time (D3).** The same rule as `POST /api/capture/text`: an ISO-8601 value with an offset, at most 10 minutes ahead of the server. A missing, unparseable, naive or future value gives 422; the coded cases answer `invalid_device_time`.

## API contract

`GET /api/capture/suggestions?device_time=<ISO-8601 with offset, URL-encoded>` accepts the session cookie or the companion's Bearer API key and answers:

```json
{"ok": true, "suggestions": [
  {"text": "parkir ptc 10rb", "label": "Parkir PTC", "amount": 10000, "category_name": "Parkir",
   "account_name": "Tunai", "reason": "biasanya jam segini"}
]}
```

`amount` is an integer in IDR. `category_name` and `account_name` can be null; a pocket's account name is "Parent · Pocket". The list is empty for an owner without history and holds at most 6 items, best first.

| Error | When |
|---|---|
| 401 | No session or API key |
| 422 `{"ok": false, "detail": {"code": "invalid_device_time"}}` | Naive, unparseable, empty or more than 10 minutes ahead |
| 422 (FastAPI validation body) | `device_time` missing or longer than 64 characters |

## Migration Plan

None. The endpoint reads existing tables; roll back by reverting the code.

## Open decisions for owner

- **O1. Factor shares.** N4 reads the time and weekday factors as shares of the group's entries. The alternative, 1 when any entry fits, ranks an item with a single morning entry among many evening ones as a morning habit.
