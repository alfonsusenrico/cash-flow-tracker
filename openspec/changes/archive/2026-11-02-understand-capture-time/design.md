# Design

## Context

`parse_date` in `capture_parsing.py` read three relative phrases and two date forms; everything else took `device_time`. The `text-capture` spec forbade the model from setting a date. Captures are recorded by `text_capture.record_plan` from `draft.facts.date`. The phone (`financial-tracker-mobile-listener`, read-only check at `35dfbeb`) parses `CaptureView` with Gson, which ignores unknown fields, and accepts a `needs_detail` view only when the question type is one of `amount`, `kind`, `account`, `category` (`CaptureRules.isAnswerable`). An unknown type makes the view invalid, and the capture is resent until it changes.

## Decisions

### D1. A deterministic time reader of pieces

The folded text is scanned for pieces, in this order so that longer phrases win: explicit dates (`dd/mm[/yy]`, `3 okt [2026]`, `tgl N`), weekdays (`senin` … `sabtu`, `hari minggu`, `minggu pagi`, with `kemarin`/`lalu`/`minggu lalu`), vague periods (`minggu lalu`, `bulan lalu`, `last week`), `N hari lalu`, relative days (`kemarin`, `kemaren`, `kmrn`, `kemarin lusa`, `hari ini`, `yesterday`, `today`), offsets (`2 jam lalu`, `30 menit yang lalu`, `sejam lalu`, `an hour ago`), clock times (`jam 2`, `jam 14.30`, `pukul 9`, `jam dua`, `jam setengah tiga`, `jam 2 lewat 15`, `jam 2an`, `jam 2 siang`, `2pm`, `14:30`, `tengah malam`), then parts of the day (`semalam`, `tadi malam`, `tadi pagi`, `sore tadi`, `pagi ini`, `this morning`), `barusan`/`sekarang` and `tadi`. A bare part of the day counts only next to another time word, and never after `makan` ("makan siang" is a meal). The pieces' spans are removed from the amount search, the description and the memory key.

Rejected: a model-first reading (the model would guess times the owner can check only after the fact, and every capture would pay a model call); a third-party date parser (none reads Indonesian phrases such as "jam 2 malam" or "setengah tiga", and it would add a dependency).

### D2. The resolution rule for 12-hour ambiguity

Times are read in the configured `TZ` (Asia/Jakarta, no daylight saving; the arithmetic is wall-clock and stays correct in a zone that has it).

1. Readings. A stated hour of 13–23, 0 or with a leading zero is 24-hour. An hour of 1–12 reads as both halves (h and h+12; 12 reads as 00 and 12). A part of the day keeps only the readings in its range: pagi 00–10, siang 10–15, sore 14–19, malam 18–03, dini hari 00–05, subuh 03–06, am 00–11, pm 12–23. With `malam`, 00–03 belongs to the night of the named day, so "kemarin jam 1 malam" is 01:00 today. No reading in range is a conflict.
2. Days. A named past day (`kemarin`, a weekday, a date, `N hari lalu`) gives only that day (mode *anchored*). `tadi`, `hari ini` or `tadi siang` give today, and `tadi malam`/`semalam` today and yesterday (mode *today*). Otherwise today, yesterday and the day before (mode *unanchored*).
3. Never in the future. A candidate after the device time is dropped, except that one at most 15 minutes ahead becomes the device time (the owner rounding "jam 2" at 13:55).
4. Plausibility. A reading from 00:00 to 04:59 is implausible unless a part of the day or a 24-hour form names it, or it is at most 6 hours before the device time (the owner is clearly awake).
5. Choice. *Unanchored* keeps candidates from the last 24 hours and takes the most recent plausible one, but asks when it is more than 12 hours old and another candidate exists. *Today* takes the most recent plausible candidate. *Anchored* takes the one plausible candidate and asks when there are two, because recency says nothing about a day the owner named. With no plausible candidate, a single candidate is taken and several are asked.

Behaviour change: "tadi malam" sent in the evening (for example at 22:00) is now 20:00 today; the old reader always took 20:00 yesterday. Sent in the morning it is still 20:00 yesterday.

Worked examples (all tested): "jam 2" at 15:00 → 14:00 today; at 01:00 → 14:00 yesterday; at 03:00 → 02:00 today; at 10:00 → ask 02:00 today / 14:00 yesterday. "jam 8" at 07:00 → 20:00 yesterday. "kemarin jam 2" → 14:00 yesterday; "kemarin jam 8" → ask. "jam 11 malam" at 00:30 → 23:00 yesterday.

Rejected: always the most recent past reading (at 10:00 "jam 2" would be 02:00, which the owner called "not logical enough"); always a daytime reading (wrong for a night owl at 03:00); asking every 12-hour time (the owner wants it understood without explaining).

### D3. What is asked, and what stays an error

`minggu lalu` alone, `bulan lalu`, two clock times, an offset with a day, contradicting parts, a stated time in the future ("tgl 2 jam 14" at 10:00 on the 2nd) and an ambiguous hour are asked. Impossible or future explicit dates and two different named days stay 422 `invalid_capture_date`, as before. "senin minggu lalu" names Monday of the previous calendar week.

### D4. Basis and evidence

Every capture moment has a basis: `device_time` (no time words), `parsed` (the reader), `model` (the accepted fallback) or `owner` (an answer, or a later date edit). Evidence is the owner's own words the moment was read from, joined in text order ("tadi jam 2"), and exists only for `parsed` and `model`. `owner` is added to the three bases in the brief because an answered or edited time is neither parsed nor guessed.

### D5. The model fallback

A phrase the reader cannot place is flagged only from a small lexicon: prayer times (`abis maghrib`, `isya`), activity anchors (`pas makan siang`, `abis pulang kerja`) and a clock word before an unreadable number (`jam 25`, never a price such as `jam 150rb`). Only then is the model asked: in the same classification call, or for the time alone when merchant memory already classifies. The context gains `sent_at`, `sent_weekday`, `time_phrase` and `time_day`; the strict schema gains `time: {span, local_datetime, confidence}` (prompt `capture-normalization-2`).

The reading is used only when every check passes: the time was unresolved; the span is the owner's words (case and spacing aside) and contains the flagged phrase; the datetime has an offset and is not after the device time; it falls on the parsed day or explicit date when there is one, and is otherwise within 31 days; and the confidence reaches the capture threshold (default 0.8). A reading that passes everything but the confidence is offered as the first chip. Otherwise the owner is asked. The model's output can never set an amount: the schema has no amount and the amount comes from the parser alone. Logs record only the outcome code (`accepted`, `low_confidence`, `span_not_in_text`, …).

Rejected: using the model for every capture with a time (cost and latency for phrases the parser already reads); letting the model resolve D2 ambiguity (it has no more evidence than the text, and the owner asked to be asked when a person would ask). Evaluation: no live model call is permitted in this job; the fallback is covered by mocked-provider tests of acceptance and each rejection. A capture evaluation set with real model runs is a documented follow-up before the owner relies on the fallback in production.

### D6. The `time` question

It is asked right after the amount and before the kind, account and category, because it is a fact of what happened, not a classification. Prompt "Jam berapa?" ("Hari apa?" for a day question), up to 7 chips labelled like "Hari ini 14.00", "Kemarin 14.00" or "Min 25 Okt 15.00", `allows_text = true`. An unresolved, conflicting or future phrase also offers the device time (or the named day at the device's time of day). A typed answer is read by the same parser with the open phrase, first after it and then before it ("malam" with "kemarin jam 8" is 20:00 yesterday, "senin" with "minggu lalu" is Monday of last week), then alone. An answer that does not resolve, names a later day (`besok`, `nanti`) or lies after the device time is 422 `invalid_capture_answer`. Answers stay idempotent per reply id and stale questions return 409 as before.

### D7. Older phones (compatibility)

`POST /api/capture/text` accepts an optional `question_types`. Without it the client is treated as answering only the original four types, and a time the reader cannot decide is recorded as before: at the device time (basis `device_time`), or on the named day at the device's time of day (basis `parsed`). This keeps the deployed companion from receiving a question it rejects and resending the capture. A client that sends `question_types` with `time` gets the question. Unknown types are 422 `invalid_question_types`.

Rejected: always asking (the deployed phone would loop on the capture); a version-header check (`X-Companion-Version`) (couples the backend to app version numbers instead of a declared ability).

### D8. Storage and edits

Migration V40 adds `transactions.time_basis VARCHAR(12) NULL` (check: `device_time`, `parsed`, `model`, `owner`) and `transactions.time_evidence VARCHAR(200) NULL` (check: only with `parsed` or `model`). Only capture recording writes them; both legs of a typed movement carry the same values. Every other source, notifications included, keeps NULL and its own date. An owner edit that changes a row's date sets `time_basis = 'owner'` when it was set and clears the evidence. Captures stored before V40 keep their parsed date with a NULL basis, and in-flight drafts without a time reading are resolved as before.

Rollback: revert the code; the columns can stay (old code never reads them) or be dropped with their checks. No existing row is changed.

### D9. Response fields

`summary` gains `time_basis`, `time_evidence` and `time_label` (for example "Hari ini 14.00"), and its `date` is null while the time is being asked. The capture's `transaction` (and both movement legs) gain `time_basis` and `time_evidence`. Gson ignores them on the current phone; the web does not show capture summaries, so no frontend change is needed.

### D10. Suggestions

`capture_suggestions` reused only a prefill text without date words, using the three old patterns. It now uses the reader's full phrase list, so "parkir tadi jam 2 10rb" is never offered as a prefill.

## Risks

- The lexicon reads phrases that are not times in rare texts ("beli jam 2 buah" reads a clock time). The summary label shows the time so the owner notices, and an edit makes it `owner`.
- Migration numbers: other Phase 0 branches may also add V40; whichever merges second must renumber (Flyway rejects a duplicate version).

## Open decisions for owner

1. Older phones keep the device time for a time the reader cannot decide (D7), until the monorepo phone declares `time`. The conservative alternative, asking anyway, would make the deployed phone resend the capture.
2. `owner` is a fourth basis beyond the brief's three (D4).
3. Times are read in the server `TZ`, not the phone's offset, to match ledger days and reports; per-user time zones belong to the multi-user work.
4. "kemarin jam 8" is asked rather than taken as 20:00 (D2, anchored mode). If this asks too often, the alternative is the most recent plausible reading for named days too.
