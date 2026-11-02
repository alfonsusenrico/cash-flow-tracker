# Proposal

## Approval

Owner approved on 2026-11-02, as part of Phase 0: "ok I approve all you recommend" (the audit recommendations), phone direction B (a tracker companion with no financial dashboards), the monorepo move of the Android app into `cash-flow-tracker/mobile`, scope limited to the personal book, and "Phase 0 starts now". The requirement for this job is the owner's own, of 2026-11-02: "I pay for parking fee using cash maybe it's around 2PM … I might use it later … 'bayar parkir di ptc tadi jam 2' and it can write the record correctly", and "I want the agent intelligent enough to understand and be aware of the datetime context, without the user having to explain the detail specifically. Simply saying parkir at 2PM, when it sent on 3PM the AI I expected will think it's might be 2PM, there's possibility in 2AM but … it's not logical enough".

## Why

A typed capture ("Catat") only understands `kemarin`, `tadi malam`, `tadi pagi`, `tgl N` and `dd/mm[/yy]`; every other time word is ignored and the row gets the phone's send time. "bayar parkir di ptc tadi jam 2" typed at 15:00 is stored at 15:00, so the ledger, the time-of-day suggestions and the habit evidence all learn the wrong hour. The owner wants the time understood the way a person would, asked only when a person would ask.

## Expected Outcome

- "bayar parkir di ptc tadi jam 2 10rb" sent at 15:00 is recorded at 14:00 today, and the capture summary says "Hari ini 14.00" with the words it was read from ("tadi jam 2").
- "jam 2" sent at 01:00 is 14:00 yesterday; sent at 03:00 it is 02:00 today; sent at 10:00 (02:00 today or 14:00 yesterday) the owner is asked "Jam berapa?" with both as chips.
- Common Indonesian and English time words are understood without a model: clock times, parts of the day, "X jam lalu", weekdays, "kemarin sore" and a date with a time. "minggu lalu" is asked as "Hari apa?".
- A phrase the parser flags but cannot place ("abis maghrib") is read by the model only as a checked fallback; otherwise the owner is asked. No time is ever stored in the future, and the model never sets the amount.
- Every capture row stores how its time was decided (`device_time`, `parsed`, `model` or `owner`) and the owner's words it came from. Notification rows keep the notification's own post time.

## What Changes

- A deterministic time reader for captures (`services/capture_time.py`) with a documented resolution rule for 12-hour ambiguity, replacing the old three-phrase date reader.
- The capture model's strict output gains a `time` interpretation (span, local datetime, confidence); prompt `capture-normalization-2`. The model is also asked, for the time alone, when merchant memory already classifies the capture.
- A `time` question in the one-question-at-a-time flow, after the amount, with chips and a free-text answer checked by the same parser. It is asked only of a client that lists `time` in the new optional `question_types` of `POST /api/capture/text`.
- The capture summary gains `time_basis`, `time_evidence` and `time_label`; the capture's transaction view gains `time_basis` and `time_evidence`.
- Migration V40: nullable `transactions.time_basis` and `transactions.time_evidence` with checks. An owner edit of the date makes the basis `owner`.
- Capture suggestions strip every time word from a reused prefill text.

## Non-goals

- Android changes. The current companion keeps working: it ignores the added fields and, because it does not declare `time`, is never asked a time question (design D7). Showing the time and answering the question on the phone is a follow-up for the monorepo app.
- Per-user time zones. Times are read in the configured `TZ` (Asia/Jakarta), as ledger days and reports already are.
- Future-dated captures ("besok bayar listrik"), recurring plans, or changing the date of notification rows.
- Production access, deployment or production data. All verification is local on a disposable PostgreSQL with synthetic data and a mocked provider; release goes through the GitHub pipeline after the owner merges.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `text-capture`: the date and time of a capture follow the new reading, fallback and question rules ("Deterministic Money Facts" now keeps only the amount fully deterministic), the question order gains `time`, and capture rows store their time basis.

Standards: the `llm` capability's rules (a versioned prompt, strict structured output validated in code, a confidence gate, deterministic facts, content-free logs), `money` (the amount stays deterministic; no ledger effect changes), `database` (an additive Flyway migration verified V1→V40 on a fresh PostgreSQL 16), `api-consumers` (additive response fields and an opt-in request field, so the deployed phone keeps working), `personal-data` (logs carry outcome codes only). No new capability class is added. Documented follow-ups: a capture evaluation set for the model fallback before relying on it in production (design D5), and the phone's time question (D7).

## Impact

- Backend: new `services/capture_time.py`; `capture_parsing.py`, `capture_resolution.py`, `capture_normalization.py`, `text_capture.py`, `capture_suggestions.py`, `prompts/capture_normalization.md`, `routers/capture.py`, `routers/transactions.py` (the date edit rule).
- Database: `V40__capture_time_basis.sql` (additive, two nullable columns, two checks).
- API: `POST /api/capture/text` accepts an optional `question_types`; capture `summary` and `transaction` gain optional fields; a new question type `time` for clients that ask for it.
- Project context: the LLM boundary in `openspec/config.yaml` now names the checked capture-time fallback.
- Web and Android: no change; the web does not show capture summaries.
