## Approval

Approved by the owner on 2026-10-29 ("approved, build the backend"). Owner decisions recorded the same day:
- When the text names no account, learn it from history and ask when unsure.
- Confident captures count as reviewed.
- Offer "Batalkan" for 10 seconds.
- Phone only: no web input. The backend endpoint serves the companion.

## Why

Cash spending and anything without a bank notification (parking, street food, cash top-ups, money handed to friends) is never captured. Today the owner must open the web form. The goal is to type one line on the phone, such as "bayar parkir ptc 10rb", and have it recorded correctly. The app should ask only what it cannot work out.

## Expected Outcome

- The owner types a short Indonesian sentence in the companion.
- When the amount, the account and the classification are certain, the transaction is recorded with an informative name, category and Kakeibo pillar, counted as reviewed. The owner has 10 seconds to press "Batalkan".
- When something is missing or uncertain, the app asks only for that detail, with tappable choices, then records.
- "tarik tunai 200rb" or "pindah 50rb ke GoPay" become own-account transfers, not spending.
- Every answer teaches the merchant memory, so the next "parkir ptc" needs no questions.

## What Changes

- A new backend capability, `text-capture`, with these endpoints:
  - `POST /api/capture/text` takes the text, a client capture id and the device time. It returns either `recorded` or `needs_detail` with one question.
  - `POST /api/capture/{id}/answer` answers the question. It returns `recorded` or the next question.
  - `POST /api/capture/{id}/undo` works within the undo window.
  - `GET /api/capture/{id}` lets the phone refresh after being offline.
- Amounts, dates and own-account mentions are parsed by deterministic code, which understands Indonesian shorthand such as "10rb", "1,5jt", "25k", "kemarin" and "tunai". The model never sets an amount or a date.
- A new strict-schema provider call with its own prompt version proposes the merchant, description, type, category, pillar and an account suggestion. Owner merchant memory is consulted first, and a memory hit skips the model.
- Migration V33 adds:
  - a `text_captures` table holding the text, status, draft, open question, linked transaction, model and prompt version;
  - the transaction source value `capture` and the feedback kind `capture`;
  - the remembered account of a merchant memory entry (`merchant_memory.account_id`), which D2 and D3 read.
- Recorded captures write classification feedback and merchant memory through the existing owner-edit path.

## Non-goals

- The companion UI. It follows the owner's choice among the five redesign options, in a separate mobile change.
- A web input (owner: phone only).
- Splitting one sentence into several transactions. For "bayar parkir 10rb dan kopi 25rb" the app asks the owner to send them one at a time.
- Production authority. Verification is local. Deployment is through the pipeline after the owner merges.

## Capabilities

### New Capabilities

- `text-capture`: free-text transaction capture with deterministic money facts, model classification, targeted follow-up questions, undo and learning.

### Modified Capabilities

- `transaction-provenance`: the source list gains `capture`.

## Impact

- **Backend:**
  - new `routers/capture.py`, `services/text_capture.py`, `services/capture_resolution.py`, `services/capture_parsing.py`;
  - provider method `capture_normalization` with the prompt `prompts/capture_normalization.md`;
  - updates to `services/provenance.py`, `services/merchant_memory.py` (reuse) and settings (`CAPTURE_CONFIDENCE_THRESHOLD`);
  - the transaction and movement delete paths move into `services/ledger_mutations.py`, shared by the ledger endpoints and undo;
  - tests.
- **Database:** `db/migrations/V33__text_captures.sql`.
- **API:** additive endpoints only.
- **Cost:** one model call per capture, none when owner memory matches. It uses the existing provider and model allowlist.
