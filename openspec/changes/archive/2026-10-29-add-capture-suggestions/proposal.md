## Approval

Approved by the owner on 2026-10-29 ("approved, build both"). Owner decision on 2026-10-29: the AI capture screen shows suggestions from the owner's history, never static "contoh cepat" examples.

## Why

When the capture screen opens, the most useful thing to offer is what the owner usually types at this moment. Examples: "Parkir PTC · Rp10.000" on a weekday morning, or "Kopi Senja · Rp32.000" in the afternoon. One tap then fills the input. Bank-notified spending is captured automatically, so the suggestions should come from what the owner records by hand.

## Expected Outcome

Opening "Catat" shows up to 6 suggestion chips ranked from the owner's own typed and manual history. They favour the same time of day and weekday, frequent items and recent items. Tapping one fills the input, for example "parkir ptc 10rb", and the owner can edit it before sending. A new owner with no history sees no chips and a short hint.

## What Changes

- `GET /api/capture/suggestions?device_time=…` returns at most 6 suggestions, each with:
  - `text`: the input to prefill;
  - `label`;
  - `amount`: the usual amount;
  - `category_name`;
  - `account_name`;
  - `reason`: "biasanya jam segini", "sering" or "terakhir".
- Ranking is deterministic and uses no model. The sources are recorded captures plus owner-reviewed `manual` and `capture` transactions from the last 90 days, grouped by merchant key.

## Non-goals

- Learning habits from bank notifications. That is step 9.
- Any model call.

## Capabilities

### Modified Capabilities

- `text-capture`: adds history-based suggestions.

## Impact

- **Backend:** `services/capture_suggestions.py`, `routers/capture.py`, tests.
- **Database:** no migration.
- **API:** one additive endpoint.
