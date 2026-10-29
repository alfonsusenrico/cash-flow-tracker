## Context

See proposal.md. The failure was reproduced locally with tests that fail on `origin/main` (2716468) and pass with this change:
- an existing Internal Movement income on the chosen account was not linked, so a third row was created;
- an ordinary income match was not offered;
- two matches were not refused;
- an older one-sided row appeared under "awaiting".

## Decisions

### D1. Awaiting means flagged

The awaiting section requires the stored review reason `awaiting_counterpart`. The phase 5 pairing writes that reason when it records a one-sided own transfer.

Older or manual one-sided rows carry no such reason. They are listed with the ordinary groups, where "Terima semua" marks them reviewed.

*Rejected:* hiding all rows created before phase 5. The cut-off date is arbitrary, and a reason-based rule also covers manual rows.

### D2. Completion looks for the existing leg first

The candidates are unlinked rows on the chosen account that meet all of these:
- the opposite type and the same amount;
- dated within ±3 days of the leg;
- not a fee row;
- not tied to a goal, debt or recurring rule.

They are ordered by date distance, at most 5, and locked.

| Candidates found | Result |
|---|---|
| Exactly one, in "Internal Movement", never split by the owner | Linked through the existing link path; both legs are marked reviewed and the notification result is refreshed. |
| Any other non-empty set | 409 `counterpart_candidates` with the candidate summaries, and nothing is written. |
| None, or the owner sent `create_new: true` | A new leg is created, as before. |

*Rejected:* linking an ordinary-category match automatically. That would silently reclassify spending or income.

*Rejected:* fee-tolerant matching. Equal-leg linking through merge needs equal amounts, and a fee case is rare enough to leave to the owner.

### D3. Idempotency

A completed movement whose partner leg is on the same chosen account returns 200 with `idempotent: true`, whether the partner was created or linked. A linked partner is reported as `linked_transaction_id` with `created_transaction_id: null`.

### D4. Tinjau

On 409 the card lists the candidates as radio rows showing date, category, notes and amount. "Hubungkan" calls `POST /api/movements/merge`. "Tetap buat baru" resends with `create_new: true`.

## Risks / Trade-offs

- [A genuine second transfer of the same amount within 3 days is offered as a candidate] → The owner chooses "Tetap buat baru". Nothing is linked automatically unless exactly one Internal Movement match exists.
- [Production may already hold a duplicate leg from a completion made before this fix] → It is detected read-only and reported to the owner. The owner removes it in the ledger; no ad-hoc production edit is made.

## Migration Plan

There is no schema change. Deploy through the normal pipeline. Roll back by reverting the merge; the API change is additive.
