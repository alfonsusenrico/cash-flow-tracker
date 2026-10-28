# Automation quality: phased plan

Date: 2026-10-28. Status: phase order approved by the owner on 2026-10-28. Each phase is
proposed as its own OpenSpec change and needs separate approval.

Findings come from `2026-10-28-automation-quality-audit.md` and from a read-only production
and device evidence pass. Production figures are kept out of this tracked note.

## Outcome

The phone captures every real transaction and drops noise. The backend and the LLM record
each transaction with a recognisable name and the correct type, category and Kakeibo pillar.
Every correction the owner makes is learned. A weekly or monthly review touches only rows
that are truly uncertain, and reports are trustworthy.

## Owner decisions (2026-10-28)

- Use the full Kakeibo set: need, want, culture and unexpected, plus saving.
- Split food into daily meals (need) and snacks/cafés (want), learned per merchant.
- A friend repaying a shared expense offsets that spending instead of counting as income.
- Ask "Apa ini?" on every payment whose notification carries no merchant name.
- Retire the Telegram bot completely.
- Rebuild the automation layer and the reports. Keep the ledger core; no full rewrite.

## Phases

| # | Phase | Scope | Gate |
|---|---|---|---|
| 1 | Correctness baseline | Full Kakeibo set end to end. One effective-pillar rule. Noise rules that keep settled payments and stop marketing/news from reaching review. A system category for reconcile adjustments. Insights status badge. Retire the Telegram bot. Companion: one-time-code filter. | Backend, frontend and companion tests. Migrated disposable PostgreSQL. Owner acceptance. |
| 2 | Provenance and review model | Transaction columns: source, merchant, description, review status, classified-by, confidence. Reimbursement handling. Backfill from existing links. | Backfill proven on a copy of production-shaped synthetic data. Reports unchanged except for the intended fixes. |
| 3 | Ingestion pipeline | Always record and flag. Merchant memory written by owner edits. LLM context with category pillars and similar history. Confidence gate. | Real-notification corpus benchmark: name, category and pillar accuracy against the owner's own corrections. |
| 4 | Companion capture layer | Classify on the device (drop, send, keep locally). Durable synchronous capture. One row per notification key. Per-row failure isolation. All app toggles. Cross-app duplicates. "Apa ini?" prompt for merchant-less payments. | Device evidence. Release after phase 3 is live. |
| 5 | Movement pairing | Fee tolerance. Deferred re-pairing. Manual rows as candidates. One-sided transfers kept as pending. | Pairing test matrix, including fees and late legs. |
| 6 | Web review and reports | "Tinjau" queue grouped by merchant, with bulk accept and recategorise. Uncategorised bucket. Calendar month view. Drill-down. | Browser and accessibility evidence. |
| 7 | History re-enrichment | AI suggestions for past rows, accepted or rejected per merchant. | Owner reviews the suggestions. Nothing changes without acceptance. |
| 8 | Cleanup | Dead backend modules. Duplicate startup DDL. Old OpenSpec changes archived. Companion dead code. | Full test suites. |
| 9 | Behaviour learning (vision) | See below. | Proposed after phases 2–6 have produced labelled data. |

## Phase 9 vision: learning habits over time

The owner's example: on most Wednesday nights they eat at the same restaurant. The first
time, the system knows nothing and asks "Apa ini?". The owner answers "makan di restoran A".
After the pattern repeats often enough, an unnamed payment with the same shape is met with a
suggestion: "Sepertinya makan di restoran A?" The owner confirms with one tap or corrects it.

What earlier phases must provide so this is possible later:

- **Phase 2:** keep, per transaction, the facts a pattern needs: local day of week and time
  of day, amount, account or channel, merchant, description, category, pillar, and who
  classified it (rule, AI or owner).
- **Phase 3:** treat every owner answer and correction as a labelled example, never
  overwritten in place. Merchant memory is the first, exact-match form of this learning.
- **Phase 4:** the "Apa ini?" prompt carries ranked suggestions as tappable choices. Phase 9
  improves the ranking without changing the interaction.
- **Phase 6:** accepting or rejecting a suggestion is recorded as feedback.

Likely approach, to be validated against real data first:

1. Score candidates statistically: merchant frequency by weekday and time window, amount
   similarity, account match, recency, and how often past suggestions were confirmed.
2. Suggest only above a confidence threshold, and fall back to plain "Apa ini?" below it.
3. Use the LLM to phrase the suggestion and to normalise free-text answers into merchants,
   not to decide confidence.
4. Show at most three suggestions. Measure the acceptance rate, and lower confidence for
   patterns that are often rejected.

Risks: few data points per pattern (a personal ledger is small), habits drift, and a wrong
suggestion accepted carelessly becomes wrong data. Mitigations: decay old evidence, require
repeated confirmations before auto-suggesting, never auto-record a suggestion without a tap.
