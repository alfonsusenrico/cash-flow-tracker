# Design

## Context

The review queue (`routers/review.py`) lists every expense and income row whose `review_status` is `auto` or `needs_review`. Internal movements recorded by the model are created `auto/ai` (`notification_application.py`), and pairing two legs resets `needs_review` to `auto` (`movement_pairing.py`), so a finished movement stays in the queue as a non-editable "Pindah saldo antar rekening" group until the owner presses "Terima semua". The badge, the summary and the monthly report count those legs too.

An unlinked outgoing leg in the "Internal Movement" category that the pairing flagged `awaiting_counterpart` is shown under "Pindah saldo menunggu pasangan", whose picker lists liquid accounts only. A debit that bought a mutual fund can be fixed only in the ledger edit dialog ("Jadikan Top up Investasi", `POST /api/investment-topups/from-transaction`, `convert_transaction_to_topup`). The conversion keeps the debit as the outgoing leg, keeps its notification link and idempotency key, adds the product leg, raises the product's value and cost basis, and stores the original classification in `investment_topups.converted_from` so that deleting the top-up restores the expense.

Learned memory today: merchant memory (exact merchant keys) and habits (`services/habits.py`, owner labels over `classification_feedback`, at least 3 labels and 90% weighted agreement, corrections weigh 2, `HABIT_AUTO_RECORD_ENABLED`). Neither looks at movement rows.

## Decisions

### D1. Linked legs are excluded at query time

The queue query, the summary and the monthly report's `review_counts` add `t.movement_id IS NULL` (the summary and report keep a linked leg only while it carries an open purpose question, which is attention by definition). The summary now reuses the monthly report's single count query, so the badge, the Tinjauan header and the report cannot drift apart.

Nothing is rewritten: the legs keep `auto`, the ledger still labels their provenance honestly, and a later unlink brings an unlinked leg back into the queue with its stored status.

Rejected: recording system-completed movements as `reviewed`. It changes provenance for new rows only, so old legs would still need a data migration that rewrites review states, and an AI-recorded movement is not something the owner confirmed. Both options hide the rows; the query exclusion is the one that does not rewrite history.

### D2. Tinjauan sections and counts

The API keeps one `groups` list with per-group `needs_review_count` and `auto_count`. The page shows a group under "Perlu jawaban" when it has at least one `needs_review` row, otherwise under "Dicatat otomatis", a collapsed `<details>` element whose summary says it is optional. A mixed group stays together under "Perlu jawaban", so "Terima semua" still acts on what the owner sees.

The queue response adds `counts.attention` and `counts.unreviewed`, computed by the same query as `GET /api/review/summary`. The header shows "<attention> perlu jawaban" and, separately, "<unreviewed> dicatat otomatis (opsional)". The movement group kind and the `editable` flag that only that kind needed are removed.

### D3. Top-up option on an awaiting card

For an awaiting outgoing leg whose account is liquid, the card adds "Ini top up investasi". It opens a product list from `GET /api/accounts`: amount-tracked mutual-fund products (`listAmountInvestmentProducts`), labelled "Induk · Produk". The choice calls the existing `POST /api/investment-topups/from-transaction`. Unit-tracked mutual funds (`canSwitchToAmountTracking`) are named with a link to Rekening, where "Ubah ke pelacakan nominal" lives; switching tracking is irreversible and changes the product's units, so it is not done from Tinjauan. The account picker for an ordinary movement stays as it is.

In the ledger edit dialog the conversion block moves directly under the type toggle, ahead of amount and category, so at 390 px it is on the first screen.

`GET /api/transactions` returns `recurring_rule_id`. The web check `canConvertExpenseToTopup` already reads it, and the server refuses a conversion of a recurring-linked expense, so returning the field is the contract the check was written against.

### D4. Top-up evidence

A label is one transaction's latest state for the pattern key (owner, source account, exact amount):

- **Confirmation:** a live (not deleted) top-up converted from that transaction (`converted_from ->> 'transaction_id'`), whose outgoing leg is still that transaction and was not recorded by the learner itself (`classified_by <> 'habit'`). Weight 1, or 2 when the owner chose it after rejecting another suggested product (a correction).
- **Rejection:** the owner pressed "Bukan top up" for that transaction and it is not converted now. Weight 2, against every product of the key.

Confirmations are derived from `investment_topups` at read time. That is the backfill: the October conversions count as soon as the code ships, and nothing is copied or rewritten. Only the owner's answers to suggestions, which exist nowhere else, are stored (D6). Undoing a top-up the owner made removes its confirmation; it is not read as a rejection (an automatic one is, D8).

When both the label's and the current debit's notifications have a `match_key`, they must be equal; when either has none, the account and amount alone decide. Only the latest 10 labels of a key count, as for habits.

Rejected: a memory table written on each conversion. It would duplicate `investment_topups`, need its own undo handling, and a backfill migration would derive the same rows.

### D5. Per-product agreement

Within a key, each product P has:

- `support(P)`: confirmations of P;
- `agree(P)`: their weight;
- `against(P)`: the weight of rejections of the key plus corrections that moved away from P.

P is **learned** when `agree(P) > against(P)`. P qualifies for **automatic recording** when `support(P) >= 3` and `10 * agree(P) >= 9 * (agree(P) + against(P))` (the habit thresholds). Products are ranked by agreement weight, then support, then name and id. A product that is archived, foreign, unit-tracked or has children is skipped.

Agreement is per product rather than per key, because two products filled from the same account with the same amount in the same month are both correct: confirming one product is not evidence against the other.

### D6. Suggestion outcomes are stored

Migration V43 adds `investment_topup_feedback (id, user_id, transaction_id, source_account_id, amount, outcome, suggested_target_account_id, chosen_target_account_id, created_at)`, with `outcome` in `accepted`, `corrected`, `rejected`, foreign keys to the owner's user, transaction and accounts with `ON DELETE CASCADE` (feedback has no meaning without its transaction), and an index on `(user_id, source_account_id, amount)`.

- "Ya, top up" sends the conversion with `suggested_target_account_id` equal to the product: `accepted`.
- "Pilih produk lain" sends it with the other product: `corrected`.
- "Bukan top up" calls `POST /api/review/topup-suggestions/reject`: `rejected`; the card then shows the ordinary account picker.

Feedback is written only when a conversion was newly made, so an idempotent replay of the conversion writes nothing; a repeated rejection whose latest outcome is already `rejected` writes nothing either.

### D7. One-to-one assignment and the window

The window of a debit is ±3 days around it. For a debit d:

- **pending** = the owner's unlinked awaiting debits of the key in d's window (Internal Movement category, `needs_review`, flagged `awaiting_counterpart`, not kept separate, not rejected), ordered by date then id;
- **filled** = products that already received a live top-up from that account with that amount in d's window;
- **open** = learned products not filled, in rank order.

With one learned product, every pending debit is assigned to it. With two or more, d is assigned to `open[i]` (i = d's position in pending) only when pending and open have the same size; otherwise the owner is asked to choose among the open products, with no default. A lone debit with two learned products is therefore asked until its partner arrives; the learner never guesses which subset the owner filled this month.

The explanation names the rule: "2 debit Rp 112.590 di jendela ini dan 2 produk yang biasa diisi dari RDN Contoh: satu debit untuk satu produk, urut nama produk."

### D8. Automatic recording at ingestion

When notification recording leaves an expense unlinked with `awaiting_counterpart` (no ambiguity, no purpose question), the learner evaluates every pending debit in the new debit's window and converts each one that is assigned (D7) to a product that qualifies (D5), while `TOPUP_AUTO_RECORD_ENABLED` is true. The conversion uses the existing path with learner provenance: the outgoing leg becomes `classified_by = 'habit'`, `review_status = 'reviewed'`, no owner feedback or merchant memory is written, and the event's `interpretation.habit` stores `{kind: "investment_topup", target_account_id, support, agreeing, agreement}` so the ledger can explain it. `converted_from` additionally keeps the debit's review status and classifier, and the restore path puts them back, so undoing an automatic top-up returns the debit to Tinjauan.

Ingestion holds the owner's notification lock, so two debits of a pair are evaluated one after the other. The conversion keeps the debit's idempotency key and notification link, so a replayed notification finds the recorded event and records nothing. The funding balance is not changed (the bank debit is already recorded) and the product's value and cost basis grow by the debit's amount.

A separate switch is used rather than `HABIT_AUTO_RECORD_ENABLED`: this action moves value into a product, and the owner may want to stop it without stopping category habits. Default true, as the owner chose "learn".

### D9. Multi-user scoping

Every query of the learner filters by the owner's id, joins accounts and transactions by owner, and the conversion locks owned rows only. Another user's conversions and feedback with the same account id and amount are never read.

## Failure behaviour

- A conversion refused at ingestion (for example a product that was archived between evaluation and conversion) would roll back the whole notification transaction. The learner checks eligibility first and, to stay safe, runs the conversion in a savepoint: a refusal leaves the debit awaiting, as today, and logs only the error code.
- A suggestion whose product became ineligible is not shown; the conversion endpoint still returns its existing 409/422 codes.

## Migration and rollback

V43 only creates a table and an index. Rolling back the application leaves the table unused. Learned confirmations are derived, so there is nothing to undo. Automatic top-ups made while enabled are ordinary converted top-ups and can be deleted one by one, which restores the debit.

## Open decisions for owner

1. **Window of ±3 days.** Chosen so that a pair sent in the same minute, or on consecutive days, counts as one window. A longer window would make two top-ups to the same product in one month look like a filled slot.
2. **A product filled twice in one window.** One-to-one assignment treats a second equal debit as asking. If the owner sometimes tops up the same product twice with the same amount in three days, that second debit will be asked rather than recorded.
3. **Default of `TOPUP_AUTO_RECORD_ENABLED`.** True, following "learn". With the current history (one confirmation per product) nothing is recorded automatically before two more months of confirmations.
4. **Equal debits within 30 seconds of a converted one.** Notification ingestion treats a recorded movement leg as a possible duplicate report of the same movement when another debit of the same account and amount arrives within 30 seconds, and holds the new event as `ambiguous_existing_movement` for the owner. A converted top-up leg is such a leg. This change keeps that rule (it never double-counts). Consequence: with a single learned product, the second of two equal debits sent in the same half-minute is held rather than recorded; a pair with two learned products is unaffected because the first debit waits for its partner. Excluding top-up legs from that check would record such debits but would also record a re-posted notification twice; that is left for the owner to decide.
