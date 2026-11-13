## Context

`observed_endpoint_with_basis` resolves an expense's account. When the notification names no pocket of an account that has pockets, it returns the parent's default pocket with basis `default_pocket`. Jago payment wordings never name the pocket, so every such payment lands on Main Pocket. Only unnamed transfers ("You've transferred/sent") already go through the source-pocket question.

How the app learns today:
- Habits learn a category per account and text signature from owner labels in `classification_feedback`. Thresholds: support 3, agreement 90%, corrections weighted 2, the latest 10 labels.
- An edit that changes only the account writes no feedback (`record_owner_corrections` compares classification fields only). The owner's pocket corrections therefore teach nothing today.

The source-pocket question, its suggestion and the companion's preselection exist after `read-unrecognised-notifications`. This change builds on that branch.

## Goals / Non-goals

**Goals:**
- No assumed pocket for outgoing payments.
- Learn from the owner's own choices.
- Ask with a good preselection.
- The same rules for any user, account with pockets, and merchant.

**Non-goals:**
- Incoming money.
- Named pockets.
- Model-based guessing.
- Rewriting past records.

## Decisions

### 1. Which payments

An expense notification whose institution account has active pockets and whose text names none, on the path that today returns basis `default_pocket`.

Not included:
- income: the bank decides where it lands;
- named pockets;
- broker trades and RDN legs, which resolve their own accounts.

Rejected: keeping the default pocket when the owner configured one. The owner asked not to assume, and a default chosen for convenience is not evidence of which pocket paid.

### 2. Ask and hold, or record learned

- **Learned decision:** record on that pocket, provenance `pocket_habit`, with its basis stored.
- **Otherwise:** the existing source-pocket question, with Jago pockets as options and a preselected suggestion. No row is written before the answer, as for unnamed transfers today. Companions older than 1.6.0 get `needs_review`, as today.

Holding means the payment is missing from balances and Aman dipakai until it is answered. This is the owner's choice ("ask rather than assume").

Rejected: record on the default pocket and flag. It shows a wrong pocket balance in the meantime, which is the assumption the owner rejected.

### 3. Evidence: the owner's pocket choices, read from what the ledger holds

Choices are read from records that already exist, so there is no new table and nothing to backfill:

| Record | Choice | Weight |
| --- | --- | --- |
| An answer to the source-pocket question (`notification_source_pocket_answers`) | the answered pocket | 1 |
| A Tinjau resolution where the owner picked the account (`interpretation.source = manual`) | that account | 1 |
| A notification-recorded payment whose transaction now sits on another account than recorded (`interpretation.account_id`) | the current account | 2 (correction) |

Each event counts once, a correction before an answer. The newest come first, ordered by the latest of the answer and the transaction's last update. A payment never moved is not evidence. Automatic records, model output and defaults never count.

Rejected:
- **A `pocket_choices` table written on answers and edits, plus a backfill.** Older events lack the stored merchant key, so the backfill would need Python over every event anyway. Reading the existing records gives the same evidence, counts web corrections without a hook in the edit path, and cannot drift from the ledger.
- **Adding columns to `classification_feedback`.** Its rows are classification labels keyed by account.

### 4. Learning key: the merchant's first word

The key is the institution plus the first word of the merchant or recipient name, lower-cased and letters only. Payments that name no merchant (for example "You've paid Rp… using your debit card") use the institution plus the habit text signature (`jago|expense|payment`). The same keys serve:

- unnamed transfers ("You've transferred Rp… to X"), keyed by the recipient;
- money added into a pocket with no named source, keyed by the target pocket.

Evidence (production, read-only, 2026-11-14): the owner's 10 Jago payments that named no pocket, 28 Sep to 13 Nov.

| Key tested | Fuel stations | Verdict |
| --- | --- | --- |
| Full name without digits | "spbu hr m", "spbu no id mbl", "spbu ry mengn" (three keys for one habit) | rejected |
| Category | "Tagihan & Utilitas" paid from Cicilan, Subscription and Main Pocket; "Makanan & Minuman" from four pockets | rejected |
| First word | "spbu" 3/3 Transportasi; "kredivo" 2/2 Cicilan; debit-card signature 2/2 Subscription | chosen |

A generic first word ("toko", "warung") may merge different merchants. If they go to different pockets, the rule keeps asking; if they always go to the same pocket, merging is harmless.

### 5. Confidence: rule of succession

The probability that the next payment of a key uses pocket P is (agreeing weight + 1) ÷ (total weight + 2), over the key's latest 10 choices, with corrections weighing 2. The payment is recorded on P when this is at least 0.75 (a setting); otherwise the top pocket is only the preselection.

Small evidence must be unanimous, and more evidence tolerates a rare exception:

| History | Result |
| --- | --- |
| 2 agreeing | learned (0.75) |
| 3 agreeing, 1 different | asks (0.67) |
| 5 agreeing, 1 different | learned (0.75) |
| 2 corrections away from a learned pocket | asks again until the new habit settles |

Replayed on the owner's 10 payments in order:

| | Today (default pocket) | Habit rule (3 / 90%) | This rule |
| --- | --- | --- | --- |
| Wrong pocket recorded | 8 | 0 | 0 |
| Questions | 0 | 10 | 9 |
| Questions with the right pocket preselected | — | 3 | 3 |
| Recorded automatically | — | 0 | 1 (correct) |

The automatic one is the 13 Nov fuel payment the owner had to correct by hand.

A kill switch `POCKET_LEARNING_AUTO_RECORD_ENABLED` (default on) turns learned recording into asking with the suggestion.

Rejected:
- **3 choices at 90% (the habit rule):** it never learned anything in seven weeks of the owner's data.
- **"Last 2 agree, none of the last 5 differ":** one exception blocks learning for four more payments.

### 5a. Learning from history at launch

Because evidence is read from existing records (decision 3), the owner's earlier corrections count from the first deploy, with no migration.

### 6. Suggestion when asking

The preselection is the learned favourite when there is any evidence, else the parent's configured default pocket. The owner always confirms.

### 7. Marking a learned record

- The event result carries `pocket_habit: {label, basis}`, for example "Kantong dari kebiasaan" and "3 dari 3 pilihan Anda".
- The companion shows it on the recorded detail.
- The web ledger already allows changing the pocket. That edit is a correction (decision 3).

### 8. Interplay with existing rules

The duplicate-arrival rule that lets a named report replace a default-pocket one (`keeps_new_observation`) stays for income and for legs recorded before this change. A learned or answered pocket counts as `named`.

## Data, API and failure behaviour

- **Migration:** none. The kill switch is the new setting `POCKET_LEARNING_AUTO_RECORD_ENABLED`.
- **API:** additive (`pocket_habit` in results). Same question shape.
- **Rollback:** revert the code. Nothing persistent was added.
- A missing or archived learned pocket is not used: the payment is asked.

## Risks

- **More questions at first.** One per new merchant until 3 weighted choices agree; with a correction that is fewer.
- **Held payments understate spending until answered.** The pending count in the companion and Tinjau shows them.
- **Merchant-name normalisation may merge two merchants.** The worst case is a wrong learned pocket, which the owner corrects; the correction weighs double.

## Owner decisions (2026-11-14)

- **Scope:** learn wherever one target usually has one answer: paying pockets, transfer sources and sources of money added into a pocket.
- **Rule:** chosen from the owner's real data rather than by preference.
