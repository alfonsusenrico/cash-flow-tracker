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

### 3. Evidence: the owner's pocket choices

A new append-only table `pocket_choices`, one row per owner choice:

| Column | Meaning |
| --- | --- |
| `user_id` | the owner |
| `transaction_id` | the payment |
| `event_id` | the notification |
| `parent_account_id` | the account with pockets |
| `pocket_account_id` | the chosen pocket |
| `institution` | the bank |
| `pocket_key` | the learning key (decision 4) |
| `kind` | `answer` or `correction` |
| `created_at` | when the choice was made |

Rows come from:
- source-question answers for these payments;
- edits that move a notification-recorded expense to another pocket of the same parent.

The latest row per transaction counts, so a later correction replaces an earlier answer. Automatic records, model output and defaults are never evidence.

Rejected: adding columns to `classification_feedback`. Its rows are classification labels keyed by account, while pocket choice is keyed above the account. Mixing them would change habit queries.

### 4. Learning key

- **Merchant named:** institution plus the merchant name, lower-cased, with digits and punctuation removed. "SPBU No ID 54 601 08" and "SPBU No ID 31 102 02" both become "spbu no id". Station or branch numbers would otherwise split one merchant into many.
- **Merchant-less** (for example "You've paid Rp… using your debit card"): institution plus the habit text signature, such as `jago|expense|payment`.
- **Unnamed transfers** ("You've transferred Rp… to X"): the recipient name in the same way.

Rejected: an amount band. Which pocket pays depends on the merchant far more than the amount, and a correction re-learns quickly.

### 5. Confidence

The habit thresholds are reused as defaults, and are settable:
- at least 3 weighted support;
- at least 90% of the weight on one pocket;
- corrections weigh 2, answers 1;
- only the latest 10 choices of the key count.

Below that, the top pocket (if any) is only the preselection.

A kill switch `POCKET_LEARNING_AUTO_RECORD_ENABLED` (default on) turns learned recording into asking with the suggestion.

### 6. Suggestion when asking

The preselection is the learned favourite when there is any evidence, else the parent's configured default pocket. The owner always confirms.

### 7. Marking a learned record

- The event result carries `pocket_habit: {label, basis}`, for example "Kantong dari kebiasaan" and "3 dari 3 pilihan Anda".
- The companion shows it on the recorded detail.
- The web ledger already allows changing the pocket. That edit is a correction (decision 3).

### 8. Interplay with existing rules

The duplicate-arrival rule that lets a named report replace a default-pocket one (`keeps_new_observation`) stays for income and for legs recorded before this change. A learned or answered pocket counts as `named`.

## Data, API and failure behaviour

- **Migration:** V57 `pocket_choices` with an append-only trigger, as `classification_feedback` has.
- **API:** additive (`pocket_habit` in results). Same question shape.
- **Rollback:** revert the code. The table is ignored by older code.
- A missing or archived learned pocket is not used: the payment is asked.

## Risks

- **More questions at first.** One per new merchant until 3 weighted choices agree; with a correction that is fewer.
- **Held payments understate spending until answered.** The pending count in the companion and Tinjau shows them.
- **Merchant-name normalisation may merge two merchants.** The worst case is a wrong learned pocket, which the owner corrects; the correction weighs double.

## Open questions for the owner

- **Thresholds:** keep the habit defaults (3 / 90%), or require fewer for pockets, such as 2 agreeing with none disagreeing?
- **Unnamed transfers:** include them in learning (decision 4), or keep asking every time?
