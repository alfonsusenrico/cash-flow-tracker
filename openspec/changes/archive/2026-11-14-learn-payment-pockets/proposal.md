## Why

Jago payment notifications never name the pocket that paid ("You have paid Rp… to SPBU…", "You've paid Rp… using your debit card"). For any account with pockets, the server records such a payment on the configured default pocket (Main Pocket). On 2026-11-13 the owner paid for fuel from "Transportasi"; the payment was recorded on Main Pocket and had to be corrected on the web.

The owner asked (2026-11-13) for two things:
- the app learns the paying pocket from their own choices;
- when there is no learning, or it is not confident, the app asks instead of assuming.

The design must work for any user and merchant. It must not be shaped around this owner's habits.

## Expected Outcome

- An outgoing payment from an account with pockets whose notification names no pocket is never put on the default pocket by assumption.
- **Learned case:** when the owner's own earlier choices for the same merchant (or, for payments that name no merchant, the same kind of notification) agree strongly enough, the payment is recorded on that pocket straight away, marked as learned with how many choices agree.
- **Otherwise:** the companion asks which pocket paid. The likely pocket is preselected: the learned favourite if there is one, else the default pocket. Nothing is recorded until the owner confirms.
- Every answer, and every pocket correction of such a payment on the web, teaches the app. A correction counts more than an answer, so a changed habit is learned quickly.

## What Changes

- **Server:** the unnamed-pocket outflow path asks or uses a learned pocket, replacing the default-pocket fallback. Pocket choices are stored as owner labels, and the learned decision follows the habit thresholds.
- **Web edits:** moving a notification-recorded payment to another pocket counts as a correction, read from the ledger itself.
- **Companion:** shows "Kantong dari kebiasaan" with its basis on a learned record. The question and preselection already exist.
- **Settings:** a kill switch for learned recording, like the habit one.

## Non-goals

- Incoming money: the bank decides where it lands (Jago puts transfers in Main Pocket), so the default pocket stays.
- Notifications that name a pocket, and moves between pockets, are unchanged.
- No model involvement: learning is deterministic statistics over the owner's own choices.
- Payments already recorded are not changed.

## Capabilities

- New: `payment-pocket-learning`.
- `notification-record-and-flag` already holds an event that needs a source-pocket answer, so it needs no change.
- No new project capability.

## Impact

- **Migration:** none. Choices are read from existing answers, Tinjau resolutions and moved payments.
- **Backend:** `notification_evidence.py`, `notification_source_pocket.py`, `notification_application.py`, `notification_processing.py`, a new `pocket_learning.py` service, settings, messages, and tests.
- **Companion:** the recorded-detail basis line.
- **Effect on the owner:** more questions at first, one per new merchant, until choices agree. A payment waiting for an answer is not yet in the ledger, so Aman dipakai does not include it until then.

## Approval

Planned 2026-11-13 at the owner's request. Approved 2026-11-14 ("ok") with the rule chosen from the owner's data.
