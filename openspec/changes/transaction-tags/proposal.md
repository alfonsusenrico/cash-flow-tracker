## Why

The owner wants to group transactions across categories by something they choose, such as the rows of one community event ("fellowship coach") filed under Budaya. They want to review them together and see what the event cost.

A note marks a row, but cannot be filtered or totalled reliably. Tags the owner creates can. The owner also wants the AI input in Catat to know their tags (2026-11-16):
- use an existing tag;
- create a new one when told;
- read a prefix such as "fellow coach: bayar makan di Kafe Contoh cash jam 19".

## Expected Outcome

- **Tags:** the owner creates them while recording or editing (no separate setup), picks existing ones with autocomplete, and can put several on one transaction.
- **Ledger:** filters by a tag and shows the filtered totals the way it does for other filters. Each row shows its tags.
- **Pengaturan:** rename, merge or archive tags.
- **Catat AI input:**
  - recognises a tag in the text: a "label:" prefix, "#label", or "tag …";
  - maps it to the owner's existing tag when one is close ("fellow coach" → "fellowship coach");
  - shows any tag it would create as "tag baru" in the preview;
  - creates it only when the owner saves.

## What Changes

- **Migration:**
  - `tags`: owner, name, normalized name unique per owner, archived;
  - `transaction_tags`: transaction, tag, owner; primary key on the pair; cascades.
- **API:**
  - tag list, create, rename, merge, archive;
  - `tags` on transaction responses;
  - `tag_ids` plus `new_tags` on create and update;
  - a `tag` filter on the ledger list and its summary.
- **Text capture:**
  - the model sees the owner's active tags;
  - it returns `tags` as existing IDs, or new names with their quoted span;
  - the backend checks every reference, and the preview marks new tags.
  - Prompt `capture-normalization-3`.
- **Web:** a tag picker in Catat (manual and AI preview) and the edit dialog; tag chips on rows and in the detail; a ledger filter; a Pengaturan section; messages in both languages.

## Non-goals

- An "active tag" applied automatically during an event (declined by the owner on 2026-11-16).
- Tags on notification-recorded rows by the AI. Notifications carry no tag wording; the owner tags them afterwards.
- Tagging from the companion (later), report breakdowns by tag (later), tags on movements, plans, goals or debts.

## Capabilities

- New: `transaction-tags`.
- Modified: `text-capture` (tags in the proposal and preview).
- No new project capability.

## Impact

- **Database:** V58 (two tables).
- **Backend:** transactions router, a new `tags` router and service, `text_capture` and `capture_normalization`, both capture prompts.
- **Web:** Catat, the edit dialog, ledger rows and filters, Pengaturan, messages.
- **Cost:** one paid benchmark run of the capture prompt with the owner's OK.
- **Order:** it depends on `separate-transaction-name-and-note` only for wording, and can ship after it.

## Approval

Planned 2026-11-16 at the owner's request; waiting for approval.
