## Why

A transaction has two describing fields: an optional merchant and `notes`. In practice `notes` is the transaction's name. Production, read-only, 2026-11-16:

- every AI-recorded row holds a name there ("Bayar makan di Groove", "Isi bensin di SPBU TPI");
- the owner's 114 noted manual rows are names too;
- the ledger title is built from it.

So there is no place for the owner's own context, such as "fellowship coach" on the rows of a community event filed under Budaya. The owner asked on 2026-11-16 for a separate transaction name, with the note becoming an optional remark of their own.

## Expected Outcome

- Every transaction has a **Nama transaksi**: the title shown in the ledger, detail, review, reports and companion. The AI writes it, and the owner can change it.
- **Catatan** is an optional remark that only the owner writes, never the AI. It appears under the name in the ledger and in the detail.
- Ledger search finds text in the name, the merchant and the note.
- Existing data is unchanged in meaning: every current note becomes the name, and notes start empty.

## What Changes

- **Migration:** rename the `transactions.notes` column to `name`, and add a nullable `note` for the owner's remark.
- **API:**
  - Transactions return `name` and `note`.
  - Requests accept `name` and `note`.
  - During a compatibility window, `notes` is still returned as a copy of `name` and accepted as an alias for it, for older companions and clients.
- **AI and learning:** notification interpretation, text capture, purpose answers, merchant memory and habits produce and learn the name. None of them read or write the note.
- **Web:**
  - Catat and the edit dialogs get "Nama transaksi" and "Catatan (opsional)".
  - Rows show the name with the note as a second line.
  - Search covers notes.
  - Copy is updated in both languages.
- **Companion (`mobile/`):** shows the name, and the note when present. Its existing requests keep sending `notes`, which the server reads as the name.

## Non-goals

- Tags. They are in the separate change `transaction-tags`.
- Notes on movements, plans, goals and debts. Those keep their own `notes` fields unchanged.
- An AI-written note.

## Capabilities

- Modified: `transaction-ledger-management`.
- No new project capability.

## Impact

- **Database:** V57 (column rename and a new column, no data rewrite).
- **Backend:** about 50 files that read `transactions.notes`, plus API models and both prompt files.
- **Web:** Catat, the edit dialogs, rows, search, review items, reports and messages.
- **Companion:** models and the row title.
- **Clients:** the API contract is additive, with `notes` kept as an alias; its removal is a later change once no companion older than this release is in use.

## Approval

Planned 2026-11-16 at the owner's request. Approved 2026-11-16 ("ok approved, build plan 1 first").
