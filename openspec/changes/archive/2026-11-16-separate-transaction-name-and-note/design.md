## Context

`transactions.notes` is read or written in about 50 backend modules: ledger, review, reports, learning, prompts and capture. It is also read in about 30 web components and sent by three companion requests (resolve, capture, label edit). The web title is built as `ownRowNote(notes) || merchant || notes || category`.

Every non-movement row with text holds a name there, so no existing value is an owner remark.

## Goals / Non-goals

**Goals:**
- One explicit name.
- An owner-only note.
- No change in meaning for existing data.
- Old clients keep working.

**Non-goals:**
- Tags.
- Notes on other entities.

## Decisions

### 1. Rename the column, then add the note

- **Migration V57:** `ALTER TABLE transactions RENAME COLUMN notes TO name`, then `ADD COLUMN note TEXT`, with the same 500-character limit enforced by the API.
- A rename is a catalogue change: instant, with no data rewrite, and every current value is already a name.

Rejected:
- **Keep the `notes` column as the name and add a differently named remark column.** The code would then say "notes" for a name forever, which every later reader would trip over.
- **Copy notes into a new `name` column.** It rewrites every row for the same result, and leaves a dead column to drop later.

### 2. API compatibility window

- Responses carry `name` and `note`, plus `notes` (a copy of `name`) for older companions.
- Requests accept `name` or the alias `notes`. If both are sent, `name` wins.
- `note` is new and optional.
- Removing the alias is a later change, once every companion in use is at least the release that ships this.

### 3. Who writes what

| Writer | `name` | `note` |
| --- | --- | --- |
| AI (notification interpretation, text capture), purpose answers, merchant memory, habits | writes and learns it, as they write `notes` today | never reads or writes it |
| Reconcile and system rows | write their fixed text | — |
| The owner (Catat, edit dialog, Tinjau resolution) | sets it | sets it |

The model never sees the note, so a private remark never leaves the ledger.

### 4. Display

- **Title:** name, else merchant, else category. The placeholder rule (`GENERIC_ROW_NOTES`) applies to the name.
- **Note:** shown as a muted second line in ledger rows, the transaction detail and the companion detail, and nowhere else.
- **Search:** `q` also matches `note`.

### 5. Exports and reports

The monthly report and exports label the column "Nama transaksi" and add "Catatan".

## Data, migration and rollback

- **Forward:** V57 renames the column and adds `note`.
- **Rollback:**
  - `note` values would be lost if the column were dropped;
  - a rollback migration renames `name` back to `notes` and keeps `note` unused;
  - the previous code reads `notes`, so the rollback needs both the code revert and that migration.
- The owner's checks after the migration:
  - the row count is unchanged;
  - the name equals the old notes for a sample;
  - `note` is NULL everywhere.

## Risks

- **The rename touches many modules.** The full backend suite, frontend suite and companion suite must pass, and a grep check confirms that no `t.notes` / `transactions.notes` reference is left outside the compatibility alias.
- **A client writing `notes` after the rename** lands in `name` through the alias, which is its meaning today.
