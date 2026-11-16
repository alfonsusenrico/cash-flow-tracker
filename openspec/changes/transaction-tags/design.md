## Context

The ledger list builds its summary with the same filter conditions as its rows, so a tag filter gives tag totals without new figures.

Text capture already runs one model call (`CaptureNormalization`):
- it is given the owner's categories and accounts;
- it returns references the backend validates;
- a preview shows the result before saving.

## Goals / Non-goals

**Goals:**
- Tags the owner creates and uses in a few taps.
- A filter with totals.
- AI capture that reuses tags and never creates one silently.

**Non-goals:**
- An automatic active tag.
- AI tagging of notifications.
- Companion tagging.
- Report breakdowns.

## Decisions

### 1. Data

The tables:
- `tags(id, user_id, name, name_normalized, archived_at, created_at)`, unique on `(user_id, name_normalized)`;
- `transaction_tags(transaction_id, tag_id, user_id, created_at)`, primary key on the pair; deleting a transaction or a tag deletes its links.

The normalized name is lower-cased with spaces collapsed. It keeps the owner's spelling for display and blocks a second "Fellowship coach".

A merge moves the links to the kept tag and deletes the other. Archiving hides a tag from pickers and the AI, and keeps it on existing rows and in the filter.

Rejected:
- **A text array on `transactions`:** renames and merges would rewrite rows, and the filter could not use an index cleanly.
- **Categories as tags:** categories drive Kakeibo and reports, and one row has exactly one.

### 2. Creating tags

- **Picker:** typing a name that matches no active tag offers "Buat tag "…"". The tag is created when the transaction is saved, in the same database transaction.
- **API:** create and update take `tag_ids` (existing) and `new_tags` (names). An existing normalized name reuses that tag instead of failing.
- **Limits:** at most 10 tags per transaction; names of 1–40 characters.

### 3. Ledger filter and totals

- `GET /api/transactions?tag=<id>` (repeatable; rows with any of the tags) adds an `EXISTS` condition. The summary uses the same conditions, so its totals follow.
- The filter bar shows the chosen tags as chips.
- Search keeps matching text. A tag name is matched through the filter, not through `q`.

### 4. AI input in Catat

**Context:** the model gets the owner's active tags as `{id, name}`, at most 200 by most recent use.

**Schema:** `tags: [{tag_id: UUID | null, new_name: str | null, span: str}]`, with at most 3 entries.

**Prompt rules:**
- A tag is something the owner marks the entry with: a "label:" prefix, "#label", or "tag …" / "tag-nya …".
- Prefer an existing tag when the span clearly means it (same words, a shortening, a typo, or a different word order).
- Propose `new_name` only when no existing tag fits.
- Never derive a tag from the merchant, the category or the account.
- The tag span is not part of the name.

**Backend checks:**
- `tag_id` must be one of the owner's active tags;
- `span` must appear in the text;
- `new_name` must be 1–40 characters after normalization;
- if `new_name` normalizes to an existing tag, it becomes that tag;
- anything that fails a check is dropped, and the capture stays usable.

**Preview:** existing tags as chips; new ones as "tag baru: …". The owner can remove any of them before saving. Saving creates the new tags.

Rejected:
- **A fixed syntax parsed by code only.** The owner asked for the AI to understand their phrasing, and a prefix alone cannot map "fellow coach" to "fellowship coach".
- **Creating new tags at parse time.** Typos would leave stray tags that the owner never confirmed.

### 5. Pengaturan

A "Tag" section lists tags with their row counts, and lets the owner rename, merge into another tag, and archive or restore.

## API and failure behaviour

- **Errors:** `TAG_NOT_FOUND` (foreign or unknown ID), `TAG_LIMIT` (more than 10), `TAG_NAME_INVALID`, and `TAG_MERGE_SAME`. Each is in both catalogues.
- Other owners' tags are never visible: 404, without revealing anything.

## Migration and rollback

V58 is additive. Rollback is the code revert; the tables can stay, unused.

## Risks

- **Tag sprawl from near-duplicates.** Normalization, the AI preferring existing tags, and merge in Pengaturan limit it.
- **Capture prompt change.** Measured with the benchmark on synthetic cases (tag prefix, hashtag, no tag, a near-duplicate of an existing tag) before release, with the owner's OK for the paid run.
