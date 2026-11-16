# Design

## Context
Tracker PR #119 (`transaction-tags`) applies tags to a typed capture on send and returns them on the capture view's transaction as `tags: [{id, name}]`. The companion stores that view as JSON (`summary_json`) and derives what the result card and Rincian show from it (`CapturePresentation`). It reads only the fields it models, so the tags are dropped today.

## Goals
- Show the attached tags where the owner checks a capture: the result card right after sending, and Rincian.
- Keep every capture without tags, and every older tracker, exactly as today.

## Non-goals
- Tag editing on the phone, tags in Aktivitas rows, tags on notification-recorded rows.

## Decisions

### 1. Model the field as optional
`CaptureTag(id, name)` and `CaptureTransaction.tags: List<CaptureTag>? = null`. A missing field (older tracker, or a capture stored before this version) parses to null and means no tags. Blank names are skipped.

Rejected: a database column for tags. The view is already stored as JSON and refreshed from the tracker, so a column would only duplicate it.

### 2. Result card: one muted line, not more fact chips
The tags go on one `WhyLine` with a tag icon, "Tag: fellowship coach, Malang trip", after the note line. TalkBack reads the same text.

Rejected: adding each tag as a `FactTags` chip. The chips are what the tracker decided (time, name, category, pillar, account); tags are the owner's own grouping, and mixing them makes a tag read like a category.

### 3. Rincian: a "Tag" row
"Yang dicatat tracker" gets a "Tag" row after "Catatan", with the names joined by ", ". It uses the existing fact row, so long names wrap like every other value.

### 4. Edits keep tags
The phone's edit request has no tag fields, and the tracker only replaces tags when `tag_ids` or `new_tags` is sent. A test pins that the serialized edit request carries neither key, so a later change cannot clear tags by sending `null`. After an edit, the capture view is refreshed from the tracker, which brings the current tags.

## Risks
- A tag added or removed later on Tracker Web shows on the phone only after the capture's view is refreshed, as for any other web edit. Accepted: the phone shows what the tracker last reported for that capture.

## Release
Version from the tag: `android-v2.4.5`, pushed on `main` after the merge and green Android checks, then the APK is verified (checksum, version, signer, dex strings "Tag: ").
