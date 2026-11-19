# Design: learn-accounts-by-item

## Context

`account_for` (`services/capture_resolution.py`) picks a capture's account in this order: the owner's answer; an explicit mention (one candidate, or ask); the exact phrase's merchant memory; the exact phrase's history (3 of the latest 5 rows); the model's account when it is in that history and at 0.85 or more; otherwise ask. The phrase key (`capture_key`) is the whole description after fillers ("parkir ptc"), so learning never crosses phrases.

Production on 2026-11-19 (read-only): 27 recorded captures, 20 distinct keys, 11 distinct first words. All 14 parking captures are on Cash, under 9 keys. One key is "20 45 parkir ptc" from "kemarin 20.45 bayar parkir ptc cash 10rb".

`pocket_learning.py` already learns a notification's pocket from the owner's choices with the rule of succession; this change reuses that rule for captures.

## Decisions

### 1. Item group of a phrase

A capture's item group is decided once per owner and phrase key, in this order:
1. a group stored for the phrase key (`capture_item_groups`);
2. the phrase's first word: the first run of at least two letters after `KEY_FILLERS`, so numbers and one-letter words are skipped ("20 45 parkir ptc" → "parkir", "07 55 e money" → "money"), when the owner has recorded captures in that group;
3. the model's `item_group`, when the capture's model call ran;
4. otherwise the first word.

The group is stored when the capture is recorded, with `source` `word` or `ai`. An owner edit does not change a group; it changes the account evidence (D2).

Rejected: the first word only (misses "parkiran" for "parkir"); the model for every phrase (a call for wording the owner already uses).

### 2. Account by item group

After the exact phrase's memory and history, and before the model's account, the account comes from the item group's evidence:
- the final account of the latest 10 recorded captures in the group (expense or income, not moves), weight 1;
- a capture whose transaction the owner moved to another account later counts 2 for the new account.

With `p = (agreeing + 1) / (total + 2)` for the leading account, `p ≥ 0.75` records on it ("item", shown as "dari kebiasaan"); otherwise the account question preselects it. The thresholds are the pocket-learning ones, so one owner behaviour gives one answer across the app.

An explicit mention still wins. When a mention has several candidates, the question preselects the learned account if it is one of them.

Rejected: lowering the exact-phrase history threshold instead (still never crosses phrases).

### 3. The model names the group

`CaptureNormalization` gains `item_group: {group: str | None, span: str}`:
- the model gets `item_groups`: the owner's groups with their recorded count, at most 100, most used first, and only when the phrase's group is not known without it;
- `group` is one of `item_groups`, or a new group of 1–3 words taken from the text, lower case;
- `span` must be words of the text; an unknown group or a span not in the text is dropped and the first word is used.

The prompt rules: the group names what was paid for, not the place, the brand or the account ("parkir ptc" → "parkir", "bensin pertamina" → "bensin", "potong rambut" → "potong rambut"); prefer an existing group for the same kind of purchase; never a group from an amount or a time. Prompt version `capture-normalization-4`.

No extra call: the field is added to the capture's existing model call, which already runs for any phrase the owner's memory does not know.

### 4. Reasoning effort for captures

`CAPTURE_AI_REASONING_EFFORT` (default: the notification setting, today `none`) lets captures differ from notifications. A synthetic benchmark (task 3.1) compares `none` and `low` on grouping and on the existing category results, with latency and cost; the owner chooses the production value from it.

### 5. Preview

The preview stays model-free. It shows the account the capture would be recorded on from the owner's habit, in the recording order without the model (the exact phrase's memory, its history, then the item group of D1 steps 1–2 at `p ≥ 0.75`), as "Cash · dari kebiasaan" (`account_hint.basis = "habit"`). Showing only the item group could name a different account than the one recorded when the exact phrase has its own. Otherwise "ditentukan saat dicatat" as today.

The account question carries `suggested_account_id`, the same field as the source-pocket question, and lists that account first; the web marks it "Saran". The companion lists it first and does not mark it yet.

### 6. Bare clock time after a day word

`HH.MM` or `HH:MM` (hours 0–23, minutes 00–59) directly after "kemarin", "tadi", "hari ini", "semalam" or a weekday is a clock time. Amounts never take that form ("20.45" is not money in the parser), and "jam 20.45" already works.

## Data

Migration V59:
```sql
CREATE TABLE capture_item_groups (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  match_key VARCHAR(220) NOT NULL,
  item_group VARCHAR(60) NOT NULL CHECK (char_length(btrim(item_group)) BETWEEN 1 AND 60),
  source VARCHAR(10) NOT NULL CHECK (source IN ('word', 'ai')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (user_id, match_key)
);
```
No backfill and no cleanup: existing captures fall to the first word (D1 step 2), which covers the owner's parking history. Three learned names with a leaked time or a stray word ("20 45 parkir ptc", "07 55 e money", "in", never reused) stay; new text no longer produces them, and the owner can delete them in Pengaturan → Kebiasaan.

## Risks

- A wrong group joins two items with different accounts. The 0.75 threshold then fails and the owner is asked; corrections count double.
- The model may invent groups. The span check and the 1–3 word bound keep groups tied to the text; the benchmark measures it.
- Privacy: the model gets the owner's group words (their own item words), as it already gets their categories and memory.

## Verification

- Backend: account order with item learning (auto, preselect, mention wins, ambiguous mention preselects); grouping by stored key, first word and model; the model's group checks; preview's learned account; bare times after day words, and existing time scenarios unchanged.
- Benchmark: synthetic phrases (same item new place, synonyms, brand vs item, unrelated) at `none` and `low`; one paid run with the owner's OK.
- Browser: Catat preview with "Cash · dari kebiasaan".
