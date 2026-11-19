## Approval

Pending. Requested by the owner on 2026-11-19 after "kemarin jam 18.40 cash 10rb buat tip potong rambut" showed no account in the preview. Decisions made the same day: learn the paying account per item, not only per exact phrase; hybrid, with the model grouping new wording and deterministic counting deciding the account; `gpt-6-luna` at low reasoning is acceptable for the grouping.

## Why

The owner writes captures in free form: "parkir ptc 10rb cash", later "parkir 10rb", "parkir k24 2rb". Every one of 14 parking captures went to Cash, yet the app learns the account only per exact phrase ("parkir ptc", "parkir k24", "parkir groove" are separate). A parking spot not seen before starts from nothing and is asked. Production holds 27 recorded captures with 20 distinct phrases but 11 distinct first words, so most new phrases repeat an item the owner already paid for. A bare time before the item ("kemarin 20.45 bayar parkir ptc …") also leaks into the phrase, so that capture taught the key "20 45 parkir ptc".

## Expected Outcome

- "parkir 10rb" at a new place is recorded on Cash when the owner's parking captures agree, and the preview says "Cash · dari kebiasaan" before sending.
- Different wording for the same item ("parkiran mall", "bayar parking") joins that item through the model, once per new wording.
- When the history does not agree clearly enough, the owner is asked, with the learned account preselected.
- "kemarin 20.45 …" is read as yesterday at 20:45 and the time is not part of the item.

## What Changes

- **Item group:** each capture phrase belongs to an item group: a phrase seen before keeps its group; otherwise its first meaningful word; for wording that word does not match, the capture's existing model call also names the item group, chosen from the owner's groups or a new one from the text, and the code checks it. A phrase's group is stored once.
- **Account by item:** after the exact phrase's memory and history, the account is learned from the latest choices in the item group by the rule already used for payment pockets: the rule of succession at 0.75 or more over the latest 10 choices, a correction counting twice. Otherwise the account question preselects the learned account.
- **Preview:** shows the learned account with "dari kebiasaan" when the phrase's group is known without the model.
- **Time:** a bare clock time ("20.45", "8.30") directly after a day word ("kemarin", "tadi", "hari ini") is read as the time.
- **Model:** the grouping is one more field of the capture's existing model call, which runs only for phrases the owner's memory does not know; no extra call. The reasoning effort for captures becomes its own setting, `CAPTURE_AI_REASONING_EFFORT`, chosen by a synthetic benchmark between `none` (today) and `low`.

## Non-goals

- Notification payments (they keep pocket learning) and moves (their accounts come from the text or the move question).
- A sentence that names two accounts ("cash … potong rambut" when "Potong Rambut" is a pocket) is still asked; only the preselection changes.
- Reading "cash in" / "cash out" as deposit and withdrawal forms (the move is already recorded correctly).

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `text-capture`: account learning by item group, the preview's learned account, and bare clock times after a day word.

No project capability is added. `llm` applies: a new structured output field, validated by code, with a synthetic benchmark before release. `database` applies: one new table.

## Impact

- **Backend:** `services/capture_resolution.py` (account order, item group), `services/capture_parsing.py` (bare time after a day word), `services/capture_normalization.py` and both capture prompts (`item_group`, prompt `capture-normalization-4`), `services/capture_preview.py`, a new `services/item_learning.py`, `core/config.py` (`CAPTURE_AI_REASONING_EFFORT`), `.env.example`.
- **Database:** migration V59, `capture_item_groups(user_id, match_key, item_group, source, created_at)`, unique per owner and phrase key.
- **Web and companion:** the preview's account line ("Cash · dari kebiasaan") on the web; the companion's question already shows a preselected account.
- **Evaluation:** a synthetic item-grouping benchmark; one paid run with the owner's OK.
