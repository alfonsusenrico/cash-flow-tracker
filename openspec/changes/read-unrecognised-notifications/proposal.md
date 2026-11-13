## Why

When the owner adds money to a Jago Locked Pocket, Jago sends only: "You've added Rp47.619 into Bayar Kredit Locked Pocket." The server cannot record it:

- The evidence rules keep a fixed list of settlement phrases. "You've added" is not on it, so the event stops at `needs_review` / `settlement_unproven` before the model ever reads it. "Coba Proses Ulang" repeats the same check, and a pocket move offers no manual record, so the event is stuck.
- The Jago "into … Pocket" parser rule accepts any verb and assumes the main pocket as the source. Here the text names no source, and the owner's real sources vary (Tabungan, Dana Darurat, Makan-Jalan).
- "Locked" is Jago's pocket type ([Jago: Locked Pocket](https://www.jago.com/en/digital/pocket/locked)), so "Bayar Kredit Locked" does not equal the registered "Bayar Kredit".

Production holds five such events since 2026-11-05. The owner had already entered four of them by hand, dated up to 14 hours before Jago's notification.

The owner asked not to keep adding fixed wording and name rules, since Jago wording and pocket names vary too widely. Instead, the model should read the activity, source and target, with Tinjau when it is not confident enough. Approach and trade-offs approved on 2026-11-13: one model call per unrecognised notification, a paid benchmark asked for separately, and an AI-suggested source in the source question.

## Expected Outcome

- A notification the fixed rules do not recognise, but which quotes one Rupiah amount from a supported app, is read by the model instead of being parked or dropped. The model decides whether money already moved, which way, and which pockets the text names, quoting the words it relied on.
- A confident reading continues through the normal checks. An unsure one goes to Tinjau without a ledger row.
- Pocket names the model quotes go through the existing account mapping, which already learns names such as "Bayar Kredit Locked" and asks once when unsure. No new name lists.
- When a Jago move names no source pocket, the companion asks "Dari rekening atau kantong mana?" and lists all of the owner's own accounts and pockets, at any bank or wallet. The model's likely source is preselected; the owner confirms with one tap or picks another. The source is never recorded without the owner.
- A source at another bank or wallet becomes one move, completed on that account the way Tinjau completes a one-sided transfer, so that account's own notification never debits it twice.
- A move the owner already entered by hand is confirmed instead of recorded twice.
- After deploy, "Coba Proses Ulang" on the five stuck events resolves them: four confirm the owner's entries, the 5 Nov one first asks to map "Spaylater", and the Rp47.619 one asks for its source.

## What Changes

- Evidence: an unrecognised notification that is not noise and quotes one amount becomes a candidate marked for model reading, rather than `settlement_unproven`. This applies only when the model is available.
- Model reading: prompt guidance in both languages for unrecognised wording. The backend accepts a reading only with verbatim amount and activity quotes and enough confidence. The reading is stored on the event, so later steps such as the source answer, mapping confirmation and the record itself use the same facts without asking the model again.
- Parser: the Jago "into … Pocket" rule requires a move verb, so it no longer guesses the main pocket for other wording.
- Source question: also used for a model-read Jago move with no named source. It offers every owned account except the target and carries the model's suggestion. A source at another institution is completed with the existing movement completion.
- Hand-entered moves: a unique matching move is confirmed, before asking and after the answer.
- Companion (separate repository): preselect and mark the suggestion; neutral wording ("Dari rekening atau kantong mana?", "Konfirmasi sumber dana") since the options are no longer only pockets.
- Benchmark: synthetic cases for unrecognised wording, including the "added into" shape and noise that must stay ignored.

## Non-goals

- Known formats keep their fixed rules. The model reads only what the rules don't recognise.
- No change to unnamed outbound transfers: they keep their question without a suggestion.
- No matching of hand-entered moves for notifications that name both pockets.
- No production data edits; the owner reprocesses the stuck events from the companion.

## Capabilities

- Modified: `ai-notification-processing`, `notification-ingestion`.
- Uses the existing `llm` capability (OpenAI interpretation); no new project capability.

## Impact

- Backend: `notification_evidence.py`, `notification_parser.py`, `notification_processing.py`, `notification_application.py`, `notification_source_pocket.py`, `notification_interpretation.py`, both interpretation prompts, and the prompt version.
- Migration `V56__notification_model_reading.sql`: a nullable `model_reading JSONB` on `notification_events`.
- `evaluation/notification_cases.py` benchmark cases; backend tests.
- Companion: source-pocket controls and their API model.
- Cost: one model call per unrecognised notification. Before production, a paid benchmark run is made with the owner's OK.
