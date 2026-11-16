# Verification: transaction-tags

Tested on 2026-11-16, branch `feat/tags` at `8bbf8b66` (base `8ad55484`) plus the uncommitted implementation. The data and API layer (tasks 1.1–1.3) and the web layer (task 3.1) were built by delegated agents and reviewed here; the AI capture part (task 2.1), the picker's keep-on-leave behaviour, the row priority fix and the benchmark were done here.

## Checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V58) | 2405 passed |
| Frontend type check, lint, tests (vitest), build | 0 errors, lint passed, 1146 passed (102 files), build passed |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 86 passed, 0 failed |
| Capture tag benchmark, offline (`python -m evaluation.capture_tag_benchmark`, `tests/test_capture_tag_benchmark.py`) | 13 cases build; scoring tests pass |
| Capture tag benchmark, live (owner's OK 2026-11-16; `gpt-6-luna`, effort `none`, `capture-normalization-3`, Indonesian, 3 repeats) | 39 of 39 correct: no unwanted tag, no duplicate, no missed tag, no invalid answer; 0 names repeat the tag words; observed cost US$0.022. Output: `research/evidence/transaction-tags/capture-tag-benchmark-live.json` |

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| Owner-created tags: create inline, reuse by normalized name, limit 10, name 1–40 | `tests/test_transaction_tags.py`; web `TagPicker.test.tsx`, `ManualView.test.tsx`, `TransactionEditDialog.test.tsx` |
| Managing tags: rename (taken name refused), merge (no duplicate links), archive and restore | `tests/test_transaction_tags.py`; web `TagsSection.test.tsx`; browser: a merge of "felowship coach" into "fellowship coach" left one tag with 4 transactions |
| What an event cost (filter with totals) | `tests/test_transaction_tags.py`; browser: `?tag=` lists the 3 tagged rows and the summary shows Rp 80.500, the sum of those rows |
| A prefix maps to an existing tag | `tests/test_capture_tags.py::test_a_prefix_maps_to_the_owners_existing_tag` (fake provider): tag attached, name from the rest of the text, result lists the tag |
| A new tag on instruction, removed by undo | `test_a_new_tag_on_instruction_is_created_and_undo_removes_it`; `test_undo_keeps_a_created_tag_another_transaction_carries`; `test_an_existing_tag_attached_by_a_capture_survives_its_undo` |
| No tag wording | `test_text_without_tag_wording_sends_no_tags_to_the_model`; `test_tag_wording_is_structural` (a clock time "12:30" is not a label) |
| Unproven references dropped | `test_unproven_tags_are_dropped_and_a_duplicate_name_reuses_the_tag`: a foreign ID, a span not in the text, and a new name equal to an existing tag |
| Tag wording reaches the model even when memory knows the merchant | `test_tag_wording_asks_the_model_even_when_memory_knows_the_merchant` |
| Strict schema sent to the provider | `test_capture_normalizer_uses_its_own_strict_schema_and_prompt` (`tags` required, at most 3) |

## Browser check

**Setup:** a production build (`next start`), the backend and a loopback proxy, with the prototype's synthetic dataset migrated to V58. Four rows were tagged through the API, one with a note as well.

**Coverage:** the ledger, the ledger filtered by a tag, the filter dialog, the detail, the edit dialog with the picker open, Catat (manual form) with the picker open, Pengaturan → Tag, and a merge; at 1440 × 900 and 390 × 844, light and dark (32 captures, then 8 after the fix below).

**Automated findings:** 0 serious or critical axe violations and 0 console errors in all 40 captures. The style checker's "layer shadow" (dialogs) and "coloured borders" (Catat's type switch) findings are existing elements, not part of this change.

**Found and fixed during the check:**
- on desktop, a row with both a note and a tag squeezed both to about four characters ("fell…"). Tags now stay readable first, and the note narrows to its icon; the note stays in full in the detail and on the phone;
- the picker dropped a typed name when the owner left the field without picking it. Leaving the field now keeps it: the matching tag, or a new one (the filter, which never creates, still leaves it behind).

Selected captures are in `research/evidence/transaction-tags/`.

## Not verified

- **Tags on an AI result in the browser.** The browser run had the model off. The recorded result's tags are covered by the backend tests (fake provider) and `SentenceView.test.tsx`; the live model's reading is measured by the benchmark above.
- **Saving from Catat in the browser.** The picker was exercised in the browser; the save path is covered by `ManualView.test.tsx` and the API tests.
- **The companion.** A capture typed on the phone gets its tags on the server like one from the web, and the API result carries `tags`. Design D4 also says the companion's recorded result lists them, but `tasks.md` has no companion task and no companion code changed: the phone does not show the tags yet.

## Known limits

- A tagged row that is later paired into a movement keeps its tags; movements otherwise carry none.
- A capture that names an archived tag restores it. Undo then leaves the tag active, because the capture did not create it.
