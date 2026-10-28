# Verification: add-ingestion-memory-and-purpose-questions

Date: 2026-10-28.

**Revision tested:** branch `feat/ingestion-memory-and-purpose`, based on 422ef71 (phase 2, now merged into
`main`). The working tree was uncommitted when tested; the commit that follows contains exactly that tree.

**Data:** synthetic users, accounts, categories, notification texts and feedback only. No production data
was read; production texts quoted in the proposal are paraphrased shapes, not tracked records.

**Providers:** every model call is mocked (`AsyncMock` or an `httpx2.MockTransport`). No OpenAI request
was made.

## Backend

| Check | Result |
|---|---|
| Disposable `postgres:16`, recreated, migrated V1→V29 by `flyway/flyway:10` | 29 migrations applied |
| Full suite: `PYTHONPATH=backend pytest backend/tests` with `SKIP_TEST_MIGRATIONS=1` | **801 passed, 0 skipped, 0 failed** |
| Baseline before any change (same DB setup at V28) | 763 passed |
| Real corpus, benchmark and new phase 3 tests run together | 102 passed |
| New tests | 38: `test_ingestion_memory_and_purpose.py` (36) and two normaliser tests in `test_openai_notification_provider.py` |

Existing tests adjusted to the new contract:
- `test_unresolvable_model_uncertainty_reports_backend_reason` became
  `test_unusable_model_answer_without_category_records_uncategorised_and_flagged`: the event is now recorded,
  uncategorised, `needs_review`, reasons `model_fallback`, `uncertain_category`, `purpose_question`.
- Two manual-resolution tests used an uncertain category to obtain a held event; they now use a conflicting
  companion direction hint, which is still held.
- Context tests read `similar_history` and `memory` instead of `history`; the fake-cursor tests script the
  extra memory query; the owner-scope assertion accepts named query parameters.
- The stored prompt version assertion is now `notification-interpretation-9`.

What the new tests prove:
- **Record and flag:** uncategorised myBCA payment recorded on the BCA account with `needs_review`; model
  `needs_review` resolved deterministically → `model_fallback`; confidence 0.92 → `auto` (and an `ai` memory
  entry), 0.6 → `low_confidence` (no memory); ambiguous counterparts → unpaired leg with
  `ambiguous_movement`; automatic pairing closes the counterpart's question and sets both legs `auto`. Held
  with no row: two amounts, unknown pocket, Jago transfer without source pocket (existing tests keep
  covering ambiguous existing movements and consumed roles).
- **Memory and feedback:** an amount-only edit writes nothing; a recategorisation writes one feedback row
  with before/after values, local weekday and hour, amount, institution, match key and source, and an owner
  memory entry; `UPDATE` of a feedback row raises `classification_feedback is append-only`; a movement
  rename writes two feedback rows and no memory; a memory hit records without any provider call
  (`classified_by = rule`, `auto`, memory notes and merchant, hit count 1); an archived memory category
  disables the hit; an AI upsert never replaces an owner entry; a signature memory entry does not skip the
  model.
- **Context:** similar history returns the owner-corrected Kedai Awan row first, then the other one, and no
  unrelated rows; memory lists the exact owner key first; the budget trims history before memory and fits
  60 000 bytes; internal underscore keys never reach the provider; an invalid review threshold sets
  `processor_configuration_invalid`.
- **Purpose questions:** created for myBCA category-only and QRIS without merchant, not for a named merchant;
  masked incoming transfer after the sender answer gets a question naming the sender and a repayment
  suggestion; ranking puts three Wednesday-evening ~Rp200.000 answers above four answers at another time
  and amount and ignores an example with no shared feature; cold start offers only the current
  classification; companion 1.7.0 sees neither `question` nor `review_status`.
- **Answer endpoint:** suggestion answer updates the row, feedback, memory and result description; replay
  returns the identical response without a second feedback row; a different answer with the same `reply_id`
  → 409 `purpose_answer_conflict`; a new reply after closing → 409 `stale_question`; free text uses the
  mocked normaliser and stores merchant, description, category and pillar; provider failure, invalid
  references and AI disabled keep the text as notes, keep the category, flag `needs_review` and learn no
  memory; invalid bodies → 422; unknown suggestion → 422; another owner → 404 and an empty list; the list
  shows open questions only; a ledger edit closes the question and records a correction.
- **Keys over the real corpus:** myBCA category-only, masked third-party and salary texts and the Jago
  debit-card text are signature keys; ShopeePay top-up is a merchant key; owner, pocket, RDN and broker
  texts get no key.
- **Migration:** V29 equals its mirror, the mirror is idempotent, the trigger exists once.

## Frontend

Not run: this change touches no frontend file.

## OpenSpec and diff

| Check | Result |
|---|---|
| `openspec validate add-ingestion-memory-and-purpose-questions` | valid, also with `--strict` |
| `openspec validate --all` before archive | 47 passed, 0 failed |
| `openspec validate --all --strict` before archive | 42 passed, 5 failed: the same five main specs with one over-long requirement each as on phase 2 (`account-reconciliation`, `clean-core-ledger`, `kakeibo-decoupling`, `kakeibo-reconciliation`, `notification-filtering`) |
| Open changes modifying the same capabilities | none |
| `openspec archive add-ingestion-memory-and-purpose-questions --yes` | exit 0; 3 specs created, `transaction-provenance` updated |
| `openspec validate --all` after archive | 49 passed, 0 failed |
| `openspec validate --all --strict` after archive | 44 passed, 5 failed: the same five pre-existing specs; the three new specs got written Purpose sections and pass |
| `git diff --check` | clean |

## Not verified, and why

- **Live model behaviour** of prompt version 9 and the normaliser prompt: no real OpenAI calls are allowed.
  The live benchmark (`evaluation/notification_benchmark.py`) should be re-run by the owner before relying
  on the confidence gate's calibration.
- **Production:** not touched. Post-deploy checks are in the design's migration plan.
- **Companion UI:** phase 4b implements the question against design D13.
- **Accuracy against the owner's real corrections:** the plan's corpus benchmark needs production labels;
  only synthetic and redacted corpus texts are in the repository.

## Follow-ups

- Model `ignored` answers stay final (open decision 1).
- `merchant_category_rules` is still read and still empty; phase 8 removes it.
- myBCA salary income gets a purpose question because the text names no sender; if that is noise, a later
  change can exempt bank categories the owner marks as self-explanatory.
- Same five pre-existing strict warnings as on `main`.
