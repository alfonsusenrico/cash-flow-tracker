# Spec Delta

## MODIFIED Requirements

### Requirement: Classification Undo Restores the Earlier State
Undoing an accept, recategorise, rename, keep separate, purpose answer or history-suggestion decision SHALL restore each touched row's category, pillar, notes, merchant, offset category, review status, classifier, confidence, owner-edit time and owner-split time. It SHALL reopen a purpose question the action closed, with its original question id, and return a decided suggestion to pending.

#### Scenario: Recategorise undone
- **GIVEN** an AI-classified automatic row recategorised by the owner into "Jajan & Nongkrong"
- **WHEN** the owner undoes it
- **THEN** the row SHALL have its earlier category, pillar, `review_status = auto` and `classified_by = ai`, and its owner-edit time SHALL be empty again

#### Scenario: Purpose answer undone
- **GIVEN** a merchant-less payment answered with a suggestion
- **WHEN** the owner undoes the answer
- **THEN** the row SHALL have its earlier values, the event's purpose question SHALL be open again with the same `question_id`, and the row SHALL be listed under "Belum dikenali" again

#### Scenario: Keep separate undone
- **GIVEN** an ambiguous leg kept separate
- **WHEN** the owner undoes it
- **THEN** the leg SHALL have no owner-split time and its earlier review status, and automatic pairing MAY link it again

#### Scenario: Suggestion accept undone
- **GIVEN** an accepted history suggestion
- **WHEN** the owner undoes the accept
- **THEN** the row SHALL have its earlier values and the suggestion SHALL be pending again
