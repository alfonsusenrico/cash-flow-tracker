# Spec Delta

## MODIFIED Requirements

### Requirement: Mobile Bottom-Sheet Quick Capture
The application SHALL render the canonical transaction-capture form as a touch-first bottom sheet on mobile screens:
1. The sheet SHALL have rounded top corners, a visible handle, contained scrolling, accessible dialog semantics, and safe focus behavior.
2. Amount entry SHALL use high-visibility tabular figures and a touch-appropriate numeric interaction without preventing hardware-keyboard entry.
3. Category selection SHALL use horizontally scrollable, named, stateful icon choices filtered by transaction type.
4. Kakeibo choices SHALL be one-tap targets whose selected state is exposed programmatically and whose default follows the selected category.
5. Account, date, notes, goal or obligation context, receipt attachment, errors, and submission results SHALL be functionally equivalent to the desktop canonical form.
6. The sheet SHALL not force mobile keyboard focus immediately when doing so obscures context or causes unwanted viewport movement.

#### Scenario: Opening quick capture on mobile
- **WHEN** a user opens Quick Capture on a 390px viewport
- **THEN** the complete canonical transaction fields are available in a touch-first bottom sheet without losing keyboard or assistive-technology operation

#### Scenario: Selecting a category on mobile
- **WHEN** a user selects a category chip classified as `want`
- **THEN** the chip announces its selected state and the Kakeibo selection defaults to `want`
