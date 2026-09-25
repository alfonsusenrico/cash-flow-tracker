# Spec Delta

## MODIFIED Requirements

### Requirement: Name-Based Entity Resolution on Transaction Creation
The `POST /api/transactions` endpoint SHALL accept supported human-readable references while preserving ownership and deterministic resolution:
1. An explicit account or target UUID SHALL resolve only to an active account owned by the authenticated user; unknown or foreign identifiers SHALL be rejected.
2. A supplied account name SHALL resolve by case-insensitive canonical match or an explicitly documented synonym among the user's active accounts.
3. Ambiguous or unmatched free-form names SHALL be rejected and SHALL NOT silently create an account or pocket.
4. Provisioning a new investment instrument or parsed bank pocket SHALL occur only through its explicit supported workflow, not as a fallback for a public transaction typo.
5. A category reference SHALL resolve only to a user-owned category compatible with the transaction type.
6. Any internal movement request SHALL require a valid, distinct target and SHALL use the canonical bilateral movement operation.

#### Scenario: Rejecting a foreign target identifier
- **WHEN** a client supplies a target account UUID owned by another user
- **THEN** the server returns not found and creates no transaction

#### Scenario: Rejecting an unmatched account typo
- **WHEN** a client supplies an account name that has no unambiguous owned match
- **THEN** the server rejects the request without auto-creating an account

#### Scenario: Creating a transfer using account names
- **WHEN** a client submits an internal movement with unambiguous owned source and target account names, an Internal Movement category, and a positive amount
- **THEN** the server resolves the names and creates one canonical bilateral movement

#### Scenario: Missing target account on transfer
- **WHEN** a client submits an internal movement without a target ID or target name
- **THEN** the server returns a bad-request response stating that a target account is required
