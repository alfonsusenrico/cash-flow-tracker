# Spec Delta

## ADDED Requirements

### Requirement: Goal Progress Action Presentation
The Goals interface SHALL present progress actions according to goal backing:
1. A standalone goal SHALL offer a clearly labeled progress adjustment that changes no account or ledger balance.
2. An account-backed goal SHALL display that progress follows linked account balances and SHALL not offer a synthetic goal deposit action.
3. Linking or unlinking accounts SHALL explain the resulting progress source before saving.
4. The interface SHALL never label a progress-only adjustment as an account transfer or ask for a source account that will not be debited.

#### Scenario: Viewing an account-backed goal
- **WHEN** a goal has one or more linked accounts
- **THEN** the interface identifies those accounts as the progress source and omits standalone deposit controls

#### Scenario: Adjusting a standalone goal
- **WHEN** a user records progress for a goal without linked accounts
- **THEN** the confirmation states that no account transaction will be created
