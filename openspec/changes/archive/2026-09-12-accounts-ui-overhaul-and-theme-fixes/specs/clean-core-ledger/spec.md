## MODIFIED Requirements

### Requirement: Account Management & Real Liquid Balances
The system SHALL support creating, updating via both `PUT` and `PATCH`, archiving, and querying liquid cash and investment accounts (`cash`, `bank`, `wallet`, `investment`):
1. Each account's current balance SHALL equal its initial balance plus all debits (income/inbound transfers) minus all credits (expenses/outbound transfers), or its market valuation when instrument tracking is enabled.
2. Account entities SHALL support a customizable tag color (`color`, hex string default `#3b82f6`) and display position index (`display_order`, integer default `0`).
3. `GET /api/accounts` SHALL order top-level accounts by `display_order ASC, created_at ASC, name ASC`.
4. The system SHALL expose `POST /api/accounts/reorder` accepting `account_ids: list[UUID]` to persist updated card order.
5. In the user interface, account cards SHALL prominently display the account name as a large, tactile tag badge using the account's configured color with high-contrast text, support draggable reordering arranged in natural visual reading order (left-to-right, top-to-bottom) without printing numerical index numbers, and adapt to light mode with a rich, soft slate canvas tone and crisp structural contrast.

#### Scenario: Creating an account with custom tag color
- **WHEN** an authenticated user creates an account with name "BCA Prioritas", type "bank", and color "#059669"
- **THEN** the system persists the account with the chosen color and renders the account name tag in emerald with high-contrast text

#### Scenario: Reordering accounts via drag-and-drop in visual order
- **WHEN** an authenticated user drags an account card to a new position in the grid
- **THEN** the UI rearranges the cards in natural visual sequence (left-to-right, top-to-bottom) and sends the updated ID order to `POST /api/accounts/reorder`

#### Scenario: Viewing accounts in rich light mode
- **WHEN** a user switches the application to light mode
- **THEN** the canvas displays a refined soft slate background (`#F4F5F8`), account cards render with crisp white surfaces and visible borders, and cards avoid unstyled dark inversions
