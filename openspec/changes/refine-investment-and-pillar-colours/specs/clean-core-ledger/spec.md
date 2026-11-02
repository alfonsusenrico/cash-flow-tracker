## MODIFIED Requirements

### Requirement: Account Management & Real Liquid Balances
The system SHALL support creating, updating via both `PUT` and `PATCH`, archiving, and querying liquid cash and investment accounts (`cash`, `bank`, `wallet`, `investment`).

#### Scenario: Required behavior and constraints
- **WHEN** an owner creates, updates, archives or queries accounts
- **THEN** the following required behavior and constraints hold:

  The system SHALL support creating, updating via both `PUT` and `PATCH`, archiving, and querying liquid cash and investment accounts (`cash`, `bank`, `wallet`, `investment`):
  1. Each account's current balance SHALL equal its initial balance plus all debits (income/inbound transfers) minus all credits (expenses/outbound transfers), or its market valuation when instrument tracking is enabled.
  2. Investment valuation updates (via market data or manual updates) SHALL update market valuation parameters (`last_price`, `units`, `initial_balance`) directly and SHALL NOT insert income, expense, or transfer transactions into the ledger.
  3. Account entities SHALL support a customizable tag color (`color`, hex string) and display position index (`display_order`, integer default `0`). An account created without a color SHALL get the lavender investment color `#8B5CF6` when its type is `investment`, including an investment pocket and a stock position opened from a broker notification, and `#3b82f6` otherwise. The web form for a new account SHALL start on that type's default (`#8B5CF6` for `investment`, `#2563EB` otherwise) and follow the type until the owner picks a color. Defaults SHALL apply only when an account is created and SHALL NOT rewrite a stored color.
  4. `GET /api/accounts` SHALL order top-level accounts by `display_order ASC, created_at ASC, name ASC`.
  5. The system SHALL expose `POST /api/accounts/reorder` accepting `account_ids: list[UUID]` to persist updated account order.
  6. The Accounts page SHALL display top-level accounts as equal-height cards in their persisted order, with three cards per row on wide desktops, two on medium desktops, and one on narrow screens. Each card SHALL prominently identify the account by name and configured color with high-contrast text, show its balance, and keep its associated pockets in an expandable, bounded scroll region below the persistent summary and actions. Cards with and without pockets SHALL align to the same height. The hierarchy SHALL remain legible in light and dark modes.
  7. The Accounts page SHALL let users reorder top-level accounts using drag-and-drop and explicit, keyboard- and touch-operable move controls; both methods SHALL persist the resulting order through `POST /api/accounts/reorder`. The UI SHALL NOT print numerical position indexes merely to indicate order.

#### Scenario: Updating investment valuation without ledger transactions
- **WHEN** a user updates the market value of an investment account from 10,000,000 IDR to 11,500,000 IDR
- **THEN** the account's balance updates to 11,500,000 IDR without creating any income or expense transactions in the transaction history

#### Scenario: Creating an account with custom tag color
- **WHEN** an authenticated user creates an account with name "BCA Prioritas", type "bank", and color "#059669"
- **THEN** the system persists the account with the chosen color and renders the account name tag in emerald with high-contrast text

#### Scenario: Viewing a parent with many pockets
- **WHEN** a user opens an account containing twelve pockets on a desktop-sized viewport
- **THEN** the account occupies one equal-height card in the ordered grid, its summary and actions remain visible, and all twelve pocket rows are reachable by scrolling the bounded pocket region without clipped actions or horizontal overflow

#### Scenario: Compact account actions and pocket expansion
- **WHEN** a user views an account card with pockets
- **THEN** the account's supported actions are grouped at the upper-right and a small chevron beside the pocket count expands or collapses the list without implying that the pockets will be deleted

#### Scenario: Adding the first pocket from a liquid account card
- **WHEN** a user views a top-level liquid account with no pockets
- **THEN** the card shows the same accessible add-pocket `+` primary action as an account with pockets, and activating it opens the new-pocket form for that account

#### Scenario: Reordering accounts via drag-and-drop in visual order
- **WHEN** an authenticated user drags an account group to a new position
- **THEN** the UI displays the new top-to-bottom order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Reordering accounts with a move control
- **WHEN** an authenticated user activates an account group's move up or move down control
- **THEN** the UI displays the new top-to-bottom order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Viewing accounts in rich light mode
- **WHEN** a user switches the application to light mode
- **THEN** the canvas displays a refined soft slate background (`#F4F5F8`), account groups render with crisp white surfaces and visible borders, and the hierarchy avoids unstyled dark inversions

#### Scenario: Default color of a new investment account
- **WHEN** an authenticated user creates an investment account, or an investment pocket, without choosing a color
- **THEN** the system persists the account with `#8B5CF6`, a new bank, wallet or cash account without a color still gets `#3b82f6`, and an account whose request names a color keeps that color

#### Scenario: Stored colors are not rewritten
- **WHEN** an investment account was saved with `#3b82f6` before this default existed and the owner later renames it
- **THEN** the account keeps `#3b82f6`
