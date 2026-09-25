# Spec Delta

## MODIFIED Requirements

### Requirement: Account Management & Real Liquid Balances
The system SHALL support creating, updating via both `PUT` and `PATCH`, archiving, and querying liquid cash and investment accounts (`cash`, `bank`, `wallet`, `investment`):
1. Each account's current balance SHALL equal its initial balance plus all debits (income/inbound transfers) minus all credits (expenses/outbound transfers), or its market valuation when instrument tracking is enabled.
2. Investment valuation updates (via market data or manual updates) SHALL update market valuation parameters (`last_price`, `units`, `initial_balance`) directly and SHALL NOT insert income, expense, or transfer transactions into the ledger.
3. Account entities SHALL support a customizable tag color (`color`, hex string default `#3b82f6`) and display position index (`display_order`, integer default `0`).
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
- **AND** a source-specific transfer remains available in `Opsi` when the account is eligible to transfer

#### Scenario: Reordering accounts via drag-and-drop in visual order
- **WHEN** an authenticated user drags an account group to a new position
- **THEN** the UI displays the new top-to-bottom order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Reordering accounts with a move control
- **WHEN** an authenticated user activates an account group's move up or move down control
- **THEN** the UI displays the new top-to-bottom order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Viewing accounts in rich light mode
- **WHEN** a user switches the application to light mode
- **THEN** the canvas displays a refined soft slate background (`#F4F5F8`), account groups render with crisp white surfaces and visible borders, and the hierarchy avoids unstyled dark inversions

### Requirement: Parent Balance Aggregation and Grouped Representation
The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account:
1. `GET /api/accounts` SHALL return accounts with `parent_id`, `is_parent`, `children: [...]`, and aggregated total balance for master accounts.
2. Transactions recorded against a child pocket SHALL directly debit or credit that specific pocket and automatically reflect in the master account's aggregated balance.
3. Account selectors across Quick Capture, Ledger, and Transfers SHALL present pockets grouped under their respective master accounts using `<optgroup>`.
4. The Accounts page SHALL render master accounts with expandable/collapsible pocket lists and clear visual hierarchy. Expanded pockets SHALL be shown as compact rows in the card's bounded scroll region, with pocket names, balances, and relevant investment details readable without visually repeating full account cards.

#### Scenario: Querying accounts with pockets
- **WHEN** a user with Bank Jago having "Kantong Utama" (Rp 5.000.000) and "Kantong Darurat" (Rp 15.000.000) calls `GET /api/accounts`
- **THEN** Bank Jago reports a total aggregated balance of Rp 20.000.000 with both child pockets nested under it

#### Scenario: Recording expense from a specific pocket
- **WHEN** a user records an expense of Rp 50.000 from "Kantong Utama"
- **THEN** "Kantong Utama" balance decreases by Rp 50.000 and the parent "Bank Jago" aggregated balance decreases by Rp 50.000

#### Scenario: Expanding a pocket list on a narrow screen
- **WHEN** a user expands a parent account on a narrow mobile viewport
- **THEN** the pockets remain visibly associated with that parent, values and actions reflow without horizontal page overflow, and the parent can be collapsed again

## ADDED Requirements

### Requirement: Account and Pocket Action Clarity
The Accounts page SHALL provide a clearly labelled primary action appropriate to each account or pocket and a discoverable, labelled secondary-action disclosure for other supported operations. The action presentation SHALL preserve available transfers, child-pocket creation, investment buy and sell, manual valuation, balance reconciliation where eligible, editing, and archiving. Action controls SHALL have sufficiently large touch targets, visible keyboard focus, meaningful accessible names, and an operable disclosure that does not trap focus or conceal an action from keyboard users.

#### Scenario: Acting on a liquid account
- **WHEN** a user views a top-level liquid account, with or without pockets
- **THEN** its primary action adds a pocket and its supported secondary actions, including an eligible source-specific transfer, are available through an accessible options disclosure

#### Scenario: Acting on an ordinary pocket
- **WHEN** a user views an ordinary liquid pocket
- **THEN** its transfer action remains identifiable with an accessible name and icon, and its supported secondary actions are available through an accessible options disclosure

#### Scenario: Acting on an investment holding
- **WHEN** a user views an investment pocket that supports trading and valuation
- **THEN** buy, sell, valuation, edit, and archive actions remain available and the row clearly distinguishes balance from investment performance information

#### Scenario: Reading dense investment pockets
- **WHEN** an expanded account contains long-named stock, mutual-fund, and gold pockets with units, market price, cost, value, and gain or loss
- **THEN** each row separates identity and instrument details from valuation and trading controls, remains readable in the bounded card scroller, and exposes all supported actions without overlapping another row

#### Scenario: Opening and closing secondary actions by keyboard
- **WHEN** a keyboard user opens an account or pocket options disclosure, moves through its actions, and dismisses it
- **THEN** all offered actions are reachable, focus remains visible, and focus returns to a predictable control

### Requirement: Responsive Settings Entry
The application SHALL show one direct Settings entry in the desktop sidebar and SHALL NOT duplicate it in the desktop top bar. On mobile/tablet, a top-bar menu button SHALL open the navigation drawer, where Settings SHALL remain reachable. Icon-only menu and settings controls SHALL have meaningful accessible names and visible keyboard focus. This replaces the older mobile top-bar Settings shortcut without removing mobile access.

#### Scenario: Opening Settings on desktop
- **WHEN** a desktop user views the application header and sidebar
- **THEN** Settings is available in the sidebar and no Settings gear is shown in the header

#### Scenario: Opening Settings from mobile navigation
- **WHEN** a mobile user activates the top-bar menu button and then the Settings control in the drawer
- **THEN** the drawer opens, the Settings dialog opens from that drawer, and the header shows no duplicate Settings gear
