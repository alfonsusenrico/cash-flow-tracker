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
  6. The Accounts page SHALL list top-level accounts in their persisted order as rows of two tables: "Uang siap pakai" for cash, bank and wallet accounts and "Investasi" for investment accounts, each with its total in the card header. A "Semua / Siap pakai / Investasi" switch, kept in the address as `?tampil=`, SHALL show both tables or one. Each row SHALL identify the account by name with its configured color as a mark beside the name and never as the only carrier of meaning, state its kind in words, and show its balance with tabular figures. Pockets SHALL be indented rows directly below their account, named as pockets of that account. Investment rows SHALL be grouped under their platform with a row-group heading and SHALL say whether the product is tracked by amount or by units and lots. Activating a row SHALL open that account's drawer (a side drawer on wide screens, a bottom sheet on phones) holding its details and actions. The hierarchy SHALL remain legible in light and dark themes.
  7. The Accounts page SHALL let users reorder top-level accounts in an "Ubah urutan" mode, using drag-and-drop and explicit, keyboard- and touch-operable move controls; both methods SHALL persist the resulting order through `POST /api/accounts/reorder`. Leaving the mode SHALL return focus to the control that opened it. The UI SHALL NOT print numerical position indexes merely to indicate order.

#### Scenario: Updating investment valuation without ledger transactions
- **WHEN** a user updates the market value of an investment account from 10,000,000 IDR to 11,500,000 IDR
- **THEN** the account's balance updates to 11,500,000 IDR without creating any income or expense transactions in the transaction history

#### Scenario: Creating an account with custom tag color
- **WHEN** an authenticated user creates an account with name "BCA Prioritas", type "bank", and color "#059669"
- **THEN** the system persists the account with the chosen color and the account's row shows a mark in that color beside its name, with the name in the text token

#### Scenario: Viewing a parent with many pockets
- **WHEN** a user opens the Accounts page with an account containing twelve pockets on a desktop-sized viewport
- **THEN** the account's row is followed by its twelve indented pocket rows, every row and its drawer action is reachable by keyboard, and the page has no clipped action or horizontal overflow

#### Scenario: Compact account actions and pocket expansion
- **WHEN** a user views an account with pockets
- **THEN** the account row states the pocket count, a labelled disclosure on the row collapses or expands the pocket rows without implying that pockets will be deleted, and the account's actions are in its drawer

#### Scenario: Adding the first pocket from a liquid account card
- **WHEN** a user opens the drawer of a top-level liquid account with no pockets
- **THEN** the drawer offers the same "Tambah kantong" action as an account with pockets, and activating it opens the new-pocket form for that account

#### Scenario: Reordering accounts via drag-and-drop in visual order
- **WHEN** an authenticated user turns on "Ubah urutan" and drags an account row to a new position
- **THEN** the UI displays the new top-to-bottom order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Reordering accounts with a move control
- **WHEN** an authenticated user turns on "Ubah urutan" and activates an account row's move up or move down control
- **THEN** the UI displays the new top-to-bottom order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Viewing accounts in rich light mode
- **WHEN** a user switches the application to light mode
- **THEN** the page sits on the canvas and window tokens, the tables render on the surface token with hairline row dividers from the line token, and the hierarchy avoids unstyled dark inversions

#### Scenario: Default color of a new investment account
- **WHEN** an authenticated user creates an investment account, or an investment pocket, without choosing a color
- **THEN** the system persists the account with `#8B5CF6`, a new bank, wallet or cash account without a color still gets `#3b82f6`, and an account whose request names a color keeps that color

#### Scenario: Stored colors are not rewritten
- **WHEN** an investment account was saved with `#3b82f6` before this default existed and the owner later renames it
- **THEN** the account keeps `#3b82f6`

### Requirement: Parent Balance Aggregation and Grouped Representation
The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account.

#### Scenario: Required behavior and constraints
- **WHEN** the application computes or displays an account hierarchy balance
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account:
  1. `GET /api/accounts` SHALL return accounts with `parent_id`, `is_parent`, `children: [...]`, and aggregated total balance for master accounts.
  2. Transactions recorded against a child pocket SHALL directly debit or credit that specific pocket and automatically reflect in the master account's aggregated balance.
  3. Account selectors across Quick Capture, Ledger, and Transfers SHALL present pockets grouped under their respective master accounts using `<optgroup>`.
  4. The Accounts page SHALL render each master account as a row followed by its pockets as indented, collapsible rows with clear visual hierarchy. Pocket rows SHALL show the pocket name, its parent in words for assistive technology, its balance and relevant investment details without repeating the account row's layout. The master row's balance SHALL be the server's aggregated balance; the page SHALL NOT add pocket balances itself.

#### Scenario: Querying accounts with pockets
- **WHEN** a user with Bank Jago having "Kantong Utama" (Rp 5.000.000) and "Kantong Darurat" (Rp 15.000.000) calls `GET /api/accounts`
- **THEN** Bank Jago reports a total aggregated balance of Rp 20.000.000 with both child pockets nested under it

#### Scenario: Recording expense from a specific pocket
- **WHEN** a user records an expense of Rp 50.000 from "Kantong Utama"
- **THEN** "Kantong Utama" balance decreases by Rp 50.000 and the parent "Bank Jago" aggregated balance decreases by Rp 50.000

#### Scenario: Expanding a pocket list on a narrow screen
- **WHEN** a user expands a parent account on a 390 px wide viewport
- **THEN** the pocket rows remain visibly associated with that parent, values and the row disclosure reflow without horizontal page overflow, and the parent can be collapsed again

### Requirement: Account and Pocket Action Clarity
The Accounts page SHALL provide a clearly labelled primary action appropriate to each account or pocket and a discoverable, labelled set of other supported operations in that item's drawer.

#### Scenario: Required behavior and constraints
- **WHEN** an owner inspects available actions for an account or pocket
- **THEN** the following required behavior and constraints hold:

  The Accounts page SHALL provide a clearly labelled primary action appropriate to each account or pocket and a discoverable, labelled set of other supported operations in that item's drawer (a side drawer on wide screens, a bottom sheet on phones). The drawer SHALL preserve child-pocket creation, investment buy and sell or top-up, manual valuation ("Perbarui nilai"), the switch to amount tracking, balance reconciliation where eligible, editing, and archiving. Action controls SHALL have targets of at least 24 by 24 CSS pixels (44 by 44 on phones), visible keyboard focus and meaningful accessible names. The drawer SHALL trap focus while open, close with Escape, and return focus to the row that opened it.

#### Scenario: Acting on a liquid account
- **WHEN** a user opens the drawer of a top-level liquid account, with or without pockets
- **THEN** its primary action adds a pocket and its other supported actions are listed in the drawer

#### Scenario: Acting on an ordinary pocket
- **WHEN** a user opens the drawer of an ordinary liquid pocket
- **THEN** its supported actions, including `Sesuaikan Saldo`, edit, move and archive, are listed in a drawer titled with the pocket and its parent

#### Scenario: Acting on an investment holding
- **WHEN** a user opens the drawer of an investment pocket that supports trading and valuation
- **THEN** buy and sell (or `Top up` for an amount-tracked product), valuation, edit, and archive actions remain available and the drawer clearly separates the balance from investment performance information

#### Scenario: Reading dense investment pockets
- **WHEN** an investment platform holds long-named stock, mutual-fund, and gold pockets with units, market price, cost, value, and gain or loss
- **THEN** each row shows the product name, how it is tracked and its value without overlapping another row, and units, market price, cost and gain or loss are shown in the product's drawer

#### Scenario: Opening and closing secondary actions by keyboard
- **WHEN** a keyboard user opens an account or pocket drawer, moves through its actions, and closes it
- **THEN** all offered actions are reachable, focus remains visible, and focus returns to the row that opened the drawer
