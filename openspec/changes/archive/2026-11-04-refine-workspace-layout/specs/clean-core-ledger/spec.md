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
  6. The Accounts page SHALL show KPI cards (Total aset, Siap pakai, Investasi, Kekayaan bersih), a "Komposisi aset" card, then the top-level accounts in their persisted order as cards in two columns. Each card SHALL show the account name in a badge of its color (never the only carrier of meaning), its kind in words, its pocket or product count and its balance in large tabular figures, with its pockets or products as rows inside the card on a rail in the account color. Investment products SHALL say whether they are tracked by amount or by units and lots. The whole card head and every pocket row SHALL open that item's detail (a centered dialog from 761 px, a bottom sheet below) holding its details and actions, without scrolling the page. Top-level accounts still on the default blue SHALL be shown with distinct hues from a fixed list without rewriting the stored color. The hierarchy SHALL remain legible in light and dark themes.
  7. On wide screens the Accounts page SHALL let users reorder top-level accounts by dragging a visible handle on each card and with move controls shown on hover and keyboard focus; on phones the move controls SHALL be offered in an "Ubah urutan" mode. Both methods SHALL persist the resulting order through `POST /api/accounts/reorder`, announce each move, and keep focus on the moved item's control. The UI SHALL NOT print numerical position indexes merely to indicate order.

#### Scenario: Updating investment valuation without ledger transactions
- **WHEN** a user updates the market value of an investment account from 10,000,000 IDR to 11,500,000 IDR
- **THEN** the account's balance updates to 11,500,000 IDR without creating any income or expense transactions in the transaction history

#### Scenario: Creating an account with custom tag color
- **WHEN** an authenticated user creates an account with name "BCA Prioritas", type "bank", and color "#059669"
- **THEN** the system persists the account with the chosen color and the account's card shows its name in a badge of that color, with the name meeting text contrast

#### Scenario: Viewing a parent with many pockets
- **WHEN** a user opens the Accounts page with an account containing twelve pockets on a desktop-sized viewport
- **THEN** the account's card holds its twelve pocket rows, every row opens its detail by keyboard and pointer, and the page has no clipped action or horizontal overflow

#### Scenario: Compact account actions and pocket expansion
- **WHEN** a user views an account with pockets
- **THEN** the account card states the pocket count, its pockets are listed inside the card, and the account's actions are in its detail dialog

#### Scenario: Adding the first pocket from a liquid account card
- **WHEN** a user opens the detail of a top-level liquid account with no pockets
- **THEN** the detail offers the same "Tambah kantong" action as an account with pockets, and activating it opens the new-pocket form for that account

#### Scenario: Reordering accounts via drag-and-drop in visual order
- **WHEN** an authenticated user on a wide screen drags an account card by its handle to a new position
- **THEN** the UI displays the new order and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Reordering accounts with a move control
- **WHEN** an authenticated user activates an account card's move up or move down control, on a wide screen or in "Ubah urutan" on a phone
- **THEN** the UI displays the new order and announces it and sends the updated account ID sequence to `POST /api/accounts/reorder`, with the order retained after refresh

#### Scenario: Viewing accounts in rich light mode
- **WHEN** a user switches the application to light mode
- **THEN** the page sits on the window token, the cards render on the surface token with hairline pocket dividers from the line token, and the hierarchy avoids unstyled dark inversions

#### Scenario: Default color of a new investment account
- **WHEN** an authenticated user creates an investment account, or an investment pocket, without choosing a color
- **THEN** the system persists the account with `#8B5CF6`, a new bank, wallet or cash account without a color still gets `#3b82f6`, and an account whose request names a color keeps that color

#### Scenario: Stored colors are not rewritten
- **WHEN** an investment account was saved with `#3b82f6` before this default existed and the owner later renames it
- **THEN** the account keeps `#3b82f6`

### Requirement: Account and Pocket Action Clarity
The Accounts page SHALL provide a clearly labelled primary action appropriate to each account or pocket and a discoverable, labelled set of other supported operations in that item's detail dialog.

#### Scenario: Required behavior and constraints
- **WHEN** an owner inspects available actions for an account or pocket
- **THEN** the following required behavior and constraints hold:

  The Accounts page SHALL provide a clearly labelled primary action appropriate to each account or pocket and a discoverable, labelled set of other supported operations in that item's detail (a centered dialog from 761 px, a bottom sheet below), opened without scrolling the page. The detail SHALL preserve child-pocket creation, investment buy and sell or top-up, manual valuation ("Perbarui nilai"), the switch to amount tracking, balance reconciliation where eligible, editing, and archiving. Action controls SHALL have targets of at least 24 by 24 CSS pixels (44 by 44 on phones), visible keyboard focus and meaningful accessible names. The detail SHALL trap focus while open, close with Escape, and return focus to the row that opened it.

#### Scenario: Acting on a liquid account
- **WHEN** a user opens the detail of a top-level liquid account, with or without pockets
- **THEN** its primary action adds a pocket and its other supported actions are listed in the detail

#### Scenario: Acting on an ordinary pocket
- **WHEN** a user opens the detail of an ordinary liquid pocket
- **THEN** its supported actions, including `Sesuaikan Saldo`, edit, move and archive, are listed in a detail titled with the pocket and its parent

#### Scenario: Acting on an investment holding
- **WHEN** a user opens the detail of an investment pocket that supports trading and valuation
- **THEN** buy and sell (or `Top up` for an amount-tracked product), valuation, edit, and archive actions remain available and the detail clearly separates the balance from investment performance information

#### Scenario: Reading dense investment pockets
- **WHEN** an investment platform holds long-named stock, mutual-fund, and gold pockets with units, market price, cost, value, and gain or loss
- **THEN** each row shows the product name, how it is tracked, its value and its gain or loss amount and percentage without overlapping another row, and units, market price and cost are also shown in the product's detail

#### Scenario: Opening and closing secondary actions by keyboard
- **WHEN** a keyboard user opens an account or pocket detail, moves through its actions, and closes it
- **THEN** all offered actions are reachable, focus remains visible, the page has not scrolled, and focus returns to the row that opened the detail
