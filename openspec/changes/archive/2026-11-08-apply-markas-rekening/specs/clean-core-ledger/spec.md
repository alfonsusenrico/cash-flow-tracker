## MODIFIED Requirements

### Requirement: Parent Balance Aggregation and Grouped Representation
The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account.

#### Scenario: Required behavior and constraints
- **WHEN** the application computes or displays an account hierarchy balance
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account:
  1. `GET /api/accounts` SHALL return accounts with `parent_id`, `is_parent`, `children: [...]`, and aggregated total balance for master accounts.
  2. Transactions recorded against a child pocket SHALL directly debit or credit that specific pocket and automatically reflect in the master account's aggregated balance.
  3. Account selectors across Quick Capture, Ledger, and Transfers SHALL present pockets grouped under their respective master accounts using `<optgroup>`.
  4. The Accounts page SHALL show each pocket as a tile naming its parent ("Kantong di {parent}") in words for assistive technology, with its balance and relevant investment details. Under "Per rekening", a master account SHALL be one group whose total is the server's aggregated balance; the page SHALL NOT add pocket balances itself, and a master's own balance SHALL be one tile "saldo di luar kantong".

#### Scenario: Querying accounts with pockets
- **WHEN** a user with Bank Jago having "Kantong Utama" (Rp 5.000.000) and "Kantong Darurat" (Rp 15.000.000) calls `GET /api/accounts`
- **THEN** Bank Jago reports a total aggregated balance of Rp 20.000.000 with both child pockets nested under it

#### Scenario: Recording expense from a specific pocket
- **WHEN** a user records an expense of Rp 50.000 from "Kantong Utama"
- **THEN** "Kantong Utama" balance decreases by Rp 50.000 and the parent "Bank Jago" aggregated balance decreases by Rp 50.000

#### Scenario: Expanding a pocket list on a narrow screen
- **WHEN** a user views a parent with many pockets under Per rekening on a 390 px wide viewport
- **THEN** each pocket tile remains associated with that parent by its group and its meta line, values reflow without horizontal page overflow, and the group total equals the server's aggregated balance

### Requirement: Account and Pocket Action Clarity
The Accounts page SHALL open each account, pocket or product in a detail dialog whose footer shows one primary action, one secondary action and every other supported operation under a labelled "Lainnya" menu. The detail SHALL preserve pocket creation, buy, sell or top-up, "Perbarui nilai", the switch to amount tracking, balance checks and adjustments where eligible, editing, moving and archiving.

#### Scenario: Required behavior and constraints
- **WHEN** an owner inspects available actions for an account or pocket
- **THEN** the following required behavior and constraints hold:

  The detail SHALL be a centered dialog from 768 px and a bottom sheet below, opened without scrolling the page. Action controls SHALL have targets of at least 24 by 24 CSS pixels (44 by 44 on phones), visible keyboard focus and meaningful accessible names. The "Lainnya" menu SHALL move with the arrow keys and close with Escape. The detail SHALL trap focus while open, close with Escape, and return focus to the tile or row that opened it.

#### Scenario: Acting on a liquid account
- **WHEN** a user opens the detail of a top-level liquid account that can be checked
- **THEN** "Cocokkan saldo" is primary, "Ubah rekening" secondary, and "Tambah kantong" and "Arsipkan" are under "Lainnya"; for a parent with pockets "Tambah kantong" is primary

#### Scenario: Acting on an ordinary pocket
- **WHEN** a user opens the detail of an ordinary liquid pocket
- **THEN** its supported actions, including `Sesuaikan saldo`, edit, opening its parent and archive, are offered in a detail titled with the pocket and naming its parent

#### Scenario: Acting on an investment holding
- **WHEN** a user opens the detail of an investment pocket that supports trading and valuation
- **THEN** "Beli" is primary and "Jual" secondary (or "Top up" and "Perbarui nilai" for an amount-tracked product), valuation, edit and archive remain under "Lainnya", and the detail separates the value from the gain

#### Scenario: Reading dense investment pockets
- **WHEN** an investment platform holds long-named stock, mutual-fund, and gold pockets with units, market price, cost, value, and gain or loss
- **THEN** each tile shows the product name, how it is tracked, its value and its gain amount and percentage without overlapping another tile, and units, market price and cost are shown in the product's detail

#### Scenario: Opening and closing secondary actions by keyboard
- **WHEN** a keyboard user opens a detail, opens "Lainnya", moves through its items, and closes both
- **THEN** all offered actions are reachable, focus remains visible, the page has not scrolled, and focus returns to "Lainnya" and then to the tile that opened the detail
