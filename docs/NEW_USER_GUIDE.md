# Cash Flow Tracker New User Guide

This guide explains how to start using the app and how each menu relates to the others. The app is not only a transaction list. It is a connected personal finance system:

- Accounts hold balances.
- Transactions change account balances and feed summaries, analysis, and categories.
- Buckets describe money purposes.
- Allocation plans decide how income should be distributed into buckets.
- Strategy rules help preview allocation logic.
- Goals track long-term targets and can be linked to buckets.
- Assets and net worth track investments and overall wealth.

## Recommended Starting Order

Use the app in this order when setting it up for the first time.

1. Open Settings and set your pay cycle.

   Use the top bar `Set pay cycle` button. This defines the day of the month your financial cycle starts. Many summary and analysis views use this cycle instead of a plain calendar month.

2. Create Accounts.

   Add every cash account you want to track: bank accounts, wallets, spending accounts, savings accounts, and payroll accounts. Transactions need accounts, so accounts should be created first.

3. Review Categories.

   Categories are system-wide labels such as income, expense, transfer, and adjustment. They are used when recording transactions and later drive spending analysis.

4. Add Transactions.

   Record income, expenses, transfers, and adjustments. This is the main source of truth for cash flow. Summary, accounts, dashboard, analysis, and categories all depend on transaction data.

5. Create Buckets.

   Buckets are purposes for money: emergency fund, rent, food, tax, vacation, investing, debt repayment, and similar planned uses.

6. Create Allocation Plans.

   Allocation plans turn expected income into planned bucket funding. This is where you decide where money should go during a pay cycle.

7. Add Goals.

   Goals are longer-term targets. Link goals to buckets when a bucket represents the money container for that goal.

8. Add Assets and Record Net Worth.

   Assets track investments or holdings. Net worth combines cash balances plus invested assets and records snapshots over time.

9. Use Summary and Analysis for review.

   After data exists, Summary and Analysis become useful for understanding whether spending, savings, investments, and goals are on track.

## Core Concepts

### Pay Cycle

The app uses a pay cycle to decide the active reporting window. If your payday is the 25th, then the current cycle may run from the 25th of one month to the 24th of the next. This affects Summary, Transactions, Analysis, Dashboard metrics, and exports.

If no pay cycle has been saved yet, the top bar shows `Set pay cycle`. Save it in Settings before relying on the cycle-based numbers.

### Account

An account is a place where cash is stored. Examples:

- Main bank account
- Payroll account
- Cash wallet
- Savings account
- Fixed spending account

Transactions belong to accounts. Account balances are calculated from account starting balance plus transactions.

### Transaction

A transaction is a cash movement. It can be:

- Income: cash in
- Expense: cash out
- Transfer: moving money between two accounts
- Adjustment: correcting a balance

Transactions are the main data source for Summary, Accounts, Analysis, Dashboard, and Category spending.

### Bucket

A bucket is a planned purpose for money. It does not replace accounts. Accounts say where money is physically stored; buckets say what the money is for.

Examples:

- Emergency Fund
- Rent
- Groceries
- Annual Insurance
- Investment Contribution
- Vacation

Buckets can be linked to one or more accounts so the bucket can show an actual current amount. Buckets can also be linked to allocation items, strategy rules, and goals.

### Buckets vs Allocation vs Strategy vs Goals

These four menus are related, but they answer different questions:

| Menu | Main Question | What It Stores | What It Does Not Do |
| --- | --- | --- | --- |
| Buckets | What is this money for? | Named money purposes, targets, linked accounts. | Does not decide monthly funding by itself. |
| Allocation | How should this pay-cycle income be assigned? | A monthly/pay-cycle plan and allocation items. | Does not invent rules automatically. |
| Strategy | What reusable rule should guide allocation? | Rule templates, previews, and generated allocation plans. | Does not move money by itself. |
| Goals | What long-term target am I trying to reach? | Target amount, deadline, projection, optional linked bucket. | Does not represent a bank account by itself. |

Use them in this order when starting fresh: create accounts, create buckets for purposes, optionally create goals for long-term targets, use strategy to preview income rules, then generate or create an allocation plan for the current pay cycle.

### Allocation Plan

An allocation plan is a pay-cycle plan. It starts with expected income and breaks that amount into allocation items. Each item can be a fixed amount or a percentage of expected income. Items can optionally link to buckets.

Allocation plans influence Dashboard health metrics because active plan items become planned or committed allocations.

### Strategy Rule

A strategy rule is reusable allocation logic. It can say things like:

- Put 20 percent into emergency fund.
- Put a fixed amount into rent.
- Send the remainder to investing.

The Strategy page previews distribution for a sample income amount. After previewing, you can create an allocation plan from the active strategy rules.

### Goal

A goal is a target amount by a target date. Goals calculate progress and monthly requirements. If a goal links to a bucket, its progress comes from that bucket's linked account balances.

### Asset

An asset is an investment or non-cash holding, such as stocks, ETFs, crypto, gold, property, or other investments. Assets affect invested assets and net worth.

### Net Worth

Net worth is calculated from:

- Liquid assets from account balances
- Invested assets from asset holdings and latest prices
- Liabilities are not fully tracked yet

Snapshots let you record net worth over time.

## Menu Guide

## 1. Summary

Route: `/dashboard`

Summary is the main overview screen. It combines data from accounts, transactions, allocation plans, goals, and assets into a financial health view.

### What You See

- Health Score: overall financial health indicator.
- Safe to Spend: estimated money available after planned commitments.
- Net Worth: cash plus invested assets.
- Metrics: savings rate, investment rate, emergency fund, cash runway, monthly drift.
- Accounts Overview: current balances and cash movement by account.
- Goals Progress: top active goals.
- Quick Actions: add transaction, transfer, record net worth.

### Data Used

- Accounts and balances
- Transactions in the current pay cycle
- Active allocation plan items
- Goals
- Asset holdings and net-worth history

### How It Connects to Other Menus

- Transactions affect income, expenses, safe-to-spend, account balances, and analysis.
- Accounts determine liquid assets and account overview.
- Allocation affects planned spending and committed allocations.
- Goals appear in goal progress.
- Assets and Net Worth affect the net-worth card.

### When to Use It

Use Summary after you have entered accounts and transactions. It is best for answering: "Am I financially okay right now?"

## 2. Transactions

Route: `/ledger`

Transactions is the cash ledger. This is the most important operational page because it records the activity that drives the rest of the app.

### What You Can Do

- Add a transaction.
- Add a transfer between accounts.
- Edit a transaction.
- Delete a transaction.
- Filter by account, category, date range, search, and review state.
- Mark or view reviewed/unreviewed transaction state.

### Data Used

- Accounts
- Categories
- Transactions
- Pay-cycle date range

### How It Connects to Other Menus

- Accounts: every transaction belongs to an account and changes that account balance.
- Categories: transaction category selection feeds spending and income analysis.
- Summary: income, expenses, net cash movement, and safe-to-spend come from transactions.
- Analysis: charts and category breakdowns are computed from transactions.
- Net Worth: cash balances from transactions contribute to liquid assets.

### Best Practice

Enter transactions regularly. If the ledger is incomplete, almost every other screen becomes less useful.

## 3. Accounts

Route: `/accounts`

Accounts are where cash lives. Create accounts before entering transactions.

### What You Can Do

- Add an account with an initial balance.
- Edit account name and account profile.
- Delete an account.
- Search and filter accounts by profile type.
- Set profile details such as payroll source, buffer account, fixed spending, dynamic spending, no-limit account, and fixed limit amount.

### Data Used

- Account records
- Current balances from transactions
- Budgets for the current month or cycle

### How It Connects to Other Menus

- Transactions require accounts.
- Summary uses account balances and account movement.
- Net Worth uses account balances as liquid assets.
- Allocation and Buckets can refer to account-linked planning concepts.
- Assets can optionally link holdings to an account or broker.

### Account Profile Meaning

- Payroll source: account where income arrives.
- Buffer account: account used as safety reserve.
- Dynamic spending: account where spending can vary.
- Fixed spending: account intended for fixed costs.
- Savings/tabungan: account intended for holding money.

## 4. Buckets

Route: `/buckets`

Buckets describe what money is for. They are planning containers, not bank accounts.

### What You Can Do

- Create a bucket.
- Edit a bucket.
- Delete/archive a bucket.
- Set kind, target amount, linked accounts, priority, and notes.
- Search, filter by active/archived status, filter by priority, and sort.

### Data Used

- Bucket records
- Optional linked accounts
- Current account balances for linked accounts

### How It Connects to Other Menus

- Allocation items can link to buckets.
- Strategy rules can target buckets.
- Goals can link to buckets.
- Dashboard uses bucket and allocation data when computing financial health.
- Goals can use buckets as the money container behind a long-term target.

### Example

You might have one real bank savings account, but several buckets:

- Emergency Fund
- Laptop Replacement
- Vacation
- Annual Tax

The account says where the money is. The bucket says why it is reserved. If one bucket links to multiple accounts, the bucket current amount is the sum of those account balances.

## 5. Allocation

Route: `/allocation`

Allocation is where expected income becomes a plan.

### What You Can Do

- Create a pay-cycle allocation plan.
- Edit a draft plan.
- Delete a draft plan.
- Activate a draft plan.
- Add allocation items.
- Edit or delete allocation items.
- Fund an allocation item manually.

### Data Used

- Allocation plans
- Allocation items
- Buckets
- Expected income

### How It Connects to Other Menus

- Buckets: allocation items can point to buckets.
- Summary: active allocation items affect planned/committed money and safe-to-spend.
- Strategy: strategy rules can be used as a model when deciding what allocation items to create.
- Goals: if a goal is linked to a bucket, an allocation item can fund the bucket that supports the goal.

### Draft vs Active

Use draft status while planning. Activate a plan once it represents the current pay cycle. Active plans influence high-level financial metrics.

## 6. Analysis

Route: `/analysis`

Analysis explains what happened in a selected month or cycle. It is a review screen, not a data-entry screen.

### What You See

- Income
- Expenses
- Net cash flow
- Savings rate
- Daily net movement
- Spending by category
- Insights

### Data Used

- Transactions
- Categories
- Selected month
- Account movement

### How It Connects to Other Menus

- Transactions are the source data.
- Categories determine the spending breakdown.
- Accounts determine where the money moved.
- Summary links to Analysis when something needs review.

### When to Use It

Use Analysis after entering enough transactions for the month or pay cycle. It answers: "Where did my money go?"

## 7. Strategy

Route: `/strategy`

Strategy defines rules for how income should be distributed. It is useful before or during allocation planning.

### What You Can Do

- Create strategy rules.
- Edit or delete rules.
- Set rule priority.
- Set rule mode: percentage, fixed amount, target balance, or overflow/remainder.
- Link a rule to a bucket.
- Preview distribution for a sample income amount.
- Create an allocation plan from the preview.

### Data Used

- Strategy rules
- Buckets
- Preview income amount

### How It Connects to Other Menus

- Buckets are rule targets.
- Allocation is where the generated plan is reviewed and funded.
- Strategy can generate allocation items, but it does not move money or mark items funded.

### Rule Order

Rules are applied by type first, then priority inside each type:

- Fixed amount rules reserve exact amounts.
- Target balance rules fill the linked bucket shortfall.
- Percentage rules use total income as their calculation base.
- Overflow rules receive whatever remains.

Lower priority numbers run first inside the same type.

## 8. Goals

Route: `/goals`

Goals track long-term targets and show how much progress is needed.

### What You Can Do

- Create a goal.
- Edit or delete/cancel a goal.
- Set target amount and target date.
- Set inflation rate and expected return.
- Link a goal to a bucket.
- Add a manual contribution.

### Data Used

- Goal records
- Optional linked buckets and their linked account balances
- Projection math based on target date, target amount, inflation, and expected return

### How It Connects to Other Menus

- Buckets can represent the money container for a goal. When linked, goal progress uses the bucket balance.
- Allocation can fund a bucket that supports a goal.
- Summary shows top goal progress.
- Dashboard health considers active goals.

### Example

If your goal is "Emergency Fund 30,000,000", create:

- A bucket named Emergency Fund.
- A goal named Emergency Fund.
- Link the goal to the bucket.
- Add allocation items each cycle to fund that bucket.

## 9. Assets

Route: `/assets`

Assets track investments and non-cash holdings.

### What You Can Do

- Create an asset.
- Edit or delete/deactivate an asset.
- Add a holding to a specific asset.
- Record a price snapshot for a specific asset.
- Search holdings.

### Data Used

- Asset records
- Holdings
- Latest price snapshots
- Optional account/broker link

### How It Connects to Other Menus

- Net Worth uses assets as invested assets.
- Summary shows net worth and invested assets.
- Accounts can optionally be used as the account or broker for a holding.

### Important Limitation

Watchlist and investment notes are currently placeholders. They are visible as coming-soon areas and do not store user data yet.

## 10. Net Worth

Route: `/net-worth`

Net Worth combines liquid assets and invested assets, then records snapshots over time.

### What You Can Do

- Record today's net-worth snapshot.
- View current net worth.
- View liquid assets and invested assets.
- Change chart period.
- View recent or all snapshots.
- Open Assets for investment details.

### Data Used

- Account balances as liquid assets
- Asset holdings and latest prices as invested assets
- Net-worth snapshots

### How It Connects to Other Menus

- Accounts and Transactions determine liquid assets.
- Assets determine invested assets.
- Summary displays net worth and history signals.

### When to Use It

Record snapshots periodically, such as weekly, monthly, or after major asset updates. The history becomes more useful after several snapshots exist.

## 11. Categories

Route: `/categories`

Categories classify transactions. They are currently system-wide and read-only in the UI.

### What You Can Do

- Search categories.
- View categories grouped by type: income, expense, transfer, adjustment.

### Data Used

- Category records

### How It Connects to Other Menus

- Transactions use categories when you record income or expenses.
- Analysis uses categories to build spending breakdowns.
- Summary and Dashboard use categorized transaction data indirectly.

### Why Categories Matter

If transactions are uncategorized or categorized incorrectly, spending analysis becomes inaccurate. Review categories before entering many transactions so you understand the available options.

## Form Field Reference

This section documents the fields users see when creating or editing data. "Required" means the app expects a value before the record is useful or can be saved correctly. "Optional" means the field can be left blank or at its default value.

## Settings Fields

Settings is opened from the top bar.

### Payday

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Payday day | Required for accurate cycle reporting | Day of month when your pay cycle starts, from 1 to 31. | Controls the active pay-cycle range used by Summary, Transactions date range, Analysis context, exports, and cycle-based metrics. |

### Privacy

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Hide balances | Optional | Masks money values on screen. | Does not change stored data. It only hides displayed amounts for privacy. |

### API Key

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Reset API Key | Optional | Generates a new API key for external access. | The old key stops working. The new key is shown once, so copy it immediately if you use API integrations. |

## Transactions Fields

Transactions are created from `Transactions > Add Transaction` or the quick action `Add Transaction`.

### Add or Edit Transaction

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Cash In / Cash Out | Required | Direction of money movement. Cash In is income; Cash Out is spending. | Determines whether the transaction increases or decreases the selected account balance. It also filters available categories by income or expense type. |
| Account | Required | Account where the transaction happened. | Updates that account's balance and appears in account summaries. |
| Description | Required | Human-readable transaction name, such as "Lunch" or "Salary". | Used in the ledger, search, exports, and detail panels. |
| Amount | Required | Transaction amount. | Changes account balance and feeds income, expense, safe-to-spend, analysis, and dashboard metrics. |
| Date & Time | Required | When the transaction happened. | Determines which pay cycle, month, analysis period, and summary range includes this transaction. |
| Category | Optional | Income or expense category. | Feeds spending-by-category analysis. If left blank, transaction totals still count, but category analysis is less useful. |
| Notes | Optional | Extra context about the transaction. | Stored on the transaction detail. Useful for later review, but does not affect calculations. |
| Tags | Optional | Comma-separated labels such as `reimbursable` or `recurring`. | Helps classify and search transactions. Tags do not directly affect balances. |
| Mark as reviewed | Optional | Indicates that you already checked this transaction. | Supports review workflows and filters. It does not change money values. |
| Mark as Payroll / Top-up | Optional, only available for Cash In | Marks income as payroll/top-up for the cycle. | Helps distinguish regular income or cycle funding from other cash-in entries. |

### Transfer Between Accounts

Transfers are created from `Transactions > Transfer` or the quick action `Transfer`.

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| From Account | Required | Account money leaves. | Creates the outgoing side of the transfer and lowers this account's balance. |
| To Account | Required | Account money enters. Must be different from From Account. | Creates the incoming side of the transfer and raises this account's balance. |
| Amount | Required | Transfer amount. | Moves money between accounts without treating it as income or expense. |
| Date & Time | Required | When the transfer happened. | Determines where the transfer appears in the ledger and reporting period. |

## Accounts Fields

Accounts are created from `Accounts > Add Account`.

### New Account / Edit Account

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Account Name | Required | Display name of the account. Example: "BCA Main", "Cash Wallet", "Payroll Account". | Used everywhere the account appears: Transactions, Summary, Accounts, Assets broker selection, and filters. |
| Initial Balance | Optional; create only | Starting balance when the account is created. | Sets the opening balance baseline. Later transactions build on top of this amount. This field is not shown when editing an existing account. |
| Monthly Budget Limit | Optional | Budget cap for the account in the current month. | Feeds account budget progress and account overview. If left at 0, no budget limit is created. |
| Profile Type | Required | How the account should be treated conceptually. Options are Dynamic Spending, Fixed Spending, and Savings. | Used for account grouping and future spending logic. It also helps users understand account purpose. |
| Payroll source | Optional checkbox | Marks this account as where salary or income normally arrives. | Shows in account stats and helps identify income source accounts. |
| Buffer account | Optional checkbox | Marks this account as a reserve or safety account. | Shows in account stats and helps distinguish emergency or buffer cash. |
| No spending limit | Optional checkbox | Indicates this account should not be constrained by a normal budget limit. | Saved on the account profile. Useful for accounts that should not be judged by spending-limit logic. |

### Profile Type Details

| Profile Type | Use It For | Effect |
| --- | --- | --- |
| Dynamic Spending | Flexible spending accounts such as daily wallet, debit account, or card account. | Useful when spending varies and should be monitored. |
| Fixed Spending | Accounts used for predictable recurring payments. | Helps separate fixed obligations from flexible spending. |
| Savings | Savings, reserve, or holding accounts. | Helps distinguish stored money from active spending money. |

## Buckets Fields

Buckets are created from `Buckets > New Bucket`.

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Name | Required | Bucket name, such as "Emergency Fund", "Rent", or "Vacation". | Used by Allocation, Strategy, Goals, and bucket lists. |
| Kind | Required | Bucket type: spending, sinking, emergency, goal, investment. | Controls the bucket icon/color and helps classify the purpose. |
| Target Amount | Optional | Desired amount for this bucket. | Shows target and progress context. It can guide allocation and goal planning. |
| Linked Accounts | Optional | One or more accounts related to this bucket. | Bucket current amount becomes the sum of selected account balances. It does not move money automatically. If the same account is linked to several buckets, that balance appears in each linked bucket. |
| Priority | Optional, defaults to medium-style value | Numeric priority. Lower number means higher priority. | Used for sorting and priority labels. High-priority buckets should be funded earlier. |
| Notes | Optional | Description or reminder for this bucket. | Shown on bucket cards. Does not affect calculations. |

## Allocation Fields

Allocation has multiple forms: plan, item, and funding confirmation.

### Allocation Plan

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Month (YYYY-MM) | Required | Month this allocation plan belongs to. | Identifies the plan period. When editing a draft plan, this field is locked. |
| Expected Income | Optional but recommended | Income amount you expect to allocate for the plan. | Used to calculate percentage allocation item amounts and plan totals. If left at 0, percentage items calculate to 0. |

### Allocation Item

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Label | Required | Name of the item, such as "Rent", "Emergency Fund", or "Investing". | Appears in the allocation item table and plan summary. |
| Mode | Required | Percent of income or fixed amount. | Controls how planned amount is calculated. |
| Percentage | Required when mode is Percent | Percent of expected income assigned to this item. | Planned amount equals expected income multiplied by this percentage. |
| Amount | Required when mode is Fixed | Fixed rupiah amount assigned to this item. | Planned amount equals the entered amount. |
| Bucket | Optional | Bucket this allocation item funds. | Connects allocation planning to bucket purposes and goal-related buckets. |

### Fund Allocation Item

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Amount to fund | Required for funding action | Amount you want to mark as funded for that allocation item. | Increases funded amount and changes item progress/status. It does not create a bank transaction by itself. |

## Strategy Fields

Strategy rules are created from `Strategy > Add Rule`.

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Name | Required | Rule name, such as "Emergency 20%" or "Rent Fixed". | Displayed in strategy table and preview output. |
| Trigger | Required | When the rule conceptually applies. Options are Manual and On Income Arrival. | Stored with the rule. Current UI uses rules mainly for preview and planning. |
| Mode | Required | Allocation method: Percentage, Fixed Amount, Target Balance, or Remainder. | Determines how preview calculates suggested allocation. |
| Percentage (%) | Required when mode is Percentage | Percent of total income to allocate. | Preview and generated allocation plans calculate this from expected income. |
| Amount | Required for fixed/target-balance fallback modes | Rupiah amount. | Fixed rules allocate this amount. Target balance rules use the linked bucket shortfall when the bucket has a target; otherwise they use this fallback amount. |
| Target Bucket | Optional | Bucket the rule sends money to. | Shows bucket name in strategy table and preview; connects strategy to buckets. |
| Priority | Required | Numeric order inside the same rule type. Lower number runs first. | Used after the fixed/target/percent/overflow type order. |
| Active | Optional checkbox, defaults on | Whether the rule is active. | Inactive rules remain saved but are ignored by preview and plan generation. |

### Preview Distribution Field

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Income Amount | Required for preview | Sample income amount to test against the active rules. | Calculates suggested allocation results. Preview does not create transactions or allocation items. |
| Pay Cycle Ending Month | Required for plan creation | Month label for the allocation plan, such as `2026-05`. | Strategy uses this when creating the allocation plan. Duplicate months are rejected. |
| Create Allocation Plan | Optional action after preview | Writes the preview result into Allocation. | Creates a draft allocation plan and allocation items. It does not fund the items or create bank transactions. |

## Goals Fields

Goals are created from `Goals > Add New Goal`.

### New Goal / Edit Goal

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Goal Name | Required | Goal label, such as "Emergency Fund" or "House Down Payment". | Used in goal cards, Summary goal progress, and dashboards. |
| Target Amount | Required | Amount you want to reach. | Used for progress percentage and required monthly contribution calculations. |
| Target Date | Optional | Date by which you want to reach the goal. | Enables deadline and projection calculations. Without a date, the app can still track amount progress but deadline guidance is limited. |
| Inflation Rate (%/year) | Optional, defaults to 0 | Assumed annual inflation for the goal. | Increases inflation-adjusted target in projections. Useful for future expenses. |
| Expected Return (%/year) | Optional, defaults to 0 | Expected annual growth on saved/invested money. | Affects projection and required monthly contribution estimates. |
| Linked Bucket | Optional | Bucket related to the goal. | If set, goal progress is derived from the bucket's linked account balances instead of manual goal contributions. |
| Status | Required when editing | Active, Paused, or Completed. | Controls goal state and whether it appears as active progress. |

### Goal Contribution

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Amount | Required for contribution action | Manual contribution amount toward an unlinked goal. | Increases manual goal progress. Linked goals use bucket balance instead and cannot be manually contributed to from the goal card. |

## Assets Fields

Assets has forms for assets, holdings, and price updates.

### New Asset / Edit Asset

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Name | Required | Asset name, such as "BBCA", "Bitcoin", or "Gold". | Used in holdings table, asset allocation, and net-worth calculations. |
| Class | Required | Asset class: stock, ETF, mutual fund, bond, crypto, metal, property, or other. | Groups asset allocation and controls icon/color. |
| Ticker | Optional | Market symbol or shorthand, such as `BBCA.JK`. | Helps identify the asset. Current price updates are manual, so ticker does not fetch prices automatically. |

### Add Holding

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Quantity | Required | Number of units held. | Used with latest unit price to calculate current value. |
| Cost Basis | Optional but recommended | Total purchase cost for the holding. | Used to calculate average buy price and unrealized gain/loss. |
| Acquired Date | Required | Date the holding was acquired. | Stored for holding history and context. |
| Account/Broker | Optional | Account or broker associated with the holding. | Helps connect investments to where they are held. Does not change cash account balance. |

### Update Price

| Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Unit Price | Required | Latest known price per unit. | Updates current asset value and unrealized gain/loss calculations. |
| As of Date | Required | Date for the price snapshot. | Determines price date shown in holdings and net-worth calculations. |

## Net Worth Fields

Net Worth has one primary action: `Record Today`.

| Action / Field | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Record Today | Optional action | Records a snapshot using current account balances and asset values. | Adds or updates today's net-worth snapshot. This builds net-worth history over time. |

Net worth snapshots are calculated from existing data. You do not manually enter the snapshot amount on the Net Worth page.

## Categories Fields

Categories are currently read-only in the UI.

| Field / Control | Required | What It Means | Effect |
| --- | --- | --- | --- |
| Search categories | Optional | Filters the category list by name. | Helps users find available categories before assigning them to transactions. |

Category creation and editing exist in the backend, but the current UI presents categories as system-wide read-only data to keep transaction classification consistent.

## Quick Actions

Quick actions appear in the sidebar and Summary page.

- Add Transaction opens the transaction form.
- Transfer opens the transfer form.
- Record Net Worth records a net-worth snapshot.
- New Bucket opens the bucket creation form.

Use these for common daily actions after initial setup.

## Common Workflows

### Daily or Weekly Tracking

1. Open Transactions.
2. Add income and expenses.
3. Categorize each transaction.
4. Use Summary to check safe-to-spend.
5. Use Analysis to review spending patterns.

### Payday Planning

1. Confirm your pay cycle in Settings.
2. Open Strategy and preview how income should be split.
3. Create an allocation plan from the strategy preview.
4. Review the generated allocation items in Allocation.
5. Activate the plan.
6. Fund allocation items as money is assigned.

### Saving for a Goal

1. Create a bucket for the purpose.
2. Create a goal with target amount and target date.
3. Link the goal to the bucket.
4. Add allocation items each cycle to fund that bucket.
5. Track progress in Goals and Summary.

Example: if your emergency fund is split across two Jago pockets, link both accounts to one Emergency Fund bucket. Then create an Emergency Fund goal and link it to that bucket. The goal progress will use the combined balance from both linked Jago pockets.

### Investment Tracking

1. Open Assets.
2. Create assets such as stocks, ETFs, crypto, gold, or property.
3. Add holdings for each asset.
4. Record price snapshots when prices change.
5. Open Net Worth and record a snapshot.
6. Review Summary for total net worth.

## Data Dependency Map

Use this map to understand why one menu may look empty until another menu has data.

| Menu | Depends On | Feeds Into |
| --- | --- | --- |
| Summary | Accounts, transactions, allocation, goals, assets, net worth | Overall review |
| Transactions | Accounts, categories | Summary, accounts, analysis, net worth |
| Accounts | User-created accounts, transactions | Transactions, summary, net worth, assets |
| Buckets | User-created buckets, optional accounts | Allocation, strategy, goals |
| Allocation | Buckets, expected income | Summary, dashboard health |
| Analysis | Transactions, categories | Review decisions |
| Strategy | Buckets, strategy rules | Allocation planning decisions |
| Goals | Goals, optional buckets | Summary, dashboard health |
| Assets | Asset records, holdings, price snapshots | Net worth, summary |
| Net Worth | Accounts, assets, snapshots | Summary, wealth history |
| Categories | System categories | Transactions, analysis |

## What To Do If A Page Looks Empty

- Summary is empty or low value: add accounts and transactions.
- Transactions is empty: add an account first, then add transactions.
- Accounts has no balances: add transactions or create accounts with initial balances.
- Buckets is empty: create buckets for money purposes.
- Allocation is empty: create an allocation plan, then add items.
- Analysis is empty: add categorized transactions.
- Strategy is empty: create rules and target buckets.
- Goals is empty: create goals and optionally link buckets.
- Assets is empty: create assets and add holdings.
- Net Worth has no history: record snapshots.
- Categories is empty: this usually indicates categories have not been seeded or loaded.
