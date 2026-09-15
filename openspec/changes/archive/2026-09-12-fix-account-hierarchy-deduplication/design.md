## Context

In personal finance, users structure accounts hierarchically (e.g. Master Bank "Jago" with pockets "Dana Darurat", "Cicilan"; or Investment Hub "Bibit" with mutual fund pockets "Sucorinvest RDPU" and "TRIM Kas"). 
When linking accounts to Savings Goals (`goals`), the system allows selecting master accounts or individual pockets.
However, because parent accounts already aggregate their child pockets into their own balance, naive summation across both levels causes severe double-counting. Furthermore, an array flattening bug in the frontend modal duplicated every child pocket in the checklist.

## Goals / Non-Goals

**Goals:**
- Eliminate double counting in goals balance calculation: if parent is selected, child pockets under it are automatically not added again.
- Fix UI checkbox selection UX: selecting a parent selects its pockets; unchecking a pocket deselects the parent while keeping other pockets active; checking all pockets re-selects the parent.
- Provide live, accurate deduplicated feedback in the goal creation/edit modal banner.
- Ensure backend endpoints (`/goals`, `/dashboard`, `/pulse`) perform identical canonical deduplication.
- Maintain full backward compatibility for existing goals and unlinked goals.

**Non-Goals:**
- Complex partial percentage allocation of a pocket across multiple goals (pockets are either linked or unlinked).
- Dynamic restructuring of account trees within the goal modal (account management remains in `/accounts`).

## Decisions

### 1. Hierarchy Deduplication Rule
- **Rule:** Given a set of linked account IDs $S$, an account $a \in S$ contributes its balance if and only if:
  $$\text{parent\_id}(a) = \text{null} \quad \lor \quad \text{parent\_id}(a) \notin S$$
- **Rationale:** 
  - Parent account balances in our unified ledger model already represent the sum of their own ledger balance plus all child pocket balances.
  - If a user selects the parent account, the whole account is covered. Any child pocket of that parent that is also selected must be omitted from the sum to prevent double counting.
  - If the parent is NOT selected, child pockets contribute their exact individual balances.

### 2. Frontend Account Mapping and State Management
- **Indexed Map:** In `frontend/src/app/goals/page.tsx`, index accounts into `Map<string, Account>` by ID instead of flatMapping.
- **Checkbox Synchronization:**
  - When parent is toggled ON: add parent ID and all its child pocket IDs to `goalAccountIds`.
  - When parent is toggled OFF: remove parent ID and all child pocket IDs.
  - When a child pocket is toggled OFF: remove that pocket and remove parent ID (expanding all sibling pockets if parent was previously selected).
  - When a child pocket is toggled ON: add that pocket. If all siblings are now checked, also add parent ID.

### 3. Backend Goal Formatting (`goals.py`)
- In `format_goal_row`:
  - Calculate `current` by filtering `linked_accounts` to `contributing_accounts` where `not a.get("parent_id") or str(a.get("parent_id")) not in linked_ids`.
  - Expose only `contributing_accounts` in `linked_accounts` payload so that UI chips accurately represent the non-redundant funds.
  - Maintain all selected IDs in `account_ids` to preserve complete modal checkbox states on edit.

### 4. Dashboard Ketahanan Dana & Pulse Consistency
- In `backend/app/routers/dashboard.py`:
  - Construct `acc_parent_map` and apply the deduplication filter in `calculate_ketahanan_dana` and `get_dashboard_overview`.
- In `backend/app/routers/pulse.py`:
  - Query `get_accounts_with_balances` and sum accounts where `parent_id IS NULL`.

## Risks / Trade-offs

- **[Risk] Multiple sibling pockets selected without parent:**
  - *Mitigation:* The deduplication rule permits any combination of child pockets without parent to sum accurately.
- **[Risk] UI desync between parent toggle and individual pocket checkboxes:**
  - *Mitigation:* Bi-directional toggle handlers keep the selection set fully consistent.
