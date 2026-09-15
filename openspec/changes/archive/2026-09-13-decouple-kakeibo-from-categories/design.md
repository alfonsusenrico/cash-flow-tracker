# Technical Design: Decouple Kakeibo from Categories

## Technical Strategy

The objective is to eliminate the coupling between Categories and Kakeibo classifications, shifting Kakeibo entirely to a transaction-level attribute while maintaining full backward compatibility.

### 1. Frontend Modifications

#### A. `QuickCaptureModal.tsx`
- In the category `<select>` render loop, replace:
  `{c.name} {c.kakeibo_type ? '(${c.kakeibo_type})' : ''}`
  with:
  `{c.name}`
- In `handleCategoryChange`:
  Remove `setKakeiboType(found.kakeibo_type || ...)` so selecting a category preserves the active Kakeibo chip chosen by the user.

#### B. `insights/page.tsx`
- In the Category Create/Edit modal:
  Remove the `catKakeiboType` state and the 3-way Kakeibo pillar selector. Categories only send `name`, `icon`, `color`, `monthly_budget`, and `is_primary`.
- In the Category budget variance table:
  Remove the "Pilar Kakeibo" column.
- The Kakeibo breakdown tab remains intact, displaying the 50/30/20 metrics aggregated directly from transaction-level records.

#### C. `CategoryDonutChart.tsx`
- In the Dominant Category highlight banner and top categories list, remove the `Need` / `Want` pill badges so categories are presented purely as category domains.

### 2. Backend Modifications

#### `backend/app/routers/dashboard.py`
In `get_kakeibo_breakdown`:
Query `transactions` directly:
```sql
SELECT 
    COALESCE(t.kakeibo_type, 'need') AS pillar,
    COALESCE(SUM(t.amount), 0) AS total_amount
FROM transactions t
WHERE t.user_id = %s 
  AND t.type = 'expense' 
  AND t.date >= %s AND t.date <= %s
GROUP BY pillar
```
This guarantees that each transaction's own `kakeibo_type` is authoritative, without category-level fallback altering the results.
