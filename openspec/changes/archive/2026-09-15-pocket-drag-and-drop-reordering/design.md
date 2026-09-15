# Design: Pocket Drag-and-Drop Reordering

## Technical Strategy

### 1. Drag-and-Drop Lifecycle & State Isolation
Drag-and-drop state will be scoped so that reordering operations only apply between sibling pockets under the same parent account, preventing inadvertent cross-parent movement:
- `draggedPocketId: string | null` — Tracks the pocket currently being dragged.
- `dragOverPocketId: string | null` — Tracks the target pocket currently under the pointer.
- `orderedPocketsByParent: Record<string, string[]>` — Local optimistic order cache mapping `parentId` to array of `pocketId`s.

### 2. Event Handlers on Pocket Cards
- `draggable`: Set to true on each pocket container.
- `onDragStart`: Captures `draggedPocketId` and sets `dataTransfer.effectAllowed = "move"`. Stops propagation to prevent parent account card drag from triggering.
- `onDragOver`: Calls `e.preventDefault()` to allow dropping, sets `dragOverPocketId`.
- `onDragLeave`: Clears `dragOverPocketId` if leaving the target pocket.
- `onDrop`: 
  1. Prevents default and checks that `draggedPocketId` belongs to the same parent account.
  2. Calculates `fromIndex` and `toIndex` within the parent's child pocket list.
  3. Splices and reorders the pocket list optimistically in state.
  4. Calls `reorderMutation.mutate(newOrder)` to persist new `display_order` to Postgres.
  5. Cleans up drag state.
- `onDragEnd`: Resets `draggedPocketId` and `dragOverPocketId`.

### 3. Backend Persistence Contract
Reuses the existing endpoint:
```http
POST /api/accounts/reorder
Content-Type: application/json

{
  "account_ids": ["pocket-uuid-1", "pocket-uuid-2", "pocket-uuid-3"]
}
```
The backend iteratively executes:
```sql
UPDATE accounts SET display_order = %s, updated_at = NOW()
WHERE user_id = %s AND id = %s;
```
Because `get_accounts_with_balances` sorts by `ORDER BY a.display_order ASC, a.created_at ASC, a.name ASC`, subsequent queries naturally preserve the updated child order.

### 4. Visual Feedback & Anti-AI-Slop Standard
- **Elevation & Opacity**: Dragged card drops to `opacity-40 scale-[0.98]`.
- **Target Indicator**: Subtle emerald border and ring (`ring-2 ring-emerald-500/80 border-emerald-500`) indicating valid drop zone.
- **Tactile Cursor**: `cursor-grab active:cursor-grabbing` on the pocket card container.
- No slow bouncy physics or blurry glowing gradients.

### 5. Trade-offs & Guardrails
- **Scope Limit**: Sibling-only reordering. Moving pockets across different parent accounts (e.g., transferring a pocket from Bank Jago to BCA) is intentionally restricted to the explicit "Ubah Kantong" modal to avoid accidental misallocations.
