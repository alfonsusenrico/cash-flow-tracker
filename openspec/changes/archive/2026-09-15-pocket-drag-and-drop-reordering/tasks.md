## 1. Frontend Pocket Drag-and-Drop Implementation

- [x] 1.1 Add pocket drag-and-drop state (`draggedPocketId`, `dragOverPocketId`, `orderedPocketsByParent`) in `frontend/src/app/accounts/page.tsx`
- [x] 1.2 Attach drag event handlers (`onDragStart`, `onDragOver`, `onDragLeave`, `onDrop`, `onDragEnd`) with `stopPropagation` to pocket cards
- [x] 1.3 Implement optimistic local reordering and dispatch `reorderMutation.mutate(newOrder)` on drop
- [x] 1.4 Apply visual feedback classes (`opacity-40 scale-[0.98]`, `ring-2 ring-emerald-500`, grab cursor) to pocket cards

## 2. Verification & Validation

- [x] 2.1 Run frontend type-check (`npm run type-check`)
- [x] 2.2 Run frontend production build (`npm run build`)
- [x] 2.3 Rebuild and restart frontend Docker container
- [x] 2.4 Validate change with `openspec validate pocket-drag-and-drop-reordering`

## 3. Account Card Alignment & Height Reduction

- [x] 3.1 Unify the card upper section (header & balance row) to use a consistent height container so the divider line aligns across all cards (e.g. BCA vs Jago)
- [x] 3.2 Place Total Akumulasi and Capital Gain on a single aligned sub-line
- [x] 3.3 Reduce pocket accordion max height from 500px to 250px (~40% reduction, showing minimum 2 cards) and tighten pocket card internal padding
- [x] 3.4 Type-check, rebuild, and verify in browser

