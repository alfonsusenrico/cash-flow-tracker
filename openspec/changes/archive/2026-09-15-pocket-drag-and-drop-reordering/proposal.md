# Proposal: Pocket Drag-and-Drop Reordering

## Why
While top-level digital bank and cash cards support fluid drag-and-drop reordering, child pockets (e.g. stock instruments under Stockbit, or specific purpose pockets under Bank Jago/BCA) are currently locked to their database creation order. Users need the flexibility to prioritize their most active investment positions or high-priority pockets by manually dragging them into their preferred sequence.

## What Changes
- Enable drag-and-drop interactions on pocket cards inside the parent account accordion.
- Add tactile visual cues:
  - Drag handle / grip indicator (`cursor-grab`, `active:cursor-grabbing`).
  - Active drag elevation and dimming (`opacity-40 scale-[0.98]`).
  - Drop target highlight (`ring-2 ring-emerald-500/80 border-emerald-500`).
- Optimistically update local pocket order state per parent account to ensure instantaneous UI response without latency or layout shift.
- Synchronize the new sequence to the backend via the existing `POST /api/accounts/reorder` endpoint to persist `display_order` across page reloads.

## Capabilities
- `pocket-reordering`: Seamless drag-and-drop reordering of child pockets within their parent account card.

## Impact
- **Frontend**: `frontend/src/app/accounts/page.tsx`
- **Backend**: Leverages existing `POST /api/accounts/reorder` endpoint and `display_order` database column without requiring database migrations.
- **Dependencies**: No additional external libraries required (uses native HTML5 Drag and Drop API).

## Expected Outcome
Users can grab any pocket card inside an account accordion, drag it above or below sibling pockets, and drop it. The new pocket sequence reflects immediately and remains saved across browser refreshes.
