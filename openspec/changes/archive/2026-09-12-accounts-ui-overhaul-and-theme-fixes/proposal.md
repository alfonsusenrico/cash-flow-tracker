## Why

The current Accounts & Balance view and left navigation sidebar contain visual hierarchy imbalances, redundant controls, and theme styling bugs:
1. **Account Name Visual Under-Emphasis:** The account category (e.g., BANK, INVESTASI) is visually more prominent than the actual account name (e.g., BCA, Bibit). The account name should be a prominent, customizable colored tag badge with strong font contrast.
2. **Missing Card Priority & Ordering:** Users cannot prioritize or reorder their accounts according to personal importance or daily usage. The reordering should follow natural visual reading order (left-to-right, top-to-bottom) without cluttered numeric badges.
3. **Sidebar Redundancy & Clutter:** The left sidebar currently duplicates controls found in the top-right header (the "+ Catat Transaksi" button, privacy eye toggle, and theme switch), and contains unnecessary brand wordmark clutter ("CashFlow Financial OS").
4. **Light Theme Inversion & Washed-Out Appearance:** On the Accounts page, cards are hardcoded with dark backgrounds that look broken in light mode. Furthermore, the overall light theme lacks depth and contrast, feeling 90% glaring bright white without tactile card definition or accent richness.

## What Changes

- **Account Tag & Color Customization:** Highlight account names as large, prominent tags with customizable background colors (e.g. blue, emerald, purple, amber, indigo, rose, teal, slate) and calculated high-contrast typography. Existing accounts receive a clean default color.
- **Visual Order & Draggable Reordering:** Reorder accounts visually from left-to-right, top-to-bottom using native HTML5 drag-and-drop (without printing unnecessary numerical badges), persisting positions via `display_order` on the backend (`POST /api/accounts/reorder`).
- **Sidebar Streamlining:**
  - Remove redundant "+ Catat Transaksi" button from the sidebar (preserving the header button).
  - Remove redundant see/hide balance and theme toggle buttons from the sidebar footer.
  - Integrate the logout button directly into the user identity box.
  - Remove the "CashFlow Financial OS" title and avatar header to dedicate the sidebar strictly to navigation tabs.
- **Rich Light Theme Overhaul & Theme Bug Fix:**
  - Replace hardcoded dark card classes with semantic tokens (`var(--surface)`, `var(--surface-raised)`, `var(--border)`, `var(--text)`).
  - Upgrade light mode canvas from blinding white to a refined, soft slate canvas (`#F4F5F8`), giving white cards crisp structural contrast and elevation.
  - Add accent styling and ambient background mesh glows for depth and visual richness.

## Capabilities

### Modified Capabilities
- `executive-sidebar-layout`: Streamline the sidebar by removing brand wordmark header, redundant quick-action button, and redundant utility toggles, integrating logout into the user profile box.
- `clean-core-ledger`: Extend accounts persistence with `color` and `display_order`, add account reordering endpoint, overhaul account card visual hierarchy with prominent colored name tags, draggable reordering in natural reading order, and rich light theme styling.

## Impact

- **Database:** Migration `V8__account_tag_color_and_order.sql` adding `color VARCHAR(30) DEFAULT '#3b82f6'` and `display_order INT DEFAULT 0` to `accounts`.
- **Backend API:**
  - `backend/app/routers/accounts.py`: Update schemas for `color` and `display_order`, add `POST /api/accounts/reorder`, and sort account listings by `display_order ASC, created_at ASC`.
  - `backend/tests/test_clean_core.py`: Add test cases for account ordering, reordering endpoint, and color tag persistence.
- **Frontend UI:**
  - `frontend/src/components/layout/Sidebar.tsx`: Remove brand header, remove "+ Catat Transaksi" button, remove bottom utility icons row, embed logout button in user profile card.
  - `frontend/src/app/accounts/page.tsx`: Fix light theme card styles, render large colored account name tag, implement native drag-and-drop reordering, and add color picker in create/edit modals.
  - `frontend/tokens.css`: Refine light theme canvas tone, border definition, and surface contrast.
