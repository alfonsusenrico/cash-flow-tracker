## 1. Database & Backend Account Ordering & Color

- [x] 1.1 Create migration `db/migrations/V8__account_tag_color_and_order.sql` adding `color VARCHAR(30) DEFAULT '#3b82f6'` and `display_order INT DEFAULT 0` to `accounts`
- [x] 1.2 Update `backend/app/db/init_db.py` with the new columns
- [x] 1.3 Update Pydantic schemas `AccountCreate`, `AccountUpdate` in `backend/app/routers/accounts.py`
- [x] 1.4 Update `get_accounts_with_balances` and `list_accounts` in `backend/app/routers/accounts.py` to sort by `display_order ASC, created_at ASC, name ASC` and serialize `color` and `display_order`
- [x] 1.5 Implement `POST /api/accounts/reorder` endpoint in `backend/app/routers/accounts.py`
- [x] 1.6 Add backend tests for account color persistence, display ordering, and reordering API in `backend/tests/test_clean_core.py`

## 2. Sidebar Navigation Streamlining

- [x] 2.1 Remove brand avatar `CF` and titles `CashFlow` / `Financial OS` from `frontend/src/components/layout/Sidebar.tsx`
- [x] 2.2 Remove "+ Catat Transaksi" button from `frontend/src/components/layout/Sidebar.tsx`
- [x] 2.3 Remove redundant eye (see/hide balance) and theme switcher buttons from sidebar footer
- [x] 2.4 Embed logout button directly into the user profile box in `frontend/src/components/layout/Sidebar.tsx`
- [x] 2.5 Apply corresponding cleanups to `MobileDrawer` in `Sidebar.tsx`

## 3. Accounts Page UI Overhaul, Visual Ordering & Rich Light Theme

- [x] 3.1 Refine light mode design tokens in `frontend/tokens.css`: upgrade paper canvas to `#F4F5F8`, strengthen structural borders, and add ambient visual depth
- [x] 3.2 Fix light theme styling on `frontend/src/app/accounts/page.tsx`: replace hardcoded dark gradients with semantic design tokens across hero banner, cards, and pocket lists
- [x] 3.3 Highlight account name as a prominent tag badge with customizable background color and high-contrast font calculation
- [x] 3.4 Add color swatch picker to Account Create & Edit modals in `frontend/src/app/accounts/page.tsx`
- [x] 3.5 Implement native HTML5 drag-and-drop reordering reflecting natural visual reading order (left-to-right, top-to-bottom) without printed numbers, mutating to `POST /api/accounts/reorder`

## 4. Verification & Validation

- [x] 4.1 Run backend pytest suite to verify 100% pass rate
- [x] 4.2 Run frontend type-check, lint, and build
- [x] 4.3 Validate OpenSpec change integrity (`openspec validate accounts-ui-overhaul-and-theme-fixes`)
