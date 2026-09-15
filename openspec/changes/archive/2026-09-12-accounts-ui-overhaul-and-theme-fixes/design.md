## Context

The personal finance web client features a 3-tier view: Pulse, Insights, and Accounts.
User feedback on the Accounts view highlighted key usability and aesthetic needs:
1. Emphasizing account identities (BCA, Bibit, Jago) rather than generic account categories (Bank, Investasi).
2. Allowing custom priority ordering via drag-and-drop based on natural visual reading order (left-to-right, top-to-bottom) without printing redundant number badges.
3. Overhauling the light theme: fixing hardcoded dark cards on the Accounts page and adding richer accent colors/depth so the interface avoids looking like a washed-out 90% bright white sheet.
4. Streamlining the left sidebar to eliminate redundant controls and branding.

## Goals / Non-Goals

**Goals:**
- **Prominent Tagged Account Names:** Render account names as noticeable, tactile tags with customizable background colors and calculated high-contrast typography.
- **Natural Visual Reordering:** Drag-and-drop reordering reflecting natural visual reading order (left-to-right, top-to-bottom) without printing unnecessary number badges.
- **Rich & Tactile Light Theme:** Upgrade the light mode canvas to a refined soft slate tone (`#F4F5F8`), enhance border visibility, add subtle ambient background accents, and ensure 100% theme fidelity across cards and modals.
- **Unified Sidebar Navigation:** Strip out redundant "+ Catat Transaksi" buttons, eye/theme toggles, and brand wordmarks from the sidebar, focusing it strictly on navigation tabs and user identity with logout.

**Non-Goals:**
- Numeric index badges on cards (ordering is purely visual/positional).
- Multi-level nested folder dragging (reordering applies to root account cards; child pockets stay under their parent).
- Full custom hex color pickers (use a curated 8-color preset palette for design consistency).

## Decisions

### 1. Database & API Schema: Account Color and Display Order
- **Columns in `accounts`:**
  - `color VARCHAR(30) NOT NULL DEFAULT '#3b82f6'`
  - `display_order INT NOT NULL DEFAULT 0`
- **Reorder Endpoint:**
  - `POST /api/accounts/reorder`
  - Body: `{"account_ids": ["uuid1", "uuid2", ...]}`
  - Updates `display_order` based on array index in a single database transaction.
- **Listing Order:**
  - `SELECT ... FROM accounts ... ORDER BY a.display_order ASC, a.created_at ASC, a.name ASC`

### 2. High-Contrast Account Tag Design
- Account name tag badge:
  - Rendered prominently above the balance with `font-bold text-sm sm:text-base px-3 py-1 rounded-xl`.
  - Contrast calculation helper:
    $$\text{Luminance} = 0.299R + 0.587G + 0.114B$$
    If $\text{Luminance} > 150$, text color is `#0F172A` (deep slate); otherwise `#FFFFFF`.
- Preset palette options for create/edit modals:
  - Blue (`#2563EB`), Emerald (`#059669`), Violet (`#7C3AED`), Amber (`#D97706`), Rose (`#E11D48`), Indigo (`#4F46E5`), Teal (`#0D9488`), Slate (`#475569`).

### 3. Drag-and-Drop in Natural Visual Order
- Use lightweight HTML5 drag-and-drop events (`draggable`, `onDragStart`, `onDragOver`, `onDrop`, `onDragEnd`):
  - Visual order is natural: left-to-right, top-to-bottom. Top-left is 1st, adjacent is 2nd, etc.
  - No explicit number labels are printed on the cards to keep the UI clean and minimalist.
  - Drag indicator/cursor styling gives immediate tactile feedback on hover.
  - Reordering is optimistically applied in local React state, followed by mutation to `POST /api/accounts/reorder`.

### 4. Rich Light Theme & Theming Overhaul
- In `frontend/tokens.css`:
  - `--color-paper`: Updated from `#F8F9FA` to soft neutral slate `#F4F5F8` for contrast against pure white cards.
  - `--color-rule`: Strengthened from `rgba(0,0,0,0.045)` to `rgba(0,0,0,0.075)` and `--color-rule-strong` to `rgba(0,0,0,0.12)` so borders and structures are crisply defined.
- In `frontend/src/app/accounts/page.tsx`:
  - Account cards use `bg-[var(--surface)] border border-[var(--border)] text-[var(--text)] shadow-xs hover:shadow-sm`.
  - Child pockets use `bg-[var(--surface-raised)] border border-[var(--border)]/60 text-[var(--text)]`.
  - Net Worth hero card renders as a sleek, modern card with subtle gradient illumination and crisp borders in both light and dark modes.

### 5. Sidebar Streamlining & Layout Cleanup
- **Remove:**
  - Brand header (`CF` emblem, `CashFlow`, `Financial OS`).
  - "+ Catat Transaksi" quick-add button.
  - Bottom eye icon (hide balances) and sun/moon icon (theme toggle).
- **Move:**
  - Place `logout` button inside the user profile card alongside the settings icon.

## Risks / Trade-offs

- **[Risk] Existing accounts without custom colors:**
  - *Mitigation:* Migration sets default `#3b82f6`, ensuring every account has an attractive default tag.
- **[Risk] Visual jump during drag-and-drop:**
  - *Mitigation:* Subtle drag opacity (`opacity-50`) and smooth transition animations ensure stable layout behavior.
