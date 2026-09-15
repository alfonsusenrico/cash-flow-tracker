## Why

The current Cash Flow Tracker user interface functions well analytically, but visually resembles a sterile B2B developer dashboard (monotone dark slate, rigid 1px rectangular borders, dense monospace fonts, and static layout grids). 

Personal money tracking for daily life must be **tactile, fun, and habit-forming** (inspired by modern consumer fintech like **Bibit**, **Cashew**, and **Kazz**). Without personality, vibrant pop colors, and responsive physical feedback, users experience cognitive fatigue and drop the habit of logging daily transactions.

## What Changes

- **Typography Transformation:**
  - Replace the corporate `Inter` default with **Plus Jakarta Sans** via `next/font/google`.
  - Maintain `font-variant-numeric: tabular-nums` for perfect number alignment while benefiting from friendly geometric curves and playful weights.
- **Vibrant Pop Color Tokens & Candy Tints:**
  - Inject energetic neon-pop semantic accents: Electric Mint (`#00D09C`), Juicy Neon Rose (`#FF3B69`), Candy Violet / Cobalt (`#6366F1` / `#38BDF8`), and Marigold Amber (`#FFB020`).
  - Add translucent tinted card backdrops (`bg-emerald-500/10 border-emerald-500/20`) and subtle top-edge inner highlights (`shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]`) to banish flat gray boxes.
- **Squircle Geometry & Bouncy Pills:**
  - Standardize on generous super-rounded corners (`rounded-3xl` / 24px) for cards and modals.
  - Upgrade category chips and Kakeibo pillars into pill-shaped capsules (`rounded-full`) with prominent emojis.
- **Tactile Physics & Micro-Animations:**
  - Add physical spring compression feedback (`active:scale-[0.96]` with `cubic-bezier(0.16, 1, 0.3, 1)`) to all cards, buttons, and keypad tiles.
  - Implement a lightweight, zero-dependency **Animated Counter Hook** (`useAnimatedCounter`) to roll numbers up smoothly (350ms) on dashboard cards and allowance heroes.
  - Implement **Liquid Pill Sliders** for segmented type and Kakeibo controls so the active highlight glides smoothly.
  - Add animated progress bar fills (`duration-500 ease-out`) on budget consumption and goal rings.
  - Add a celebratory micro-burst checkmark (`✓ Tersimpan`) upon saving transactions in Quick Capture before modal dismiss.

## Capabilities

### New Capabilities
- `modern-pop-design-and-motion`: Design system overhaul establishing Plus Jakarta Sans typography, candy pop palette, squircle cards, animated number tickers, sliding pills, and tactile spring physics.

### Modified Capabilities
<!-- No requirement changes to existing capability contracts -->

## Impact

- **Frontend Styling:** `frontend/tokens.css`, `frontend/src/app/globals.css`, `frontend/src/app/layout.tsx`.
- **Components:** `frontend/src/components/ui/QuickCaptureModal.tsx`, `frontend/src/components/ui/Modal.tsx`, `frontend/src/components/layout/BottomNav.tsx`, `frontend/src/components/layout/TopBar.tsx`.
- **Pages:** `frontend/src/app/page.tsx` (Pulse), `frontend/src/app/insights/page.tsx`, `frontend/src/app/accounts/page.tsx`, `frontend/src/app/goals/page.tsx`.
- **Dependencies:** 0 new runtime dependencies required (pure CSS GPU transforms + React animation hooks, fully CSP-safe).
- **Backend:** No backend changes required.

## Expected Outcome

The entire web application feels snappy, energetic, and joyful to use. Tapping buttons feels like clicking tactile keys, numbers count up smoothly, cards have warm translucent candy tints with friendly squircle geometry, and transaction logging provides an immediate micro-reward.
