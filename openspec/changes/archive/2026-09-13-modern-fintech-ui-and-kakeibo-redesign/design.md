# Technical Design: Modern Fintech UI & Kakeibo Lifestyle Engine

## Technical Strategy

The goal is to deliver an interface and intelligence layer inspired by **Kazz**, **Ivy Wallet**, and **Copilot Money** without introducing schema bloat or breaking existing endpoints.

### 1. Database Schema Evolution
We extend `categories` and `transactions` with an optional `kakeibo_type` varchar column (`need`, `want`, `saving`):
```sql
ALTER TABLE categories ADD COLUMN IF NOT EXISTS kakeibo_type VARCHAR(20) DEFAULT 'need' 
  CHECK (kakeibo_type IN ('need', 'want', 'saving'));

UPDATE categories 
SET kakeibo_type = CASE 
  WHEN is_primary = FALSE THEN 'want' 
  ELSE 'need' 
END
WHERE kakeibo_type IS NULL;

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS kakeibo_type VARCHAR(20) 
  CHECK (kakeibo_type IN ('need', 'want', 'saving'));

CREATE INDEX IF NOT EXISTS idx_transactions_kakeibo ON transactions(user_id, kakeibo_type);
```

### 2. Period-over-Period Delta & Narrative Generation Engine
In `backend/app/routers/dashboard.py`:
- For any active cycle window $(W_0)$, we calculate the matching previous window $(W_{-1})$ using `cycle_offset - 1`.
- Compute totals for $W_{-1}$: `prev_inflow`, `prev_outflow`, `prev_net_cashflow`, `prev_savings_rate`.
- Calculate percentage deltas:
  $$\Delta\% = \frac{\text{current} - \text{prev}}{\max(1, \text{prev})} \times 100$$
- Category variance analysis:
  Identify category with $\max(|\text{spent}_0 - \text{spent}_{-1}|)$.
- Narrative synthesis:
  Generate formatted Indonesian summary string:
  *"Dibanding periode sebelumnya, pengeluaran [naik/turun] X% menjadi Rp Y, terutama dipengaruhi oleh [Kategori] ([+Z%]). Arus kas tercatat [surplus/defisit] Rp W."*

### 3. Desktop 2-Column Asymmetric Cockpit Architecture
- Layout container: Fluid full-width canvas with CSS Grid / Flex:
  - **Left / Primary Column (`lg:col-span-7` / ~60-62%)**:
    - `HeroWalletCard`: Total balance, privacy eye masking (`••••••`), multi-wallet count, and horizontal Quick Action Capsules.
    - `DailyBudgetBar`: Linear track with moving cursor dot and daily safe-to-spend tracking.
    - `MetricMatrixGrid`: 2x2 grid with dotted sparklines and period delta badges.
    - `RecentTransactionsTimeline`: Chronological transaction feed with category badges.
  - **Right / Secondary Column (`lg:col-span-5` / ~38-40%)**:
    - `CategoryDonutChart`: Recharts Donut with center stat (dominant % and category name), legend table, and highlight pill banner underneath.
    - `AccountsPocketsGrid`: Grid of liquid accounts, savings pockets, and investment cards.
    - `NarrativeInsightCard`: Period comparison callout card with `MENURUN` / `MENINGKAT` status badge and narrative text.
    - `PendingScheduledBanner`: Due scheduled transactions banner.

### 4. Tactile Mobile Quick Capture & Calculator Keypad
- In `QuickCaptureModal.tsx`:
  - On mobile viewports, present a 4x4 calculator keypad:
    - Row 1: `1`, `2`, `3`, `⌫` (dark red backspace)
    - Row 2: `4`, `5`, `6`, `+ - × ÷` (operator key)
    - Row 3: `7`, `8`, `9`, `=` (equals / evaluate)
    - Row 4: `.`, `0`, `000`, `✓` (large emerald submit checkmark)
  - Inline expression evaluation: `25000+15000` evaluates live to `40000`.
  - Desktop retains physical keyboard typing with instant math calculation.
  - Kakeibo 3-way chips: `[🍞 Need]` `[👑 Want]` `[💰 Saving]` for 1-tap categorization.

### 5. Floating Capsule Navigation Dock
- In `BottomNav.tsx`:
  - Detached floating bar (`bottom-4 left-4 right-4 max-w-md mx-auto rounded-full`).
  - Dark glassmorphism (`bg-[#171A21]/90 backdrop-blur-xl border border-white/[0.08] shadow-2xl`).
  - Active tab rendered inside a pill badge with high-contrast indicator.
