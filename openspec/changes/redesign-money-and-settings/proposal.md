# Proposal

## Why

Rekening & Kantong, Target & Tagihan and Pengaturan are the last pages that still look and behave like the app before the redesign. They sit inside the new shell only because phase 1 added compatibility aliases: 62 legacy token names in `tokens.css`, legacy Tailwind palette remaps, and six legacy utility classes. As long as those aliases exist, every new screen can still reach for an old colour name. Two known defects also live on these pages:
- `/accounts` has the only remaining serious axe finding: two `opacity-75` captions at 3.7:1.
- When the net worth cannot be calculated, `/accounts` falls back to `0` and to a client-side sum.

The approved phase plan (`redesign-web-workspace`, phase P5) moves these pages onto the workspace primitives, adds the Kebiasaan and Tampilan settings with a read-only `GET /api/habits`, and removes the aliases. Phase 5 runs last, after phases 3, 4 and 6 have merged, because the alias removal touches every page those phases leave behind.

## Approval

Binding earlier approvals:

1. **Phase plan, 2026-11-02.** The owner approved `redesign-web-workspace` with "approved with defaults, start phase 1". Its design is binding for this phase:
   - the P5 row: Rekening & Kantong, Target & Tagihan, and Pengaturan (HP & perangkat, Kebiasaan, Tampilan with theme and shortcuts, Akun including gaji, anggaran, pemetaan and pengirim) on the primitives; the token aliases, the legacy palette remaps and unused legacy components removed; `GET /api/habits` read-only, with forgetting a habit left to a later change;
   - the P5 gate: every modal still works, the token test has no alias left, and the browser matrix passes;
   - D2 (aliases removed in phase 5), D7 (shared primitives), D8 (one shortcut registry), D13 (the client does no money arithmetic beyond formatting), D14 (Pengaturan keeps its sub-routes; sub-views use query parameters) and D17 (WCAG 2.2 AA).
2. **Phase 1 decisions that apply here:**
   - OD1: the product name stays "CashFlow".
   - OD3: goal set-asides with a target date count in "Terjadwal".
   - OD4: no "Kekayaan bersih sejak awal siklus" delta.
3. **Combined preview, 2026-11-02.** The owner approved `cash-flow-tracker-prototypes/2026-11-02-web-redesign/combined.html` as the definition of the redesign. Its Rekening & Kantong, Target & Tagihan and Pengaturan views are the visual target. "Total aset" and "Kekayaan bersih" are separate figures. Where the preview conflicts with an approved money rule, the rule wins, as recorded in design.md.
4. **Phase 2, 2026-11-03** (`redesign-review-triage`, accepted with "accepted, defaults ok"):
   - the undo contract and `live_feedback_sql`, which every reader of labels uses;
   - the recorded debt that a capture undo leaves its `capture` feedback row.
5. **Phase 3, 2026-11-03** (`redesign-transactions-workspace`). The owner approved it with "approved, defaults ok". The same day the owner directed: "for all labels shown in the app it should be not that informal terms or language for example "Udah dicek" it's good for onboarding interactive messages, but for label or term used in the UI should not use that kind of terms. I hope you can tell the difference and decide which is when". Phase 3 records this as:
   - the capability `ui-copy-register` and its design D20 (OD27). Labels use neutral standard Indonesian, with no address of the user. Formal "Anda" on the pages phase 5 rewrites is left to phase 5.
   - the shared `TransactionDrawer` and the `lib/urlState.ts` helper. Phase 5 reuses both.
6. **Phase 0, 2026-11-02.**
   - The product is public and multi-user, and the design is not specific to the owner.
   - Optional features stay hidden until needed.
   - Amounts are never guessed.
7. **Planning of phases 4–6, 2026-11-03.** The owner said "yes, plan P4–P6 now". This authorises planning. It does not approve this plan. The same day the owner said "I'm not going to test anything until all phases done implemented properly and approved". So acceptance of this phase rests on the agent's recorded evidence (tasks.md, group 7).

**Plan approval, 2026-11-03.** After a combined summary of the phase 4, 5 and 6 plans, the owner approved all three with "approved, defaults ok". OD50–OD59 take their recommended defaults (design.md, open decisions). Phase 5 also adds the "Kebiasaan dihafal" card on Analisis Ringkasan that phase 4 deferred to it (phase 4 OD42), from `GET /api/habits`.

## Expected Outcome

- **Rekening & Kantong** matches the preview:
  - A "Semua / Siap pakai / Investasi" switch.
  - An "Uang siap pakai" table, with pockets as indented rows under their account.
  - An "Investasi" table grouped by platform. Each product says whether it is tracked by amount or by units.
  - A Total aset card: a labelled bar for Siap pakai against Investasi, each active debt as a minus line, and "Kekayaan bersih" as an explainable figure.
  - It shows the same Kekayaan bersih as Beranda.
  - When a figure cannot be calculated, the page says so instead of showing Rp 0.
  - Every action lives in a row drawer, a bottom sheet on a phone. The actions are pocket, edit, Sesuaikan Saldo, Top up, Beli/Jual, Perbarui nilai, Ubah ke pelacakan nominal and archive.
  - Reordering works by dragging and by move buttons in an "Ubah urutan" mode.
- **Target & Tagihan** shows:
  - Target tabungan with progress, pace and status.
  - Bills up to the next salary, from the same lines as Beranda's "Terjadwal".
  - Ketahanan Dana, moved here from Rekening.
  - Utang with payoff progress and due date.
  - Each goal and debt opens a drawer with its existing actions.
- **Pengaturan**:
  - Sections: HP & perangkat, Kebiasaan, Tampilan, and an Akun group (Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Nama pengirim).
  - Kebiasaan lists what the app has learned to record by itself and the learned investment top-ups, with confirmation counts and their state.
  - Tampilan sets the theme, the single-key shortcuts and hidden balances.
- **Every existing modal still writes exactly what it wrote before:** account and pocket forms, archive, reorder, valuation, reconcile, price sync, trade, top-up, amount tracking switch, plans, goal and debt forms, goal adjust, debt payment link and record, and every settings save. This is proven by request and database comparisons against a build of `main`.
- `tokens.css` holds only workspace tokens. No page, component or Tailwind setting uses a legacy name. A test fails if one comes back.
- A capture undo no longer leaves a label that habits count.
- `/accounts`, `/goals` and every `/settings` section pass WCAG 2.2 AA checks at 1440, 1280, 1024, 768 and 390 px in light and dark. Every other route shows no new finding after the alias removal.

## What Changes

- **Rekening & Kantong** (`app/accounts/page.tsx`):
  - recomposed from the preview on `Card`, `Segmented`, `Pill`, `MoneyBar`, `ExplainableFigure` and `Drawer`;
  - the existing modals reused unchanged in logic and moved to the workspace tokens and the copy register;
  - the `opacity-75` captions and the `0` and client-sum fallbacks removed.
- **Target & Tagihan** (`app/goals/page.tsx`): recomposed from the preview. It reuses `EmergencyCoverage`'s logic, `RecurringRulesModal`, `DebtAllocationEditor` and every goal and debt modal.
- **Pengaturan** (`app/settings/*`, `components/settings/*`):
  - new sections `/settings/kebiasaan` and `/settings/tampilan`;
  - a grouped section list;
  - "Perangkat & akses" renamed "HP & perangkat" at the same address.
- **Backend:**
  - New `GET /api/habits`: owner-scoped and read-only. It derives habits from live owner labels and top-up patterns from top-up answers.
  - `GET /api/dashboard/net-worth` computes through the same helper as Beranda's cycle summary. It keeps every field and adds `liquid`, `investment`, `reconciliation_pending` and `lines`.
  - Capture undo appends an `undo` feedback row that reverts its `capture` label, with no new column or table.
- **Tokens and styles:**
  - every remaining legacy token, Tailwind key and utility class replaced by its workspace equivalent, then the alias block, the Tailwind remaps and the legacy utilities deleted;
  - `tokens.test.ts` asserts none remain;
  - components with no importer left are removed, re-checked with grep at that time.
- **Copy:** the strings on these pages and their modals follow `ui-copy-register`. Examples: "Anda" goes, "Sync Harga" becomes "Perbarui harga" and "Update Nilai" becomes "Perbarui nilai". The connect-phone guide keeps its conversational prose.

## Non-goals

- Forgetting or editing a habit, a per-user auto-record switch (OD51), listing merchant memory (OD50), exporting data (OD58) and a compact table density (OD52).
- New money behaviour. Balances, pockets, valuation, trades, top-ups, reconciliation, goal progress, debt payoff and archiving keep their rules and endpoints. The only server-side figure change is that `/dashboard/net-worth` uses the shared helper (OD57).
- Analisis, Laporan, Transaksi, Tinjau, Beranda and Tanya. Phase 5 changes only their colour token names during the alias removal, with screenshots compared before and after.
- The Android companion. It does not call the money, goal or habit endpoints, and `/api/accounts` and `/api/devices` keep their contracts.
- Production. All verification is local, against a disposable migrated PostgreSQL with synthetic data and a local build. Pushing, opening the pull request, merging and releasing stay with the owner and the `deploy.yml` pipeline.

## Capabilities

### New Capabilities

- `design-tokens`: the workspace token set as the only colour, surface and line vocabulary. It covers no compatibility aliases, no legacy Tailwind keys or palette remaps, no legacy utility classes, and a test that enforces this together with the contrast pairs.

### Modified Capabilities

- `clean-core-ledger`: the Accounts page presents accounts and pockets as grouped table rows with a row drawer instead of equal-height cards. Ordering keeps drag and move controls, now in an "Ubah urutan" mode.
- `pocket-reordering`: pocket order is changed in the same mode. The card-alignment requirement becomes a table-row requirement.
- `account-reconciliation`: the "Perlu rekonsiliasi" label sits on account and pocket rows and in the drawer.
- `net-worth-analytics`: assets include investments and use the same helper as the cycle summary. The runway is shown on Target & Tagihan.
- `financial-goals-tracker`: goals render as individual rows with pace and status on the redesigned page.
- `currency-conversion-and-settings`: settings sections, order, grouping and the new sections.
- `theme-presets-switcher`: a Tampilan section for theme, shortcuts and hidden balances.
- `habit-learning`: the read-only habit list API, the Kebiasaan section, and capture undo reverting its label.
- `tactile-pulse-ui`: the label examples on the strong fill follow the copy register.
- `mobile-native-layout`: phone compositions of the three pages.

Standards that apply:
- web UI and accessibility (WCAG 2.2 AA, including 1.4.3, 1.4.10, 2.4.11, 2.5.7 and 2.5.8);
- money correctness (unchanged writes, proven by before and after comparisons; one net worth figure);
- multi-user isolation (habits are owner-scoped);
- personal-data privacy (merchant names are never logged);
- api-consumers compatibility (additive fields only).

The capability list in AGENTS.md does not change. AGENTS.md's styling notes lose the alias wording.

## Impact

- **Frontend:**
  - `app/accounts/page.tsx` and `app/goals/page.tsx` are rewritten;
  - new `components/accounts/*`, `components/goals/*`, `components/settings/HabitsSettings.tsx` and `components/settings/AppearanceSettings.tsx`;
  - `components/settings/sections.ts` and `SettingsNav.tsx` are changed;
  - restyled modals: `InvestmentTradeModal`, `InvestmentTopupModal`, `AmountTrackingSwitchModal`, `DebtAllocationEditor`, `RecurringRulesModal`, `ReconciliationFlag`, `Modal`, `FormField`, `ConfirmActionButton` and the settings sections;
  - `tokens.css`, `tailwind.config.js`, `globals.css` and `tokens.test.ts`;
  - colour-name replacements in every remaining consumer, including `app/auth/login/page.tsx`.
- **Backend:**
  - new `backend/app/services/habit_list.py` and a route in a new `backend/app/routers/habits.py`, registered in `main.py`;
  - `routers/dashboard.py` (`/net-worth` through `cycle_summary._net_worth`, renamed to a public helper) and `services/text_capture.py` (undo appends an `undo` feedback row).
- **Database:** no migration. The `undo` kind and `reverts_feedback_id` exist since V44.
- **API consumers:** `/dashboard/net-worth` only gains fields. `GET /api/habits` is new. The companion uses neither.
- **Docs:** AGENTS.md (styling notes) and `docs/NEW_USER_GUIDE.md` (Rekening, Target & Tagihan, Pengaturan).
- **No new runtime dependency.**
