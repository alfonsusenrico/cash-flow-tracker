# Design

## Context

Phase 5 of the approved web redesign. It is implemented last, after phases 3 (`redesign-transactions-workspace`), 4 (`redesign-analysis-and-reports`) and 6 (`add-tanya-conversation`) have merged. The alias removal must see the final state of every other page, and phase 3's shared drawer, URL helper and copy register are inputs here.

### Pages today (observed on `main` at `861212b`)

**Rekening & Kantong** (`app/accounts/page.tsx`, 2304 lines):
- A "Total kekayaan bersih" hero card and a "Dasar Ketahanan Dana" card (`EmergencyCoverage`).
- An "Alokasi Saldo" bar, then one equal-height card per account with its pockets inside a bounded scroller. Each card has a "+" button and a "⋯" options disclosure.
- Header buttons "Sync Harga", "Rencana" and "+ Rekening Baru".
- Modals:
  - in the page: account create/edit (bank, wallet, cash, investment, stock position with ticker search), pocket create, valuation ("Update Nilai"), reconcile ("Sesuaikan Saldo");
  - as components: `InvestmentTradeModal`, `InvestmentTopupModal`, `AmountTrackingSwitchModal` and `RecurringRulesModal`;
  - `ConfirmActionButton` for archive.
- Reordering uses drag-and-drop and move buttons for accounts and pockets.
- Defects:
  - The only serious axe finding left in the app: two `opacity-75 text-[10px]` captions at 3.7:1 light and 4.21:1 dark ("1 cicilan aktif" and the essential-cost note).
  - `netWorth` falls back to `0` and `totalAssets` to a client-side `totalBalance` when `/dashboard/net-worth` fails. This breaks "unknown is never 0" and D13.
- Copy: "Sync Harga", "Update Nilai", "Capital Gain" and "Rekening Baru". The goals page has "Anda" three times.

**Target & Tagihan** (`app/goals/page.tsx`, 1438 lines):
- Goal cards and debt cards.
- Modals: goal create/edit, standalone adjust, debt create/edit, payment link (`/obligations/{id}/payment-candidates` and `/payments`), and recording a payment as a new transaction.
- `RecurringRulesModal` and `DebtAllocationEditor`.

**Pengaturan** (`app/settings/*`, `components/settings/*`, `components/ui/*Section.tsx`):
- Six sections with their own address, in the order Profil, Gaji & siklus, Mata uang & anggaran, Perangkat & akses, Pemetaan notifikasi, Nama pengirim.
- Each section saves its own fields (spec `currency-conversion-and-settings`).
- Purposes say "Anda".

### Tokens and styles (measured with `scratchpad/p5-plan/alias_usage.py`)

`tokens.css` declares 62 compatibility aliases in one marked block. On `main`:
- 853 `var(--alias)` uses and 521 legacy Tailwind keys, the latter as aliases (`bg-surface-raised`, `text-text-secondary`, `border-border`) and as palette remaps (`emerald-*`, `rose-*`, `amber-*`, `sky-*`, `violet-*` mapped onto tokens in `tailwind.config.js`).
- The largest consumers are `accounts` (115 var uses, 102 keys), `ledger` (110, 88), `goals` (76, 70) and `insights` (65, 57), followed by modals, settings, report components, `lib/kakeibo.ts`, `components/charts/theme.ts` and `app/auth/login/page.tsx` (17).

On the phase 3 branch the totals fall to 728 and 402, because Transaksi no longer uses aliases. Phase 4 is expected to remove most of the Analisis and Laporan uses.

`globals.css` keeps six legacy utilities: `btn-primary` (2 files), `btn-secondary` (4), `cockpit-card` (7), `card-squircle` (7), `pressable` (3) and `donut-ring` (1).

Components with no importer on `main`: `FilterBar`, `DonutChart`, `Sparkline`, `MoneyInput`. `DetailDrawer` has none either, and phase 3 deletes it. `Modal` is still the app's dialog, and phase 3's `CatatDialog` uses it.

### Backend facts

- **Net worth is computed twice.**
  - `GET /dashboard/net-worth` sums top-level balances and subtracts active debts.
  - `cycle_summary._net_worth`, which Beranda shows, adds liquid and investment balances, keeps negative balances waiting for reconciliation as their own line, and subtracts debts.
  - Their `net_worth` agrees in the plain case. `total_assets` differs when a negative balance waits for reconciliation, and the endpoints have no shared helper, so they can drift.
  - The spec `net-worth-analytics` still says `total_assets` is liquid only, which the code no longer does.
- **Habits** have no table. `habits.load_labels` and `decide` derive them per key: account, institution and text signature, ±25% amount band, the latest 10 live owner labels, corrections weighing 2, and auto at 3 labels with ≥ 90% agreement.
  - Top-up patterns derive from `investment_topup_feedback` through `topup_learning`.
  - Merchant memory (`merchant_memory`) is a different thing: it powers suggestions and does not record anything by itself.
  - The auto-record switches are server-wide environment settings (`HABIT_AUTO_RECORD_ENABLED`, `TOPUP_AUTO_RECORD_ENABLED`), not per user.
- **Capture undo** (`text_capture.undo_capture`) leaves its `capture` feedback row. `habits.EVIDENCE_KINDS` includes `capture`, so an undone capture still counts as habit evidence. This is phase 2's recorded debt. V44 already provides the `undo` kind, `reverts_feedback_id` and `live_feedback_sql`.
- **The companion** (`financial-tracker-mobile-listener`, read-only) calls `/api/accounts`, `/api/devices*`, `/api/capture*`, `/api/categories`, `/api/dashboard/cash-flow-summary`, `/api/ingest/notifications*` and `/api/transactions*`. It calls no net-worth, goal, obligation or habit route.

## Goals / Non-Goals

**Goals:**
- the three pages composed from the preview on the workspace primitives, with every existing action and write kept exactly;
- one Kekayaan bersih everywhere;
- Kebiasaan and Tampilan;
- no compatibility alias, legacy key or legacy utility left;
- the copy register applied;
- WCAG 2.2 AA at five widths in both themes.

**Non-Goals:**
- new money behaviour;
- forgetting or editing habits;
- per-user auto-record switches;
- data export;
- compact table density;
- changes to other pages beyond colour token names;
- the companion;
- production.

## Decisions

### D1. Order of delivery and the alias removal baseline

Phase 5 starts from `main` after phases 3, 4 and 6 have merged. Task 1.1 rebases the branch, then:
- refreshes every MODIFIED block in this change from the then-current main specs. Phases 3 and 4 may modify the same requirements, for example `tactile-pulse-ui`;
- re-runs the usage measurement.

The alias removal (D11) is planned against that measurement, not against today's numbers.

*Rejected:* implementing phase 5 in parallel with phase 4. Both would rewrite the same colour names in shared components, and every merge would conflict.

### D2. Rekening & Kantong composition (OD54)

**Desktop**, following the preview:
- A toolbar:
  - `Segmented` "Semua / Siap pakai / Investasi", kept in the address as `?tampil=semua|siap|investasi` through `lib/urlState.ts`;
  - "Ubah urutan";
  - "Perbarui harga" (price sync);
  - "Rencana" (`RecurringRulesModal`);
  - the ink "Tambah rekening".
- A two-column grid.
- **Left column, "Uang siap pakai":** a card with the liquid total in its header and a table with columns Rekening, Jenis and Saldo.
  - Each account is a row with a colour mark beside the name, a kind pill in words, and its balance.
  - Pockets are indented rows under their parent, with "Kantong di <induk>" as the row's secondary line.
  - A parent with pockets has a labelled collapse button and states its pocket count.
  - "Perlu rekonsiliasi" appears as a button or label on the row, and the parent states the number of pockets that need it.
  - An awaiting outflow on an account is stated on its row ("2 pindah saldo belum lengkap", OD59) with a link to `/review?folder=pindah`.
- **Left column, "Investasi":** a card with the investment total and a table with columns Produk, Cara hitung and Nilai.
  - Rows are grouped by platform (`<tbody>` with a row-group heading, "Bibit Contoh · reksa dana, per nominal").
  - A product row says "Dicatat per nominal" or "<n> lot × 100 lembar × <harga>" or "<units> unit".
- **Right column, "Total aset":** an `ExplainableFigure` of `total_assets` and a `MoneyBar` of Siap pakai against Investasi. The bar is labelled, the legend repeats the amounts and shares, and the shares come from the server (D3).
  - The facts list: Total aset, "Saldo minus perlu dicocokkan" when there is any, one minus line per active debt, and Kekayaan bersih as an `ExplainableFigure` whose lines are the server's `lines`.
  - A caption says pockets are counted once.
  - When awaiting outflows exist, it says how many are not in any asset yet and links to Tinjau. It shows a count, not an amount: the cycle summary's waiting lines cover only the current cycle, and summing queue amounts in the browser would break D13.

**Row drawer:** activating a row opens a `Drawer`, side on wide screens and a sheet on phones. It holds:
- the details: kind, parent, colour, default funding account for investment parents, the cost basis, units, last price and untung/rugi for positions, and the last valuation date;
- the actions, in this order:
  - the primary action: "Tambah kantong" for liquid parents, "Top up" for amount-tracked products, "Beli" and "Jual" for unit-tracked positions;
  - "Sesuaikan Saldo" where eligible;
  - "Perbarui nilai" for positions;
  - "Ubah ke pelacakan nominal" where applicable;
  - "Ubah";
  - "Arsipkan", through `ConfirmActionButton`, confirm first.

Each action opens the existing modal from the drawer, and closing the modal returns focus to the drawer.

**Reorder mode:** "Ubah urutan" turns the tables into a reorder list:
- drag handles and move-up and move-down buttons per account;
- per pocket among its siblings;
- boundary moves disabled;
- "Selesai" leaves the mode and returns focus to "Ubah urutan".

It calls the same `POST /accounts/reorder` with the same id sequences as today.

**Phone:** the summary card comes first, then the switch and a list without columns: name, then kind and balance on one line. Rows open the sheet.

*Rejected:*
- Restyling the current cards. The preview is the approved definition, and the cards repeat the pocket layout and need a bounded scroller.
- Keeping "⋯" disclosures per row. The drawer gives room for details the cards hid, and it is the same pattern as Transaksi.

### D3. One net worth figure (OD57)

`cycle_summary._net_worth` becomes the public helper `net_worth_breakdown(cur, user_id, accounts)` in `services/cycle_summary.py`. `GET /dashboard/net-worth` calls it:
- `total_assets = liquid + investment`;
- `net_worth = breakdown.total`.

It keeps `total_liabilities`, `accounts` (with `share`), `obligations` and `runway`, and adds `liquid`, `investment`, `reconciliation_pending` and `lines`.

The page shows only these values:
- The `MoneyBar` shares come from `liquid` and `investment` relative to `total_assets`, passed through the existing percentage formatter. This is display rounding of server values, which D13 allows, and the same as Beranda's Kakeibo share (OD13).
- On error, the card shows `ErrorState` "Kekayaan bersih belum dapat dihitung" with "Coba lagi", never Rp 0.

**Tests:**
- `net_worth` equals `cycle-summary.net_worth.total` with pockets, an investment, a negative pocket waiting for reconciliation and a debt;
- the statement count is constant;
- owner isolation;
- the response is a superset of today's keys.

**Behaviour change:** `total_assets` is lower than today by the pending negative balances, which are now their own line. `net_worth` is unchanged in value.

*Rejected:*
- Leaving two computations. They can drift, and the owner would see two Kekayaan bersih.
- Moving Rekening to read `/cycle-summary`. That endpoint computes a whole cycle for one figure.

### D4. Target & Tagihan composition (OD55)

**Desktop**, following the preview:
- A toolbar with "Tambah target", "Tambah utang" and "Rencana".
- **Left column, "Target tabungan":** one row per goal:
  - name, status pill in words ("Sesuai jadwal", "Perlu Rp X/bulan", "Tercapai", "Tanpa tanggal");
  - a labelled progress bar (`aria-hidden`, with the percentage and amounts in text);
  - "Rp A dari Rp T · P%";
  - the pace line from `monthly_target_pace`;
  - linked accounts or "Dicatat manual".
  - The card caption says the set-aside of goals with a target date is counted in Beranda's Terjadwal (OD3).
- **Left column, "Tagihan sampai gaji berikutnya":** the plan lines of `GET /dashboard/cycle-summary` `scheduled.lines` (`kind = plan`), with the title from `window_basis`, or "Tagihan 30 hari ke depan" when the salary date cannot be estimated.
  - It shows the same amounts as Beranda's Terjadwal, and its total is the server's.
  - "Kelola rencana" opens `RecurringRulesModal`.
- **Right column, "Ketahanan Dana":** moved here from Rekening (OD55) and built from `EmergencyCoverage`'s logic with the same runway fields: months, a runway strip that is decorative and hidden, and the balance and cost basis in text. An unknown runway says so.
- **Right column, "Utang":** one row per active debt, with:
  - paid against total, as a bar and in text;
  - the remaining amount and the minimum payment;
  - the due day as a pill;
  - a caption on automatic archiving at payoff and reactivation on reversal.
  - An archived-debt disclosure lists paid-off debts.

**Drawers:**
- A goal's drawer offers:
  - "Ubah";
  - "Tambah progres" for standalone goals only, with the confirmation saying no account transaction is created;
  - the linked accounts for account-backed goals;
  - "Hapus", confirm first.
- A debt's drawer offers "Ubah", "Kaitkan pembayaran" (the candidates modal), "Catat pembayaran" (the existing record-as-transaction form), the allocation editor where it applies, and "Hapus", confirm first.

**Phone:** Ketahanan Dana first, then goals, debts and bills, each row opening a sheet.

*Rejected:* keeping Ketahanan Dana on Rekening as well. The preview places it with goals, Beranda already shows it, and three copies would compete.

### D5. Modals: same logic, new surface

Every modal keeps its state, validation, request bodies and query invalidations. Only these change:
- classes and colour names, to workspace tokens (D11);
- copy, to the register (D16);
- entry points, which move from the page and card buttons to the drawer actions and the toolbar.

`Modal` stays the app's dialog component, used by phase 3's `CatatDialog` too, and is restyled on the workspace tokens. Its focus trap, stacking and Escape handling are already tested.

Where a modal lives inside `app/accounts/page.tsx` today (account, pocket, valuation, reconcile), its form moves unchanged into `components/accounts/*Dialog.tsx`, so the page file only composes. The proof that writes are unchanged is D17.

*Rejected:* rewriting `Modal` on `<dialog>` now. Stacked dialogs (trade inside drawer) and the existing tests would need rework, for no user-visible gain in this phase.

### D6. Pengaturan structure (OD53)

`components/settings/sections.ts` gains `kebiasaan` and `tampilan`, and each entry gets a `group`.

| Order | Slug | Title | Group |
| --- | --- | --- | --- |
| 1 | `perangkat` | HP & perangkat (was "Perangkat & akses") | — |
| 2 | `kebiasaan` | Kebiasaan | — |
| 3 | `tampilan` | Tampilan | — |
| 4 | `profil` | Profil | Akun |
| 5 | `gaji` | Gaji & siklus | Akun |
| 6 | `anggaran` | Mata uang & anggaran | Akun |
| 7 | `pemetaan` | Pemetaan notifikasi | Akun |
| 8 | `pengirim` | Nama pengirim | Akun |

**Layout and navigation:**
- `SettingsNav` renders a labelled `nav` with the three entries and a headed "Akun" group.
- `/settings` on wide screens shows HP & perangkat, previously Profil.
- Every address keeps working, and new pages `app/settings/kebiasaan/page.tsx` and `app/settings/tampilan/page.tsx` follow the existing five-line pattern.
- The page header "Pengaturan" drops its subtitle "Setiap bagian disimpan sendiri-sendiri.", because each card's save says it.

**Sections:**
- Cards use `Card`/`CardHeader`. Fields keep `FormField`'s label-above and help-below layout, restyled.
- **HP & perangkat** combines the existing device list, pairing dialog, device health checklist, companion download and the manual API key. The connected state reads "HP terhubung · Terakhir menangkap <waktu>". The guided connect steps keep their conversational prose (`ui-copy-register`), and their headings, buttons and statuses are labels.

### D7. `GET /api/habits` contract

New `backend/app/routers/habits.py` (prefix `/api/habits`) and `backend/app/services/habit_list.py`. The endpoint is authenticated by session or API key through `get_current_user`, like every route.

```json
{
  "ok": true,
  "auto_record": {"habits": true, "topups": true},
  "thresholds": {"min_confirmations": 3, "min_agreement": 0.9},
  "habits": [{
    "key": "h_2c1f…",
    "label": "Kedai Awan",
    "account": {"id": "…", "name": "Dompet Digital"},
    "category": {"id": "…", "name": "Makanan & Minuman"},
    "kakeibo": "want",
    "amount_anchor": 30000,
    "confirmations": 3, "agreeing": 3, "agreement": 1.0,
    "state": "ready",
    "last_confirmed_at": "2026-11-02T05:40:00Z"
  }],
  "topups": [{
    "key": "t_9ab0…",
    "source_account": {"id": "…", "name": "RDN Contoh"},
    "amount": 112590,
    "product": {"id": "…", "name": "Reksa Dana A", "parent_name": "Bibit Contoh"},
    "confirmations": 2,
    "state": "learning",
    "last_confirmed_at": "2026-10-01T02:15:00Z"
  }],
  "truncated": false
}
```

**Habits:**
- One statement reads the owner's live trusted labels with the same joins and filters as `habits.load_labels`:
  - the latest live row per transaction, of the evidence kinds;
  - an active expense category, not a non-reportable one;
  - `trusted_label_sql`.
  - It reads the newest 2000 rows across keys and does not filter by amount.
- Python groups the rows by `(account_id, institution, text_signature)` and clusters them by band:
  1. The newest label anchors a band.
  2. Labels `within_band` of the anchor join it.
  3. The newest remaining label anchors the next band.
- Each band is decided with `habits.decide(labels, weekday=None, hour=None, auto_enabled=True)`, and the window of 10 is applied inside `decide`.
- `state`:
  - `learning` when support < 3;
  - `ready` when `decide(...).auto` would hold with the switch on;
  - `conflicting` otherwise.
- `label` is the leading label's `after_merchant`. Without one it is "Tanpa nama toko · <institution display name>". The signature itself is never returned.
- `key` is `"h_" + sha256(user_id, account_id, institution, signature, anchor)[:16]`, stable while the anchor label exists.

**Top-ups:**
- One statement reads `investment_topup_feedback` for the owner, newest 2000. Python groups it by `(source_account_id, amount)` and applies `topup_learning.product_stats` and its thresholds over the latest window.
- One statement reads the names of the involved accounts and products and the categories.
- So the endpoint makes 3 to 4 statements whatever the label count, which is tested.

**Ordering and limits:** each list is ordered by `last_confirmed_at` descending and cut at 100 entries, with `truncated` set when cut.

**Errors:** 401 without authentication. No 404, because an owner with nothing learned gets empty lists. The endpoint has no query parameters, so it never answers 422.

**Logging:** the endpoint logs only counts and duration. Merchant names, amounts and labels never reach the logs, which a test checks with caplog.

**Performance:** under 300 ms locally with 1000 labels over 60 keys and 200 top-up answers.

*Rejected:*
- A `habits` table. Habits are derived from append-only labels by design, and a table would need its own migration and invalidation.
- Listing per key without bands. A key can be ready for one amount and learning for another, and one row would misstate both.
- Including merchant memory (OD50). It does not record anything by itself, and mixing it in would show "learned" items that still ask.

### D8. Kebiasaan section

`components/settings/HabitsSettings.tsx` reads `queryKeys.habits.list`.

Two facts come first:
- "Pencatatan otomatis dari kebiasaan: aktif/nonaktif"
- "Top up otomatis: aktif/nonaktif"

They are followed by "Diatur oleh pengaturan server." (OD51) and one sentence on the threshold: "Kebiasaan dipakai setelah 3 jawaban yang sama, dengan kecocokan minimal 90%."

The two tables follow spec `habit-learning`:
- **Habits:** columns "Saat ditemukan", "Dicatat sebagai", "Dikonfirmasi" and "Status". "Saat ditemukan" shows the label and the account. "Dicatat sebagai" shows the category and the pillar name.
- **Top-ups:** columns "Debit", "Dicatat sebagai", "Dikonfirmasi" and "Status". "Debit" is "Rp X dari <rekening>", masked when balances are hidden. "Dicatat sebagai" is "<Induk> · <Produk>".

The status words follow the spec. On a phone each row becomes a two-line item. The empty, loading (`Skeleton`) and error (`ErrorState`) states follow the spec. There is no edit or forget action, which is a later change.

### D9. Tampilan section

`components/settings/AppearanceSettings.tsx`:
- **Theme:** a radio group with preview swatches calling the existing `setThemeChoice`.
- **Single-key shortcuts:** the switch reads and writes the shortcut registry's setting through a context value exposed by `ShortcutProvider`, so the help dialog and Tampilan share one state.
- **Hidden balances:** "Sembunyikan saldo" uses the existing `hideBalances` setter.

No new storage key is added. The preview's "Tabel rapat" is not included (OD52).

### D10. Capture undo reverts its label (OD56)

`text_capture.undo_capture` finds the capture's live `capture` feedback rows by `transaction_id` and `kind = 'capture'`. In the same transaction it calls `review_undo.revert_feedback(cur, user_id, ids)`, the helper review undo already uses. That helper copies the identity columns into an `undo` row and inserts with `ON CONFLICT (reverts_feedback_id) … DO NOTHING`, so a replay writes nothing. Every reader already filters with `live_feedback_sql`, so habits, purpose suggestions, the question rate and the habit list stop counting the capture with no reader change.

**Tests:**
- the label count before and after;
- the next payment of the key is asked again;
- a rollback mid-undo leaves no `undo` row;
- a replay leaves one `undo` row;
- the append-only trigger still rejects updates.

*Rejected:* leaving it as debt. The Kebiasaan list makes the wrong count visible.

### D11. Removing the aliases

After D1's measurement, in this order:

1. **Map every alias to its workspace token.** The mapping table in tasks 6.1 follows the alias block line by line, for example:
   - `--bg` → `--window`
   - `--border` → `--line`
   - `--text-secondary` → `--text-2`
   - `--primary` → `--ink`
   - `--color-income` → `--in-solid`
   - `--income-text` → `--in-fg`
   - `--color-kakeibo-need` → `--k-need`
   - `--sidebar*` → their window and surface tokens

   The raw-valued contrast aliases (`--income-contrast`, `--transfer-contrast`, `--investment-contrast` and `--income-hover-contrast`) get workspace names (`--in-solid-fg`, `--tr-solid-fg` and `--inv-solid-fg`) only if a consumer remains. Otherwise they go.
2. **Rewrite `var(--alias)` in every remaining file** with a scripted replacement, reviewed file by file. The script lives in the scratchpad. Only its output and the diff are evidence.
3. **Rewrite the Tailwind classes:**
   - alias keys: `bg-surface-raised` → `bg-surface-2`, `text-text-secondary` → `text-text-2`, `border-border` → `border-line`;
   - palette remaps: `text-emerald-600` → `text-in-fg`, `bg-rose-50` → `bg-out-bg`, `text-sky-600` → `text-tr-fg` and the like. They use new semantic Tailwind keys (`in`, `out`, `tr`, `inv`, `warn`, `ok` with `fg`, `bg` and `solid`) added to `tailwind.config.js` on the workspace tokens.
4. **Replace the six legacy utility classes** at their 24 call sites with primitives or token classes. Delete them from `globals.css`, together with its "Legacy utilities" banner.
5. **Delete the alias block** from `tokens.css`, with the `.dark` overrides of raw-valued aliases, and delete the legacy keys and palette remaps from `tailwind.config.js`. `darkMode: "class"` stays.
6. **Remove the components with no importer**, re-checked with `rg` at that moment, and their tests. Candidates on `main`: `FilterBar`, `DonutChart`, `Sparkline`, `MoneyInput`, and `AppShell` and `PageHeader` if phases 3, 4 and 6 leave them unused.
7. **Extend `tokens.test.ts`:**
   - the alias list is captured as a constant in the test. No name from it is declared or used, and no palette remap or default palette class appears. It reports the file and line.
   - every used token is declared in both themes.
   - the contrast pairs cover the new semantic Tailwind keys.

**Proof:** screenshots of every route at 1280 and 390 px in both themes are taken before step 2 and after step 6. A pixel comparison (D17) must show no difference outside the three restyled pages, beyond antialiasing.

*Rejected:*
- Keeping a smaller alias layer "for safety". It keeps the old vocabulary alive, and the owner approved removal.
- Removing the aliases before the restyle of the three pages. Those pages hold most of the uses and are rewritten anyway.

### D12. Phone compositions

These follow spec `mobile-native-layout`. Rekening and Target & Tagihan use lists without columns, and row actions open in sheets (`Drawer` sheet mode). Page actions sit in the page header and stay above the tab bar, and `scroll-padding` keeps focused controls visible (2.4.11). Pengaturan keeps its list-then-screen pattern with groups.

### D13. URLs

- `/accounts?tampil=semua|siap|investasi` (default `semua`, omitted from the URL).
- `/accounts?akun=<id>` opens that account's drawer, so the Tinjau and Transaksi links "Lihat di Rekening" can deep-link.
- `/goals?target=<id>` and `/goals?utang=<id>` open their drawers.

All of these go through `lib/urlState.ts` (phase 3), so the Next.js defect phase 3 found does not apply. Existing paths stay, as in `executive-sidebar-layout` "Route Compatibility".

### D14. Keyboard

There are no single-key page shortcuts on these pages. Tables are navigated with Tab, and rows are buttons. In reorder mode, move buttons are plain buttons, with drag as the pointer alternative (2.5.7). Every drawer traps focus and returns it to its row.

### D15. Accessibility

WCAG 2.2 AA:
- Tables carry `<caption>` (visually hidden where the card title says it), `scope` and row-group headings.
- Colour marks and bars are `aria-hidden`, and their meaning is in text.
- Amounts use tabular figures.
- Muted captions use `--muted` at full opacity, which removes the 3.7:1 finding.
- Targets are at least 24 px, or 44 px on phones.
- Reflow works at 320 px and 400% zoom, and reduced motion is honoured.
- The settings `nav` has `aria-current`.

### D16. Copy register

Spec `ui-copy-register` (phase 3) applies to every string on the three pages and their modals.

| Before | After |
| --- | --- |
| Sync Harga · Sinkronisasi… · Sync selesai | Perbarui harga · Memperbarui harga… · Harga diperbarui: <n> produk |
| Update Nilai | Perbarui nilai |
| Capital Gain | Untung/rugi |
| Rekening Baru · Tambah Rekening Baru | Tambah rekening |
| Target Baru · Buat Target Pertama Anda | Tambah target · Buat target pertama |
| Simpan Progres | Simpan progres |
| "… impian Anda" · "Anda bebas utang!" | impersonal captions ("Belum ada utang aktif.") |
| Perangkat & akses | HP & perangkat |
| Purposes with "Anda" (sections.ts) | impersonal ("Nama rekening di notifikasi yang dipetakan ke rekening.") |
| HP-mu nangkep … (preview) | HP menangkap notifikasi bank dan e-wallet, lalu mengirimnya ke aplikasi. |
| Aku catat (preview) | Dicatat sebagai |

The full list goes to verification.md. Phase 3's guard test gains these files.

### D17. Verification of unchanged writes

For each modal and action, the same scripted UI flow runs on:
- a production build of `main`, as the baseline;
- the phase 5 build;

each against its own copy of one seeded database. The flows cover:
- **account and pocket:** account create (bank, wallet, cash, investment parent, stock position with ticker), edit, pocket create, archive, reorder of accounts and pockets, valuation, reconcile, price sync with the market stubbed, trade buy and sell, top-up, amount tracking switch;
- **plans:** plan create, edit, toggle and delete;
- **goals and debts:** goal create, edit, adjust and delete, debt create, edit and delete, debt payment link, debt payment record;
- **settings saves:** profile, salary source, budget, device pairing code and revoke, API key reset, mapping removal, sender alias edit and removal.

For every flow the harness records:
- the request method, path and JSON body (CDP network, with ids and timestamps normalised);
- a snapshot of the touched tables after the flow: `accounts`, `transactions`, `investment_topups`, `goals`, `goal_accounts`, `obligations`, `recurring_rules`, `users` settings columns, `api_keys`, `pairing_codes`, `notification_account_aliases` and `notification_sender_aliases`. Ids, timestamps and random tokens are normalised.

The baseline and the new build must be equal. A difference is a defect unless the copy register changed a stored description, which it must not.

### D18. Performance and bundle gates

Baseline first (task 1.2): `next build` first-load JS for `/accounts`, `/goals` and `/settings/*`, the size of the built CSS, the median of 20 calls to `GET /dashboard/net-worth`, and `GET /api/accounts` on the synthetic dataset.

| Gate | Limit |
| --- | --- |
| First-load JS for `/accounts` and `/goals` | Not above baseline + 10 kB gzip |
| `/settings/kebiasaan` and `/settings/tampilan` | Within 10 kB gzip of `/settings/profil` |
| Built CSS | Not larger than the baseline (the alias removal should shrink it) |
| `/dashboard/net-worth` | Median no more than 10% slower |
| `GET /api/habits` | Under 300 ms with the dataset in D7, with a constant statement count |

No new dependency.

### D19. Failure behaviour

- Each card has its own query and its own `ErrorState` with retry, so a failed net worth does not blank the tables.
- A failed mutation keeps the modal open with the server's message, as today.
- Habits failing shows an error, never an empty table.
- Price sync failing says "Harga belum dapat diperbarui" and keeps the old values.
- Unknown runway, net worth or pace is stated, never shown as 0.

### D20. Compatibility, migration and rollback

- No migration. `/dashboard/net-worth` only gains fields, and `GET /api/habits` is new.
- The capture undo change writes one more append-only row type that V44 already allows.
- Browser storage is unchanged.
- Rollback: revert the merge commit and redeploy through `deploy.yml`.
  - Feedback `undo` rows written for captures stay. They are valid V44 rows, and the older code ignores them only in the sense that the reverted `undo_capture` no longer writes new ones. The readers keep honouring the existing ones.
  - Nothing needs a data repair.

## Preview conflicts (the approved rule wins)

| Preview | Rule | Resolution |
| --- | --- | --- |
| Kebiasaan has per-user switches "Catat otomatis…" and "Pasangkan top up otomatis" | The switches are server settings, and a per-user switch needs a migration and a new rule | Read-only facts plus "Diatur oleh pengaturan server" (OD51) |
| Akun offers "Ekspor semua data" | No export endpoint exists, and exporting personal data needs its own privacy design | Left out (OD58) |
| "Tabel rapat" in Tampilan | No density system exists | Left out (OD52) |
| Casual copy ("HP-mu nangkep", "Aku catat", "nggak dobel", "udah masuk hitungan") | `ui-copy-register` | Neutral labels and captions; the connect guide's prose stays conversational |
| Total aset as the sum of the visible table balances | D13: the client does no money arithmetic | Server `total_assets`, `liquid`, `investment` and `lines` (D3) |
| Kekayaan bersih subtracts only the debt | Negative balances waiting for reconciliation are owed too | A separate line "Saldo minus perlu dicocokkan" when present (D3) |
| Ketahanan Dana counts only the Dana Darurat pocket | `emergency-cost-estimation` and the existing runway rules decide the basis | The server's runway basis is shown as is |

## Implementation notes (2026-11-03)

### Alias-to-token mapping (task 6.1, D11 step 1)

Every alias in the old block pointed at one workspace token, so each `var(--alias)` became that token; the script is `scratchpad/web-p5/remove_aliases.py`, and its per-file counts (912 replacements in 53 files) are in verification.md.

| Alias group | Token |
| --- | --- |
| `--bg`, `--color-paper`, `--sidebar` | `--window` |
| `--color-paper-2` | `--canvas` |
| `--color-surface` | `--surface` |
| `--surface-raised`, `--color-surface-raised`, `--primary-soft`, `--sidebar-hover` | `--surface-2` |
| `--sidebar-active` | `--surface-3` |
| `--color-ink`, `--primary-text`, `--sidebar-text-active` | `--text` |
| `--color-ink-2`, `--text-secondary`, `--sidebar-text` | `--text-2` |
| `--color-muted` | `--muted` |
| `--border`, `--color-rule` / `--border-strong`, `--color-rule-strong` | `--line` / `--line-strong` |
| `--primary`, `--primary-hover`, `--primary-contrast` | `--ink`, `--ink-hover`, `--ink-fg` |
| `--color-focus`, `--accent-lime`, `--accent-text` | `--focus`, `--lime`, `--lime-text` |
| `--color-income`, `-hover`, `-soft`, `--income-text` | `--in-solid`, `--in-solid-hover`, `--in-bg`, `--in-fg` |
| `--color-expense`, `-hover`, `-soft`, `--expense-text`, `--danger`, `--danger-hover`, `--danger-text` | `--out-solid`, `--out-solid-hover`, `--out-bg`, `--out-fg` |
| `--color-warning`, `-soft`, `--warning-text` | `--warn-solid`, `--warn-bg`, `--warn-fg` |
| `--color-transfer`, `--transfer-text` | `--tr-solid`, `--tr-fg` |
| `--color-investment`, `-soft`, `--investment-text` | `--inv-solid`, `--inv-bg`, `--inv-fg` |
| `--color-kakeibo-<pillar>`, `-soft` | `--k-<pillar>`, `--k-<pillar>-soft` (`unclassified` → `--k-none`) |

Raw-valued aliases: `--income-contrast`, `--income-hover-contrast`, `--transfer-contrast` and `--investment-contrast` still had Tailwind consumers (`text-income-contrast` and the like), so they were renamed `--in-solid-fg`, `--in-solid-hover-fg`, `--tr-solid-fg` and `--inv-solid-fg` with their light and dark values unchanged.

Tailwind (steps 3 and 5): the legacy keys and palette remaps are gone. The semantic keys are `in`, `out`, `tr`, `inv`, `warn` and `ok` with `fg`, `bg`, `solid` (and `solid-hover`, `solid-fg` where used), plus `window`, `surface-2`, `surface-3`, `line`, `line-strong`, `text-2` and `ink`. Only the solids are built with `color-mix(... <alpha-value> ...)`, because the sources tint only solids (`bg-out-solid/10`). Default palette classes were mapped by hue (rose/red → out, emerald/green → in, amber/yellow/orange → warn, sky/blue → tr, violet/purple → inv): text shades to `-fg`, 50–100 fills to `-bg`, other fills and borders to `-solid` keeping their opacity; a `dark:` palette override was dropped because the token is themed.

### Deviations

- **One dialogs hook per page instead of one file per dialog (D5).** The account, pocket, valuation and reconcile forms moved byte for byte (apart from colour names and copy) into `components/accounts/AccountDialogs.tsx` as `useAccountDialogs()`, and the goal and debt forms into `components/goals/GoalDialogs.tsx`. Splitting them further would have meant rewriting shared form state; the move keeps every request body, which D17 confirms.
- **Reorder mode is one list.** The server keeps one top-level order, so "Ubah urutan" replaces both tables with a single list of accounts and their pockets, rather than a list per table.
- **`scheduled.plans_total` is new in `/dashboard/cycle-summary`.** Target & Tagihan's bills total must be the server's (D4), and the summary had only the total including goal set-asides. The field is additive.
- **The greeting rule is shared** (`greetingName`): the first word of the display name, else the capitalised username, on Beranda and Tanya.
- **"Kalau ketemu" became "Saat ditemukan"** in the Kebiasaan habit table: "ketemu" is informal under `ui-copy-register`; the habit-learning delta was updated.
- **Capture labels carry no habit key.** A capture's feedback row has no institution or text signature, so it never was evidence for a notification habit. The undo still reverts it (D10): purpose suggestions, the question rate and the habit list read live labels only. The scenario "Undone capture is not habit evidence" was reworded to what the data supports.
- **Drawers now stack above their scrim.** The overlay drawer and the phone sheet had `z-index: 75` under the scrim's 79 since phase 1, so the scrim covered them on screens up to 1180 px (taps closed the sheet). Fixed in `globals.css` with a token test; found while reading the 390 px screenshots.
- **The plans editor loads on demand on Target & Tagihan** to keep the page within the JS gate.
- **Built CSS** (D18 gate): the shared stylesheet every page loads fell by 1.5 kB gzip, but the money pages' own module adds 2.5 kB on those three routes, so the total of all CSS files is 0.6 kB above the baseline. Recorded under Gates in verification.md.

### Open decision

| # | Decision | Default applied |
| --- | --- | --- |
| OD70 (resolved 2026-11-03: owner said "fix the bug first") | Editing an account-backed goal was refused by the API (409, "Account-backed goal progress is derived from linked account balances") because the form sent `current_amount: 0`; the same happened when a standalone goal was switched to linked accounts. The same happened on `main` | **Fixed in the form**, the component that broke the contract: an account-backed payload omits `current_amount`, and the API rule stays. This is the one intended exception to D17's identical writes: the goal edit and the standalone-to-linked switch no longer send `current_amount`. Regression tests: `app/goals/page.test.tsx` (both flows, failing before the fix) and `test_account_backed_goal_edit_changes_target_and_refuses_manual_progress` |

## Open decisions for owner

Each has a recommended default that this plan already uses.

| # | Decision | Recommended default |
| --- | --- | --- |
| OD50 (resolved 2026-11-03: "approved, defaults ok") | List merchant memory (suggestion memory) in Kebiasaan? | **No.** Only habits that record by themselves and learned top-ups. Merchant memory does not record anything and would read as "learned" while still asking |
| OD51 (resolved 2026-11-03: "approved, defaults ok") | Per-user switches for automatic recording, as in the preview | **Not in phase 5.** Show the server switch states read-only. A per-user switch is its own change with a migration |
| OD52 (resolved 2026-11-03: "approved, defaults ok") | "Tabel rapat" (compact rows) in Tampilan | **No.** No density system exists |
| OD53 (resolved 2026-11-03: "approved, defaults ok") | Order and landing of Pengaturan | **HP & perangkat, Kebiasaan, Tampilan, then the Akun group.** Wide `/settings` opens HP & perangkat. Every address keeps working |
| OD54 (resolved 2026-11-03: "approved, defaults ok") | Rekening layout | **The preview's tables with a row drawer and an "Ubah urutan" mode** (D2). The alternative, restyled cards, keeps the card requirements unchanged |
| OD55 (resolved 2026-11-03: "approved, defaults ok") | Where Ketahanan Dana lives | **Target & Tagihan**, as in the preview. Beranda keeps its card, and Rekening drops it |
| OD56 (resolved 2026-11-03: "approved, defaults ok") | Fix the capture-undo label debt here | **Yes** (D10). Kebiasaan would otherwise show counts that include undone captures |
| OD57 (resolved 2026-11-03: "approved, defaults ok") | One net worth helper for Rekening and Beranda | **Yes** (D3). `total_assets` then excludes negative balances waiting for reconciliation, which become their own line. `net_worth` keeps its value |
| OD58 (resolved 2026-11-03: "approved, defaults ok") | "Ekspor semua data" | **Not in phase 5.** It needs its own change with a privacy and format design |
| OD59 (resolved 2026-11-03: "approved, defaults ok") | Unfinished movements on Rekening | **A count on the account row and a caption on Total aset with a link to Tinjau** ("Pindah saldo belum lengkap"). Counts come from `GET /review/queue` awaiting legs. No amount is shown until the server offers an all-time total |

## Expected overlap with phases 3, 4 and 6

| File or area | P3 | P4 | P6 | Phase 5's handling |
| --- | --- | --- | --- | --- |
| `frontend/tokens.css`, `tailwind.config.js`, `globals.css`, `tokens.test.ts` | adds Transaksi styles | likely adds Analisis and Laporan styles | adds Tanya styles | Rebase after all three, then remove the aliases (D1, D11) |
| `components/ui/Modal.tsx`, `FormField.tsx`, `ConfirmActionButton.tsx` | used by `CatatDialog` and the drawer | possibly `BudgetPanel` and `ClassificationDialog` | possibly | Restyled once, behaviour unchanged |
| `components/ui/DevicePairing*`, `DeviceHealthChecklist.tsx`, `lib/deviceHealth.ts` | copy register sweep | — | — | Restyled in HP & perangkat on top of P3's wording |
| `components/recurring/RecurringRulesModal.tsx` | copy sweep | — | — | Restyled |
| `components/workspace/navigation.ts`, `SideNav`, `PhoneNav` | Transaksi link | — | Tanya entry | Not changed |
| `components/workspace/shortcuts.tsx` | page groups | maybe | maybe | Exposes the switch state to Tampilan (D9) |
| `lib/urlState.ts` | created | used | — | Used |
| `copyRegister.test.ts` | created | extended | extended | Extended with phase 5 files |
| `services/text_capture.py` | not planned | — | uses capture endpoints | `undo_capture` writes the `undo` row (D10) |
| `services/cycle_summary.py` | — | maybe for the calendar | read by Tanya | `_net_worth` renamed to a public helper |
| Specs `tactile-pulse-ui`, `mobile-native-layout` | — | may modify `tactile-pulse-ui` | — | MODIFIED blocks refreshed at rebase (D1) |

## Risks / Trade-offs

- **The alias removal touches every page and could shift a colour unnoticed.** Before and after screenshots of every route in both themes, with a pixel diff (D11, D17), plus the token test.
- **Moving the account forms out of a 2304-line page could change a request.** The request and database comparison of every flow against a `main` build (D17).
- **Ketahanan Dana leaves Rekening, where some owners may look for it.** Beranda keeps it. The Rekening caption links "Lihat Ketahanan Dana" to Target & Tagihan.
- **The habit list's band clustering is an approximation.** The habit decision uses the band around each new payment, while the list anchors bands at the newest labels. A payment between two bands may behave as either. The list shows the anchor amount, and a test pins the clustering.
- **Read-only switches may disappoint against the preview (OD51).** The facts say who controls them, and a per-user switch is a small follow-up change.
- **Phase 4 or 6 may slip.** Phase 5 cannot start before both merge. If the owner wants phase 5 earlier, the alias removal (group 6) can be split off into its own change after the others merge, without changing the rest of this plan.

## Migration Plan

- No schema migration. Deploy through `deploy.yml` as one image set. The new route and the net-worth fields ship with the web that reads them.
- After the deploy, check read-only:
  - `GET /api/habits` answers 200 for the owner;
  - `/dashboard/net-worth` `net_worth` equals Beranda's figure;
  - the three pages load.

  Record this in PROJECT_STATE.md.
- Rollback: D20.

## Open Questions

- The exact Tailwind key names for the semantic families (`text-in-fg` against `text-in`) are settled in task 6.2 with the token test. They change no spec.
- Whether `AppShell` and `PageHeader` still have importers after phases 3, 4 and 6 is settled by task 6.6's grep.
