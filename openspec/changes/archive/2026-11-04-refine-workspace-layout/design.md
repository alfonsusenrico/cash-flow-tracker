# Design: refine-workspace-layout

## Context

- **State of `main`:** `main` at `2f66187` holds all six redesign phases (PRs #61–#69). Phase 5 removed the compatibility aliases and left one semantic token set in `frontend/tokens.css`. Its values are the redesign's palette: warm neutrals with a lime accent and Geist type.
- **The previous design:** the pre-redesign app is commit `6cc6704`. Its "Coral Terminal" palette and Plus Jakarta Sans are listed exactly in the comment block of the approved preview (`cash-flow-tracker-prototypes/2026-11-04-workspace-refinement/preview.html`).
- **The preview:** its sources and the synthetic seed shaped like the owner's account are in `…/2026-11-04-workspace-refinement/src/`: `seed_owner.py`, `build_preview.py`, `new_shots.mjs` and `old_shots.mjs`. The seed has:
  - Cash;
  - BCA with 4 pockets;
  - Jago with 13 pockets;
  - Gopay, ShopeePay and RDN BCA;
  - Stockbit with 2 stocks (one gain, one loss);
  - Bibit with 2 amount-tracked funds;
  - one debt and 2 goals;
  - Jul–Nov activity.

Every figure this change shows already exists in the API, except the additive fields in D4 and D8.

## Goals

- Match the approved preview screen by screen on real components, in both themes, at 1440 and 390 px. Hold the full five-width matrix to WCAG 2.2 AA.
- Change no money value. Every renamed or split figure names its source field.
- Keep the token structure from phase 5: one semantic set, no alias names, no palette remaps.

## Non-goals

- The whole-month comparisons, Analisis Ringkasan and Laporan. These are `compare-whole-months`.
- Answering questions in Tanya.
- Changes to the companion app.
- Database migrations.

## Decisions

### D1. Palette: old values on the current semantic tokens

`tokens.css` keeps its token names from phase 5. Only their values change, to the pre-redesign values in both themes:

| Role | Light | Dark |
| --- | --- | --- |
| Window | paper `#E8ECEF` / paper-2 `#D8DFE5` | `#050608` / `#0B0E14` |
| Surface / raised | `#FFFFFF` / `#D8DFE5` | `#0E1118` / `#181E2A` |
| Ink / ink-2 / muted | `#0A0D14` / `#343E4E` / `#4E5D70` | `#F8FAFC` / `#94A3B8` / `#8B9AAF` |
| Primary (coral) | `#FF334B`, hover `#F02A42`, soft `#FFE8EC`, contrast `#0A0D14` | `#FF4757`, hover `#FF6675`, contrast `#0A0D14` |
| Income / expense / warning / transfer / investment | `#059669` / `#FF334B` / `#F59E0B` / `#0EA5E9` / `#8B5CF6`, with the text inks from the preview block | `#10B981` / `#FF4757` / `#FBBF24` / `#38BDF8` / `#A78BFA` |
| Kakeibo need / want / culture / unexpected / saving / unclassified | `#2448C4` / `#A21C7E` / `#56640F` / `#9E440D` / `#296B38` / `#475569` | `#8099F5` / `#E879C6` / `#B5C350` / `#F0915A` / `#7AC68A` / `#94A3B8` |
| Sidebar / hover / active | `#F0F3F6` / `#E2E8F0` / `#D4DEE9` | `#020305` / `#0A0D14` / `#121824` |

Tokens that have no old counterpart are derived from the nearest old value and listed in verification.md. Examples are the card shadow and the soft fills of income, transfer and investment. `tokens.test.ts` keeps measuring every pair, and a pair below threshold fails the build. The values are copied from `6cc6704` `frontend/tokens.css`. The preview block is the checked copy.

- *Rejected:* restoring the old token names. That would reverse phase 5's alias removal and bring back two vocabularies.
- *Rejected:* keeping the redesign palette with only darker cards. The owner said "you shouldn't change the color scheme".

### D2. Type: Plus Jakarta Sans

- **Interface text:** Plus Jakarta Sans, the old app's self-hosted variable file and OFL licence, restored from `6cc6704` `frontend/src/app/fonts/`.
- **Code-like text:** stays Geist Mono.
- **Geist sans:** its file is deleted.
- **Hosting:** fonts are still served from the app's own origin (`web-font-hosting`).
- *Rejected:* Geist with the old colours. The preview and the old app both use Plus Jakarta Sans, and the owner compared those.

### D3. Navigation: short list, no groups, Tanya removed

**Sidebar order:**
- Beranda;
- Tinjau (with badge);
- Transaksi;
- Analisis;
- Laporan (with badge);
- Rekening & Kantong;
- Target & Tagihan.

The foot holds Pengaturan and the account card (name and username, opening the account menu). The active item uses the primary coral fill with its contrast ink.

**Removed from the sidebar:** the group headings, the companion status and Bantuan. The rail at 761–1180 px keeps the same order.

**Phone Lainnya sheet:**
- Rekening & Kantong;
- Target & Tagihan;
- Analisis;
- Laporan;
- Pengaturan;
- Keluar.

The tab bar is unchanged.

- *Rejected:* keeping the groups. With seven items they add height without helping the owner find a page.

### D4. Spending terms and the additive split

**The labels:**

| Label | Meaning | Server source |
| --- | --- | --- |
| Pengeluaran | spending without savings: need, want, culture, unexpected and unclassified, net of applied reimbursements | cycle summary `spending` (new); ledger summary `spending` (new); `daily_totals[].spending` (new) |
| Ditabung & investasi | the Kakeibo saving bucket: saving-classified expenses plus net fresh saving through linked movements and top-ups | cycle summary `fresh_saving` (new) = the existing saving line of `keluar` |
| Masuk | unchanged | `masuk` / summary `inflow` |
| Aman dipakai sampai gajian | the existing remaining amount for the whole period | `remaining` |

**The identities, tested:**
- `spending + fresh_saving = keluar`, the existing Kakeibo total;
- `masuk − spending − fresh_saving − scheduled = remaining`;
- the ledger summary `spending = outflow − saving-pillar expenses`;
- with no saving-classified expenses, `spending` equals today's "Belanja" exactly.

**What the labels replace:**
- "Keluar" and "Belanja" are no longer used as figure labels.
- The old total survives only as "Total" in the Kakeibo explanation.
- The direction words "Uang keluar" and "Pindah saldo keluar" in row text are not figure labels and stay.

**Compatibility:** every existing field keeps its name and value. The companion uses none of these endpoints.

- *Rejected:* "Pengeluaran" = today's ledger "Belanja". A saving-classified expense would then sit in both Pengeluaran and Ditabung, and the Beranda segments would not add up.
- *Rejected:* computing the split in the client. Phase 4 D13 says the client computes no money value.

### D5. Beranda: KPI cards, the period safe amount

**KPI row, in order:**
- **Aman dipakai sampai gajian (hero):** `remaining`, with the days left and the expected date in a muted line. When `safe_to_spend.status` is unavailable, the reason is given in words. No per-day figure is shown. `per_day` stays in the API.
- **Masuk:** with the source line, for example "Gaji 30 Okt".
- **Pengeluaran:** with "sejak gaji {date}" or "sejak 1 {bulan}".
- **Ditabung & investasi.**
- **Kekayaan bersih:** with "aset Rp X · utang Rp Y".

Each figure keeps its explanation popover ("Rincian hitungan").

**Body, left column:**
- "Uang siklus ini": segments Pengeluaran, Ditabung & investasi, Terjadwal and Aman dipakai, plus one formula line;
- "Aktivitas terbaru".

**Body, right column:**
- "Perlu tindakan";
- "Kakeibo siklus ini": header total Pengeluaran; rows need, want, culture, unexpected and unclassified; a divider; then Tabungan;
- "Ketahanan Dana" at full card size.

**Removed:**
- the written summary and its OD7 wording;
- the insight banner;
- every same-date chip, such as "vs 6 hari pertama".

**Header:** Beranda keeps no period selector (`beranda-cycle-summary` "Beranda Period"). The header names the period as text. The preview's stepper on Beranda is a deviation recorded here, because Beranda's figures are only defined for the open period.

**Phone order:** hero, the other KPIs as a 2×2 grid, Perlu tindakan, Uang siklus ini, Aktivitas, Kakeibo, Ketahanan Dana.

- *Rejected:* keeping a one-line summary. The owner: "better not use summary in text".

### D6. Transaksi: one toolbar row

**The row:** type tabs with counts, search, "Filter" and "⋯".
- **"Filter" popover:**
  - period mode (Siklus gaji, Bulan kalender, Semua waktu);
  - the chips (Menunggu jawaban, Belum dicek, Dari HP, Lewat Catat);
  - account, category and pillar.

  Its trigger shows the number of active filters. Active filters stay visible as removable chips under the row, and only when some are set.
- **"⋯" menu:** "Gabungkan dua transaksi" and "Rutin".

**Labels:**
- The tab "Keluar" becomes "Pengeluaran".
- The KPI row reads Masuk, Pengeluaran and Bersih (`inflow − spending`).
- The day headings read "Pengeluaran Rp X".

**URL:** state is unchanged (`lib/urlState.ts`).

- *Rejected:* a second toolbar row on wide screens. That is the density the owner rejected.

### D7. Rekening & Kantong: cards with pockets, centered modal

**Top of the page:**
- KPI cards: Total aset, Siap pakai, Investasi (with its gain), Kekayaan bersih.
- A full-width "Komposisi aset": a segmented bar and a legend grid per top-level account with share and amount, without debt.

**Account cards:** in two columns that flow without stretching. Each card holds:
- a coloured name badge;
- the kind and the pocket count;
- the balance in large type;
- for investments, the platform gain;
- its pockets or products as rows inside the card, on a rail in the account colour.

**Interactions:**
- The whole card head and every pocket row are buttons that open the detail.
- Details open in a centered modal at 761 px and wider, and in a bottom sheet below that. They hold the same actions as phase 5's drawer.
- Opening a detail never scrolls the page.
- Closing returns focus to the row.

**Account colours:**
- Stored colours are kept.
- Top-level accounts still on the default blue (`#3b82f6` or `#2563EB`) are shown with the next free hue from a fixed list. This is display only and nothing is written.

- *Rejected:* inline expansion. Long pocket lists would push other cards away, and the owner accepted the modal.
- *Rejected:* the phase 5 side drawer. The owner: "compressed at the right side panel is not good for me".

### D8. Investment gain in the list

- **Product rows** show the value and, beside it, `capital_gain` and `capital_gain_pct` from `GET /api/accounts`. Each uses ▲ or ▼, a sign and a colour, so meaning is never carried by colour alone.
- **The platform card head** shows the platform total. `GET /api/accounts` gains additive `group_capital_gain` and `group_cost_basis` on a parent whose children are investments, computed on the server from the same per-product rule.
- **No known cost basis:** a product without one shows no gain and says "Modal belum dicatat". It never shows Rp 0.

- *Rejected:* summing the products in the client (D4, phase 4 D13).

### D9. Reorder: drag with handles, move buttons

**Desktop:**
- Each card and each pocket row has a visible ⠿ handle that can be dragged.
- Move-up and move-down buttons appear on hover and on keyboard focus.
- Pockets move only among their siblings.
- Each move is announced in a live region, focus stays on the moved item's control, and boundary moves are not offered.

**Phone:**
- Dragging conflicts with scrolling, so "Ubah urutan" switches the cards to move buttons.
- The buttons are always visible there, at 44 px targets.

**Saving:** both ways persist through the existing `POST /api/accounts/reorder` with the full sibling sequence. There is no new endpoint. The order request and the resulting `display_order` must equal those of phase 5's mode for the same moves (D12).

- *Rejected:* reorder only in a mode on desktop too. The owner asked for cards that "can dragged".

### D10. Tanya: retire the page and its code

- `/tanya` redirects to `/` (308) and leaves the navigation.
- `app/tanya`, `components/tanya` and `lib/tanya` are deleted, with their tests.
- `lib/capture.ts` stays, because Catat uses it.
- The copy guard's Tanya allowlist is removed.
- A later `add-tanya-llm-answers` would design its conversation from that change's own decisions, so keeping the intent table would only be dead code.

- *Rejected:* hiding the page but keeping the code (OD82).

### D11. Target & Tagihan: balanced layout

- **Top row:** three equal cards:
  - Ketahanan Dana: months, monthly essential cost, days covered, emergency balance and target;
  - Utang: remaining, paid, minimum payment and due date;
  - Tagihan 30 hari: count, total and the next three.
- **Below:** "Target tabungan" at full width.
- **Phone:** Ketahanan Dana first, then the same order.
- Drawers and modals are unchanged in content and become centered modals like D7.

### D12. Verification on owner-shaped data

**Data:** all browser checks use the preview's synthetic seed, `src/seed_owner.py`, loaded into a disposable database. The phase seeds are kept for regression.

**Gates:**
1. **Preview match:** screen-by-screen comparison with the approved preview at 1440 dark, 1440 light and 390 dark, for Beranda, Transaksi (with the filter open), Rekening (list, detail and investments), Target & Tagihan, Pengaturan, and the sidebar and Lainnya. Each difference is fixed or recorded as a deviation with its reason.
2. **Full matrix:** 1440, 1280, 1024, 768 and 390 px in both themes, plus 200% and 400% zoom. Pass means axe serious 0, overflow 0, console errors 0, every target ≥ 24 px (44 px on phones), and focus visible and not obscured.
3. **Unchanged writes (phase 5 D17 harness):** for every Rekening and Target & Tagihan form, and for the reorder by button and by drag, the request bodies and table snapshots equal a `main` build doing the same flow.
4. **Identities of D4:** checked on the seed through the API and the UI, with and without a saving-classified expense.
5. **Speed:** `GET /api/accounts`, the summary and the cycle summary each slow by no more than 10% against the baseline. JS per route grows by no more than 5 kB gzip.
6. **Regression:** every route against a `main` build.
7. **Clean-up:** every process started for these checks is stopped by PID or port, and `ps`, `lsof` and `docker ps` are recorded.

## Data, API and failure behaviour

**New fields:**

| Endpoint | Field | Failure or absence |
| --- | --- | --- |
| `GET /api/dashboard/cycle-summary` | `spending {amount, lines}`, `fresh_saving {amount, lines}` | omitted only on error; the web shows the summary failure state, never Rp 0 |
| `GET /api/transactions` summary | `spending`, `saving` | absent on old servers; the web falls back to the error state for that KPI, not to `outflow` |
| `daily_totals[]` | `spending` | as above |
| `GET /api/accounts` parent investment rows | `group_capital_gain`, `group_cost_basis` | `null` when any child lacks a cost basis; the card then says "Modal belum lengkap" |

**Other behaviour:**
- `/tanya` answers with a 308 redirect to `/`.
- Reorder failures restore the previous order and say "Urutan tidak tersimpan".

## Compatibility, migration and rollback

- **Fields:** every change is additive or presentational, and no request body changes.
- **Database:** no migration.
- **Companion:** unaffected. It calls none of the changed endpoints; checked against the companion repo's API paths.
- **Rollback:** revert the merge. No data needs restoring.

## Risks

- [The old coral is both the primary action and the expense hue] The old app did the same. OD80 keeps it, and the contrast tests still apply.
- [Renaming "Belanja" to "Pengeluaran" changes the number on Transaksi when saving-classified expenses exist] The KPI explanation names the excluded saving lines. With none, the number is identical.
- [Drag on desktop could reorder by accident] Drag starts only from the handle, and every move is announced.
- [Removing Tanya deletes a working feature] It was misnamed and could only record. Catat keeps recording.

## Open decisions (recommended defaults)

| # | Decision | Default |
| --- | --- | --- |
| OD80 (resolved 2026-11-04: "approved, defaults ok") | Primary action and expense share the old coral | **Keep, as in the old app.** The owner asked for the old scheme unchanged |
| OD81 (resolved 2026-11-04: "approved, defaults ok") | Meaning of "Pengeluaran" | **Spending without any saving (D4)**, via additive `spending` fields |
| OD82 (resolved 2026-11-04: "approved, defaults ok") | Tanya's intent code | **Delete it** (D10); `lib/capture.ts` stays |
| OD83 (resolved 2026-11-04: "approved, defaults ok") | Detail view | **Centered modal ≥ 761 px, bottom sheet below** (owner accepted) |
| OD84 (resolved 2026-11-04: "approved, defaults ok") | Where help lives | **Pengaturan · Tampilan "Bantuan & pintasan"**, plus `?` everywhere |
| OD85 (resolved 2026-11-04: "approved, defaults ok") | Reorder on desktop | **Always-on handles and hover/focus buttons**; phones use the "Ubah urutan" mode |
| OD86 (resolved 2026-11-04: "approved, defaults ok") | Platform gain total | **Server fields on the parent** (D8) |
| OD87 (resolved 2026-11-04: "approved, defaults ok") | Period stepper on Beranda (in the preview) | **No stepper**; the period named as text (D5) |
| OD96 (open, default applied 2026-11-04) | The hero label "Aman dipakai sampai gajian" uses the casual "gajian" in a figure label | **Keep the approved preview label**; the copy guard allows this one phrase |
| OD97 (open, default applied 2026-11-04) | Source of "Tagihan 30 hari" | **Planned bills of the window, plus charges seen recurring that no plan covers, marked "perkiraan"**, as the preview's data did; the alternative is planned bills only |
| OD98 (open, default applied 2026-11-04) | Transaksi KPIs: the preview shows Masuk, Pengeluaran, Ditabung & investasi and Aman dipakai; D6 names Masuk, Pengeluaran and Bersih | **D6's three figures, drawn as cards like the preview** |
