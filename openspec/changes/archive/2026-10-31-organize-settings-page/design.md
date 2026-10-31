# Design

## Context

See proposal.md, Why. The current modal (`frontend/src/components/ui/SettingsModal.tsx`, opened from `AppLayout` through `Sidebar` and `MobileDrawer` callbacks) holds one `<form>` whose single submit sends `name`, `name_aliases`, `currency`, `emergency_fund_multiplier` and `monthly_spending_budget` together, followed by four independent sections (`DevicePairingSection`, the manual API key, notification mappings, `SenderAliasesSection`), with `SalarySettingsSection` embedded inside the form. No caller opens the modal at a specific section; there is no existing deep link to preserve.

`PATCH /auth/settings` updates only the fields present in the body (`monthly_spending_budget` is touched only when it is in `model_fields_set`), so splitting the save needs no backend change.

The live design source is `frontend/tokens.css` (Coral Terminal: `--surface`, `--border` 1px rules, `--primary` coral, `--radius-card`, Plus Jakarta Sans and Geist Mono) and the shared primitives in `components/ui` (`Button`, `Input`, `Select`, `Modal`). `AGENTS.md` says to refine without restyling; the older lime/charcoal wording in the historical instructions is superseded by the live tokens. `globals.css` stacks `grid-cols-2/3/4/6` into one column at 900 px and below.

The Beranda card (`NarrativeInsightCard`) renders `dashboard.overview.narrative`, which the backend computes from the whole previous window against the current partial window, plus a verdict badge from `comparison.outflow_delta_pct`. `MobileHomeView` renders the same narrative as "Evaluasi Arus Kas". `GET /api/dashboard/period-comparison?timeframe=cycle|month&cycle_offset=n` (archived change `2026-10-30-improve-essential-cost-and-insights`, D4) already returns the fair comparison: `label`, `compared_days`, `current_truncated`, `current`/`previous` windows with `inflow`, `outflow`, `net`, `previous_label`, `previous_has_data`, five pillars and top-three category and merchant `increases`/`decreases` with amounts. Only `cycle` and `month` are accepted, and they are the only timeframes the web can select.

## Goals / Non-Goals

**Goals:**
- One section visible at a time, each a card whose Save covers exactly its own fields.
- One consistent form layout (label above, help below, one field width per section, actions right-aligned at the card foot, 44 px targets, visible focus, tabular numerals).
- Keep every API call, validation rule and behavioural test assertion.
- Beranda insight honest about elapsed days and data sufficiency.

**Non-Goals:**
- New tokens, colours or components outside settings, except the replaced insight card.
- Guarding unsaved edits when leaving a section (see Risks).
- Server-side rendering of settings data; pages stay client components like the rest of the app.

## Decisions

### 1. Layout: route per section (chosen)

Three alternatives were compared against the owner's complaint ("too much to be all contained in 1 container") and the phone constraint:

- **A. One long page with scroll-spy navigation.** All six cards stacked, a sticky left list highlighting the card in view. Rejected: the page stays as long as today's modal (the sender list alone can run to dozens of rows), so the "everything in one place" feeling remains; scroll-spy highlighting is unreliable for short cards at the end of the page, and several Save buttons are on screen at once.
- **B. Tabs inside one `/settings` page (`?section=`).** One panel at a time. Rejected: `useSearchParams` forces a client-side rendering bailout with a Suspense boundary in Next 16, tab semantics (`role="tablist"`) fit poorly with six panels that contain forms and dialogs, and the phone would need a second pattern anyway.
- **C. A section route each (`/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/perangkat`, `/settings/pemetaan`, `/settings/pengirim`) under a shared `app/settings/layout.tsx`.** Chosen. Only one card and one Save are on screen, every section has a stable deep link, sections fetch only their own data (queries are enabled on mount), and the same routes serve desktop and phone. Navigation is a `<nav aria-label="Bagian pengaturan">` of links with `aria-current="page"`, which needs no custom keyboard handling.

On desktop (≥ 1024 px) the layout shows the section list (about 220 px, sticky) beside a content column capped at 720 px. `/settings` itself shows the Profil section on desktop, with Profil marked current.

### 2. Phone: section list that opens each section (chosen)

- **Sticky section selector over a stacked page.** Rejected: it competes with the sticky top bar for vertical space, a horizontal chip row of six long Indonesian labels overflows at 320 px, and it inherits the long-page problem of alternative A.
- **Section list (chosen).** Below 1024 px, `/settings` shows the six sections as a list of 56 px rows with title and one-line purpose; each opens its section route, which shows a "Semua pengaturan" back link above the card and hides the list. This is the conventional phone settings pattern, needs no horizontal scrolling, and keeps one task per screen.

### 3. Per-section saves through the existing partial PATCH

- Profil sends `{ name, name_aliases }` and invalidates `auth` only: the name feeds the greeting and future notification matching, not recorded figures.
- Mata uang & anggaran sends `{ currency, emergency_fund_multiplier, monthly_spending_budget }` and invalidates the same money consumers the modal invalidated (`auth`, `pulse`, `insights`, `dashboard`, `goals`, `accounts`, `transactions`, `recurring`).
- Each section keeps its own error and success status, validates only its own fields with the existing messages, and re-seeds its fields from `/auth/me` after a save. A section's Save never sends another section's fields, which is what makes "what does this Save do" answerable. Rejected: one page-level Save bar (the original ambiguity) and autosave on blur (surprising for money settings and hard to make atomic for the alias list).
- When fields differ from the saved values the card shows "Ada perubahan yang belum disimpan." next to its actions.

### 4. Shared settings primitives

`components/settings/SettingsCard.tsx` renders a `<section aria-labelledby>` card with an `h2` title, a one-line purpose, the body, and an optional right-aligned action footer separated by a 1px rule. `SettingsField` renders label above, control, help text below (wired through `aria-describedby`) and an error line. Field width is one constant per section: `max-w-md` for short text and number fields (Profil, Mata uang & anggaran), full card width for the salary income select whose options carry date, amount and sender. Rejected: reusing `FormField`, which puts help behind an `InfoHelp` button instead of below the field.

### 5. Salary section composition

The existing `SalarySettingsSection` keeps its five queries, its single `action` mutation and its invalidation list. Its markup becomes:
- a "Sumber gaji saat ini" summary block (value and field, or the truthful empty text);
- a "Ganti sumber" disclosure (`<details>`, open when no source exists) holding the income select at full width, "Lebih baru"/"Lebih lama" as small secondary buttons in one row under it, then the actions row: "Tandai sebagai gaji" (secondary) and "Jadikan sumber gaji" (primary), right-aligned;
- "Periksa pemasukan dari sumber ini" as a secondary button in the source block when a source exists;
- "Perlu ditinjau" rows with "Ini gaji" (secondary) and "Bukan gaji" (quiet) buttons whose accessible names still name the income (`Konfirmasi gaji: …`, `Bukan gaji: …`), and "Gaji terkonfirmasi" rows with "Batalkan" whose name stays `Batalkan konfirmasi gaji: …`.

### 6. Destructive actions: inline confirmation (chosen)

Sender-name and notification-mapping removal show a quiet text-style "Hapus" (danger-coloured text, no border, 44 px target). Activating it replaces the button with "Hapus <nama>?", "Batal" and "Ya, hapus"; focus moves to "Batal", Escape cancels, and focus returns to the trigger on cancel or to the section heading after removal. Device revoke keeps its existing confirmation dialog, with the same quiet "Cabut" trigger. Rejected: a modal per removal (heavy for a long list and loses the row context) and the shared `ConfirmActionButton`, whose `className` styles both trigger and confirm buttons and whose confirm label is fixed.

Contrast note: `tailwind.config.js` maps `rose-700` to `--color-expense-hover`, which is light coral (#FF6675) in dark mode, so white text on it measured 2.83:1. Confirmed destructive buttons therefore use `rose-700` in light (4.74:1) and Tailwind's default `rose-800` in dark (`confirmDangerClass`); quiet danger text uses `rose-700`/`rose-300`.

### 7. Sender search

When more than 8 sender names exist, a search field "Cari nama pengirim" filters client-side by mask, name, institution and account, case- and accent-insensitive (`NFKD`, diacritics removed). A polite status shows "n dari m nama" and an empty match says "Tidak ada nama yang cocok." The list itself is unchanged API data; nothing is paged.

### 8. Manual API key collapsed

The manual key moves into a native `<details>` "Kunci API manual (cadangan)" inside Perangkat & akses, closed by default, under the paired-device list. Its rotate confirmation and one-time display are unchanged. Native `details` gives keyboard and screen-reader disclosure semantics without custom state.

### 9. Beranda insight from the fair comparison

`NarrativeInsightCard` takes the timeframe and offset, queries `queryKeys.dashboard.periodComparison(basis, offset)` (shared cache with Analisis), and renders:
- loading and error states in words;
- fewer than 3 compared days (`MIN_COMPARISON_DAYS = 3`): "Baru n hari berjalan di <label>. Perbandingan muncul setelah 3 hari agar tidak menyesatkan." plus the outflow so far;
- no previous window or `previous_has_data: false`: "Periode lalu belum punya transaksi tercatat, jadi belum ada pembanding." plus the outflow so far;
- the previous period shorter than 3 days (`current_truncated` with fewer than 3 compared days): "Periode lalu hanya n hari, terlalu pendek untuk dibandingkan.";
- otherwise a one-sentence headline "Dalam n hari pertama, uang keluar Rp X, Rp Y lebih tinggi/rendah dari periode lalu pada hari yang sama." (or "sama dengan"), a "Pembanding: <previous_label>, <date range>" line (the backend label can carry a note such as "siklus gaji belum tersedia", which reads badly inside the sentence), then up to three category increases and decreases stacked in one column with signed amounts and `sebelumnya → sekarang`, and a link to Analisis. The truncation note appears when `current_truncated`.

The card header is "Dibanding periode lalu" with the period label.

No percentage and no verdict badge. Direction is carried by words ("naik", "turun", "lebih tinggi") as well as colour. The phone home view uses the same component in place of "Evaluasi Arus Kas". The overview `narrative` stays in the API for compatibility; the web no longer renders it.

Three days was chosen because day 1 and day 2 of a period are dominated by single bills; it is a display threshold only and does not change the endpoint.

## Risks / Trade-offs

- [Edits are lost when navigating to another section without saving] → each card shows an unsaved-changes note; a navigation guard is deferred because Next App Router has no supported route-change blocking API.
- [Bookmarks or habits that opened the modal] → there were no deep links; the sidebar gear keeps its place and accessible name "Pengaturan".
- [Tests that targeted the modal dialog] → rewritten against the page sections with the same behavioural assertions (PATCH bodies, invalidations, validation messages, focus return, rotation, removal).
- [Beranda now makes one extra request] → it shares the Analisis cache key and the endpoint is read-only.
- [The overview narrative and the new card could disagree] → the narrative is no longer rendered on the web.

## Open decisions for owner

- **Primary button contrast (pre-existing, app-wide).** White text on `--primary` measures 3.60:1 in light and 3.34:1 in dark, below the 4.5:1 AA target for the 12–14 px button labels ("Simpan profil", "Simpan anggaran", "Hubungkan ponsel", "Jadikan sumber gaji"). Changing the token or the label colour is a visual-system decision outside "refine, do not restyle", so it is left unchanged and recorded here. Options: darken `--primary` (for example to about #D61F3A in light), or use dark ink on coral.
- **`.p-5` phone override (pre-existing).** `globals.css` sets `.p-5 { padding: var(--space-sm) }` at 900 px and below, but `--space-sm` is not defined, so any `p-5` element loses its padding on phones. The settings cards avoid `p-5`; the rule itself is left for a separate fix.

## Migration Plan

Frontend only. Deploy through the approved pipeline. Rollback is redeploying the previous frontend; no data, schema or API changes are involved.
