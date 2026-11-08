## Context

Tinjau (`/review`, `components/review/`) on `main` `f1ee604`, inside the phase 2 shell:
- `TriageInbox.tsx` (804 lines) holds the state, URL, keys, actions, auto-advance and Urungkan. It renders two cards side by side when the main column is wider than 720 px (`useWidthAtMost`, `TWO_PANE_MIN`) and one at a time below that, with a "Daftar" button and a history entry.
- `TriageList.tsx`:
  - a header "Perlu ditinjau {attention}" with "{unreviewed} dicatat otomatis · opsional";
  - four `ChipGroup` chips (Semua, Belum dikenali, Perlu jawaban, Pindah saldo belum lengkap);
  - two-line rows with round avatars;
  - four `<details>` folds (Dicatat otomatis, Saran dari riwayat, Terlihat dua kali, Dilewati otomatis);
  - a sticky footer with "Cari saran dari riwayat" and the key hint strip.
- The item card:
  - a bar with "N dari M", K/J key caps and "Daftar";
  - `ItemHead`: kind pill, 24 px title, meta with provenance, 32 px amount, reason pills;
  - bordered two-column `Choice` cards with key caps (1–9, or E/T);
  - inline `Picker` boxes and forms;
  - the run panel `HistorySuggestionsSection` in place of an item (`RUN_PANEL_ID`).
- Data:
  - `GET /review/queue` and `GET /review/suggestions` through `buildTriage` (`lib/triage.ts`);
  - `/categories` and `/accounts` for the pickers;
  - `/dashboard/cycle-summary` only when nothing waits, for three figures.
- Every action goes through `act()` (`/review/bulk`, `/ingest/notifications/{id}/answer-purpose`, `/movements/complete`, `/movements/merge`, `/investment-topups/from-transaction`, `/review/topup-suggestions/reject`, `/review/seen-twice/{id}/acknowledge` and `record-separately`, `/review/suggestions/{accept|reject}`) and `POST /review/undo`.

The prototype (`option-6/js/pages.js` `PAGES.tinjau`, `styles.css` §Rail, §Tinjau, Tablet, Phone; `js/dialogs.js` `ubah-kategori`, `saran-riwayat` and the `tinjau` menu) is:
- a title block with "…";
- a hero with the count;
- three panes `232 | 336 | 1fr`: a folder rail, an item list and the item with numbered answer rows;
- on a phone, chips and a full-height sheet.

Its review data differs from the app's in three places, which D4 resolves:
- "Saran" and "Terlihat dua kali" are counted in "Semua";
- "Dilewati otomatis" lists promotions and codes;
- there is no "Dicatat otomatis" folder.

Inputs this phase builds on:
- **Phase 1:** tokens, type roles, the straight selection bar (`ws-select-row`, `selectionMarks.test.ts`), buttons, fields, segmented control, menu and dialog.
- **Phase 2:** the title block, `PageActions` with its "…" menu, `ShellDialog`, the help dialog with page key groups, the toast, the Tinjau count source.
- **Phase 3:** `HeroFigure`, `ListRow`, `SectionHeader`, `MaskedMoney`, `ExplanationDialog`, the skeleton and error rules.
- **`add-language-system`:**
  - next-intl with `messages/{en,id}.json`, the `terms` glossary namespace and `lib/format.ts`;
  - the translated-route registry (`lib/i18n/translatedRoutes.ts`);
  - the copy register per language;
  - `renderWithIntl`;
  - the both-language screenshot gate and the en-versus-id figure check.
- **`localize-server-text`:**
  - prompts, suggestion labels and reasons and error messages arrive in the account's language with `code` and `params`;
  - review groups carry `label_source`.

Constraints:
- No money logic, API, request or data change. Every figure is a server field formatted by `lib/format.ts`. The only client arithmetic is today's: the folder counts in `buildTriage`, and positions.
- Folder membership, `attention`, `unreviewed`, auto-advance (`nextAfter`), Urungkan (`UndoStack`), deep links (`?item=&folder=`, legacy hashes) and the key bindings keep their behaviour and tests.
- The held branch `fix/selection-bars-and-tinjau` is not merged; its Tinjau parts are folded in here (D21).

## Goals / Non-Goals

**Goals:**
- Tinjau matches the prototype at 1440, 1024 and 390 px in both themes and both languages, measured against `styles.css`. The only exceptions are the recorded deviations.
- Every review kind uses one detail pattern. Every row and answer that leads somewhere is one whole target.
- Tinjau's copy lives in the `review` namespace in English and Indonesian, and `/review` leaves the untranslated set.
- Nothing obsolete remains: Tinjau-only components, CSS and the last user of `ChipGroup`.

**Non-Goals:**
- Backend, API, data or companion changes; the transaction dialog (phase 5); the shell's help dialog beyond Tinjau's key group.
- Changing which items are needed or optional, or what an answer does.
- Translating server text in the web (the server already sends the account's language).

## Decisions

### D1. Page structure and breakpoints

From `styles.css`:
- **≥ 1200 px:**
  - `.tj { display: grid; grid-template-columns: 232px 336px minmax(0,1fr); gap: 24px; align-items: start }`;
  - the list pane `border-right: 1px solid var(--line); padding-right: 24px; margin-right: -8px`;
  - the item `max-width: 880px; min-width: 0`.
  - The three panes share their top edge. The page scrolls, and the panes have no inner scroll.
- **768–1199 px:** `grid-template-columns: 184px minmax(0,1fr)`. The item spans `1 / -1` below the rail and the list. The list has no right line.
- **< 768 px:** `.tj` is a block: the chip line (D5), the list, and the item in a full-height sheet (D13).

Order in the DOM: title block → hero (D3) → the `.tj` region (`section`, labelled "Tinjau"), holding the rail (`nav`), the list (`section`) and the item (`section`, labelled "Detail"). The DOM order is the reading order at every width.

At 1440 × 900 the hero starts at `y = 192` and the panes 32 px under the hero's last line (`margin-bottom: 32px` on the hero).

Rejected:
- Keeping the 720 px main-width container rule (`useWidthAtMost`): the prototype uses viewport breakpoints, and the 768–1199 range would show one pane where the prototype shows both.
- Equal-height panes that scroll inside themselves (held branch): the prototype aligns tops and lets the page scroll. Inner scroll areas hide the end of the list from the page's scroll and need their own focus handling.
- A sticky item pane: not in the prototype. J/K move focus to the item's heading, which scrolls it into view.

### D2. Title block and "…" menu

- **Title:** `nav.review` ("Tinjau" / "Review"); subtitle `review.page.subtitle`.
- **Actions:** `PageActions` with only `more`:
  - "Cari saran dari riwayat" (icon `search`) opens D12;
  - "Bantuan & pintasan" (icon `help`) calls the shell's `openHelp()`.
  - The button's name is `review.page.more` ("Tindakan lain untuk Tinjau" / "More actions for Review").
  - The menu is the phase 2 `MenuPanel` (`role="menu"`, ↑/↓, Esc, focus return), a bottom sheet on the phone.

Rejected:
- "Cari saran dari riwayat" as the list's last row (held branch) or as a pane button (today): the audit moves it to "…", and the prototype has it there.
- A visible "Bantuan" button: ≤ 2 visible actions, and the prototype has none; `?` and the menu reach the help.

### D3. Hero "Perlu ditinjau"

`HeroFigure` without a colour mark (D19):
- the label row "Perlu ditinjau" (`review.hero.label`);
- the figure: `triage.attention`, the same server count as the sidebar badge, Manrope 44/52 (32/40 below 360 px);
- one line `review.hero.line`: "{unreviewed} dicatat otomatis, opsional" / "{unreviewed, plural, one {# recorded automatically} other {# recorded automatically}}, optional".
- When `truncated`, a second muted line `review.hero.truncated` follows.
- The hero is absent while loading, on failure and when no folder has any item (D14). When needed items are 0 but optional items exist, it shows 0.

Rejected:
- **The prototype's line "4 transaksi · Rp 190.500 menunggu jawaban · 2 dilewati otomatis, opsional".** The approved `redesign-review-triage` design (2026-11-03) rejected the amount: an all-time count cannot be paired with the cycle-scoped waiting total. "Triage Folders and Counts" says the header shows no amount, and summing the queue in the browser would be new money arithmetic over a list that can be truncated. "4 transaksi" would repeat the hero's own number, because `attention` counts rows. Recorded as deviation 8.
- The item count (`neededCount`) as the figure: it would differ from the sidebar badge, which the spec requires to agree.

### D4. Folder membership and counts (the app's, not the prototype's)

`buildTriage` is unchanged:
- **Needed:** `apa` "Belum dikenali", `jawab` "Perlu jawaban", `pindah` "Pindah saldo belum lengkap" (awaiting and ambiguous legs, including awaiting debits with a `topup_suggestion`).
- **Optional:** `otomatis` "Dicatat otomatis", `saran` "Saran", `dua` "Terlihat dua kali", `lewat` "Dilewati otomatis" (reports an account link skipped).
- "Semua" lists the needed three in that order.
- The counts are rows, as today, so the needed three add up to `attention`. Positions ("1 dari 6") count items.

Rejected:
- The prototype's membership (Saran and Terlihat dua kali in "Semua", only Dilewati optional): the hero, "Semua" and the badge would stop reading `attention`. Seen-twice items count in `unreviewed`, not `attention`, by "Seen-Twice Section". That is a behaviour change, not a presentation change.
- Dropping "Dicatat otomatis": owners use it to check automatic rows. It is the destination of "Cek yang dicatat otomatis".
- Listing promotions and codes in "Dilewati otomatis": the app does not keep them as review items. That would be new behaviour and data.

### D5. Folder rail and phone chips

`Rail` and `RailItem` (D19), from `styles.css` §Rail:
- `nav` labelled `review.folders.label` ("Folder" / "Folders"), with the `t-label` heading at padding 8 12.
- Each item is a `button` (the folder changes state and the URL's `folder`, D16) with `aria-current="true"` when current:
  - h 36, padding 0 12, gap 12, radius 6, 14/500 `--muted`;
  - the label with ellipsis, and the count 12/400 `--muted` right-aligned;
  - hover and current: `--surface` + `--text`;
  - current: the phase 1 straight bar in its rail geometry (2 × 20 px, `left: -8px; top: 8px`, `--accent`), drawn as a pseudo-element.
- Order: "Semua", "Belum dikenali", "Perlu jawaban", "Pindah saldo belum lengkap", then `rail-sep` (1 px `--line`, margin 12 12), then "Dicatat otomatis", "Saran", "Terlihat dua kali", "Dilewati otomatis".
- A folder with 0 items is hidden unless it is "Semua" or current. The rule is hidden when no optional folder shows.
- The counts are `counts.*`, with "Semua" = `apa + jawab + pindah`. Each count also has a hidden accessible text ("{count} item").
- **Optional folder caption:** when an optional folder is current, its one-line caption (today's text, `review.folders.caption.*`) sits above its list as a `t-meta` line. "Dicatat otomatis" adds the mixed-group sentence when `autoInMixedGroups > 0`.
- **Phone (< 768 px):**
  - the same elements in one row: `display: flex; gap: 8px; overflow-x: auto; margin-bottom: 16px`, no label and no rule;
  - chips h 44, padding 0 12, radius 6, 1 px `--line`;
  - current: `--surface-2` fill and 1 px `--line-strong`, with no bar, because a rounded chip never carries a coloured edge;
  - the chip line scrolls inside itself, and the page does not.

Rejected:
- An "Opsional" label instead of the rule (held branch): the prototype draws a rule. Each optional folder's caption says it is optional, and a second label adds a text style without new meaning.
- Keeping `ChipGroup` chips on a wide screen (today): they wrap over three lines (audit).
- Links (`a`) for folders, as in the prototype's hash routes: the app keeps the folder in its URL query by `replaceState` (D16), so a button with `aria-current` keeps today's history behaviour.

### D6. Item list rows

The shared `ListRow` in a new `stacked` layout (D19), as `.tj-list .row` (padding 12, `align-items: flex-start`, min-h 64), with:
- **Icon column 40:** 16 px, `--muted`:
  - `help` for Belum dikenali;
  - `archive` for Dilewati otomatis;
  - `check` for Dicatat otomatis;
  - `alert` for every other kind.
  The prototype draws help, archive and alert. `check` keeps the automatic folder recognisable, a minor deviation.
- **Title:** 14/400 with ellipsis: `itemTitle` through the catalogue. User data (merchant, notes, account names) is shown as stored.
- **Meta:** 12/400 `--muted`. In "Semua": short kind · date and time (`review.list.kind.*`, `format.shortDate` + `format.time`). In any other folder: date and time only. A group uses its `last_date`, and an item without a date shows the kind only.
- **Amount:** Inter 14/600 tabular, right, with the SPEC row variants:
  - expense "−Rp" in ink;
  - income "+Rp" in `--income`;
  - awaiting or ambiguous leg "Rp" in `--transfer`, without today's "⇄" glyph;
  - a seen-twice item without an amount has an empty amount column, as in the prototype.
  - Masked with hidden balances.
- **Selected:** `data-selected` and `aria-current="true"`, so `--surface-2` and the 2 px full-height bar at x 0 (phase 1). Focus: the neutral inset outline. `data-row={item.id}` keeps today's focus return.
- The list is a `ul` labelled `review.list.label` ("Daftar {folder}"). An empty folder shows the SPEC empty state "Folder ini sudah kosong." + "Pilih folder lain." inside the list pane.

Rejected:
- A separate Tinjau row component: the stacked layout is the same row with the title over its meta. One component keeps the hover, pressed, selected and focus states identical across pages.
- Today's two-line rows with the time top-right and the extra line ("2 transaksi", "Top up X?", "Sudah tercatat: …"): the prototype keeps one meta line, and those facts move into the item (D9).
- Round avatars with tones (`ws-ava`): the prototype has a muted 16 px icon. Colour stays for money meaning only.

### D7. Item pane

From the prototype's `tjDetail`, on desktop and tablet:
1. **Head row** (`sec-h`, no bottom margin):
   - the kind label (`p.t-label`, `review.kind.*`);
   - `tj-nav`: icon buttons ‹ › (36 × 36, `aria-label` "Sebelumnya"/"Berikutnya", `aria-keyshortcuts` K/J, disabled at the ends) around the position "{index} dari {total}" (12/16 `--muted`, min-w 48, centred).
2. **Question:** `h2.t-lead` (16/24/400), 16 px below, `tabIndex={-1}`, `data-item-heading`. It is the focus target of auto-advance, J/K and deep links (today's `HEADING_ID`).
3. **Meta line:** `t-meta`, 4 px below: kind detail · amount (small figure, Inter 14/600 tabular, masked when hidden) · date and time · account (D9 per kind).
4. **Warning line** (only where the app states one today, for an awaiting leg): `t-meta` in `--warning`, 8 px below.
5. **Reasons line:** when the item has review reasons, one `t-meta` line "{reasons}" joined with " · " (`review.reasons.*`), 8 px below. The reasons are not pills.
6. **Status line:** the polite notice and the failure line (D16, D15), `t-meta`, the failure in `--expense` with `role="alert"` and "Buka di Transaksi".
7. **Answers** (D8), 24 px below.
8. **Per-kind rows** (D9), 24 px below.
9. **Notification text box** `.raw` when the item has the notification text (`transaction.notes` of a question):
   - margin-top 24, padding 12 16, `--surface`, radius 6, 12/16 `--muted`, `overflow-wrap: anywhere`;
   - a hidden prefix "Teks notifikasi:" / "Notification text:";
   - no `lang` attribute on the quoted text.

Removed: the kind pill, the 24 px title, the 32 px amount, the provenance line, the reason pills and the pane bar with "Daftar". Provenance stays in the transaction dialog.

Rejected:
- The kind label as the `h2` (the prototype's markup): the question names the item, and focusing it after auto-advance reads the next question at once. The kind label stays visible and is read first.
- Keeping a 32 px amount: the page allows one figure ≥ 33 px, and a second large number competes with the hero (audit). The amount sits once in the meta line.

### D8. Numbered answers

`AnswerList` and `Answer` replace `Choice` and `Picker` (D19), from `.answers` and `.answer`:
- **The group:** `role="group"`, labelled by the question; `border-top: 1px solid var(--line)`.
- **Each answer** is one `button`, the whole row:
  - min-h 56, padding 8 12, gap 16, `border-bottom: 1px solid var(--line)`;
  - hover `--surface`, pressed `--surface-2`;
  - the key cap (h 20, min-w 20, padding 0 4, 1 px `--line`, radius 6, 12/500 `--muted`, `aria-hidden`) showing its number 1–9;
  - the label (`t-strong` 14/500) and its detail (`t-meta`);
  - a 16 px chevron.
  - `data-choice`, `data-primary-choice` and `data-topup` keep today's markers for 1–9, E and T. `aria-keyshortcuts` carries the digit, plus "E" or "T" where they apply.
  - E and T show no key cap; they are listed in "Bantuan & pintasan" (SPEC: key caps only on numbered answers).
  - The E target has no visual highlight.
- **Nested choices** (an account list, a product list, the counterpart candidates after a 409): `AnswerList` under a `t-label` heading, 16 px below the parent answer.
  - The parent answer has `aria-expanded`.
  - While one is open, 1–9 press its answers, as today (`pressChoice` scopes to the last open picker).
- **Disabled:** 45 % opacity while a request is pending.

Rejected:
- Today's bordered two-column cards (`ws-choice`): 291 bordered boxes was an audit finding, and the prototype's rows read top to bottom.
- Showing E/T caps (today's `hint`): the prototype shows numbers only. The keys still work.

### D9. Per-kind content

Every kind uses D7 and D8. "E" marks the `data-primary-choice` answer.

| Kind (folder) | Kind label | Question (`h2`) | Meta detail | Answers | Rows below |
| --- | --- | --- | --- | --- | --- |
| question (`apa`) | Belum dikenali | the server `prompt`; with hidden balances `review.question.purposeHidden` "Apa ini? Dari {account}" | "Uang masuk" / "Pembayaran tanpa nama toko" | the server's suggestions, numbered; detail "Saran · {category} · {pillar} · {basis}" on the first (E), the rest without "Saran"; then the quiet button "Jawab sendiri" (D11) | notification text box |
| group (`jawab`) | Perlu jawaban | "Kategori untuk {label}: {category}?", or "Kategori untuk {label}?" when mixed or unknown | group kind ("Toko", "Tanpa nama toko", "Catatan", "Kategori") · {count} transaksi · date range | 1 "Terima semua ({count})" (E) · 2 "Ubah kategori" → D10 · 3 "Ganti nama" → D10 | "Transaksi · {count}": up to 10 whole-row links to the transaction dialog (`transaksiHref`), "Lihat semua {count}" |
| group (`otomatis`) | Dicatat otomatis | "Kategori {label} sudah benar?" | as above + the caption "Opsional. Transaksi ini sudah tercatat dan tetap tersimpan meskipun dilewati." | 1 "Sudah benar" (E) · 2 · 3 as above | as above |
| awaiting (`pindah`) | Pindah saldo belum lengkap | "Uang keluar dari {account}. Masuknya ke mana?" / "Uang masuk ke {account}. Dari mana?" | "Pasangan belum ditemukan", or "Top up investasi" with a suggestion | with `topup_suggestion`: 1 "Ya, top up {product}" (E, T), detail = the server's explanation · 2 "Pilih lain" → nested products and accounts · 3 "Bukan top up", detail "Diingat untuk debit seperti ini". Without one: the accounts, numbered · "Ini top up investasi" (T) → nested products · "Bukan pindah saldo, ini pengeluaran" → D10 (expense mode). After a 409: nested "Sudah ada di rekening ini": "Hubungkan: {amount}" per candidate · "Tetap catat baru" | — |
| ambiguous (`pindah`) | Pindah saldo belum lengkap | "Keluar dari {account}. Pasangannya yang mana?" / "Masuk ke {account}. Asalnya yang mana?" | "{count} kemungkinan pasangan" | each candidate (account; detail date · amount · "gabungkan sebagai pindah saldo") · "Biarkan terpisah" | — |
| suggestion (`saran`) | Saran dari riwayat | "{label}: jadikan {category} untuk {count} transaksi lama?" | "Saran dari riwayat" · {count} transaksi · the caption "Opsional. Tidak ada yang berubah sebelum diterima." | 1 "Terima saran ({count})" (E) · 2 "Tolak" | per suggestion a plain row (before → after, source) with two quiet buttons "Terima" and "Tolak" |
| seen twice (`dua`) | Terlihat dua kali | "{app} melaporkan uang yang sudah tercatat. Satu transaksi yang sama?" | "Kemungkinan tercatat dua kali" | 1 "Benar, satu transaksi" (E) · 2 "Catat terpisah" | "Sudah tercatat · {place}" as a whole-row link to the kept transaction (time, amount); "Laporan {app}" as a plain row (time, amount); one `t-meta` line with "Rekening yang sama" or "Rekening terhubung" when present |
| skipped (`lewat`) | Dilewati otomatis | "Laporan {app} dilewati karena sudah tercatat." | "Dilewati, tidak dicatat" · the link "{account} juga dilaporkan {institution}" | 1 "Catat terpisah" | as seen twice |

- **Top-up suggestion:** keeps "Diingat untuk debit seperti ini" as the third answer's detail. The prototype's checkbox is not added: rejecting a suggestion is always remembered, so a checkbox would offer an option the API does not have (deviation 4).
- **Suggestion rows:** each has two actions, so it is a plain row (`is-plain`, no hover) with two buttons. It is not one whole-row target, because a row cannot be both.
- The Indonesian wording of answers and captions is today's text, except "Ya, top up" (today "Iya, top up"). The spec scenario already says "Ya", and "Iya" is casual.

Rejected:
- A different layout per kind (today's cards): the audit's "scrambled" finding. One pattern makes every kind read the same.
- Composing questions that state amounts: the amount sits once in the meta line and is masked there. Only the server's purpose prompt names it, and it has a hidden-balance fallback.

### D10. "Ubah kategori", "Ganti nama" and "Catat sebagai pengeluaran": one dialog

`CategoryDialog` on `ShellDialog`, width 560, a bottom sheet below 768 px. Three modes, each sending exactly today's request:

| Mode | Opened by | Title | Fields | Request |
| --- | --- | --- | --- | --- |
| category | group answer 2 | "Ubah kategori" | Kategori (select of the group type's active categories, "Pilih kategori"); Pilar Kakeibo, expense only (segmented control: "Ikuti kategori" first and selected, then each pillar today's select offers) | `/review/bulk` `recategorise` with `category_id` and `kakeibo_type` only when a pillar is chosen |
| name | group answer 3 | "Ganti nama" | Nama toko (`MERCHANT_MAX_LENGTH`, prefilled for a merchant group); Catatan (500, "Biarkan kosong untuk tetap") | `/review/bulk` `rename` with the filled fields |
| expense | "Bukan pindah saldo, ini pengeluaran" | "Catat sebagai pengeluaran" | as category | `/review/bulk` `recategorise` for the leg |

- **Header:** meta "{label} · {count} transaksi". Body hint (`t-meta`): category "Diingat untuk transaksi berikutnya dari {label}." (today's caption); expense "Setelah disimpan, ini dihitung sebagai pengeluaran dan tidak dipasangkan lagi." (today's).
- **Footer:** "Batal" (secondary) and "Simpan jawaban" (primary).
- **Validation:** today's messages ("Pilih kategori.", "Isi nama toko atau catatan.") under the field, with `aria-invalid`.
- **Save:** calls the same `act()` path. On success the dialog closes, and auto-advance and the toast follow as today. On failure it stays open with the error under the footer (`role="alert"`). Esc and "Batal" send nothing and return focus to the answer that opened it.
- **Groups larger than 200** keep today's batching and partial-failure text.

Rejected:
- The prototype's single dialog with category, pillar, store name, notes and a "remember" checkbox, saved together: it would send two requests and create two undo entries, partial failure would be possible, and the checkbox offers an option the API does not have (deviation 5).
- Inline forms (today): the owner's centered-modal rule, and the prototype opens a dialog.
- A pillar `select` (today): the prototype uses the segmented control. "Ikuti kategori" keeps today's default of sending no pillar.

### D11. "Jawab sendiri" dialog

The quiet button "Jawab sendiri" (h 36, under the answers, 12 px above) opens `ShellDialog` 520 "Jawab sendiri":
- meta = the question;
- one field "Jawaban" with `maxLength = question.text_max_length ?? 200` and the placeholder "misalnya: sewa lapangan futsal";
- the footer "Batal" and "Simpan jawaban" ("Menyimpan…" while pending).
- The request, `reply_id`, the empty-answer message and auto-advance are today's. The field is a typing target, so digits never press answers.

Rejected:
- The inline field (today): the prototype puts "Jawab sendiri" behind a quiet button, and an always-open field adds a form to every question.

### D12. "Cari saran dari riwayat" dialog

`HistorySuggestionsDialog` on `ShellDialog` 520, opened from "…" (D2):
- **Header:** "Cari saran dari riwayat", meta = today's caption ("Nama, kategori dan pilar untuk transaksi lama yang dicatat otomatis. Tidak ada yang berubah sebelum diterima.").
- **Body:**
  - a two-column form grid with "Dari tanggal" and "Sampai tanggal" (default the last three months, `windowError` checks), one column on the phone;
  - after an estimate, the estimate lines (today's `EstimateBox` content as `t-meta` lines);
  - while running, the `progress` with "{processed} dari {selected} transaksi diperiksa";
  - the last run's summary and notice;
  - the polite status and the alert.
- **Footer:**
  - "Hitung perkiraan" (secondary, submits the estimate);
  - "Mulai buat saran" (primary), enabled only while an estimate for the current period is shown, and disabled after a date change;
  - "Batal" (quiet, left), which clears a shown estimate and returns focus to "Hitung perkiraan", or closes the dialog when no estimate is shown.
- The run polls only while the dialog is open. A running run continues on the server, and reopening shows it (`runs/latest`). When it finishes while open, the suggestions query is invalidated and the status names "Saran".

Rejected:
- The prototype's one-click "Mulai buat saran": `history-suggestions` "Estimate Before Start" requires an estimate first (deviation 7).
- Keeping it as a pane that replaces the item (`RUN_PANEL_ID`): it hides the current item and has no place in the three-pane layout.

### D13. Phone item sheet

Below 768 px, choosing an item opens `ShellDialog` with the `fullHeight` variant:
- `height: 100vh; max-height: 100vh; border-radius: 0`;
- the title is the kind label;
- the close button `review.sheet.close` "Kembali ke daftar";
- the body holds D7 without the head row (the sheet's title carries the kind, and J/K still move, as in the prototype).

History and focus:
- Opening pushes a history entry (today's `pushedItem`).
- Back, Esc and the close button return to the list with focus on the item's row (today's `focusRow`).
- After an answer, the sheet shows the next item, or closes to the list when the folder is empty (today's `onePane && !next`).
- A deep link with an item opens the sheet.
- The sheet is rendered inside the Tinjau region (a native `dialog`, no portal), so the scoped keys (D16) work inside it.

Rejected:
- Today's in-place swap with a "Daftar" button: the prototype uses a full-height sheet, and the swap loses the list's scroll position.
- A 92 vh bottom sheet: SPEC names the Tinjau item as the one full-height sheet, because answers and rows need the height.

### D14. "Semua sudah ditinjau" — flagged: scope

- **Needed items 0, optional items exist:**
  - the hero shows 0, and the rail shows "Semua 0" and the optional folders;
  - the item pane shows the SPEC empty state "Semua sudah ditinjau" + "Tidak ada yang perlu dijawab.";
  - when "Dicatat otomatis" has items, one secondary button "Cek yang dicatat otomatis" opens its first item (today's behaviour).
- **No item in any folder:** the prototype's `state-tinjau-kosong`: the title block and the centred empty state only, without the hero, the rail or a button.
- **Removed:**
  - the three cycle figures (Pengeluaran siklus ini, Aman dipakai sampai gajian, Belum diklasifikasi);
  - "Kembali ke Beranda";
  - Tinjau's `/dashboard/cycle-summary` request.
  - The figures are Beranda's hero and Kakeibo figures, one click away in the sidebar.

**Flag.** This removes figures that today's "Semua Beres" requirement shows, which goes beyond presenting the same behaviour. It is reviewed before implementation (task 1.1).
- Fallback: keep them as one quiet `t-meta` line of three small figures under the empty state, with the spec's "no Rp 0 on failure" rule.

**Review outcome (2026-11-08): default taken.** Under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08 "continue all"), every decision takes its recommended default, D14 included: "Semua sudah ditinjau" drops the three cycle figures and Tinjau no longer requests the cycle summary. The fallback is not built.

Rejected:
- Keeping the three figures as a `dl` (today): it repeats Beranda's figures, the prototype has none, and the owner asked for no text walls.

### D15. Loading and failure

- **Loading** (`queueQuery.isLoading`): no hero. The `.tj` region with `aria-busy="true"` and the label `review.states.loading` holds:
  - the rail skeleton: five 36 px rows with 60 % × 12 bars;
  - the list skeleton: six 64 px SPEC skeleton rows (40 × 12, 30–52 % × 12, 96 × 12 at the right);
  - the item skeleton: three bars.
  - Static, `--surface-2`, no pulse. The phone shows the chip line skeleton and list rows only.
- **Failure:** the SPEC error box (`role="alert"`, alert icon 20, "Daftar Tinjau gagal dimuat.", secondary "Coba lagi" (`common.tryAgain`) → `refetch()`). No hero, no 0.
- **Suggestions failure** (`/review/suggestions`): today's behaviour. The queue renders, and "Saran" is absent.
- **Categories or accounts failure:** the dialog's select shows the error line with "Coba lagi". No silent empty list.

Rejected:
- Today's two 420 px grey blocks: not the SPEC skeleton, and they do not match the final geometry.

### D16. Keys, focus and URL

- **Bindings unchanged:**
  - `useShortcut` for J, K, E, T and Z, and `register` for 1–9, all scoped to the Tinjau region `regionRef`, which also contains the phone sheet (D13);
  - ignored in fields and with single keys off.
  - The `PAGE_KEYS` group "Di Tinjau" keeps its six entries, from `review.keys.*`.
- **Focus:**
  - J/K from the list move focus to the next row; otherwise to the item's heading (today's `inList`);
  - after an answer, the next heading;
  - after Urungkan, the restored item's heading;
  - a dialog returns focus to its opener;
  - the "…" menu returns focus to its button.
  - Sticky elements: the 56 px top bar is the only sticky element over Tinjau. `scroll-margin-top: calc(var(--topbar-h) + 16px)` on rows and the heading keeps focus visible (WCAG 2.4.11).
- **Notices:** today's polite messages ("Ini yang terakhir.", "Item ini sudah diselesaikan di tempat lain.", "Tidak ada pilihan nomor {n}.", …) render in the status line (D7) from `review.notice.*`.
- **URL:** unchanged. `?item=<transaction id>&folder=<key>` via `replaceState`, `pushState` only for the phone sheet; the legacy hashes are read once and dropped. Folder buttons call `chooseFolder` as today.

Rejected:
- Page-wide single keys (the prototype's `pageKeys`): the spec's "Outside the region" scenario keeps J inert on the top bar, which protects typing and other shortcuts.

### D17. Hidden balances

Every Tinjau amount renders through `MaskedMoney`: list amounts, the meta line, answer labels and details that carry amounts ("Hubungkan: {amount}", candidate details), rows below, the history dialog's counts (counts are not masked, only money). With hidden balances the masked figure is "Rp ••••••" (or "$ ••••••"), `role="img"`, named `common.hidden`. Today's purpose-question fallback keeps the prompt's amount out of the heading.

### D18. Language

- **Namespace `review`** in `messages/en.json` and `id.json`, with identical key trees. Sub-namespaces: `page`, `hero`, `folders`, `list`, `kind`, `meta`, `question`, `answer`, `rows`, `dialog.category`, `dialog.answer`, `dialog.history`, `sheet`, `states`, `notice`, `toast`, `undo`, `keys`, `reasons`. Appendix A lists every key with both languages.
- **Terms:**
  - figure and concept names come from `terms` (`terms.spending` "Spending"/"Pengeluaran", `terms.transfer`, `terms.income`, the pillars, `terms.uncategorized` and `terms.undescribed` for the fallbacks);
  - `nav.review` gives the title;
  - the glossary for Tinjau is Appendix B, consistent with `research/2026-11-06-language-system.md` §7.
- **Indonesian:** today's text for every existing string, with two changes, "Ya, top up" (D9) and the composed questions (D9). New strings follow the neutral register: no "kamu" or "Anda", no slang, and "Pengeluaran" for spending.
- **English:** plain and firm, US spelling, sentence case, "Spending" only. No "you/your" outside `conversation`: "Jawab sendiri" is "Type an answer", not "Answer it yourself".
- **Whole ICU sentences:**
  - every interpolated Tinjau string (about 61 today) becomes one message with named arguments;
  - counts use `plural` (English `one`/`other`, Indonesian `other`);
  - amounts, dates, times and ranges arrive formatted from `lib/format.ts` (`money`, `shortDate`, `time`, `dateRange`);
  - toasts that add "Berikutnya: {title}." append a second whole sentence, never a fragment.
- **Server text:**
  - the purpose `prompt`, suggestion `label`s, top-up `prompt` and `explanation`, history reasons and error `message`s are shown as sent, since `localize-server-text` renders them in the account's language;
  - Tinjau's error codes (`stale_question`, `purpose_not_applicable`, `purpose_answer_conflict`, `unknown_suggestion`, `invalid_suggestion`, `invalid_purpose_answer`, `seen_twice_not_found`) move from `lib/review.ts` into `errors.*`, Indonesian unchanged;
  - group labels with `label_source: "system"` render from their key.
- **User data:** merchant, notes, account and category names, app names and notification text are shown as stored, with no `lang` attribute.
- **Formatting:** the four `Intl.DateTimeFormat("id-ID", …)` instances (`TriageList`, `ItemHead`), `dateRangeLabel` and the `bal` calls move to `useFormat()`. Region decides the formats, the language decides the words.
- **Registry and guards:**
  - `/review` joins `lib/i18n/translatedRoutes.ts`, so the untranslated notice and `lang="id"` leave Tinjau;
  - `add-language-system`'s untranslated-page test moves its example from Tinjau to Transaksi;
  - the copy register's translated roots gain `components/review`, `app/review`, `lib/triage.ts`, `lib/review.ts`, `lib/reviewUndo.ts` and `lib/historySuggestions.ts`.
  - Catalogue parity (keys, arguments, plurals) covers `review`.
- **Lengths:** "Pindah saldo belum lengkap" → "Transfer not complete" is shorter. "Dicatat otomatis" → "Recorded automatically" (+6) still fits the 232 px rail with its count, and ellipsis is allowed. The 184 px tablet rail and the phone chips are measured in both languages (task 6.2).

Rejected:
- Labels computed in `lib/triage.ts`: the lib keeps data logic (`buildTriage`, `nextAfter`, deep links). Labels come from the catalogue in components, through a small `useTriageCopy()` helper that turns an item into its title, meta and question.
- Translating server text in the web from codes for every prompt: the server already renders it in the account's language, and a second rendering could disagree.

### D19. Shared components and CSS

**Built or extended here** (each with a user in this phase and a named later user):
- `ListRow`:
  - `layout="stacked"` (`.row.is-stacked`: `.r-main` as a column, `align-items: flex-start`, padding 12, min-h 64);
  - `current` (`aria-current="true"`);
  - `rowKey` (`data-row`).
  - Later users: Transaksi search results (phase 5), Target rows (phase 9).
- `HeroFigure`: `mark` optional; the `sw-bar` is rendered only with a mark. Later user: Laporan (phase 7).
- `components/workspace/rail.tsx`: `Rail` (`nav` + `t-label`), `RailItem` (button or link, count, `aria-current`), `RailRule`; CSS `.rail`, `.rail-item`, `.rail-sep`, and the phone chip line `.rail.is-chips` under 768 px. Later users: Transaksi JENIS/PERIODE (phase 5), Pengaturan (phase 10).
- `ShellDialog`: the `fullHeight` prop (`.dialog.is-full` below 768 px).
- `components/review/triage/Answers.tsx`: `AnswerList`, `Answer` and `KeyCap`, with `pressChoice` and `pressMarked` moved from `Choice.tsx`. They stay in `components/review`, because no other page uses numbered answers.
- `components/review/CategoryDialog.tsx`, `AnswerDialog.tsx` and `HistorySuggestionsDialog.tsx` (the run logic of `HistorySuggestionsSection` moves in).
- `Icon`: `archive` (SPEC icon set, 24 grid, 1.5 stroke).
- CSS `.tj`, `.tj-list`, `.tj-detail`, `.tj-q`, `.tj-nav`, `.answers`, `.answer`, `.raw` from `styles.css`, using the phase 1 token names.

**Removed** (no user remains):
- `ChipGroup` (`primitives.tsx`; Tinjau was its only user; `.ws-chip` stays for Transaksi and Peta Aliran);
- `ItemHead.tsx`, `Choice.tsx`, `TriageRows`, `Avatar` (Tinjau);
- `HistorySuggestionsSection.tsx` (into the dialog);
- `TWO_PANE_MIN`, `RUN_PANEL_ID`, `foldOpen`;
- CSS `.ws-triage`, `.ws-tri-*`, `.ws-tri-fold*`, `.ws-tri-foot`, `.ws-tri-hints`, `.ws-choice*`, `.ws-choices`, `.ws-picker`, `.ws-facts`, `.ws-rows` (if `HabitsLearnedCard` no longer uses it, otherwise kept), `.ws-form-row` and the review uses of `.ws-notice` and `.ws-alert`.

Each removal is checked with `rg` (task 5.8).

**Kept:** `Pill`, `useWidthAtMost`, `.ws-ava` (used by `activity.tsx`), `.ws-kbd` (TopBar, help dialog) and `.ws-field` (ledger), because other pages still use them.

### D20. What happens to each removed element

| Removed | Destination |
| --- | --- |
| Folder chips | Rail (D5); chips only on the phone |
| `<details>` folds | Optional folders under the rule (D5) |
| "Cari saran dari riwayat" button and run panel | "…" → dialog (D2, D12) |
| Key hint strip, K/J caps in the bar | Key caps on numbered answers; "Bantuan & pintasan" (D8, D16) |
| Pane bar with "Daftar" | Phone sheet's close button and back (D13) |
| Header "Perlu ditinjau {n}" + caption | Hero (D3) |
| Kind pill, 24 px title, 32 px amount | Kind label, question, meta amount (D7) |
| Provenance line | Transaction dialog (unchanged) |
| Reason pills | One reasons line (D7) |
| Two-column choice cards, picker boxes | Answer rows, nested answer lists (D8) |
| Inline category, rename, expense and text forms | Dialogs (D10, D11) |
| Avatars | 16 px icon column (D6) |
| Extra list line ("2 transaksi", "Top up X?", "Sudah tercatat …") | Item meta and rows (D9) |
| Done state's three figures and "Kembali ke Beranda" | Beranda (D14, flagged) |
| "⇄" amount glyph | `--transfer` colour; the kind is in the meta and folder name (D6) |

### D21. The held branch `fix/selection-bars-and-tinjau` (`64909ee`)

Its diff against `18a4477` touches `TriageList.tsx`, `TriageInbox.tsx`, `review/page.test.tsx`, `globals.css`, the `review-queue` delta of `refine-selection-and-tinjau-layout`, and phase 1 and 8 material.

| Part | Here | How |
| --- | --- | --- |
| One vertical folder list replacing chips and folds, optional folders in the same list, the fold state removed from deep links and undo | **Taken** | D5; `foldOpen` and its three call sites are removed as in the branch |
| An optional folder's caption shown when chosen | **Taken** | D5 |
| `review-queue` wording for "Tinjau Sections by Need" and "Triage Folders and Counts" (folder list, current marking, "Optional folder chosen" scenario) | **Taken**, rewritten for the rule and the hero | spec delta |
| Folder semantics, counts and selection tests | **Taken**, adapted | task 5.2 |
| "Opsional" label | **Superseded** | the prototype's rule (D5) |
| Equal-height panes with inner scroll (`ws-tri-scroll`) | **Superseded** | top-aligned panes, page scroll (D1) |
| "Cari saran dari riwayat" as the list's last row | **Superseded** | "…" menu (D2) |
| One-line key hints at the foot of the list | **Superseded** | removed (D8, D16) |
| Phone folder line at 720 px main width | **Superseded** | chips below 768 px (D5) |
| Straight selection bar, group rail, `selectionMarks.test.ts` | Already on `main` (phase 1) | — |
| Tagihan off Rekening, summary Utang, Grafik labels and rails | Phase 8 | — |

The branch is never merged. After phase 8 merges, the branch is deleted locally and on the remote, and its worktree `cash-flow-tracker-ui-refine` is removed. The deletion is recorded as an open item in `PROJECT_STATE.md` (task 1.1) and done then, after checking that its parts are on `main`.

### D22. Verification tooling and gate

In `scripts/verify/markas/` (local only):
- **Scenarios** (`scenarios-app.json`, `scenarios-prototype.json`):
  - `tinjau` (Semua, first item);
  - `tinjau-perlu-jawaban`, `tinjau-topup`, `tinjau-dua-kali`;
  - one per kind without a prototype screen: `tinjau-belum-dikenali-masuk`, `tinjau-saran`, `tinjau-dilewati`, `tinjau-otomatis`, `tinjau-ambigu`;
  - `state-tinjau-memuat`, `state-tinjau-kosong`, `state-tinjau-galat`;
  - `dlg-ubah-kategori`, `dlg-ganti-nama`, `dlg-jawab-sendiri`, `dlg-saran-riwayat`;
  - the phone sheet `tinjau-sheet`.
  Each in `en` and `id`.
- **`tinjau-checklist.js` for `measure.js`**, generated from D1, D5 to D8, D13 to D15 and SPEC §2, §5, §7 and §13:
  - `.tj` columns and gap at 1440 (232/336/≥ 488, 24) and 1024 (184, item full width);
  - top edges equal within 1 px;
  - rail item 36, padding 12, radius 6, bar 2 × 20 at −8/8;
  - rule 1 px;
  - row min-h 64, padding 12, icon column 40;
  - answer min-h 56, padding 8 12, key cap 20;
  - question 16/24/400;
  - raw box padding 12 16 radius 6;
  - chips 44 with radius 6 and no bar;
  - sheet height = viewport;
  - first content `y = 192`;
  - one 44 px figure and no other ≥ 33 px;
  - sizes ⊆ {12, 14, 16, 20, 24, 32, 44}, weights ⊆ {400, 500, 600}, ≤ 8 text styles;
  - no border ≥ 2 px and no coloured border except the bars;
  - no horizontal overflow at 1440, 1024, 390 and 320.
- **`tinjau-keys.mjs`**, the keyboard flows:
  1. tab order: skip link, sidebar, top bar, "…", rail, list, item;
  2. J/K from the list and from the item, focus and URL;
  3. 1 on a group, the toast, Z, the restored focus;
  4. E and T;
  5. the "…" menu with arrows and Esc;
  6. the category dialog: open, Esc with focus return, save;
  7. "Jawab sendiri" with "1 kopi" typed and no answer pressed;
  8. the phone sheet: open, 1 inside, back with focus on the row;
  9. a deep link `?item=&folder=pindah`;
  10. the single-key switch off;
  11. no focused element under the top bar.
- **Parity seed (`seed_parity.py`)** gains the kinds it skips today, built from synthetic data that does not double-book:
  - an ambiguous leg (two sends competing for one credit);
  - an awaiting debit with a learned top-up (its own debit, not the duplicate of `t017`);
  - a pending history-suggestion group;
  - a report skipped by an account link;
  - a seen-twice item with a kept leg;
  - an automatic group.
  Each addition is listed in the seed report; any kind that cannot be seeded without a backend change stays a recorded skip.
- **Stress and USD seed:**
  - the audit seed (17 pockets, long names);
  - a 250-row group;
  - a queue over 1,000 rows (`truncated`);
  - 60-character merchants;
  - Rp 1.234.567.890;
  - the USD display currency.

The gate (tasks group 6) is the plan's common gate plus the both-language gate of `add-language-system` (D16 there).

### D23. Spec reconciliation

`review-queue`:
- **Modified:** Tinjau Page, Tinjau Sections by Need, Triage Folders and Counts, One Item at a Time, Semua Beres, Phone List and Item, Seen-Twice Section, Skipped by a Linked Account, Question Row Direction, Triage Keys.
- **Added:** Tinjau Hero, Tinjau Page Actions, Category and Name Dialog, Text Answer Dialog, Tinjau Loading and Failure, Tinjau in Both Languages.
- **Unchanged** (behaviour kept, presentation neutral): Review Summary, Review Queue, Bulk actions, Awaiting Transfer Completion, Top-up From an Awaiting Debit, Ambiguous Leg Resolution, Accessible Tinjau Actions, Batched Group Actions, Auto-Advance, Tinjau Deep Links, Urungkan on Tinjau, Awaiting Leg as an Expense.

`history-suggestions`:
- **Modified:** Saran Perbaikan Riwayat Section, Suggestion Period and Estimate Controls, Suggestion Progress and Last Run.
- The rest is backend behaviour, unchanged.

`topup-learning`, `notification-purpose-questions`, `movement-pairing` and `habit-learning` name Tinjau only for behaviour, which does not change. `interface-language` and `ui-copy-register` need no delta (proposal, Capabilities).

## Decisions and their defaults

Approved 2026-11-06 under the owner's standing instruction "complete all until finished". Every decision takes its recommended default. **Flag** marks a decision that is reviewed before implementation, because it goes beyond the approved behaviour or touches money presentation.

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Layout and breakpoints | Prototype grid; 1200/768 viewport breakpoints; page scroll | 720 px container rule; inner-scroll equal panes; sticky item | — |
| D2 | Page actions | One "…" with "Cari saran dari riwayat" and "Bantuan & pintasan" | List row; pane button; visible Bantuan | — |
| D3 | Hero line | `attention` + "{unreviewed} dicatat otomatis, opsional"; no amount | Prototype's "Rp … menunggu jawaban" (reverses the 2026-11-03 decision, mixed scope); client sum; item count | — (the rejected option would be a money-presentation change) |
| D4 | Folder membership | App's (needed three; four optional) | Prototype's membership; dropping Dicatat otomatis; promos in Dilewati | — (deviations 1–3) |
| D5 | Rail and chips | Rule between needed and optional; empty hidden; chips < 768 | "Opsional" label; chips on desktop; link folders | — |
| D6 | List rows | Shared `ListRow` stacked; SPEC amount variants; muted icons | Own row component; two-line rows; avatars | — |
| D7 | Item pane | Kind label, nav, question as `h2`, one meta line, reasons line | Kind label as heading; 32 px amount; pills | — (deviation 6) |
| D8 | Answers | 56 px rows, digit caps only, nested lists | Bordered cards; E/T caps | — |
| D9 | Per-kind content | One pattern; "Ya, top up"; no top-up checkbox | Per-kind layouts; amounts in questions; checkbox | — (deviation 4) |
| D10 | Category and name | One 560 dialog, three modes, one request each | Combined prototype dialog with checkbox; inline forms; pillar select | — (deviation 5) |
| D11 | Text answer | 520 dialog behind a quiet button | Inline field | — |
| D12 | History suggestions | 520 dialog, estimate then start | One-click start; pane | — (deviation 7) |
| D13 | Phone item | Full-height sheet with history entry | In-place swap; 92 vh sheet | — |
| D14 | Done state | Empty state only; cycle figures removed | Keep the three figures | **Flag: scope** (removes figures a requirement shows); reviewed 2026-11-08: default taken |
| D15 | Loading and failure | SPEC skeleton rows and error box | Grey blocks | — |
| D16 | Keys and URL | Unchanged bindings and scope (region + sheet) | Page-wide keys | — |
| D17 | Hidden balances | `MaskedMoney` everywhere on Tinjau | — | — |
| D18 | Language | `review` namespace; server text as sent; `/review` translated | Labels in lib; client re-rendering of server text | — |
| D19 | Shared pieces | Extend `ListRow`, `HeroFigure`, `ShellDialog`; new rail; answers local | Tinjau-only copies; answers in workspace | — |
| D20 | Removed elements | As the table | — | — |
| D21 | Held branch | Fold Tinjau parts; delete after phase 8 | Merge it first | — |
| D22 | Tooling | Extended seeds, scenarios, checklist, key flows | Existing seed only | — |
| D23 | Specs | As listed | — | — |

No default changes money behaviour, a request, a count or a figure's value.

## Risks / Trade-offs

- [Breakpoint change from container width to viewport] At 768–1199 px both list and item show, where today one shows. The tablet gets denser. Mitigation: the 1024 and 768 captures and the checklist; the item spans the full width below.
- [Folder buttons inside a `nav`] A `nav` of buttons is unusual. It is labelled "Folder", each button has `aria-current`, and axe is run. If axe or the screen-reader pass objects, the container becomes `div role="group"`, a one-line change recorded in `verification.md`.
- [Scoped keys inside the sheet] The sheet must be inside the region for `scope.contains`. Covered by the keyboard flow 8.
- [English lengths in the 184 px tablet rail and the chips] "Recorded automatically" and "Transfer not complete" may ellipsize at 184 px. Ellipsis is allowed for rail labels; the full name stays in the accessible name. Measured in both languages.
- [Parity seed kinds that need learned state] A top-up suggestion needs a learned pattern. If it cannot be seeded through the API or plain inserts in the disposable database, the kind is captured from the stress seed and recorded as a skip.
- [Copy volume] About 307 strings move to keys. The Indonesian text is checked byte for byte against today's by a snapshot of the rendered Indonesian page before and after (task 3.4).
- [Interim look] Transaksi, opened from group rows, keeps its old layout until phase 5.

## Migration and rollback

No data, API or schema change. Rollback is a revert of the merge commit. The URL contract (`?item=&folder=`, legacy hashes) is unchanged, so links from Beranda, the bell and Transaksi keep working across a rollback.

## Open Questions

None. The flagged D14 was reviewed on 2026-11-08 and takes its default (D14).

## Appendix A. Tinjau copy as message keys (`review` namespace; Indonesian · English)

Arguments in braces arrive formatted (D18). `{count}` selects plurals. Existing Indonesian text is kept byte for byte; the keys marked "new" are new strings. The remaining existing strings (undo outcomes, partial-failure messages, the run notices, the estimate lines) move under the same sub-namespaces with their Indonesian unchanged and English written with this glossary; their full list is produced by task 3.1 and reviewed in task 3.4.

### `review.page`, `review.hero`, `review.states`, `review.sheet`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.subtitle` | Yang perlu dijawab dan yang dicatat otomatis | What needs an answer and what was recorded automatically |
| `page.more` | Tindakan lain untuk Tinjau | More actions for Review |
| `page.findSuggestions` | Cari saran dari riwayat | Find suggestions from history |
| `page.region` | Tinjau | Review |
| `hero.label` | Perlu ditinjau | Needs review |
| `hero.line` (new) | {unreviewed} dicatat otomatis, opsional | {unreviewed} recorded automatically, optional |
| `hero.truncated` | Hanya {limit} transaksi terbaru yang ditampilkan. Selesaikan yang ada untuk melihat sisanya. | Only the latest {limit} transactions are shown. Finish these to see the rest. |
| `states.loading` | Memuat Tinjau | Loading Review |
| `states.error` | Daftar Tinjau gagal dimuat. | The review list could not be loaded. |
| `states.done.title` | Semua sudah ditinjau | All reviewed |
| `states.done.line` | Tidak ada yang perlu dijawab. | Nothing needs an answer. |
| `states.done.checkAuto` (new) | Cek yang dicatat otomatis | Check what was recorded automatically |
| `states.emptyFolder.title` | Folder ini sudah kosong. | This folder is empty. |
| `states.emptyFolder.line` (new) | Pilih folder lain. | Choose another folder. |
| `sheet.close` (new) | Kembali ke daftar | Back to the list |

### `review.folders`, `review.list`, `review.kind`

| Key | Indonesian | English |
| --- | --- | --- |
| `folders.label` (new) | Folder | Folders |
| `folders.all` | Semua | All |
| `folders.apa` | Belum dikenali | Not recognized |
| `folders.jawab` | Perlu jawaban | Needs an answer |
| `folders.pindah` | Pindah saldo belum lengkap | Transfer not complete |
| `folders.otomatis` | Dicatat otomatis | Recorded automatically |
| `folders.saran` (new label) | Saran | Suggestions |
| `folders.dua` | Terlihat dua kali | Seen twice |
| `folders.lewat` | Dilewati otomatis | Skipped automatically |
| `folders.countName` (new) | {count} item | {count, plural, one {# item} other {# items}} |
| `folders.caption.otomatis` | Dicatat dari aturan atau AI. Pengecekan opsional. | Recorded by a rule or AI. Checking is optional. |
| `folders.caption.mixed` | {count} lainnya ikut kelompok di Perlu jawaban. | {count} more are in groups under Needs an answer. |
| `folders.caption.saran` | Usulan untuk transaksi lama. Tidak dihitung sebagai yang perlu dijawab. | Proposals for older transactions. Not counted as needing an answer. |
| `folders.caption.dua` | Satu transaksi dilaporkan dua aplikasi dan hanya dicatat sekali. | One transaction was reported by two apps and recorded once. |
| `folders.caption.lewat` | Laporan rekening yang juga dilaporkan aplikasi lain, sudah tercatat sekali. 30 hari terakhir. | Account reports also sent by another app, already recorded once. Last 30 days. |
| `list.label` (new) | Daftar {folder} | {folder} list |
| `list.kind.apa` | Belum dikenali | Not recognized |
| `list.kind.jawab` | Perlu jawaban | Needs an answer |
| `list.kind.pindah` (new) | Pindah belum lengkap | Transfer incomplete |
| `list.kind.otomatis` | Dicatat otomatis | Recorded automatically |
| `list.kind.saran` | Saran | Suggestion |
| `list.kind.dua` (new) | Dua kali | Seen twice |
| `list.kind.lewat` | Dilewati | Skipped |
| `list.title.moneyIn` | Uang masuk | Money in |
| `list.title.noStore` | Pembayaran tanpa nama toko | Payment without a store name |
| `list.title.outOf` | Keluar dari {account} | Out of {account} |
| `list.title.into` | Masuk ke {account} | Into {account} |
| `list.title.secondReport` | {app}: laporan kedua | {app}: second report |
| `list.title.skipped` | {app}: dilewati | {app}: skipped |
| `kind.apa` … `kind.lewat` | the folder names, with `kind.saran` "Saran dari riwayat" | the folder names, with `kind.saran` "Suggestions from history" |

### `review.detail`, `review.meta`, `review.question`, `review.reasons`

| Key | Indonesian | English |
| --- | --- | --- |
| `detail.region` (new) | Detail | Details |
| `detail.position` | {index} dari {total} | {index} of {total} |
| `detail.previous` (new) | Sebelumnya | Previous |
| `detail.next` (new) | Berikutnya | Next |
| `detail.rawPrefix` (new) | Teks notifikasi: | Notification text: |
| `meta.moneyIn` | Uang masuk | Money in |
| `meta.noStore` | Pembayaran tanpa nama toko | Payment without a store name |
| `meta.topup` (new) | Top up investasi | Investment top-up |
| `meta.noCounterpart` (new) | Pasangan belum ditemukan | Other side not found yet |
| `meta.candidates` | {count} kemungkinan pasangan | {count, plural, one {# possible match} other {# possible matches}} |
| `meta.fromHistory` (new) | Saran dari riwayat | Suggestion from history |
| `meta.maybeTwice` (new) | Kemungkinan tercatat dua kali | May be recorded twice |
| `meta.skipped` | Dilewati, tidak dicatat | Skipped, not recorded |
| `meta.transactions` | {count} transaksi | {count, plural, one {# transaction} other {# transactions}} |
| `meta.groupKind.merchant` | Toko | Store |
| `meta.groupKind.signature` | Tanpa nama toko | No store name |
| `meta.groupKind.notes` | Catatan | Notes |
| `meta.groupKind.category` | Kategori | Category |
| `meta.warning.outgoing` | Selama belum lengkap, ini belum dihitung sebagai pengeluaran. | Until it is complete, this is not counted as spending. |
| `meta.warning.incoming` | Selama belum lengkap, ini belum masuk Masuk. | Until it is complete, this is not counted as income. |
| `meta.optionalAuto` | Opsional. Transaksi ini sudah tercatat dan tetap tersimpan meskipun dilewati. | Optional. These transactions are recorded and stay saved if skipped. |
| `meta.optionalSuggestion` | Opsional. Tidak ada yang berubah sebelum diterima. | Optional. Nothing changes until accepted. |
| `question.purposeHidden` | Apa ini? Dari {account} | What was this for? From {account} |
| `question.groupCategory` (new) | Kategori untuk {label}: {category}? | Category for {label}: {category}? |
| `question.groupUnknown` (new) | Kategori untuk {label}? | Category for {label}? |
| `question.groupAuto` (new) | Kategori {label} sudah benar? | Is the category for {label} right? |
| `question.awaitingOut` | Uang keluar dari {account}. Masuknya ke mana? | Money out of {account}. Where did it go? |
| `question.awaitingIn` | Uang masuk ke {account}. Dari mana? | Money into {account}. Where did it come from? |
| `question.ambiguousOut` | Keluar dari {account}. Pasangannya yang mana? | Out of {account}. Which one is the other side? |
| `question.ambiguousIn` | Masuk ke {account}. Asalnya yang mana? | Into {account}. Which one did it come from? |
| `question.suggestion` (new) | {label}: jadikan {category} untuk {count} transaksi lama? | {label}: change {count, plural, one {# older transaction} other {# older transactions}} to {category}? |
| `question.seenTwice` (new) | {app} melaporkan uang yang sudah tercatat. Satu transaksi yang sama? | {app} reported money that is already recorded. Is it the same transaction? |
| `question.skipped` (new) | Laporan {app} dilewati karena sudah tercatat. | The {app} report was skipped because it is already recorded. |
| `reasons.uncertain_category` | Kategori belum pasti | Category not certain |
| `reasons.model_fallback` | AI ragu, dicatat dengan aturan cadangan | AI was unsure, recorded by a fallback rule |
| `reasons.low_confidence` | Keyakinan AI rendah | Low AI confidence |
| `reasons.model_flagged` | Ditandai AI untuk dicek | Flagged by AI for a check |
| `reasons.ambiguous_movement` | Pasangan pindah saldo belum pasti | Transfer match not certain |
| `reasons.purpose_question` | Menunggu jawaban: belum dikenali | Waiting for an answer: not recognized |
| `reasons.awaiting_counterpart` | Menunggu rekening pasangan | Waiting for the other account |

### `review.answer`, `review.rows`

| Key | Indonesian | English |
| --- | --- | --- |
| `answer.suggested` | Saran | Suggestion |
| `answer.typeOwn` | Jawab sendiri | Type an answer |
| `answer.acceptAll` | Terima semua ({count}) | Accept all ({count}) |
| `answer.acceptAllDetail` | Kategorinya sudah benar | The category is right |
| `answer.correct` | Sudah benar | Correct |
| `answer.correctDetail` | Tandai sudah dicek | Mark as checked |
| `answer.changeCategory` | Ubah kategori | Change category |
| `answer.changeCategoryDetail` | Kategori dan pilar Kakeibo | Category and Kakeibo pillar |
| `answer.rename` | Ganti nama | Rename |
| `answer.renameDetail` | Nama toko dan catatan | Store name and notes |
| `answer.isTopup` | Ini top up investasi | This is an investment top-up |
| `answer.isTopupDetail` | Masuk ke produk, dihitung Tabungan | Goes to a product, counted as Savings |
| `answer.isSpending` | Bukan pindah saldo, ini pengeluaran | Not a transfer, this is spending |
| `answer.isSpendingDetail` | Pilih kategorinya, lalu dihitung sebagai pengeluaran | Choose its category; it then counts as spending |
| `answer.topupYes` (changed: "Iya" → "Ya") | Ya, top up {product} | Yes, top up {product} |
| `answer.topupOther` | Pilih lain | Choose another |
| `answer.topupOtherDetail` | Produk lain atau rekening pasangan | Another product or the other account |
| `answer.topupNo` | Bukan top up | Not a top-up |
| `answer.topupNoDetail` | Diingat untuk debit seperti ini | Remembered for debits like this |
| `answer.productsTitle` | Ini top up investasi. Ke produk mana? | Investment top-up. Which product? |
| `answer.candidatesTitle` | Sudah ada di rekening ini | Already in this account |
| `answer.link` | Hubungkan: {amount} | Link: {amount} |
| `answer.recordNew` | Tetap catat baru | Record as new anyway |
| `answer.recordNewDetail` | Rekening itu memang punya transaksi lain | That account has another transaction |
| `answer.mergeDetail` | {date} · {amount} · gabungkan sebagai pindah saldo | {date} · {amount} · combine as a transfer |
| `answer.keepSeparate` | Biarkan terpisah | Keep separate |
| `answer.keepSeparateDetail` | Bukan pindah saldo, jangan dipasangkan otomatis | Not a transfer; do not pair automatically |
| `answer.acceptSuggestion` | Terima saran ({count}) | Accept suggestion ({count}) |
| `answer.acceptSuggestionDetail` | Nama, kategori dan pilar diperbarui | Name, category and pillar are updated |
| `answer.reject` | Tolak | Reject |
| `answer.rejectDetail` | Saran ini tidak diusulkan lagi | This suggestion is not proposed again |
| `answer.sameOne` | Benar, satu transaksi | Yes, one transaction |
| `answer.sameOneDetail` | Rekening ditautkan; laporan serupa dilewati otomatis | Links the account. Similar reports are skipped automatically. |
| `answer.recordSeparately` | Catat terpisah | Record separately |
| `answer.recordSeparatelyIn` | Transaksi lain; catat di {account} | Another transaction. Record it in {account}. |
| `rows.transactions` (new) | Transaksi · {count} | Transactions · {count} |
| `rows.edit` | Ubah | Edit |
| `rows.showAll` | Lihat semua {count} | Show all {count} |
| `rows.recorded` (new) | Sudah tercatat · {place} | Already recorded · {place} |
| `rows.thisReport` (new) | Laporan {app} | {app} report |
| `rows.sameAccount` | {account} juga terlihat di {app} | {account} also appears in {app} |
| `rows.linked` | {account} juga dilaporkan {institution} | {account} is also reported by {institution} |

### `review.dialog.*`

| Key | Indonesian | English |
| --- | --- | --- |
| `dialog.category.title` | Ubah kategori | Change category |
| `dialog.category.renameTitle` | Ganti nama | Rename |
| `dialog.category.spendingTitle` (new) | Catat sebagai pengeluaran | Record as spending |
| `dialog.category.meta` | {label} · {count} transaksi | {label} · {count, plural, one {# transaction} other {# transactions}} |
| `dialog.category.category` | Kategori | Category |
| `dialog.category.choose` | Pilih kategori | Choose a category |
| `dialog.category.pillar` | Pilar Kakeibo | Kakeibo pillar |
| `dialog.category.followCategory` | Ikuti kategori | Follow the category |
| `dialog.category.remembered` | Diingat untuk transaksi berikutnya dari {label}. | Remembered for the next transactions from {label}. |
| `dialog.category.spendingHint` | Setelah disimpan, ini dihitung sebagai pengeluaran dan tidak dipasangkan lagi. | After saving, this counts as spending and is not paired again. |
| `dialog.category.store` | Nama toko | Store name |
| `dialog.category.notes` | Catatan | Notes |
| `dialog.category.keepEmpty` | Biarkan kosong untuk tetap | Leave empty to keep |
| `dialog.category.save` | Simpan jawaban | Save answer |
| `dialog.category.needCategory` | Pilih kategori. | Choose a category. |
| `dialog.category.needName` | Isi nama toko atau catatan. | Fill in a store name or notes. |
| `dialog.answer.title` | Jawab sendiri | Type an answer |
| `dialog.answer.field` (new) | Jawaban | Answer |
| `dialog.answer.placeholder` | misalnya: sewa lapangan futsal | for example: futsal court rental |
| `dialog.answer.empty` | Tulis jawaban singkat, misalnya "makan siang di kantin". | Write a short answer, for example "lunch at the canteen". |
| `dialog.answer.save` | Simpan jawaban | Save answer |
| `dialog.answer.saving` | Menyimpan… | Saving… |
| `dialog.history.title` | Cari saran dari riwayat | Find suggestions from history |
| `dialog.history.meta` | Nama, kategori dan pilar untuk transaksi lama yang dicatat otomatis. Tidak ada yang berubah sebelum diterima. | Names, categories and pillars for older automatic transactions. Nothing changes until accepted. |
| `dialog.history.from` | Dari tanggal | From |
| `dialog.history.to` | Sampai tanggal | To |
| `dialog.history.estimate` | Hitung perkiraan | Estimate |
| `dialog.history.start` | Mulai buat saran | Make suggestions |
| `dialog.history.estimateTitle` | Perkiraan: {count} transaksi akan diperiksa | Estimate: {count, plural, one {# transaction} other {# transactions}} will be checked |
| `dialog.history.progress` | Sedang membuat saran: {processed} dari {selected} transaksi diperiksa | Making suggestions: {processed} of {selected} transactions checked |
| `dialog.history.done` (changed: the folder is now "Saran") | Pembuatan saran selesai. Sarannya ada di "Saran". | Suggestions are ready in "Suggestions". |
| `dialog.history.nothing` | Tidak ada transaksi otomatis yang bisa diperbaiki di periode ini. | No automatic transaction in this period can be improved. |

### `review.notice`, `review.toast`, `review.keys`

| Key | Indonesian | English |
| --- | --- | --- |
| `notice.last` | Ini yang terakhir. | This is the last one. |
| `notice.first` | Ini yang pertama. | This is the first one. |
| `notice.linkDone` | Item yang dibuka sudah selesai. | The item you opened is already done. |
| `notice.doneElsewhere` | Item ini sudah diselesaikan di tempat lain. | This item was finished elsewhere. |
| `notice.noSuggested` | Belum ada saran untuk diterima. | There is no suggestion to accept. |
| `notice.topupOnly` | T hanya untuk pindah saldo keluar yang belum lengkap. | T works only on an incomplete outgoing transfer. |
| `notice.noAnswer` | Tidak ada pilihan nomor {n}. | There is no answer number {n}. |
| `notice.openInLedger` | Buka di Transaksi | Open in Transactions |
| `toast.saved` | Tersimpan: {message}. | Saved: {message}. |
| `toast.next` | Berikutnya: {title}. | Next: {title}. |
| `toast.allDone` | Semua sudah ditinjau. | All reviewed. |
| `toast.groupAccepted` | {label}, {count} transaksi ditandai sudah ditinjau | {label}: {count, plural, one {# transaction} other {# transactions}} marked as reviewed |
| `toast.linkCancelled` | Tautan dibatalkan. Laporan {institution} serupa akan ditanyakan lagi. | Link cancelled. Similar {institution} reports will be asked again. |
| `keys.group` | Di Tinjau | On Review |
| `keys.move` | Pindah ke item berikutnya atau sebelumnya | Move to the next or previous item |
| `keys.open` | Buka item yang dipilih | Open the selected item |
| `keys.accept` | Terima saran yang disorot | Accept the suggested answer |
| `keys.topup` | Ini top up investasi | This is an investment top-up |
| `keys.numbered` | Pilih jawaban bernomor | Choose a numbered answer |
| `keys.undo` | Urungkan jawaban terakhir | Undo the last answer |

`errors.*` gains the seven Tinjau codes of D18 with today's Indonesian text and English, for example `errors.stale_question` "This question was already answered or changed. The list was reloaded."

## Appendix B. Tinjau glossary (Indonesian → English)

Consistent with `research/2026-11-06-language-system.md` §7; figure and concept names are `terms.*` and are spelled the same everywhere.

| Indonesian | English | Note |
| --- | --- | --- |
| Tinjau | Review | page title and nav (`nav.review`) |
| Perlu ditinjau | Needs review | hero label |
| Semua sudah ditinjau | All reviewed | |
| Belum dikenali | Not recognized | not "Unknown" |
| Perlu jawaban | Needs an answer | |
| Pindah saldo | Transfer | own accounts only |
| Pindah saldo belum lengkap | Transfer not complete | list short form "Transfer incomplete" |
| Dicatat otomatis | Recorded automatically | |
| Saran · Saran dari riwayat | Suggestions · Suggestions from history | rail · kind label |
| Terlihat dua kali | Seen twice | not "Duplicate" |
| Dilewati otomatis | Skipped automatically | not "Auto-skipped" |
| Opsional | Optional | |
| Pengeluaran | Spending | the only English name of the figure and the kind (`terms.spending`) |
| Masuk · Pemasukan · Uang masuk | Income · Income · Money in | "Money in" only as row direction |
| Uang keluar · Keluar dari | Money out · Out of | row direction only, never a figure label |
| Tabungan | Savings | |
| Top up | Top-up (noun), top up (verb) | |
| Rekening · Kantong | Account · Pocket | |
| Kategori · Pilar Kakeibo | Category · Kakeibo pillar | |
| Kebutuhan · Keinginan · Budaya · Tak Terduga | Needs · Wants · Culture · Unexpected | `terms.*` |
| Belum diklasifikasi | No pillar yet | |
| Tanpa kategori · Tanpa keterangan | No category · No description | `terms.uncategorized`, `terms.undescribed` |
| Toko · Nama toko | Store · Store name | |
| Catatan | Notes | |
| Terima · Terima semua · Tolak | Accept · Accept all · Reject | |
| Sudah benar · Sudah dicek | Correct · Checked | |
| Ubah kategori · Ganti nama | Change category · Rename | |
| Jawab sendiri | Type an answer | no "you/your" in labels |
| Hubungkan · Biarkan terpisah · Catat terpisah | Link · Keep separate · Record separately | |
| Cari saran dari riwayat · Hitung perkiraan · Mulai buat saran | Find suggestions from history · Estimate · Make suggestions | |
| Urungkan | Undo | |
| Bantuan & pintasan | Help & shortcuts | shell term |
| Folder | Folders | rail label |
| Apa ini? | What was this for? | conversational, server text |
