## Context

See proposal.md for the motivation. The code facts below were read on `origin/main` `0308b7b` (after #89 `add-language-system`); the other phases' facts come from their planned changes in the `markas-tinjau`, `markas-transaksi`, `markas-analisis`, `markas-rekening` and `markas-target` worktrees.

**Today's Pengaturan** (`app/settings/`, `components/settings/`, five settings components in `components/ui/`):
- `layout.tsx` renders `SettingsSectionNav` (the rail) beside the content and, at `/settings` only, `SettingsSectionList` (the phone index). `/settings` renders `DevicesSettings` with no redirect. Below a 900 px container width (`money.module.css` `@container main`) the rail hides, `/settings` shows the index and each section gets a "Semua pengaturan" back link. With the 240 px sidebar that happens below about 1237 px.
- `sections.ts` holds the nine sections with Indonesian titles and purposes. The first three have no group heading; the other six sit under "Akun". The rail heading is an `h2`.
- Each section is one `SettingsCard` (`ws-card`, 20/22 padding, `h2` title, purpose line, optional right-aligned footer). Inner parts are `SettingsSubsection`s with `border-t`.
- **HP & perangkat** (`DevicesSettings`): `CompanionStatus` (`shell.companion`, translated), `CompanionDownloadSection` (QR 160 px, `qrcode` 1.5.4), `DevicePairingSection` (rows with "Siap"/"Perlu dicek" badges on tinted fills, problems, "Cabut" through a centered `Modal`), `ManualApiKey` (a closed `<details>`). `DevicePairingDialog` is a `Modal` with a 240 px QR, the code, a countdown, "Buat kode baru", polling every 3 s for 10 minutes, and `DeviceHealthChecklist` (tinted step fills).
- **Kebiasaan** (`HabitsSettings`): read-only facts for the two server switches, two `ws-tbl` tables (payment habits, investment top-ups), empty and error states. Phase 6 adds "Seberapa sering ditanya" (`QuestionRate`, `GET /api/dashboard/question-rate?weeks=8`) after the tables, in the old styling.
- **Tampilan** (`AppearanceSettings`): the translated Language radio group, the theme radio group with swatches (`common.theme`), two `ws-switch` rows (single-key shortcuts, hide balances), and a help row opening `HelpDialog`.
- **Profil**, **Mata uang & anggaran** (with the translated Region select), **Gaji & siklus**, **Pemetaan notifikasi**, **Nama pengirim**, **Sesi uji**: forms and lists with their requests listed in D12 to D15.
- **Language:** `/settings` is in `untranslatedPages.ts` and not in `TRANSLATED_ROUTES`; everything but the two controls, the theme names, the companion line and the test-session banner is literal Indonesian. Dates use `toLocaleDateString("id-ID")`; salary amounts are hard-coded IDR.
- **Sign-out:** `AppLayout.logout` (`POST /api/auth/logout`, then `/auth/login`). It is reached from the account menu (desktop) and the last row of `MoreSheet` (phone), which waits for this phase (`apply-markas-shell` owner decision 7).

**The prototype** (`pages2.js` `PAGES.pengaturan`, `styles.css` `.set`, `.rail`, `.toggle`, `.switch`, `.qr`, `.form-grid`, `.stack`, the phone block):
- `titleBlock("Pengaturan")`, then `.set` = `grid-template-columns: 200px minmax(0, 720px); gap: 48px`, frame 1040.
- Rail: `t-label` group names PERANGKAT, TAMPILAN, AKUN; 36 px rail items; `.rail-sep`; "Keluar" with the logout icon.
- Form column: `h2.t-dtitle` (Manrope 20/28), one `t-meta` line (margin 4 above, 32 below), then `.stack` (`> * + * { margin-top: 48px }`).
- Below 768 px: `.set { display: block }`, `.set-rail { margin-bottom: 24px }`, `.form-grid` one column.
- `SPEC.md` §8 says "sections 32 apart"; `styles.css` `.stack` is 48 and wins (plan, "Fidelity rule"). The 32 px is the gap between the description and the first part.

**Shared pieces from earlier phases this phase uses:** `ShellDialog` (phase 2, with phase 4's `fullHeight` unused here), `Rail`, `RailItem`, `RailRule` (phase 4, `components/workspace/rail.tsx`; Pengaturan uses the vertical rail on every width, never the phone chips), `SectionHeader` and `ListRow` (phase 3, with phase 4's `stacked`), `Segmented` (phase 1 `.seg`), `EmptyState`, `ErrorState`, `Skeleton`, `MaskedMoney`, `useMoney`, `lib/format.ts` (`add-language-system`), the phase 5 `RowAmount` for the salary amounts.

## Re-based on the merged code (2026-11-09)

Recorded before implementation (task 1.2). The branch is stacked on phase 7 (`origin/feat/markas-laporan` `27f0033`,
PR #97, which contains `origin/main` `baa90a8` with phases 1 to 6, 8, 9, the language system, the server text and the
credit flow); the coordinator's instruction replaces D23's "merged into `main` first" for phase 7. Where the plan and
the merged code differ, the code wins and the plan adapts:

- **Rail.** `RailItem` is a button with `aria-current="true"`. Pengaturan's sections are addresses, so `RailItem`
  gains an `href` that renders a `Link` with `aria-current="page"` (the bar and fill already style both). The groups
  use `Rail showLabel={false}` with one list per group labelled by its `RailLabel`.
- **Segments.** `Segmented` is the `aria-pressed` group; the radio group of D11 and D13 is `RadioSegments`.
- **CSS that exists.** `.stack`, `.field`, `.field-control`, `.form-grid` (`.full-row`), `.form-stack`, `.sec-h`,
  `.row`/`.list`, `.c-income`, `.c-warning` and `.c-expense` came with earlier phases and are reused. `.box` is
  Transaksi's flex box (padding 16), so the prototype's settings box is `.set-box` (padding 24, 16 on a phone).
- **Breakpoints (D3).** `styles.css` also has a tablet rule (`max-width: 1199px`: `.set` 184 | `minmax(0, 1fr)`,
  gap 32). The fidelity rule makes `styles.css` win, so the grid is 200 | ≤ 720 gap 48 from 1200 px, 184 | 1fr gap 32
  from 768 to 1199 px, and one column below 768 px. On a phone the rail items are 44 px high (SPEC §11 targets).
- **Salary amounts.** `RowAmount` takes a transaction row; the salary rows use its `KindAmount` with `kind="income"`.
- **Copy register.** `beres` and `gajian` are in the register's slang list and English `OK` in its slang list, so a
  passed check reads "Baik" / "Good" (D7, Appendix A `health.ok`), and the salary source meta keeps today's
  "Dikenali dari nama pengirim" without "gajian sekitar tanggal {day}": `/salary/source` returns no day and
  `payday_day` is deprecated, so there is no value to show (D12).
- **Pairing QR (owner override 2026-11-08).** The pairing QR stays at least 200 css px (drawn at the device pixel
  ratio, in a 218 px white `.qr` box with an 8 px quiet border), not the prototype's 118 px, because scan reliability on
  a real phone matters more than the prototype's size. The download QR keeps the 136 px box. This replaces D9's
  module-size fallback for the pairing QR.
- **Untranslated example (D19).** After `/settings` leaves `untranslatedPages.ts`, only `/komponen` (the local
  component sheet) remains there, so the untranslated-page test's example moves to `/komponen`.
- **Process.** `PROJECT_STATE.md` lives in the owner's checkout and is not edited by this run; its notes go to
  `verification.md` under "For PROJECT_STATE.md". No pull request is created (coordinator); the branch is pushed.
- **Phase 6** already moved "Seberapa sering ditanya" (`QuestionRate`, same request) into Kebiasaan in the old
  styling; this phase restyles and translates it.

## Goals / Non-Goals

**Goals:** the prototype's frame, rail and section rhythm; every section restyled without losing a control or a request; both dialogs on `ShellDialog`; Keluar in Pengaturan; the whole page in English and Indonesian.

**Non-Goals:** new settings or switches, server-side theme, a new QR library, the Catat and global dialogs (phase 11), the CSS tokens sweep (phase 12).

## Decisions

### D1. Page frame

The settings layout renders `.set` inside the shell's page frame (1040, `frameWidth('/settings')` unchanged): the rail (200) and the form column (`minmax(0, 720px)`), gap 48, `align-items: start`. The rail is not sticky in the prototype; it stays static. The title block is the shell's (`TitleBlock`, "Pengaturan", no subtitle, no actions).

Rejected: a sticky rail (not in the prototype; the rail is short); a wider form column (fidelity).

### D2. Rail

`SettingsNav` is rebuilt on phase 4's `Rail` with `aria-label` "Bagian pengaturan" / "Settings sections":
- three groups with `t-label` names PERANGKAT (HP & perangkat, Kebiasaan), TAMPILAN (Tampilan), AKUN (Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Nama pengirim, Sesi uji). Each group is a list labelled by its name, so a screen reader hears the group;
- `RailItem` links with `aria-current="page"` and the selection bar on the current one;
- `RailRule`, then "Keluar" (D5).

`sections.ts` keeps the nine slugs and their order, with message keys instead of strings and the group of each section (`devices`, `appearance`, `account`).

Rejected: keeping the unnamed first group (the prototype names all three); group headings as `h2` (the page's `h2` is the section title; a list label avoids a heading level that the prototype draws as a label).

### D3. Breakpoints — flagged: scope

- **From 1024 px:** the `.set` grid, whatever the sidebar state. The rule is a viewport media query, as `styles.css`, replacing today's 900 px container query.
- **768 to 1023 px:** the grid still fits (64 px icon rail, 32 px margins: 200 + 48 + 640 at 1024; at 768 the form column is 392 px); it stays.
- **Below 768 px:** one column. The rail sits above the section with 24 px below it, every item listed, as the prototype's phone captures show. `/settings` shows HP & perangkat under the rail.
- The phone index (`SettingsSectionList`) and the "Semua pengaturan" back link are removed; the rail is the way to every section on every width.

This changes spec "Sectioned Settings Page" (the phone list-then-section pattern and its "from 1024 px" rule).

Rejected:
- Keep the container query: the rail vanishes on a 1280 px laptop with the sidebar open, which the prototype never does.
- Keep the phone index with a back link: one more step per section, and the prototype's phone captures show the stacked rail.
- Phase 4's phone chips: Pengaturan's prototype keeps a vertical list.

### D4. Section header and parts

`SettingsSection` (replacing `SettingsCard`):
- `h2.t-dtitle` with `id` and `tabIndex=-1` (the focus target after navigation, as today's `data-settings-heading`), then one `p.t-meta` description (margin 4 above, 32 below);
- children in `.stack` (48 apart);
- no card, no padding box, no footer row.

Parts inside a section use the shared `SectionHeader` (12/500 uppercase label, optional aside such as a count or a button), as the prototype's `secH`.

Section saves sit at the foot of their form, left-aligned (`<div><button class="btn btn-primary">`), as the prototype. Section-scoped saving (which fields, which request) is unchanged.

Rejected: keeping the cards (the prototype has none; a card per section duplicates the frame); right-aligned footers (prototype).

### D5. "Keluar" in Pengaturan — flagged: scope

- The rail ends with `RailRule` and a `RailItem` button "Keluar" / "Sign out" (`shell.signOut`, logout icon 16) that calls the workspace `logout` (`POST /api/auth/logout`, then `/auth/login`). No confirmation, as the account menu today.
- `MoreSheet` drops its Keluar row; phone users reach it through Lainnya › Pengaturan (two taps, the prototype's path).
- The desktop account menu keeps "Profil" and "Keluar", as the prototype's `akun` menu.

Rejected: removing Keluar from the account menu too (the prototype keeps it there); a confirmation dialog (none today, none in the prototype).

### D6. HP & perangkat — order

The section reads, top to bottom: "Status HP" (D7), "Unduh aplikasi pendamping", "Perangkat terhubung" with "Hubungkan ponsel" as the header's action, "Kunci API manual (cadangan)" (D8). That is today's order with the status first, as the prototype.

- **Unduh aplikasi pendamping:** `.qr` box (136 × 136, 1 px `--line`, radius 6, padding 8, white) holding the download QR drawn at 118 css px with the device pixel ratio; beside it (wrapping on a phone, gap 24) the secondary button "Unduh cashflow-companion.apk" (upload icon) and one `t-meta` line "Butuh Android 8.0 atau lebih baru. Cocokkan SHA-256 berkas di [halaman rilis]." The QR failure alert stays.
- **Perangkat terhubung:** one plain row per paired phone: title the phone's name; meta "dipasangkan {date} · terakhir aktif {date} · versi {x}"; a quiet destructive "Cabut" (`--expense` text). The empty line "Belum ada ponsel yang dipasangkan." and the load error with "Coba lagi" stay.
- **Cabut:** a centered `ShellDialog` (480): "Cabut {name}?", today's sentence, "Batal" and a destructive "Cabut perangkat". Same `DELETE /api/devices/{id}`, same notice after success, same error line.

Rejected: dropping the download section on a paired account (today shows it always).

### D7. "Status HP" box — flagged: scope

The prototype's box (`.box`, radius 12, padding 24) holds:
- the companion line (`CompanionStatus`, translated): "HP terhubung · Terakhir menangkap kemarin 16.05";
- per paired phone: an `icon phone` row "{name} · siap" (status word in `--income`) or "· perlu dicek" (`--warning`), with "Laporan terakhir {time} · aplikasi {version}"; then its checks as 32 px lines: check icon in `--income` and "beres", or alert icon in `--warning` with the problem's short fix, for "Izin baca notifikasi", "Jalan di latar belakang", "Tes tangkap notifikasi" and "Mengirim kabar" (more than 24 hours silent). A phone that never reported says "Belum pernah mengirim status."
- several phones: one block each, separated by a 1 px rule; no phone: the companion line only.

The checks come from today's `deviceProblems` and `readiness` (same `GET /api/devices` fields). The rows in "Perangkat terhubung" keep identity and actions only (D6). This replaces spec "Device Readiness in Settings"' per-row presentation; every fact stays on the page.

Rejected: the problems both in the box and in the rows (redundant); the box for the first phone only (hides a second phone's problem).

### D8. Manual API key — flagged: scope

A section "Kunci API manual (cadangan)" (open, not a disclosure), as the prototype:
- a plain row: "Kunci cadangan · {prefix}" (`t-amt`) and "dibuat {date} · terakhir dipakai {date | belum pernah}"; "Belum ada kunci" when none;
- one `t-meta` line: "Hanya untuk ponsel yang tidak bisa memindai kode QR. Ponsel yang dipasangkan lewat QR tetap terhubung.";
- secondary "Buat kunci baru (reset)" → the inline confirmation stays (sentence, "Batal", "Ya, buat kunci baru"), then the once-only box (`.box`): "Salin sekarang. Kunci ini hanya ditampilkan sekali.", the key in a read-only field, "Salin" / "Tersalin", "Tutup".
- Requests unchanged: `GET /api/auth/api-key`, `POST /api/auth/api-key/reset`. The key is never logged or stored in the page state after "Tutup".

Rejected: keeping the `<details>` (spec today; the prototype shows it open, and the section is already last on the page).

### D9. "Hubungkan ponsel" dialog

`DevicePairingDialog` moves to `ShellDialog` (520, sheet below 768), title "Hubungkan ponsel", meta "Pindai dengan aplikasi pendamping". Body, 24 apart:
1. a flex row (gap 24, wraps): the `.qr` box with the pairing QR (`qr_payload`) at 118 css px, and an ordered list: "Buka aplikasi pendamping di HP.", "Pilih Hubungkan ke web.", "Pindai kode ini. Berlaku {mm:ss}." (the countdown inside the third step, `aria-live="off"`; a polite announcement at one minute left and at expiry);
2. a `.box` "Atau ketik kode" with the code in `t-fig` and `letter-spacing: 0.12em`;
3. the expired, failed and "membuat kode" states replace the QR box with a 136 px box of the same size and a `t-meta` line; "Buat kode baru" (secondary) appears there;
4. once a phone pairs, the live checklist (`DeviceHealthChecklist`, restyled: four 32 px lines with icons and status words, no tinted fills) replaces 1 and 2, with the polite live region unchanged;
5. "Belum punya aplikasinya? Unduh aplikasi pendamping" stays a collapsed disclosure under the steps while no phone has paired.

Footer: "Selesai" (secondary) when paired or ready, "Nanti saja" before; Escape and the close button close only the dialog and return focus to "Hubungkan ponsel".

Requests and timers unchanged: `POST /api/devices/pairing-codes`, `GET /api/devices` every 3 s for at most 10 minutes, stopped on ready, timeout or close; a new code invalidates the old one.

**QR size:** the prototype's box is 136 px. A QR is readable when each module is at least 3 css px; task 3.3 measures `QRCode.create(payload).modules.size` for the longest real payload and the download URL. If either falls under 3 px per module, that QR keeps 136 px of module area (box 152) and the difference is recorded as a deviation.

Rejected: keeping the 240 px QR (fidelity); dropping the checklist (spec "Live Pairing Checklist").

### D10. Kebiasaan — flagged: scope

- **Two status rows** (`.toggle` layout, 56 px, 1 px bottom line) "Pencatatan otomatis dari kebiasaan" and "Top up otomatis", each with a status word on the right ("Aktif" / "Nonaktif") instead of a switch, and one `t-meta` line under the first: "Diatur di server. Dipakai setelah {n} jawaban yang sama, kecocokan minimal {p}%." No switch, because no endpoint changes `HABIT_AUTO_RECORD_ENABLED`; a switch that cannot switch is a false control.
- **"Kebiasaan pembayaran"** and **"Top up investasi"** sections (count aside): plain rows instead of tables. Habit row: title "{pattern} · {account}", meta "{category} · {pillar} · {n}× dikonfirmasi", status word right (today's four words from `lib/habits.ts`, coloured only as text: "Dicatat otomatis" ink, the others muted). Top-up row: title "{amount} dari {source}", meta "{product} · {n}× dikonfirmasi", same status words. "Menampilkan 100 terbaru dari setiap daftar." stays as the list's foot when 100 are shown.
- **"Seberapa sering ditanya"** (from phase 6): its content and request, restyled with a `t-fig` percentage and a `t-meta` line, as the prototype.
- Empty, loading and error states as today, with `EmptyState`, `Skeleton` and `ErrorState`.

Rejected: switches calling a new endpoint (backend change, a new setting); the prototype's "% cocok" and "Dipakai/Belajar" badges (no per-habit match percentage in `GET /api/habits`; the words are the spec's).

### D11. Tampilan

Order: "Bahasa" (the existing control, restyled as a `Segmented` radiogroup "English · Bahasa Indonesia", each option in its own `lang`; same request, cookie, refresh, focus kept, polite status), "Tema" (`Segmented` radiogroup "Terang · Gelap · Ikuti sistem", prototype order, no swatches; same stored choice and pre-paint script), then three `.toggle` rows: "Pintasan satu tombol" (switch; meta "N catat, ? bantuan, J K E T Z di Tinjau"), "Sembunyikan saldo" (switch; meta "Semua jumlah uang tampil sebagai titik"), "Bantuan & pintasan" (meta "Cara pakai dan daftar tombol"; secondary "Buka bantuan").

The switch is the prototype's `.switch` (36 × 20, `role=switch`, `aria-checked`, labelled by the row title); today's `ws-switch` wrapper row goes.

Rejected: the radio cards with swatches (prototype is a segment); the "forced theme" caption (the app has no forced-theme link).

### D12. Profil and Gaji & siklus

- **Profil:** `.form-grid`: "Nama pengguna" (read-only input "{username}"), "Nama tampilan", "Nama di notifikasi bank" (full width, hint "Pisahkan dengan koma, paling banyak 10 nama. Nama tampilan selalu ikut dipakai."); primary "Simpan profil" with the saved/unsaved status beside it. Validation messages unchanged in Indonesian, new in English. Request unchanged: `PATCH /api/auth/settings {name, name_aliases}`.
- **Gaji & siklus:** "Sumber gaji saat ini" box: the source name, "Dikenali dari nama pengirim · gajian sekitar tanggal {day}" (or from description), secondary "Periksa pemasukan dari sumber ini", and a quiet "Ganti sumber" that opens today's picker (select with paging "Lebih baru"/"Lebih lama", "Tandai sebagai gaji", "Jadikan sumber gaji") inline under it. "Perlu ditinjau" rows with "Ini gaji" / "Bukan gaji"; "Gaji terkonfirmasi" rows: title the sender or description, meta the full date and "Dikenali otomatis"/"Dikonfirmasi pemilik", the amount through phase 5's `RowAmount` (income "+Rp", `--income`), quiet "Batalkan". Requests unchanged (the ten salary calls). The intro sentence becomes the section description "Sumber gaji menentukan awal siklus."; the rule "Akhir siklus berjalan baru diketahui saat gaji berikutnya diterima." stays as the source box's meta because it explains a figure.

Rejected: dropping the picker's paging (owners with many incomes need it).

### D13. Mata uang & anggaran

`.form-stack`: "Wilayah" select (existing control and copy), "Mata uang tampilan" `Segmented` radiogroup "IDR · USD" with hint "Kurs hari ini 1 USD ≈ Rp {rate}. Data tetap tersimpan dalam rupiah." ("Kurs belum tersedia." when absent), then `.form-grid`: "Target dana darurat (bulan)" (numeric, 1–36) and "Batas pengeluaran bulanan" (money input, hint "Kosongkan untuk memakai total anggaran kategori."); primary "Simpan anggaran". Same `PATCH /api/auth/settings {currency, region, emergency_fund_multiplier, monthly_spending_budget}`, same invalidations, same validation ("Target dana darurat harus antara 1 dan 36 bulan").

Rejected: a select for the currency (prototype segment); dropping the rate (it tells the owner what USD shows).

### D14. Pemetaan notifikasi and Nama pengirim

- **Pemetaan:** a section "Nama rekening di notifikasi" with the count aside; rows: title "“{text}”", meta "→ {account} · {institution} · Dikonfirmasi pemilik|Dipetakan otomatis"; a 36 px icon button (trash) labelled "Hapus pemetaan {text}" that opens today's inline `ConfirmRemove` ("Hapus “{text}”?", "Ya, hapus", "Batal"; Escape cancels and returns focus). `GET /api/ingest/aliases`, `DELETE /api/ingest/aliases/{id}` unchanged. The intro line becomes the description "Teks di notifikasi yang menunjuk ke rekening. Menghapus tidak mengubah transaksi yang sudah tercatat."
- **Nama pengirim:** "Nama pengirim terkonfirmasi" with the count; the search field above eight names (unchanged threshold, label and count); rows: title "{name}", meta "{mask} · {institution} · {account}" and, when ambiguous, the ambiguity sentence in `--warning` text; icon buttons edit (pencil) and delete (trash). Edit opens today's inline field with "Simpan nama"/"Batal". Requests unchanged.

Rejected: a dialog for edit (today inline; the prototype draws no edit surface).

### D15. Sesi uji

`.form-grid` with "Durasi" (select, today's seven options, default 2 jam), primary "Mulai sesi" / "Akhiri sesi"; while running, one `t-meta` line "Sesi berjalan sampai {time} · {n} notifikasi disimpan, tidak dicatat" and "Lihat laporan". "Sesi sebelumnya": rows with title the start date and time, meta "{minutes} menit · {n} notifikasi", badge "Selesai", and the actions "Lihat laporan", "Unduh CSV" and the inline "Hapus data". The report rows and their labelling fields ("Nomor kasus", "Catatan", "Simpan") stay, restyled with `.form-grid`. Requests unchanged, including the 409 message and the polling intervals. The shell's test-session notice (owner decision 4) is unchanged.

### D16. Wording

- One description line per section (Appendix A), hints only where they change a decision (the alias rule, the limit's empty value, the rate, the API key's fallback role, the Android version and checksum).
- "Batas belanja operasional bulanan (IDR)" → "Batas pengeluaran bulanan"; the section description "Mata uang, dana darurat dan batas pengeluaran.".
- Status words, not tinted fills: "siap", "perlu dicek", "beres", "Aktif", "Nonaktif", "Selesai".
- Indonesian follows the neutral register (no "kamu/-mu", no slang); casual voice is not used on this page (D17).

### D17. Pairing copy — flagged: copy

The pairing dialog and checklist are fixed interface text, so they take the neutral register:
- "Buka aplikasi pendamping di HP, pilih pindai kode QR, terus arahin kamera ke kode ini." → the three steps of D9;
- "HP-mu siap! Mulai sekarang transaksi tercatat sendiri." → "HP siap. Transaksi dari notifikasi bank kini tercatat otomatis.";
- "Aku berhenti ngecek dulu. Lanjutin di aplikasi HP-mu, ya. Statusnya tetap kelihatan di daftar perangkat." → "Pengecekan dihentikan. Lanjutkan di aplikasi HP; statusnya tetap tampil di Status HP.";
- "Tenang, aplikasi di HP-mu bakal nunjukin cara benerinnya." → "Aplikasi di HP menunjukkan cara memperbaikinya.";
- "Kodenya gagal dibuat. Coba lagi, ya." → "Kode gagal dibuat. Coba lagi.";
- "Belum pernah kirim status. Buka aplikasinya di HP, ya." → "Belum pernah mengirim status. Buka aplikasinya di HP."

Spec "Live Pairing Checklist" quotes the ready line, so its scenario changes.

Rejected: keeping the casual voice as onboarding (`ui-copy-register` "Conversational Voice" allows it in onboarding steps, but the owner's 2026-11-08 feedback asks for neutral, firm labels without slang, and this dialog is also reached long after onboarding).

### D18. Hidden balances

With "Sembunyikan saldo" on:
- salary amounts, habit top-up amounts and the rate line render through `MaskedMoney` ("Rp ••••••", accessible name `common.hidden`);
- "Batas pengeluaran bulanan" shows "Rp ••••••" in a read-only field with the hint "Saldo disembunyikan. Matikan Sembunyikan saldo untuk mengubah." "Simpan anggaran" then sends the stored limit unchanged, never an empty value.

Rejected: an empty field (the prototype; saving would clear the limit); an unmasked field (leaks the figure the owner chose to hide).

### D19. Language

- Namespaces `settings.page`, `settings.nav`, `settings.devices`, `settings.pairing`, `settings.health`, `settings.apiKey`, `settings.habits`, `settings.appearance` (absorbing `settings.language`), `settings.profile`, `settings.salary`, `settings.budget` (absorbing `settings.region`), `settings.mappings`, `settings.senders`, `settings.testSession`, `settings.remove`. Existing keys keep their names or move once with a test that both catalogues have the same tree.
- `lib/deviceHealth.ts` and `lib/habits.ts` return codes; components render them. Dates and times through `lib/format.ts` (region), amounts through `useMoney`/`MaskedMoney`.
- `/settings` joins `TRANSLATED_ROUTES`; its `untranslatedPages.ts` entry goes; the copy register's translated roots gain `app/settings` and `components/settings` (and the moved `components/ui` settings files), and the copy guard's "Anda" check keeps covering them.
- The untranslated-page test example moves to whichever route is still untranslated when this phase lands (`/komponen` if every page is done). If none remains but `/komponen`, the example uses it.
- User data (phone names, mapping texts, sender names, masks, descriptions) is shown as stored, with no `lang`.

### D20. Keys, focus and URL

- Section addresses, their order and the not-found page for an unknown slug are unchanged; `/settings` renders HP & perangkat.
- After choosing a rail item, focus moves to the section title (`tabIndex=-1`); on a phone the page scrolls it below the 56 px header.
- Segments: arrow keys move and select; switches: Space toggles; rows: Tab reaches each action in visual order.
- Both dialogs trap Tab, close on Escape and return focus to their opener ("Hubungkan ponsel", the row's "Cabut").
- No new shortcut.

### D21. Loading, failure and empty states

Each list part keeps its own state: `Skeleton` rows with `aria-busy` while loading, `ErrorState` ("Coba lagi") on failure, and its empty sentence (Appendix A). A section never shows an empty list for a failure.

### D22. Shared pieces and removed elements

**Reused:** `ShellDialog`, `Rail`, `RailItem`, `RailRule`, `SectionHeader`, `Segmented`, `EmptyState`, `ErrorState`, `Skeleton`, `MaskedMoney`, `RowAmount`, `ConfirmRemove` (restyled in place), `FormField`'s label/hint/error association.

**New:** `components/settings/SettingsSection.tsx` (title, description, `.stack`), `components/settings/SettingRow.tsx` (the `.toggle` row with a switch, a status word or a button), the `.switch` button (`components/ui/Switch.tsx`) if phase 1 did not add one; CSS `.set`, `.toggle`, `.switch`, `.qr`, `.form-grid`, `.form-stack`, `.stack` where missing.

**Moved:** `DevicePairingSection`, `DevicePairingDialog`, `DeviceHealthChecklist`, `CompanionDownload`, `SalarySettingsSection` and `SenderAliasesSection` from `components/ui/` to `components/settings/` (their only users are settings), with their tests.

| Removed | Destination |
| --- | --- |
| `SettingsCard`, `SettingsSubsection`, `SettingsList` | `SettingsSection` and `SectionHeader` (D4) |
| `SettingsSectionList`, "Semua pengaturan" back link | the rail on every width (D3) |
| `money.module.css` settings rules (`.settingsPage` … `.emptyBlock`, `.themePick`, `.swatch`, `.switchRow`, `.drawerFacts`, `.habits`, `.habitTable`, `.tableCaption`) | `.set`, `.toggle`, `.form-grid`, plain rows |
| `ws-card` on settings, `ws-switch`/`ws-switch-track` where no other user remains | no card; `.switch` |
| Theme swatches | `Segmented` (D11) |
| Tinted readiness badges and checklist step fills | status words and icons (D7, D9) |
| `Modal` in `DevicePairingDialog` and the revoke dialog | `ShellDialog` (D6, D9); after this phase `Modal`'s users are Catat only, which phase 11 replaces |
| Keluar in `MoreSheet` | the rail (D5) |
| `/settings` in `untranslatedPages.ts` | `TRANSLATED_ROUTES` (D19) |
| Hard-coded `id-ID` date and IDR formatting in settings | `lib/format.ts`, `useMoney` |

Each removal is checked with `rg` (task 4.10); a remaining user is named in `verification.md` instead of deleting it.

### D23. Dependencies and order

Merged into `main` before this phase starts: `localize-server-text` (settings endpoints return `{code, params, message}`; the web renders `errors.<code>`), phase 4 `apply-markas-tinjau` (`Rail`, `RailItem`, `RailRule`, `ListRow` `stacked`), phase 5 `apply-markas-transaksi` (`RowAmount`, `ShellDialog` conventions), phase 6 `apply-markas-analisis` ("Seberapa sering ditanya" in Kebiasaan), and phases 7 to 9 (they edit the same catalogues and `untranslatedPages.ts`). If a planned contract differs when this phase starts, the phase adapts to the merged code and records each difference in `verification.md`; a missing piece stops the phase (task 1.2).

Phase 11 (`apply-markas-catat`) builds on this phase: it removes `Modal` once Catat moves off it and relies on Keluar having left `MoreSheet`.

Rejected: stacking this branch on unmerged phases (conflicts in `messages/*.json` and `globals.css`).

### D24. Verification tooling, gate and spec reconciliation

Tooling (`scripts/verify/markas/`, local only): Pengaturan scenarios for the nine sections, the pairing dialog (code shown, expired, paired with checklist, ready), the revoke dialog, the API key once-only box, Kebiasaan empty and error, Nama pengirim with search, Sesi uji running and with a report; `pengaturan-checklist.js` (SPEC §7, §8 Pengaturan and §13 values from `styles.css`); `pengaturan-keys.mjs` with nine flows: rail navigation and focus, Keluar, Language switch, theme segment, shortcuts switch, profile save, budget save with an invalid month, mapping remove and cancel, pairing open/expire/new code/close; a request recorder that logs method, path and body of every settings request in the scripted flows on `main` and on the branch.

Spec deltas:

| Spec | Requirement | Change |
| --- | --- | --- |
| `currency-conversion-and-settings` | Sectioned Settings Page | MODIFIED: rail groups, grid from 768 px, phone rail above the section |
| | Section-Scoped Settings Save | MODIFIED: "Batas pengeluaran bulanan", region in the request, masked limit |
| | Consistent Settings Form Layout | MODIFIED: no cards, saves at the foot of the form, 1 px field focus |
| | Help and Phone Status in Pengaturan | MODIFIED: "Status HP" box |
| | Sign Out in Pengaturan | ADDED |
| | Pengaturan in Both Languages | ADDED |
| | Hidden Balances in Pengaturan | ADDED |
| `device-pairing` | Pairing in Web Settings, Companion Download in Settings | MODIFIED: section name, dialog, QR box |
| `device-health` | Live Pairing Checklist, Device Readiness in Settings | MODIFIED: neutral ready line; readiness in the box |
| `web-api-key-management` | API Key Section in Settings | MODIFIED: open section |
| `habit-learning` | Kebiasaan Settings Section | MODIFIED: lists instead of tables, status rows |
| `theme-presets-switcher` | Tampilan Settings Section | MODIFIED: Bahasa first, theme segment order, help row |
| `executive-sidebar-layout` | Phone Tab Bar | MODIFIED: Lainnya without Keluar |

"Confirmed Removal and Search of Notification Names", "Device List and Revoke", "Pairing Checklist Problems" and "Pairing Checklist Announcements" keep their behaviour and wording and need no delta.

## Decisions and their defaults

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08). Every decision takes its recommended default. **Flag** marks a decision that changes money behaviour (none here) or goes beyond the phased plan's scope; it is reviewed before implementation (task 1.1).

| # | Decision | Default (taken) | Rejected alternatives | Flag |
| --- | --- | --- | --- | --- |
| D1 | Frame | `.set` 200 / ≤ 720, gap 48, frame 1040, static rail | Sticky rail; wider form | — |
| D2 | Rail | Phase 4 `Rail`, PERANGKAT / TAMPILAN / AKUN, list labels | Unnamed first group; `h2` group headings | — |
| D3 | Breakpoints | Viewport rule: grid ≥ 768, stacked rail below; no phone index | Container query; index + back link; chips | **Flag: scope** |
| D4 | Section header | `t-dtitle` + one line, `.stack` 48, no cards, saves left | Cards; right footers | — |
| D5 | Keluar | Rail button; out of `MoreSheet`; account menu keeps it | Remove from account menu; confirm dialog | **Flag: scope** |
| D6 | HP & perangkat | Status, download, devices, key; revoke on `ShellDialog` 480 | Hide download when paired | — |
| D7 | Status HP | Box with each phone's checks; rows keep identity | Problems twice; first phone only | **Flag: scope** |
| D8 | API key | Open section, inline confirm, once-only box | `<details>` | **Flag: scope** |
| D9 | Pairing dialog | `ShellDialog` 520, QR 136 box, steps, code box, checklist, disclosure | 240 QR; no checklist | — (deviation 2) |
| D10 | Kebiasaan | Status rows, two lists, moved panel restyled | Switches; "% cocok" badges | **Flag: scope** |
| D11 | Tampilan | Bahasa, Tema segment, three rows | Swatches; forced-theme caption | — |
| D12 | Profil, Gaji | Form grid; source box, picker inline, `RowAmount` | Drop paging | — |
| D13 | Mata uang & anggaran | Wilayah, IDR/USD segment with rate, grid, "Batas pengeluaran bulanan" | Currency select; drop the rate | — |
| D14 | Pemetaan, Pengirim | Plain rows, icon buttons, inline confirm and edit | Edit dialog | — |
| D15 | Sesi uji | Grid, running line, rows with actions, report kept | — | — |
| D16 | Wording | One line per section; hints that change decisions | — | — |
| D17 | Pairing copy | Neutral sentences | Keep casual onboarding voice | **Flag: copy** |
| D18 | Hidden balances | Mask amounts; limit read-only and sent unchanged | Empty field; unmasked | — |
| D19 | Language | `settings.*`; `/settings` translated | Keep the island controls only | — |
| D20 | Keys and focus | Unchanged addresses; title focus; trap and return | — | — |
| D21 | States | Per part | One page error | — |
| D22 | Shared and removed | As the table | Page-only copies | — |
| D23 | Dependencies | Server text and phases 4–9 merged first | Stacked branches | — |
| D24 | Tooling and specs | Scenarios, checklist, keys, request recorder; deltas as listed | Existing seed only | — |

No default changes a figure, a stored value, a request or a rule.

## Risks / Trade-offs

- [QR readability at 136 px] The pairing QR shrinks from 240 to 118 css px. → D9's module-size check with the longest real payload, drawn at device pixel ratio; the fallback size is recorded. The owner's real-phone scan joins the end-review list, since agents do not operate the owner's phone without asking.
- [Phone rail length] Twelve rail rows push the section below the fold at 390 px. → The prototype does this; focus and scroll move to the section title after a choice (D20). Captured for the owner.
- [Keluar one tap further on the phone] → Two taps, as the prototype; the account menu keeps it on desktop.
- [Phase contracts drift] `Rail`, `RowAmount`, the moved `QuestionRate` are planned in other changes. → D23's adapt-and-record rule and task 1.2's stop condition.
- [Copy volume] About 230 strings move to keys. → A rendered-text snapshot of the Indonesian page before and after (task 4.9) lists every changed Indonesian string, each one intended (Appendix A marks them).

## Migration and rollback

No data, schema or API change and no migration. Rollback is a revert of the merge commit; section addresses are unchanged, so bookmarks survive either way.

## Open Questions

None. Every flagged item takes its default under the standing instruction; task 1.1 records the review outcome.

## Appendix A. Copy as message keys (Indonesian · English)

Arguments arrive formatted. `{count}` selects plurals (English `one`/`other`, Indonesian `other`). Strings not listed (validation messages, load errors, salary picker states, test-session report states) move under the same namespaces with their Indonesian unchanged and English written with Appendix B; task 4.1 lists them in full. "changed" marks an Indonesian string this phase rewrites.

### `settings.page`, `settings.nav`

| Key | Indonesian | English |
| --- | --- | --- |
| `page.title` | Pengaturan | Settings |
| `nav.label` | Bagian pengaturan | Settings sections |
| `nav.groupDevices` | Perangkat | Devices |
| `nav.groupAppearance` | Tampilan | Appearance |
| `nav.groupAccount` | Akun | Account |
| `nav.signOut` | `shell.signOut` (Keluar) | Sign out |

### Section titles and descriptions (`settings.sections.<slug>.title|description`)

| Slug | Indonesian | English |
| --- | --- | --- |
| perangkat | HP & perangkat · Status HP, ponsel yang mengirim notifikasi bank dan kunci API cadangan. | Phone & devices · Phone status, the phones that send bank notifications, and the backup API key. |
| kebiasaan | Kebiasaan · Jawaban yang berulang dicatat otomatis. (changed) | Habits · Answers you repeat are recorded automatically. |
| tampilan | Tampilan · Bahasa, tema, pintasan dan saldo tersembunyi. (changed) | Appearance · Language, theme, shortcuts and hidden balances. |
| profil | Profil · Nama dan nama yang muncul di notifikasi bank. (changed) | Profile · Your name and the names that appear in bank notifications. |
| gaji | Gaji & siklus · Sumber gaji menentukan awal siklus. (changed) | Salary & cycle · The salary source sets when each cycle starts. |
| anggaran | Mata uang & anggaran · Mata uang, dana darurat dan batas pengeluaran. (changed) | Currency & budget · Currency, emergency fund and spending limit. |
| pemetaan | Pemetaan notifikasi · Teks di notifikasi yang menunjuk ke rekening. Menghapus tidak mengubah transaksi yang sudah tercatat. (changed) | Notification mapping · Notification text that points to an account. Removing one does not change recorded transactions. |
| pengirim | Nama pengirim · Nama pengirim yang sudah dikenali. Perubahan berlaku untuk transfer berikutnya. (changed) | Sender names · Sender names already recognized. Changes apply to the next transfer. |
| sesi-uji | Sesi uji · Notifikasi selama sesi disimpan untuk pengujian tanpa dicatat ke transaksi. | Test session · During a session, notifications are saved for testing and not recorded as transactions. |

### `settings.devices`, `settings.health`, `settings.apiKey`

| Key | Indonesian | English |
| --- | --- | --- |
| `devices.status` | Status HP | Phone status |
| `devices.ready` / `needsCheck` | siap · perlu dicek | ready · needs a check |
| `devices.lastReport` | Laporan terakhir {time} · aplikasi {version} | Last report {time} · app {version} |
| `devices.neverReported` (changed) | Belum pernah mengirim status. Buka aplikasinya di HP. | No status sent yet. Open the app on the phone. |
| `health.notificationAccess` | Izin baca notifikasi | Notification access |
| `health.background` | Jalan di latar belakang | Runs in the background |
| `health.selfTest` | Tes tangkap notifikasi | Notification capture test |
| `health.contact` | Mengirim kabar | Reports in |
| `health.ok` | beres | OK |
| `health.silent` | Lebih dari 24 jam tidak ada kabar dari HP ini. | No report from this phone for more than 24 hours. |
| `devices.download` | Unduh aplikasi pendamping | Download the companion app |
| `devices.downloadButton` | Unduh cashflow-companion.apk | Download cashflow-companion.apk |
| `devices.downloadNote` (changed) | Butuh Android 8.0 atau lebih baru. Cocokkan SHA-256 berkas di {link}. | Needs Android 8.0 or later. Check the file's SHA-256 on the {link}. |
| `devices.releasePage` | halaman rilis | release page |
| `devices.qrFailed` | Kode QR gagal ditampilkan. Pakai tautan unduhan. | The QR code could not be shown. Use the download link. |
| `devices.paired` | Perangkat terhubung | Connected devices |
| `devices.connect` | Hubungkan ponsel | Connect a phone |
| `devices.rowMeta` (changed) | dipasangkan {date} · terakhir aktif {date} · versi {version} | paired {date} · last active {date} · version {version} |
| `devices.revoke` | Cabut | Revoke |
| `devices.revokeTitle` | Cabut {name}? | Revoke {name}? |
| `devices.revokeBody` | Ponsel ini langsung terputus dan tidak bisa mengirim transaksi lagi sampai dipasangkan ulang. Perangkat lain tetap terhubung. | This phone is disconnected at once and cannot send transactions until it is paired again. Other devices stay connected. |
| `devices.revokeConfirm` | Cabut perangkat | Revoke device |
| `devices.empty` | Belum ada ponsel yang dipasangkan. | No phone is paired yet. |
| `apiKey.title` | Kunci API manual (cadangan) | Manual API key (backup) |
| `apiKey.row` | Kunci cadangan · {prefix} | Backup key · {prefix} |
| `apiKey.meta` | dibuat {date} · terakhir dipakai {date} | created {date} · last used {date} |
| `apiKey.note` (changed) | Hanya untuk ponsel yang tidak bisa memindai kode QR. Ponsel yang dipasangkan lewat QR tetap terhubung. | Only for a phone that cannot scan the QR code. Phones paired by QR stay connected. |
| `apiKey.new` (changed) | Buat kunci baru (reset) | Create a new key (reset) |
| `apiKey.confirm` (changed) | Ya, buat kunci baru | Yes, create a new key |
| `apiKey.once` (changed) | Salin sekarang. Kunci ini hanya ditampilkan sekali. | Copy it now. This key is shown only once. |
| `apiKey.copy` · `copied` | Salin · Tersalin | Copy · Copied |

### `settings.pairing`

| Key | Indonesian | English |
| --- | --- | --- |
| `title` · `meta` | Hubungkan ponsel · Pindai dengan aplikasi pendamping | Connect a phone · Scan with the companion app |
| `step1` · `step2` · `step3` (changed) | Buka aplikasi pendamping di HP. · Pilih Hubungkan ke web. · Pindai kode ini. Berlaku {time}. | Open the companion app on the phone. · Choose Connect to web. · Scan this code. Valid for {time}. |
| `typeCode` | Atau ketik kode | Or type the code |
| `expired` · `failed` · `creating` | Kode sudah kedaluwarsa. · Kode gagal dibuat. Coba lagi. (changed) · Membuat kode… | The code has expired. · The code could not be created. Try again. · Creating a code… |
| `newCode` | Buat kode baru | Create a new code |
| `ready` (changed) | HP siap. Transaksi dari notifikasi bank kini tercatat otomatis. | The phone is ready. Transactions from bank notifications are now recorded automatically. |
| `stopped` (changed) | Pengecekan dihentikan. Lanjutkan di aplikasi HP; statusnya tetap tampil di Status HP. | Checking stopped. Continue in the phone app; its status stays under Phone status. |
| `fixOnPhone` (changed) | Aplikasi di HP menunjukkan cara memperbaikinya. | The phone app shows how to fix it. |
| `later` · `done` | Nanti saja · Selesai | Not now · Done |
| `noApp` | Belum punya aplikasinya? Unduh aplikasi pendamping | No app yet? Download the companion app |

### `settings.habits`, `settings.appearance`

| Key | Indonesian | English |
| --- | --- | --- |
| `habits.autoRecord` | Pencatatan otomatis dari kebiasaan | Automatic recording from habits |
| `habits.autoTopup` | Top up otomatis | Automatic top-ups |
| `habits.on` · `off` | Aktif · Nonaktif | On · Off |
| `habits.rule` (changed) | Diatur di server. Dipakai setelah {count} jawaban yang sama, kecocokan minimal {pct}%. | Set on the server. Used after {count, plural, one {# matching answer} other {# matching answers}}, with at least {pct}% agreement. |
| `habits.payments` · `topups` | Kebiasaan pembayaran · Top up investasi | Payment habits · Investment top-ups |
| `habits.status.*` | Dicatat otomatis · Masih belajar · {done} dari {needed} · Jawaban berbeda, tetap ditanya · Siap, pencatatan otomatis nonaktif | Recorded automatically · Still learning · {done} of {needed} · Answers differ, still asked · Ready, automatic recording off |
| `habits.empty` | Belum ada kebiasaan yang dipelajari. | Nothing learned yet. |
| `habits.emptyLine` | Setiap jawaban di Tinjau untuk pembayaran tanpa nama toko atau top up investasi menjadi bahan belajar. | Every answer in Review for a payment without a shop name, or for an investment top-up, teaches the app. |
| `appearance.language` | `settings.language.label` (Bahasa) | Language |
| `appearance.theme` | `common.theme.label` (Tema) | Theme |
| `appearance.singleKeys` | Pintasan satu tombol | Single-key shortcuts |
| `appearance.singleKeysMeta` (changed) | N catat, ? bantuan, J K E T Z di Tinjau | N to record, ? for help, J K E T Z in Review |
| `appearance.hide` | Sembunyikan saldo | Hide balances |
| `appearance.hideMeta` (changed) | Semua jumlah uang tampil sebagai titik | Every amount shows as dots |
| `appearance.help` | Bantuan & pintasan | Help & shortcuts |
| `appearance.helpMeta` (changed) | Cara pakai dan daftar tombol | How to use the app and the key list |
| `appearance.openHelp` | Buka bantuan | Open help |

### `settings.profile`, `settings.salary`, `settings.budget`

| Key | Indonesian | English |
| --- | --- | --- |
| `profile.username` | Nama pengguna | Username |
| `profile.displayName` | Nama tampilan | Display name |
| `profile.bankNames` | Nama di notifikasi bank | Names in bank notifications |
| `profile.bankNamesHint` (changed) | Pisahkan dengan koma, paling banyak 10 nama. Nama tampilan selalu ikut dipakai. | Separate with commas, up to 10 names. Your display name is always included. |
| `profile.save` | Simpan profil | Save profile |
| `salary.source` | Sumber gaji saat ini | Current salary source |
| `salary.sourceMeta` | Dikenali dari nama pengirim · gajian sekitar tanggal {day} | Recognized by sender name · payday around day {day} |
| `salary.cycleNote` | Akhir siklus berjalan baru diketahui saat gaji berikutnya diterima. | The current cycle's end is known only when the next salary arrives. |
| `salary.check` · `change` | Periksa pemasukan dari sumber ini · Ganti sumber | Check income from this source · Change source |
| `salary.review` · `isSalary` · `notSalary` | Perlu ditinjau · Ini gaji · Bukan gaji | Needs review · This is salary · Not salary |
| `salary.confirmed` · `undo` | Gaji terkonfirmasi · Batalkan | Confirmed salaries · Undo |
| `budget.currency` | Mata uang tampilan | Display currency |
| `budget.rateHint` (changed) | Kurs hari ini 1 USD ≈ {rate}. Data tetap tersimpan dalam rupiah. | Today's rate 1 USD ≈ {rate}. Data stays stored in rupiah. |
| `budget.emergencyMonths` | Target dana darurat (bulan) | Emergency fund target (months) |
| `budget.limit` (changed) | Batas pengeluaran bulanan | Monthly spending limit |
| `budget.limitHint` (changed) | Kosongkan untuk memakai total anggaran kategori. | Leave empty to use the total of category budgets. |
| `budget.limitHidden` | Saldo disembunyikan. Matikan Sembunyikan saldo untuk mengubah. | Balances are hidden. Turn off Hide balances to change this. |
| `budget.save` | Simpan anggaran | Save budget |

### `settings.mappings`, `settings.senders`, `settings.testSession`, `settings.remove`

| Key | Indonesian | English |
| --- | --- | --- |
| `mappings.list` | Nama rekening di notifikasi | Account names in notifications |
| `mappings.owner` · `auto` | Dikonfirmasi pemilik · Dipetakan otomatis | Confirmed by you · Mapped automatically |
| `mappings.remove` | Hapus pemetaan {text} | Remove mapping {text} |
| `mappings.empty` | Belum ada pemetaan. | No mappings yet. |
| `senders.list` | Nama pengirim terkonfirmasi | Confirmed sender names |
| `senders.search` | Cari nama pengirim | Search sender names |
| `senders.matches` | {shown} dari {total} nama | {shown} of {total} names |
| `senders.ambiguous` | Nama berbeda pernah dikonfirmasi. Transfer berikutnya akan ditanyakan lagi. | A different name was confirmed before. The next transfer will be asked again. |
| `senders.edit` · `saveName` | Ubah {name} · Simpan nama | Edit {name} · Save name |
| `testSession.duration` · `start` · `end` | Durasi · Mulai sesi · Akhiri sesi | Duration · Start session · End session |
| `testSession.running` (changed) | Sesi berjalan sampai {time} · {count} notifikasi disimpan, tidak dicatat | Running until {time} · {count, plural, one {# notification} other {# notifications}} saved, not recorded |
| `testSession.previous` · `done` | Sesi sebelumnya · Selesai | Previous sessions · Finished |
| `testSession.report` · `csv` | Lihat laporan · Unduh CSV | View report · Download CSV |
| `remove.ask` · `yes` · `cancel` | Hapus {subject}? · Ya, hapus · Batal | Remove {subject}? · Yes, remove · Cancel |

## Appendix B. Glossary (Indonesian → English)

Consistent with `add-language-system` Appendix A, the credit change's D14 glossary and phases 4 to 9.

| Indonesian | English | Note |
| --- | --- | --- |
| Pengaturan | Settings | `nav.settings` |
| HP · ponsel | phone | never "handset", "device" only for the list |
| Perangkat terhubung | Connected devices | |
| Aplikasi pendamping | Companion app | |
| Hubungkan ponsel · Pasangkan | Connect a phone · Pair | |
| Cabut | Revoke | not "Disconnect" (the phone may still be on) |
| Kunci API manual (cadangan) | Manual API key (backup) | |
| Kebiasaan | Habits | |
| Top up | Top-up (noun), top up (verb) | |
| Tampilan | Appearance | |
| Bahasa · Wilayah | Language · Region | `settings.language`, `settings.region` |
| Tema · Terang · Gelap · Ikuti sistem | Theme · Light · Dark · Match system | `common.theme` |
| Pintasan satu tombol | Single-key shortcuts | |
| Sembunyikan saldo | Hide balances | `shell.hideBalances` |
| Bantuan & pintasan | Help & shortcuts | `shell.help.title` |
| Profil · Nama tampilan · Nama pengguna | Profile · Display name · Username | |
| Gaji & siklus · Sumber gaji | Salary & cycle · Salary source | |
| Mata uang & anggaran | Currency & budget | |
| Target dana darurat | Emergency fund target | |
| Batas pengeluaran bulanan | Monthly spending limit | never "operational", never "expenses" |
| Pemetaan notifikasi | Notification mapping | |
| Nama pengirim | Sender name | |
| Sesi uji | Test session | |
| Keluar | Sign out | `shell.signOut`; never a figure |
| siap · perlu dicek · beres | ready · needs a check · OK | status words |
| Aktif · Nonaktif | On · Off | settings states |
