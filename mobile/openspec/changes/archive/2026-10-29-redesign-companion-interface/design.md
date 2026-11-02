## Context

See `proposal.md`. The base is `origin/main` at `8425f68` (1.8.3, code 18, Room 7). The approved
target is `research/prototypes/2026-10-29-mobile-ui-options/src/option-final.html` with its README
"Final direction" table, in the main checkout's untracked `research/` folder. Facts used here:

- Before 2.0 the UI was `ui/inbox` (a feed with a stats bar, per-app chips and a sync FAB, and a
  `ModalBottomSheet` holding every action and every question type), `ui/settings/SettingsScreen` and
  `ui/onboarding/PermissionScreen`, all in fixed dark colours.
- `InboxViewModel` held every write path (edit, manual record, send, retry, resend, delete with undo,
  clear ignored, all question answers, source-pocket options, sync now). Its rules and messages are
  kept; only the sheet-closing side effects go.
- Recorded results carry `type`, `description`, `source`, `target`, `category` and `amount` once
  `completeRecordedResult` holds. Nothing on the phone knows a category's Kakeibo pillar or an
  account balance.
- Notifications open the app with `MappingConfirmationNotifier.EXTRA_ROW_ID`.

## Goals / Non-Goals

**Goals:** the approved look and navigation; every existing action reachable; figures that are
truthful about where they come from; WCAG 2.2 AA in both themes; an in-place upgrade from 1.8.3.

**Non-Goals:** QR pairing (phase 2), AI capture and the Catat button (phase 3), any change to
capture, sync, completion, question delivery or system notifications, any Room schema change.

## Decisions

### D1. Tokens and type

`CompanionColors` holds the E palette for light and dark exactly as in the prototype `:root` blocks,
with one change: dark `switchOff` is `#6F737B` instead of `#5F636B`, because the unchecked switch
reached only 2.9:1 against the tile. Material 3's colour scheme is mapped from the tokens so dialogs,
fields and switches follow them. `ContrastTest` computes every pair the screens draw.

Archivo is bundled as `res/font/archivo.ttf`, the variable `Archivo[wdth,wght].ttf` (version 2.001,
SHA-256 `0e094a7d3c7c4c25cf1310c4b30014f1dae9332220b1c2c88f4fa996f0b05053`) from `google/fonts` at
commit `95f4904fc8bcf26d3420fe315560c96417c6dec7` (`ofl/archivo`, upstream Omnibus-Type/Archivo
`b5d63988`), with its OFL in `assets/licenses/archivo-OFL.txt`. Text uses width 100 at weights
400–750; display text uses weight 700 at width 104 through `FontVariation`, with the README's sizes
(totals 34, question count 38, counts 30, question titles 21, amounts 28–32, onboarding titles 25,
page titles 18–20). Money, counts and times use `tnum`.

Rejected: Google Fonts downloadable fonts (needs Play services and a network at first run) and
static instances (no width axis, several files).

### D2. Theme choice

`ThemeMode` (`system`, `light`, `dark`) is stored in the existing preferences as `theme_mode`. It is
absent until the owner chooses, and an unknown value reads as `system`. `CompanionViewModel` exposes
it; `MainActivity` applies it and sets the system bar icons with `enableEdgeToEdge`. The window
background has light and night values so the first frame matches.

Rejected: DataStore (a new dependency for one value) and Room (a schema change).

### D3. Navigation

A `NavHost` with `home`, `activity`, `questions?row=`, `settings` and `detail/{row}`. The bottom bar
shows on the four top-level routes; tabs keep their state (`saveState`/`restoreState`). The bar takes
an optional centre action, laid out between the second and third tab, which phase 3 fills with Catat;
phase 1 passes none. Transitions are 150 ms fades, none when `ANIMATOR_DURATION_SCALE` is 0.

A notification's row opens Tanya when the row carries a question of any kind, answered or not, and
the detail page otherwise. A question opened from Beranda, Aktivitas, Rincian or a notification
becomes the Tanya tab's page (the other tab's stack is saved first), so returning to Aktivitas shows
the feed rather than a question pushed on top of it; the device check found that case.

### D4. One open-question rule for Tanya, its count and "Perlu dijawab"

`OpenQuestions` decides the kind from the stored result with the existing parsers (purpose, sender,
source pocket, mapping proposal) and whether it still waits for the owner: purpose rows come from
the existing `getNeedsAnswerFlow`; sender rows wait until a reply other than `invalid` exists;
pocket rows until a reply other than `invalid` or `stale` exists, the same states in which the
controls appear; mapping rows until the server settles them. Tanya pages through this list newest
first; the tab count, the home tile and the "Perlu dijawab" filter use the same list.

This widens "Perlu dijawab" from purpose rows to every question type, as the prototype does.
"Perlu cek" keeps its 1.8.3 rule, so a confirmation row appears under both.

### D5. Home figures

`HomeFigures` sums only rows whose stored result is a complete recorded result without an error:
expense as out, income as in, internal movement and investment as moved. Rows still waiting today
are counted ("Menunggu 3 notifikasi") rather than summed, because their amounts are the phone's own
reading. The three-day strip is today and the two days before in the phone's time zone; categories
are the tracker's category names of recorded expenses in those days. The today tile says "Dari
notifikasi di ponsel ini" and the page says the figures are not tracker balances. "Perlu cek" says
"Belum tuntas di tracker" instead of the prototype's "Gagal dicatat otomatis", because the filter
also holds unsent and processing rows.

### D6. Detail page from the sheet

`RowAbilities` carries the sheet's rules unchanged: edit only a recorded expense or income of the
current pairing with a valid transaction ID; manual record only for an unresolved row of the
current pairing that is not a pocket move; send only for an unsent row. One change: rows awaiting a
confirmation no longer offer "Coba Proses Ulang" and "Catat manual". Those buttons existed in the
sheet, but `retryNotification` and `resolveNotification` accept only `needs_review` and `failed`, so
both always failed; such rows now show their question and link to Tanya.

`TransactionForm` is the sheet's form inline: the same defaults, pickers, validation messages and
write paths. Successful writes close the form; the page stays and reflects the stored row through
`NotificationDao.observeById`. Deleting from the page goes back first, then shows the undo
snackbar.

### D7. Messages

Toasts become one shell snackbar with a dismiss action, so results are announced in the app and
delete offers "Urungkan" wherever it starts. The message texts are the sheet's.

### D8. Settings

The health tiles show the listener state, last sync, failed stores and drops; every other listener
figure stays under "Rincian pendengar". "Sinkron sekarang" uses the feed's former sync-now path,
which also refreshes stored results. The connection section keeps 1.8.3's fields and behaviour;
saving now also reloads accounts and categories. Android settings links fall back to the general
battery page, or say the page is missing, instead of failing silently.

### D9. Accessibility

Tiles and feed rows are single merged controls with a spoken summary; the feed row carries the
"Hapus dari aplikasi" action itself. Headings are marked; badges pair an icon with text; icon-only
buttons have descriptions; targets are at least 48 dp (options 56 dp). At font scale 1.5 and above,
Beranda, the health tiles, answer options and the theme choice switch to one column, the day bars put
their value under the day, and the app tile of a quoted notification grows with its initials.

### D10. Metadata requests need credentials

`loadMetadata` now returns without a request when no API key is set. 1.8.3 sent an unauthenticated
request to the default server on every start of an unpaired phone, which the server refused.

### D11. A restored row is not swiped away again

The device check found that "Urungkan" after a swipe restored the row and deleted it again at once:
the swipe state was saveable, the list keys rows by ID, so the restored row came back with its
saved dismissed state and fired the delete again. 1.8.3 used the same component. The state is now
plain `remember`, so a restored row starts settled. A delete through the TalkBack action never set
that state, which is why only swipes were affected.

## Data, API and failure behaviour

- No API change. No Room change: two read queries (`observeById`, `getHiddenDuplicateCountFlow`); two
  unused count queries are removed. One preference key, `theme_mode`.
- A row deleted while its page is open shows "Item tidak ditemukan".
- A missing Android settings page is reported in the snackbar.

## Migration and rollback

Installing 2.0.0 over 1.8.3 keeps the database (still version 7) and preferences; nothing is
migrated. The theme follows the system until chosen. Rollback is installing the 1.8.3 APK over 2.0.0
with `adb install -r -d`; the database version is unchanged and 1.8.3 ignores `theme_mode`.

## Risks / Trade-offs

- A confirmation row is counted under both "Perlu dijawab" and "Perlu cek".
- The dark day bars (2.1:1) are below 3:1; each bar's value is printed next to it.
- The detail page is longer than the sheet; the state box keeps the action at the top.

## Open decisions for owner

1. **Per-app filters.** Kept after the five status filters, because removing them would drop an
   existing feature. The prototype shows only the five; they can be removed if not used.
2. **"Perlu cek" with questions.** Confirmation rows also stay in "Perlu cek" (1.8.3 rule). They
   could be left only under "Perlu dijawab".
3. **Undo label.** The snackbar says "Urungkan" as in the prototype; 1.8.3 said "Batalkan".
4. **Button labels kept from 1.8.3 where specs name them:** "Konfirmasi nama" and "Catat tanpa nama"
   instead of the prototype's "Balas nama" and "Tanpa nama"; "Kirim Notifikasi ke Tracker" for the
   send form.
5. **Prototype items not built in phase 1:** "Lihat contoh notifikasi" and "Ulangi pengaturan awal"
   in Atur; the onboarding step numerals (phase 2 rebuilds setup).
