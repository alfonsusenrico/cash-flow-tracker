# Proposal

## Approval

Owner approved the companion redesign plan with defaults on 2026-10-29
(research/notes/2026-10-29-companion-redesign-plan.md); coordinator-reviewed.

This change is phase 1 of that plan: the redesigned app shell, version 2.0.0 (code 19).

## Why

The owner finds the current companion hard to read: one feed of log-like cards, a bottom sheet that
holds every action, and fixed dark colours. The owner chose a new direction on 2026-10-29
(`research/notes/2026-10-29-mobile-ui-direction.md`, prototype `option-final.html`): option E's colours
and bento home with quieter titles, option A's detail page, and option E's paged questions with the
real notification shown first. Every existing feature has to stay.

## Expected Outcome

- The owner sees at a glance what this phone recorded today, what needs an answer, and whether
  capture and sync are healthy.
- Questions of every type are answered one at a time, with the original notification text above
  each question.
- Every existing action still works: statuses and filters, delete with undo, clear ignored, resend,
  retry, manual record, edit, every question type, app toggles, auto-sync, sync now and listener
  health. System notifications are unchanged.
- The app follows the system theme until the owner picks one, and meets WCAG 2.2 AA contrast in both
  themes.
- Installing 2.0.0 over 1.8.3 keeps every row, the pairing and the pairing context.

## What Changes

- **Visual system:** E colour tokens for light and dark, the bundled Archivo variable font (OFL), and
  the quieter title sizes and weights from the prototype README.
- **Theme:** follows the system until the owner chooses Terang or Gelap under Atur → Tampilan; the
  choice is stored in the app preferences.
- **Navigation:** a bottom bar with Beranda, Aktivitas, Tanya (with the open-question count) and
  Atur. The bar keeps a centre slot free for Catat, which phase 3 adds.
- **Beranda:** the E bento with "Keluar tercatat hari ini" and a three-day strip, questions, sync
  health, top categories over three days, perlu cek and di ponsel. Every figure comes from this
  phone's own rows and says so.
- **Aktivitas:** the day-grouped feed with the filters Semua, Perlu dijawab, Perlu cek, Di ponsel and
  Diabaikan (the per-app filters stay after them), delete with undo, "Hapus semua" for ignored items
  behind a confirmation, and the duplicates note.
- **Rincian (detail):** a full page replaces the bottom sheet: back and delete, hero, a state box with
  its action, "Yang dicatat tracker" with "Ubah transaksi" and the inline form, the original
  notification, and "Hapus dari aplikasi". A row with an open question links to Tanya.
- **Tanya:** a paged view of every open question (purpose, sender, Jago source pocket, account
  mapping), the original notification first, then the question card; pager and closed state.
  "Perlu dijawab" and the Tanya count cover every question type.
- **Atur:** health tiles, auto-sync and sync now, monitored apps, connection (fields and behaviour
  unchanged until phase 2), Android settings with the HiOS note, and Tampilan.
- **Onboarding:** the permission screen is restyled; its flow is unchanged.
- Tapping a question notification opens that question in Tanya; other rows open in Rincian.
- **Removed:** the inbox screen and the detail bottom sheet, whose logic moves to the new screens.
- Version 2.0.0 (code 19).

## Non-goals

- QR pairing, the device name and the connection rebuild (phase 2).
- AI capture and the Catat button (phase 3).
- Any change to notification capture, parsing, filtering, sync, completion or question delivery.
- Any change to system notifications or their actions.
- Any Room schema change or backend change.

## Capabilities

### New Capabilities

- `companion-interface`: navigation, home figures from the phone's own rows, the theme choice, the
  activity feed and detail page, and accessibility of the companion's screens.

### Modified Capabilities

- `mobile-purpose-questions`: purpose questions are answered in Tanya instead of the detail sheet;
  "Perlu dijawab" lists every open question type; a question notification opens Tanya.
- `mobile-sender-replies`: sender questions are answered in Tanya; a sender notification opens Tanya;
  account-selection questions keep their controls in Tanya.
- `mobile-jago-pocket-confirmation`: the pocket chooser opens in Tanya.
- `notification-capture`: kept notifications are listed under "Di ponsel"; the send and edit forms
  live on the detail page.

## Impact

- Code: `MainActivity`, a new `ui/shell` (navigation, bottom bar, view model), new screens under
  `ui/home`, `ui/activity`, `ui/detail` and `ui/questions`, restyled `ui/settings` and
  `ui/onboarding`, a new `ui/theme`, and shared `ui/components`. `ui/inbox` is removed.
- Resources: `res/font/archivo.ttf` and its licence; light and dark window themes.
- Preferences: one new key, `theme_mode`. Room stays at version 7: two new read queries, no schema
  change.
- No API contract change and no backend change. The owner installs 2.0.0 over 1.8.3.
