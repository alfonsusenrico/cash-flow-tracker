# Spec Delta

## ADDED Requirements

### Requirement: Tanya Page
The web app SHALL provide Tanya at `/tanya`: a greeting with a lead line, suggestion cards, a conversation thread and a composer where the owner types a transaction or a question. The greeting SHALL use the owner's name when one is known and the time of day on the device. The lead line SHALL show today's safe-to-spend figure and the number of items waiting, or the remaining amount when safe-to-spend is unavailable.

#### Scenario: Opening Tanya
- **WHEN** a signed-in owner with an available safe-to-spend figure of Rp 232.000 and 3 waiting items opens `/tanya` at 15.10
- **THEN** the page SHALL show "Sore, {name}." with a lead line naming Rp 232.000 and 3 waiting items, four suggestion cards and an empty thread, and the composer SHALL be ready for input

#### Scenario: Safe-to-spend unavailable
- **WHEN** the cycle summary reports safe-to-spend as unavailable
- **THEN** the lead line SHALL name the remaining amount and SHALL NOT show Rp 0 as the safe figure

### Requirement: Deterministic Question Answers
Tanya SHALL answer a fixed set of question kinds by matching the typed text against a deterministic table of Indonesian and English phrases. The kinds are cycle summary, waiting items, upcoming bills, Kakeibo, the latest report and help. Each matched question SHALL be answered with a card built only from figures the server returned. The same text SHALL always give the same kind of answer.

#### Scenario: Cycle question in Indonesian
- **WHEN** the owner sends "aman hari ini berapa?"
- **THEN** Tanya SHALL reply with a cycle card showing Aman dipakai hari ini, Keluar, Masuk, Kekayaan bersih and Menunggu jawaban, each equal to the value in `GET /api/dashboard/cycle-summary`

#### Scenario: Same question in English
- **WHEN** the owner sends "how much is safe to spend today"
- **THEN** Tanya SHALL reply with the same cycle card

#### Scenario: Bills this week
- **WHEN** the owner sends "tagihan minggu ini apa aja?"
- **THEN** Tanya SHALL list only the scheduled plans due within 7 days of today, with their sum, SHALL say that goal set-asides are counted per period, and SHALL show "Tidak ada tagihan atau setoran dalam periode ini." when there are none

#### Scenario: Kakeibo question
- **WHEN** the owner sends "kakeibo siklus ini"
- **THEN** Tanya SHALL show the Keluar lines per Kakeibo bucket, including Belum diklasifikasi, whose sum equals the cycle summary's Keluar total

#### Scenario: Waiting items
- **WHEN** the owner sends "apa yang nunggu jawaban?" while the Tinjau badge shows 9
- **THEN** Tanya SHALL show "9 perlu ditinjau" with up to three items, each linking to its exact Tinjau item, and SHALL NOT answer the items inside Tanya

### Requirement: Waiting Money Stays Counted
Any Tanya card that mentions items waiting in Tinjau SHALL describe their money as recorded and already inside the totals, as Beranda does. It SHALL NOT describe that money as not yet counted.

#### Scenario: Waiting line in the cycle card
- **WHEN** 3 items totalling Rp 357.000 wait in Tinjau
- **THEN** the cycle card SHALL label them "Menunggu jawaban" with Rp 357.000, SHALL name the part already inside Keluar as the cycle summary reports it, and SHALL NOT say "belum dihitung"

### Requirement: Unknown Text Gets Choices, Not Guesses
When the typed text matches no question kind and is not a recordable transaction, Tanya SHALL reply that it cannot answer that yet and offer the suggestion chips. It SHALL NOT guess an answer, record anything or send the text anywhere other than the deterministic capture preview.

#### Scenario: Unknown question
- **WHEN** the owner sends "kenapa harga emas naik?"
- **THEN** Tanya SHALL reply with the cannot-answer message and the suggestion chips, no transaction SHALL be created, and the only request made for that text SHALL be the parse-only capture preview

#### Scenario: Greeting only
- **WHEN** the owner sends "halo"
- **THEN** Tanya SHALL reply with the same message and chips

### Requirement: Recording From the Composer
When the sent text matches no question kind and the server's parse-only capture preview finds exactly one amount, Tanya SHALL record it through the existing text capture with a client capture id generated once for that message. It SHALL then show a card with the description, amount, time, account and category, or "Masuk Tinjau" when the row waits for an answer.

#### Scenario: Recording a typed expense
- **WHEN** the owner sends "parkir 5rb jam 2 tadi" at 14.03
- **THEN** one transaction of Rp 5.000 at 14.00 SHALL be recorded through `POST /api/capture/text`, and Tanya SHALL show its card with "Dicatat", "Urungkan" while the capture's undo window is open, and "Ubah"

#### Scenario: Resending after a network error
- **GIVEN** a sent capture whose request failed on the network
- **WHEN** the owner chooses "Kirim ulang"
- **THEN** the request SHALL reuse the same client capture id, so at most one transaction exists

#### Scenario: Question with a number
- **WHEN** the owner sends "berapa keluar 2 hari ini?"
- **THEN** nothing SHALL be recorded, and Tanya SHALL answer it as a question or as unknown text

#### Scenario: Recording mixed text on purpose
- **WHEN** the owner sends "tagihan listrik 200rb", sees the bills card, and chooses "Catat sebagai transaksi"
- **THEN** the same text SHALL go through the capture path once

### Requirement: Amounts Are Never Guessed
Tanya SHALL take an amount only from the server's capture parser and SHALL NOT record anything when the parser reports a missing amount or several amounts. It SHALL ask for one amount when there are several, and for the amount when the parser read a transaction without one (a time phrase, an account or a category). Other text without an amount SHALL get the unknown reply, which says that a transaction needs its amount.

#### Scenario: No amount
- **WHEN** the owner sends "parkir tadi"
- **THEN** Tanya SHALL ask for the amount with an example, and no capture SHALL be submitted

#### Scenario: No amount and nothing read as a transaction
- **WHEN** the owner sends "halo" and the parser finds no amount, time, account or category
- **THEN** Tanya SHALL give the unknown reply with the chips and its line on recording with an amount, and no capture SHALL be submitted

#### Scenario: Several amounts
- **WHEN** the owner sends "kopi 18rb roti 12rb"
- **THEN** Tanya SHALL ask for one amount, and no capture SHALL be submitted

### Requirement: Capture Follow-up and Undo in the Thread
A capture that needs a detail SHALL show the server's question in the thread with its options, and an answer SHALL be sent with a fresh reply id. A recorded capture SHALL offer "Urungkan" only inside the capture's undo window. "Ubah" SHALL open the shared transaction drawer. Recording, answering and undoing SHALL refresh the figures shown elsewhere.

#### Scenario: Answering a follow-up
- **WHEN** a capture returns `needs_detail` with an account question and the owner picks "Tunai"
- **THEN** `POST /api/capture/{id}/answer` SHALL be sent with a fresh reply id, and the card SHALL update to the recorded state

#### Scenario: Undo inside the window
- **WHEN** the owner chooses "Urungkan" before `undo_until`
- **THEN** `POST /api/capture/{id}/undo` SHALL reverse the capture, the card SHALL say it was cancelled, focus SHALL return to the composer, and Beranda's figures SHALL no longer include the row

#### Scenario: Undo refused
- **WHEN** the server answers the undo with 409 or 410
- **THEN** Tanya SHALL show the server's reason as a notice and keep the card's recorded state

### Requirement: Conversation Kept Only in Memory
The conversation SHALL live only in the open tab's memory, capped at 100 messages, oldest first out. It SHALL be empty after a reload, after signing out and whenever a different user is signed in. It SHALL NOT be written to browser storage, cookies, the URL, analytics or logs. "Hapus percakapan" SHALL empty it after confirmation.

#### Scenario: Reload
- **WHEN** the owner reloads `/tanya` after asking two questions
- **THEN** the thread SHALL be empty

#### Scenario: Navigating away and back
- **WHEN** the owner opens Beranda from Tanya and returns through the nav without reloading
- **THEN** the earlier messages SHALL still be shown

#### Scenario: Another user on the same browser
- **GIVEN** the owner asked questions and then a different user signs in on the same browser
- **WHEN** the second user opens `/tanya`
- **THEN** none of the first user's messages or figures SHALL be shown

#### Scenario: Nothing persisted
- **WHEN** a full scripted session of questions and a capture runs
- **THEN** no write to `localStorage`, `sessionStorage`, IndexedDB or cookies SHALL carry any conversation content

### Requirement: No Model Calls From Tanya
Tanya SHALL answer without any language model. The browser SHALL send requests only to the app's own `/api` endpoints for the cycle summary, review summary and queue, the latest report, the signed-in user and the capture endpoints, and to no model provider or other host. Recording keeps the text capture's existing server-side behaviour unchanged.

#### Scenario: Whole session request log
- **WHEN** a scripted session sends every question kind, unknown text, a capture with a follow-up and an undo
- **THEN** every request SHALL be same-origin under `/api/` and on that list, and none SHALL go to a model provider or another host

### Requirement: Owner-Scoped Answers
Every figure and item in Tanya SHALL come from the signed-in user's own data through the session-authenticated API. A signed-out session SHALL be sent to sign-in.

#### Scenario: Two owners
- **GIVEN** two users with different cycle figures
- **WHEN** each signs in and asks "ringkasan siklus ini"
- **THEN** each SHALL see only their own figures

#### Scenario: Session expired
- **WHEN** a card's request is answered with 401
- **THEN** the app SHALL redirect to sign-in as on every other page, and the conversation SHALL be gone

### Requirement: Failures Without False Figures
When a figure cannot be loaded, Tanya SHALL show that it could not be loaded with a retry, and SHALL NOT show 0 or a stale figure as if current. Each card SHALL show the time its figures were taken and offer to refresh them.

#### Scenario: Cycle summary fails
- **WHEN** `GET /api/dashboard/cycle-summary` fails while the owner asks "ringkasan siklus ini"
- **THEN** the reply SHALL show "Data ini tidak dapat dimuat." with "Coba lagi", and no amount SHALL be shown

#### Scenario: Refreshing an old card
- **WHEN** the owner chooses "Perbarui" on a cycle card stamped "Per 14.03" after recording a new expense
- **THEN** the card SHALL be rebuilt in place from a fresh request with the new figures and a new stamp

#### Scenario: Capture rate limit
- **WHEN** the capture preview or recording answers 429 `capture_rate_limited`
- **THEN** Tanya SHALL say there were too many entries in one minute, record nothing, and keep the typed text in the composer

### Requirement: Accessible Conversation
The thread SHALL be a polite live region whose new replies are announced with a short summary of their card. Each message SHALL carry a speaker and time label. After sending, focus SHALL stay in the composer. Enter SHALL send and Shift+Enter SHALL add a line. The page SHALL meet WCAG 2.2 AA at 1440, 1280, 1024, 768 and 390 px in light and dark.

#### Scenario: Screen-reader announcement
- **WHEN** the owner sends "ringkasan siklus ini"
- **THEN** the reply SHALL be added inside the `log` region and announced with a one-sentence summary of its figures, and focus SHALL remain in the composer

#### Scenario: Keyboard only
- **WHEN** a keyboard user types a question, presses Enter, then Shift+Tab
- **THEN** the message SHALL be sent, and focus SHALL reach the newest reply's buttons with a visible focus indicator

#### Scenario: Keys in the help
- **WHEN** the owner opens the help on `/tanya`
- **THEN** it SHALL list "Di Tanya" with Enter to send and Shift+Enter for a new line

#### Scenario: Zoom and reflow
- **WHEN** the page is shown at 400% zoom or 320 CSS px wide
- **THEN** the thread, cards and composer SHALL reflow into one column without horizontal page scrolling

### Requirement: Copy Register in Tanya
What the assistant says in the conversation MAY use the casual assistant voice. The page title, buttons, suggestion and chip labels, the composer label and placeholder, card headings, figure names, status pills, speaker labels, notices and errors SHALL use neutral standard Indonesian, as `ui-copy-register` requires. User data SHALL be shown as entered.

#### Scenario: Labels stay neutral
- **WHEN** the owner opens Tanya and receives a capture card and an error notice
- **THEN** the placeholder, suggestion labels, card headings, pills, speaker labels and the notice SHALL contain no listed slang token and SHALL NOT address the owner as "kamu"

#### Scenario: Assistant voice
- **WHEN** Tanya cannot find an amount in "parkir tadi"
- **THEN** the assistant's reply MAY read "Jumlahnya berapa? Aku nggak nebak. Tulis juga angkanya, misalnya \"parkir 5rb jam 2\"."

### Requirement: Phone Composition
At 760 px and below Tanya SHALL show a compact greeting, the suggestions as one horizontally scrolling row, the thread and the composer kept above the phone tab bar. The last message and any focused control SHALL never be hidden behind the composer or the tab bar, and the page SHALL NOT scroll horizontally.

#### Scenario: Tanya at 390 px
- **WHEN** the owner opens Tanya at 390 px and sends three messages
- **THEN** the composer SHALL sit above the tab bar, the newest message SHALL be fully visible above the composer, and the page width SHALL not exceed the viewport
