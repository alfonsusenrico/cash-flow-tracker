# ui-copy-register Specification

## Purpose
Keep the web interface's own words neutral and consistent while the assistant keeps its friendly voice in conversation.

## Requirements

### Requirement: Neutral Interface Labels
Everything the web interface names or labels SHALL be worded in neutral standard Indonesian: navigation, titles, headings, tabs, folders, chips, buttons, form labels, placeholders, table headers, status pills, figure names, captions, toasts, errors, empty states and figure explanations. That text SHALL use "sudah" and "tidak", SHALL NOT use slang such as "nggak", "udah", "beres", "nangkep", "bikin" or "aja", and SHALL NOT address the user with "kamu" or "-mu".

#### Scenario: Labels in the neutral register
- **WHEN** the owner opens Beranda, Tinjau, Transaksi or the Catat dialog
- **THEN** headings, buttons, chips, status pills, toasts and empty states SHALL read, for example, "Perlu tindakan", "Perlu ditinjau", "Semua sudah ditinjau", "Sudah dicek", "HP terhubung" and "Tidak ada yang perlu dijawab", and none SHALL contain a listed slang token

#### Scenario: Regression guard
- **WHEN** the frontend test suite runs
- **THEN** it SHALL fail if a listed slang token appears in a string of the label-type sources outside the conversational allowlist

### Requirement: Conversational Voice
The casual assistant voice SHALL be allowed only where the app speaks in a conversation: the connect-phone and onboarding guide prose, AI, purpose and learned-suggestion questions, the Tanya chat, and the written Beranda summary sentence.

#### Scenario: Conversation keeps its voice
- **WHEN** Tinjau shows a learned top-up suggestion from the server
- **THEN** its question text SHALL be shown as the server wrote it, for example "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?"

#### Scenario: Written summary keeps its voice
- **WHEN** Beranda shows its summary sentence
- **THEN** the sentence MAY address the owner casually, for example "3 hal nunggu jawabanmu", while the figure labels beside it stay neutral

### Requirement: User Data Is Shown As Named
Names the user gave, such as category, account and merchant names, SHALL be shown exactly as entered in every position, whatever their register.

#### Scenario: User data untouched
- **GIVEN** a category the owner named "Jajan / ngopi"
- **WHEN** it is shown as a choice label in Tinjau
- **THEN** it SHALL read "Jajan / ngopi"
