## MODIFIED Requirements

### Requirement: Neutral Interface Labels
In the Indonesian interface, everything the web names or labels SHALL be worded in neutral standard Indonesian: navigation, titles, headings, tabs, folders, chips, buttons, form labels, placeholders, table headers, status pills, figure names, captions, toasts, errors, empty states and figure explanations. That text SHALL use "sudah" and "tidak", SHALL NOT use slang such as "nggak", "udah", "beres", "nangkep", "bikin" or "aja", and SHALL NOT address the user with "kamu" or "-mu".

#### Scenario: Labels in the neutral register
- **WHEN** the owner opens Beranda, Tinjau, Transaksi or the Catat dialog in Indonesian
- **THEN** headings, buttons, chips, status pills, toasts and empty states SHALL read, for example, "Perlu tindakan", "Perlu ditinjau", "Semua sudah ditinjau", "Sudah dicek", "HP terhubung" and "Tidak ada yang perlu dijawab", and none SHALL contain a listed slang token

#### Scenario: Regression guard
- **WHEN** the frontend test suite runs
- **THEN** it SHALL fail if a listed slang token appears in the Indonesian catalogue outside the conversation namespace, or in a string of the label-type sources not yet moved to the catalogue

### Requirement: Conversational Voice
The casual assistant voice SHALL be allowed, in either language, only where the app speaks in a conversation: the connect-phone and onboarding guide prose, AI, purpose and learned-suggestion questions, the Tanya chat, and the written Beranda summary sentence. In English it MAY say "you" and use contractions, and SHALL still use no slang.

#### Scenario: Conversation keeps its voice
- **WHEN** Tinjau shows a learned top-up suggestion from the server
- **THEN** its question text SHALL be shown as the server wrote it, for example "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?" or "Looks like a top-up to Reksa Dana A, like last month?"

#### Scenario: Written summary keeps its voice
- **WHEN** Beranda shows its summary sentence
- **THEN** the sentence MAY address the owner casually, for example "3 hal nunggu jawabanmu", while the figure labels beside it stay neutral

#### Scenario: Conversation lives in its namespace
- **WHEN** a catalogue message outside the conversation namespace says "you" in English or uses a listed Indonesian slang token
- **THEN** the copy guard SHALL fail and name the key

### Requirement: User Data Is Shown As Named
Names the user gave, such as category, account and merchant names, SHALL be shown exactly as entered in every position, whatever their register and whatever the interface language.

#### Scenario: User data untouched
- **GIVEN** a category the owner named "Jajan / ngopi"
- **WHEN** it is shown as a choice label in Tinjau
- **THEN** it SHALL read "Jajan / ngopi"

#### Scenario: User data in the English interface
- **GIVEN** the same category
- **WHEN** the English Beranda lists a transaction in it
- **THEN** it SHALL read "Jajan / ngopi", and the copy guard SHALL NOT inspect it

### Requirement: Spending Terms
Every page SHALL name spending figures with the same terms. Indonesian: "Pengeluaran" for spending without savings, "Ditabung & investasi" for the saving bucket, "Masuk" for income, "Aman dipakai sampai gajian" for the period's remainder. English: "Spending", "Saved & invested", "Income", "Safe to spend until payday". "Keluar", "Belanja", "Expenses", "Spend", "Spent" and "Outflow" SHALL NOT be figure labels. Direction words in row text SHALL stay allowed.

#### Scenario: Same figure, same name
- **WHEN** Beranda, Transaksi, Analisis and Laporan show the period's spending without savings
- **THEN** each SHALL label it "Pengeluaran" in Indonesian and "Spending" in English, and no figure on these pages SHALL be labelled "Keluar", "Belanja", "Expenses" or "Spent"

#### Scenario: Copy guard
- **WHEN** a change labels a figure "Keluar", "Belanja", "Expenses" or "Spent" in a catalogue or a component the copy guard covers
- **THEN** `copyRegister.test.ts` SHALL fail and name the key or file

#### Scenario: Row text keeps direction words
- **WHEN** Tinjau shows "Uang keluar dari RDN Contoh. Masuknya ke mana?" or "Money out of RDN Contoh. Where did it go?"
- **THEN** the copy guard SHALL accept it, because it describes a row and is not a figure label

## ADDED Requirements

### Requirement: Plain English Interface Labels
In the English interface, labels SHALL be plain, firm and impersonal: US spelling, sentence case, familiar words, no slang such as "gonna", "yep", "kinda", "stuff", "awesome" or "oops", no finance jargon such as "outflow", "inflow", "ledger", "reconcile", "liabilities", "runway" or "cost basis", and no "you" or "your" outside conversation.

#### Scenario: English labels
- **WHEN** the owner opens Beranda in English
- **THEN** headings and actions SHALL read, for example, "Needs action", "Recent activity", "All transactions", "Open Review" and "Breakdown", and none SHALL contain a listed slang or jargon word

#### Scenario: English guard
- **WHEN** an English catalogue message outside the conversation namespace contains "ledger" or "your"
- **THEN** the copy guard SHALL fail and name the key

### Requirement: One Term per Concept
Each money concept SHALL have exactly one name per language, kept in one glossary, and every figure label SHALL use it. Two glossary names SHALL NOT differ only by letter case, plural or "&" versus "and". The bank, e-wallet or cash "Account" SHALL never name the sign-in identity, which is "Profile".

#### Scenario: Near-duplicate terms
- **WHEN** the English glossary holds both "Saved & invested" and "Saved and invested"
- **THEN** the copy guard SHALL fail and name both keys

#### Scenario: Figure label outside the glossary
- **WHEN** a Beranda figure is labelled with a message that is not a glossary term
- **THEN** the copy guard SHALL fail and name the key
