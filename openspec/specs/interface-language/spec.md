# interface-language Specification

## Purpose
Let each person use the app in plain English or in Indonesian, chosen per account and before sign-in, while names they entered, figures and formats stay exactly the same in either language.

## Requirements

### Requirement: Supported Interface Languages
The web interface SHALL be available in English (`en`), the default, and Indonesian (`id`), with the same features, figures and layout in both. A new account SHALL start in English unless it is created from an Indonesian interface. Every account that existed before languages were introduced SHALL be Indonesian.

#### Scenario: New account from an English sign-up page
- **WHEN** a person registers from the sign-up page shown in English
- **THEN** the account's language SHALL be `en`, and after sign-in the shell SHALL read "Home", "Review" and "Transactions"

#### Scenario: Existing account stays Indonesian
- **GIVEN** an account created before the language migration
- **WHEN** its owner signs in from a browser that prefers English
- **THEN** the interface SHALL be in Indonesian, with "Beranda", "Aman dipakai sampai gajian" and "Pengeluaran" exactly as before

### Requirement: Interface Language Resolution
When signed in, the interface language SHALL be the account's. Before sign-in it SHALL be the language last chosen on that browser, then the supported language the browser prefers most (`Accept-Language`, `id` or legacy `in` meaning Indonesian), then English. Unsupported or malformed values SHALL be ignored. URLs SHALL NOT carry a language.

#### Scenario: Browser prefers Indonesian
- **WHEN** a signed-out browser sending `Accept-Language: id-ID,id;q=0.9,en;q=0.8` opens the sign-in page
- **THEN** the page SHALL be in Indonesian

#### Scenario: Browser prefers another language
- **WHEN** a signed-out browser sending `Accept-Language: ms, fr` opens the sign-in page
- **THEN** the page SHALL be in English

#### Scenario: Account overrides the browser
- **GIVEN** a browser whose last choice was Indonesian
- **WHEN** an account whose language is English signs in on it
- **THEN** the interface SHALL switch to English after sign-in, and the browser's remembered choice SHALL become English

#### Scenario: Same address in both languages
- **WHEN** the owner switches language on `/ledger?period=month`
- **THEN** the address SHALL stay `/ledger?period=month`

### Requirement: Account Language Follows the Account
The account's language SHALL be saved on the server and followed on every device. When a device's remembered choice differs from the account's, the device SHALL adopt the account's language once, without a sign-out.

#### Scenario: Changed on another device
- **GIVEN** the owner switched the account to English on a laptop
- **WHEN** the owner next opens the app on a phone browser that still remembers Indonesian
- **THEN** the phone browser SHALL show English after one refresh, and SHALL keep showing English

#### Scenario: Another account on a shared browser
- **GIVEN** an English account signed out of a browser
- **WHEN** an Indonesian account signs in on the same browser
- **THEN** the interface SHALL be Indonesian, and nothing of the first account's choice SHALL persist for the second

### Requirement: Choosing the Interface Language
Pengaturan › Tampilan and the sign-in page SHALL offer the language as two options named in their own language, "English" and "Bahasa Indonesia", without flags. Choosing one SHALL apply it at once, without a full reload or a change of address, and when signed in SHALL save it to the account. A failed save SHALL keep the previous language and announce why.

#### Scenario: Switching in Pengaturan
- **WHEN** the owner chooses "English" in Pengaturan › Tampilan
- **THEN** the shell SHALL read "Settings" and "Home", the account SHALL be saved as `en`, and focus SHALL stay on the chosen option

#### Scenario: Save fails
- **GIVEN** the server refuses the change
- **WHEN** the owner chooses "English"
- **THEN** the interface SHALL stay Indonesian, "Bahasa Indonesia" SHALL stay selected, and a polite live region SHALL announce the error message

#### Scenario: Switching before sign-in
- **WHEN** a signed-out visitor chooses "Bahasa Indonesia" on the sign-in page
- **THEN** the sign-in page SHALL be in Indonesian, and SHALL stay Indonesian on the next visit from that browser

### Requirement: Document Language
Every page SHALL declare the interface language on its root element. Interface text in another language SHALL be marked with its own language. Text the user entered and quoted notification text SHALL carry no language override.

#### Scenario: English interface
- **WHEN** a screen reader loads Beranda in English
- **THEN** the root element SHALL carry `lang="en"`, and the option "Bahasa Indonesia" SHALL carry `lang="id"`

#### Scenario: User data keeps no override
- **WHEN** the English Beranda lists a transaction named "Bayar parkir PTC"
- **THEN** the title SHALL be inside no element with a `lang` attribute other than the root

### Requirement: Untranslated Pages Inside a Translated Shell
In English, a page whose redesign phase has not translated it yet SHALL show its Indonesian content inside the English shell. Its title block and main region SHALL carry `lang="id"`, and one muted line under its title SHALL read "This page is in Indonesian for now." The Indonesian interface SHALL show no such line. The set of untranslated pages SHALL only shrink.

#### Scenario: Tinjau before phase 4
- **WHEN** an English account opens Tinjau
- **THEN** the sidebar SHALL read "Review", the page content SHALL be the Indonesian page with `lang="id"`, and the line "This page is in Indonesian for now." SHALL appear under its title

#### Scenario: Translated page has no notice
- **WHEN** an English account opens Beranda or the sign-in page
- **THEN** no untranslated-page line SHALL appear and no region SHALL carry `lang="id"`

### Requirement: User Data Is Never Translated
Names, notes and merchants the user entered, categories and accounts once created (including starter ones), quoted notification text and stored generated descriptions SHALL be shown exactly as stored in either language. Switching language SHALL NOT change any stored row.

#### Scenario: Indonesian names in English
- **GIVEN** a category "Jajan & Nongkrong" and a note "Pindah saldo ke BCA Harian"
- **WHEN** the owner switches to English
- **THEN** both SHALL read exactly the same, and no transaction, category or account row SHALL be updated by the switch

### Requirement: Quoted Interface Copy Applies per Language
Where a requirement quotes interface copy in Indonesian, the Indonesian interface SHALL show that text, and the English interface SHALL show its English counterpart from the approved glossary. Quoted server text and user data SHALL follow their own requirements.

#### Scenario: Beranda hero in both languages
- **WHEN** Beranda shows its hero, which `beranda-cycle-summary` quotes as "Aman dipakai sampai gajian" with "Rincian"
- **THEN** the Indonesian interface SHALL show exactly those words, and the English interface SHALL show "Safe to spend until payday" with "Breakdown"

### Requirement: Message Catalogue Parity
Interface text SHALL come from one catalogue per language, with the same keys and the same arguments in both. A sentence SHALL be one message, never built by joining fragments. English messages with a count SHALL have singular and plural forms. A missing message SHALL fail tests and development builds.

#### Scenario: Missing English message
- **WHEN** a key exists in the Indonesian catalogue but not in the English one
- **THEN** the type check and the catalogue parity test SHALL fail and name the key

#### Scenario: Argument mismatch
- **WHEN** the English message for the days-left line uses `{days}` and the Indonesian one `{n}`
- **THEN** the catalogue parity test SHALL fail and name the message

#### Scenario: Plural forms
- **WHEN** the days-left line is shown with 1 and with 20 days left
- **THEN** English SHALL read "1 day until payday on 25 Nov" and "20 days until payday on 25 Nov"

### Requirement: Region-Based Formatting
Numbers, dates, weekday and month names, compact amounts and money SHALL be formatted by the interface language together with the account region, never by the language alone. For region Indonesia and IDR, money SHALL read exactly "Rp 1.234.567" in both languages. Every figure SHALL be identical in both languages for the same data.

#### Scenario: English with region Indonesia
- **WHEN** an English account in region Indonesia views Beranda on Thursday 5 November with Rp 1.234.567 safe to spend
- **THEN** the title SHALL read "Thursday, 5 November" and the hero "Rp 1.234.567", with a normal space after "Rp"

#### Scenario: Figures identical across languages
- **GIVEN** an English and an Indonesian account with the same transactions, region Indonesia
- **WHEN** both open Beranda and its Rincian
- **THEN** every amount, count and date number SHALL be the same string in both

### Requirement: Account Region
Each account SHALL have a region, Indonesia (`ID`) or the United States (`US`), chosen at sign-up (default Indonesia) and changeable in Pengaturan. The region SHALL change number and date style only, never words, stored amounts or the display currency.

#### Scenario: Region changes style, not words
- **GIVEN** an English account with display currency IDR
- **WHEN** its region changes from Indonesia to the United States
- **THEN** Beranda's title SHALL read "Thursday, November 5", its hero "Rp 1,234,567", and every label SHALL stay the same

### Requirement: Server Labels Carry Their Source
Labels and notes in the cycle summary, the review queue's groups and the waiting lines SHALL state whether a label is user data or fixed wording, and SHALL carry fixed wording and notes as stable codes with parameters. The web SHALL render fixed wording from those codes in the interface language and SHALL show user labels as stored.

#### Scenario: System and user lines
- **WHEN** the cycle summary returns the Kakeibo bucket "need", the net-worth line "liquid" and a goal line "Liburan Bali"
- **THEN** the first two SHALL carry `label_source` `system` and English SHALL show "Needs" and "Available cash", and the goal SHALL carry `user` and read "Liburan Bali" in both languages

#### Scenario: Note with an amount
- **WHEN** a Kakeibo line was reduced by Rp 250.000 of reimbursements
- **THEN** it SHALL carry `note_codes` with `reduced_by_reimbursements` and `amount` 250000, and English SHALL show "Reimbursements of Rp 250.000 already subtracted."
