## ADDED Requirements

### Requirement: Every Page in the Interface Language
Every signed-in page, its dialogs and the sign-in page SHALL render their interface copy in the interface language from the catalogues. No route SHALL show an untranslated-page notice, and no interface copy SHALL be wrapped in a `lang` other than the document's, except a language's own name in the language choice.

#### Scenario: English across the pages
- **WHEN** an English account opens Beranda, Tinjau, Transaksi, Analisis, Laporan, Rekening & Kantong, Target & Tagihan and each Pengaturan section
- **THEN** no page SHALL show "This page is in Indonesian for now." and no interface element SHALL carry `lang="id"` except the option "Bahasa Indonesia" in the language choice, while user data such as an account named "Tabungan Rumah" keeps no language override

#### Scenario: Indonesian across the pages
- **WHEN** an Indonesian account opens the same pages
- **THEN** the document SHALL carry `lang="id"` and no element SHALL carry `lang="en"` except the option "English" in the language choice

## MODIFIED Requirements

### Requirement: Region-Based Formatting
Numbers, dates, weekday and month names, compact amounts and money SHALL be formatted by the interface language together with the account region, never by the language alone. For region Indonesia and IDR, money SHALL read exactly "Rp 1.234.567" in both languages. Every figure SHALL be identical in both languages for the same data. English short month names SHALL have three letters ("Sep", never "Sept"), as the server's labels do.

#### Scenario: English with region Indonesia
- **WHEN** an English account in region Indonesia views Beranda on Thursday 5 November with Rp 1.234.567 safe to spend
- **THEN** the title SHALL read "Thursday, 5 November" and the hero "Rp 1.234.567", with a normal space after "Rp"

#### Scenario: Figures identical across languages
- **GIVEN** an English and an Indonesian account with the same transactions, region Indonesia
- **WHEN** both open Beranda and its Rincian
- **THEN** every amount, count and date number SHALL be the same string in both

#### Scenario: September in a short date
- **WHEN** an English account in region Indonesia views Analisis for the cycle from 29 September to 27 October
- **THEN** the web's cycle label SHALL read "Cycle 29 Sep – 27 Oct", with the same "Sep" as the server's period labels, and no date on any page SHALL read "Sept"

## REMOVED Requirements

### Requirement: Untranslated Pages Inside a Translated Shell
**Reason**: Every page is translated since apply-markas-pengaturan and apply-markas-catat; the only remaining user, the development `/komponen` sheet, is removed by apply-markas-cleanup.
**Migration**: None for users. A new page writes its copy in `messages/{en,id}.json`; "Every Page in the Interface Language" guards that no untranslated path returns.
