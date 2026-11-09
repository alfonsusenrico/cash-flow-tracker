## ADDED Requirements

### Requirement: Plans Dialog
"Rencana & Pengingat" SHALL be one centered 880 px dialog (a full sheet below 768 px) with a filter ("Semua", "Cicilan", "Rutin"), the plans as whole rows (name, status in words, next date, schedule, amount) and the chosen plan's form beside them, plus "Tambah rencana". It SHALL send today's plan requests only, SHALL never post a transaction, and SHALL open the same way from every page that opens it today.

#### Scenario: Editing a plan
- **WHEN** the owner opens "Rencana & Pengingat" from Target & Tagihan, chooses "Token Listrik PLN" and changes its amount
- **THEN** the row SHALL be marked chosen with `aria-current` and the straight bar, "Simpan" SHALL send `PATCH /recurring/{id}` once, and no transaction SHALL be created

#### Scenario: A due occurrence
- **GIVEN** "Tagihan SPayLater" has an occurrence due today in `GET /recurring/pending`
- **WHEN** the owner chooses it in the dialog
- **THEN** the detail SHALL show the same link, skip and record actions as "Rencana jatuh tempo", with the same confirmations and requests, and recording SHALL still name the same-amount captures first

#### Scenario: Same entry points
- **WHEN** the dialog is opened from Rekening & Kantong, Analisis with a detected charge, Beranda, the "Rencana jatuh tempo" view or a top-up's "Buat rencana bulanan"
- **THEN** it SHALL open with the same prefilled draft as before the redesign, and closing it SHALL return focus to its opener
