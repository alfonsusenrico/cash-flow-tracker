## MODIFIED Requirements

### Requirement: Top categories come from the tracker or are not shown
Beranda SHALL show "Kategori teratas" only from the `top_categories` the tracker sends with its cash-flow summary: at most three categories, one of which may be the spending without a category, with their amount, Kakeibo pillar and a bar relative to the largest, and the summary's period and freshness, stored, restored and marked stale with the summary. When the field is absent or breaks its contract, Beranda SHALL show no categories tile, and the phone SHALL never compute category totals.

#### Scenario: Tracker offers no categories for the period
- **WHEN** Beranda is shown with a summary from a tracker that does not send `top_categories`
- **THEN** Beranda shows no "Kategori teratas" tile and no category amount

#### Scenario: Recorded categories on the phone
- **WHEN** the phone holds recorded expenses with categories from the last three days
- **THEN** no category name or amount from them appears on Beranda

#### Scenario: Tracker sends its top categories
- **WHEN** the summary's `top_categories` are Belanja Rumah Rp410.000 (need), Hiburan Contoh Rp150.000 (want) and Lain-lain Rp80.000 (no pillar)
- **THEN** "Kategori teratas" lists them in that order with their amounts, "Kebutuhan" and "Keinginan", bars relative to Rp410.000, the summary's "Periode" and "Diperbarui" lines, and TalkBack reads the first as "Belanja Rumah, Rp410.000, Kebutuhan"

#### Scenario: Spending without a category
- **WHEN** the summary's `top_categories` are Belanja Rumah Rp410.000 (need), an entry with a null `category_id` named "Tanpa kategori" Rp150.000, and Lain-lain Rp80.000
- **THEN** "Kategori teratas" lists "Tanpa kategori" second with Rp150.000, its bar and no pillar, TalkBack reads it as "Tanpa kategori, Rp150.000", and it is stored and restored with its null id
- **AND** the same entry with a blank or missing name, or with a pillar, is shown and read the same way

#### Scenario: No spending in the period
- **WHEN** the summary's `top_categories` is an empty list
- **THEN** the tile says "Belum ada pengeluaran pada periode ini" and shows no Rp0

#### Scenario: Stored or not updated
- **WHEN** the summary shown was restored from the phone, or the last refresh failed
- **THEN** the tile shows the stored categories with the same "Tersimpan di ponsel, terakhir diperbarui" or "Belum diperbarui. Terakhir berhasil" line and reason as the "Arus kas tracker" tile

#### Scenario: Categories that break the contract
- **WHEN** `top_categories` holds more than three entries, entries not sorted by outflow descending, a negative or missing outflow, a blank name on an entry with a category id, an unknown pillar, or more than one entry with a null `category_id`
- **THEN** Beranda shows no "Kategori teratas" tile and still shows the summary's totals
