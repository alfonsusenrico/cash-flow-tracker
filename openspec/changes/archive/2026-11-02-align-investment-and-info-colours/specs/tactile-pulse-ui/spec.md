## MODIFIED Requirements

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank).

#### Scenario: Required behavior and constraints
- **WHEN** the application renders its financial interface
- **THEN** the following required behavior and constraints hold:

  The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
  1. **Typography:** Plus Jakarta Sans for interface text and Geist Mono for numeric code text, both self-hosted from the app's own origin as defined by `web-font-hosting`, paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
  2. **Seamless Surface Elevation:** Card surfaces SHALL be separated from the page by the tonal contrast between `--surface` and `--bg` and a 1px hairline `--border` (ink at 9% opacity in the light theme, white at 8% in the dark theme), never by heavy wireframe outlines. A raised surface SHALL use the `--shadow-xs`, `--shadow-sm` or `--shadow-md` token of `frontend/tokens.css`, where `--shadow-xs` is the 1px raise of plain cards, chips and buttons and stays lighter than `--shadow-sm` in both themes; deeper shadows are reserved for floating layers such as dialogs, menus, the phone navigation bar and the selection bar.
  3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, sky blue for internal transfers ("Pindah Saldo") and lavender for investments, investment top-ups included. Every transfer element (a "Pindah Saldo" action, a movement row's icon, amount or badge, and a movement's detail amount) SHALL use sky blue, never amber or another blue; amber SHALL be reserved for warnings such as "Waspada". Sky blue SHALL be used only for transfer elements; information that is neither a transfer nor an investment, such as the "Pilar Kakeibo" card, an explanatory note or a "Ditautkan otomatis" label, SHALL use the neutral surface, text and muted tokens. Every investment element (an investment balance, an investment account's badge, card or allocation share, an instrument control, and a top-up's badge, row, amount or note) SHALL use lavender, never amber, sky, blue, indigo or purple. Spring Lime, emerald and coral keep their vivid colours for fills (navigation pills, chart peaks, buttons, chart marks); lime, emerald, coral, sky and lavender used as text or as a meaningful icon on a theme surface SHALL use a text ink of the same hue (darker than the fill in the light theme), measuring at least 4.5:1 for text and 3:1 for icons against the surface it sits on in the light and the dark theme. Text on the solid lavender fill SHALL use a contrast ink that measures at least 4.5:1 against that fill in both themes. Text on an emerald fill, the strong `bg-emerald-600` fill included, SHALL use an ink that measures at least 4.5:1 against that fill in both themes. Amber used as text SHALL use a warning text ink of the same hue, measuring at least 4.5:1 against the surface it sits on in both themes. An icon tile that repeats its button's label SHALL be hidden from assistive technology, and a tile icon that conveys meaning SHALL measure at least 3:1 against its tile.
  4. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation

#### Scenario: Light theme lime and coral text contrast
- **WHEN** the light theme is active and a page shows lime text such as "Kelola →" or "Lihat Semua →", or coral text such as an expense amount, the "Uang Keluar" badge or "+ Tambah Tagihan"
- **THEN** the text measures at least 4.5:1 against the surface it sits on, it still reads as lime or coral, and the lime pill, chart peak and coral fills are unchanged

#### Scenario: Plain cards carry a subtle raise
- **WHEN** a card, chip or button styled with the `shadow-xs` utility renders in the light or the dark theme
- **THEN** its computed `box-shadow` SHALL be the `--shadow-xs` value of that theme (a 1px offset with at most 2px blur, lighter than `--shadow-sm`), and a surface that already has `--shadow-sm` or a deeper shadow SHALL keep it

#### Scenario: Emerald income text and buttons
- **WHEN** Beranda, Transaksi or Analisis shows emerald text such as an income amount or the "Uang Masuk" badge, or text on an emerald button such as "Tambah Kategori" or "Catat Transaksi", in the light or the dark theme at 390 or 1280 px
- **THEN** the text measures at least 4.5:1 against the surface or fill it sits on, income still reads as emerald, and the emerald fills are unchanged

#### Scenario: Amber text contrast
- **WHEN** a page shows `text-amber-400` to `text-amber-700` text, such as a "Waspada" badge, on `--surface`, `--bg`, `--surface-raised` or the warning soft fill, in the light or the dark theme
- **THEN** the text measures at least 4.5:1 against that background, it still reads as amber, and the amber fills are unchanged

#### Scenario: Text on the strong emerald fill
- **WHEN** Target & Tagihan shows "Target Baru", "Buat Target Pertama Anda" or "Simpan Progres" on the `bg-emerald-600` fill, in the light or the dark theme
- **THEN** the text measures at least 4.5:1 against the fill, and the fill is unchanged

#### Scenario: Decorative icon tiles
- **WHEN** the Beranda wallet card shows its "Pengeluaran", "Pemasukan" and "Pindah Saldo" buttons
- **THEN** each button's icon tile SHALL be hidden from assistive technology, the button SHALL be named by its text, and the emerald tile's icon and the sky "Pindah Saldo" tile's icon SHALL each measure at least 3:1 against the tile in both themes

#### Scenario: Sky blue transfers
- **WHEN** Beranda, Transaksi or the phone-width home view shows the "Pindah Saldo" text button or capsule, a movement row's icon, amount or "Pindah Saldo" badge, or a movement's detail amount, in the light or the dark theme at 390 or 1280 px
- **THEN** the element SHALL be sky blue and not amber or another blue, its text SHALL measure at least 4.5:1 and its icon at least 3:1 against the surface or sky tint it sits on, and amber SHALL remain on warnings only

#### Scenario: Lavender investments
- **WHEN** Beranda, the phone-width home view, Transaksi or Rekening shows an investment element, such as the "Investasi" wallet pill or balance, the investment wallet card, the "Invest" badge, the "Investasi" allocation share, a selected instrument button, or a "Top up Investasi" badge, amount or note, in the light or the dark theme at 390 or 1280 px
- **THEN** the element SHALL be lavender and not amber, sky, blue, indigo or purple, its text SHALL measure at least 4.5:1 against the surface, lavender soft fill or lavender tint it sits on, text on the solid lavender fill SHALL measure at least 4.5:1, and transfers SHALL stay sky, warnings amber, income emerald and expenses coral

#### Scenario: Sky only for transfers
- **WHEN** a page shows the "Pilar Kakeibo" card, the Target & Tagihan progress note, a "Ditautkan otomatis" label or the phone-width "Dipilih" selection label
- **THEN** the element SHALL use the neutral surface, text and muted tokens and not sky blue, and every sky blue element on the web SHALL be a transfer element
