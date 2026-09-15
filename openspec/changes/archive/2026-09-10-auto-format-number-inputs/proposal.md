## Why

Entering large financial amounts in Indonesian Rupiah (e.g., `20000000`) without thousand separators makes it difficult to read and verify digits, leading to input errors and cognitive friction. Users should not have to manually count zeroes or guess whether they typed 2 million or 20 million.

Number inputs across the application should automatically format with thousand dots (e.g., `20.000.000`) as the user types, while seamlessly submitting raw integer amounts to the backend.

## What Changes

- **Automatic Thousand Separator Formatting (`id-ID` dots)**:
  - Create a reusable numeric formatting utility and custom input helper (`formatDots`, `cleanDigits`, or enhanced input components).
  - Automatically format numbers with thousands separators (`.`) dynamically upon typing (e.g., entering `20000000` displays `20.000.000`).
  - Support natural editing: pasting formatted or unformatted numbers, backspacing, and digit deletion without cursor misplacement or invalid characters.
- **Form Coverage Across Entire Application**:
  - **Goals & Obligations (`/goals`)**:
    - Target Nominal (`goalTarget`)
    - Nominal Awal Terkumpul (`goalCurrent`)
    - Total Tagihan / Pinjaman (`obTotal`)
    - Sisa Tagihan Saat Ini (`obRemaining`)
    - Cicilan Minimal per Bulan (`obMinPayment`)
    - Setor Tabungan modal (`actionAmount`)
    - Bayar Tagihan modal (`actionAmount`)
  - **Accounts (`/accounts`)**:
    - Saldo Awal / Initial Balance (`accInitBal`)
    - Saldo Aktual di Rekening / Reconcile (`actualBalStr`)
    - Nominal Transfer modal (`transferAmount`)
  - **Dashboard Quick Transfer (`/`)**:
    - Nominal Transfer modal (`transferAmount`)
  - **Ledger (`/ledger`)**:
    - Edit Transaksi Nominal (`editAmount`)
  - **Insights (`/insights`)**:
    - Batas Anggaran Bulanan (`budgetInput`)
  - **Quick Capture Modal (`QuickCaptureModal.tsx`)**:
    - Sanitize thousands dots so math expressions (`20.000 + 5.000`) and formatted inputs resolve accurately to integer amounts.

## Capabilities

### Modified Capabilities
- `indonesian-daily-finance-ui`: Add requirement for automatic thousand dot separation on all numeric money and currency input fields.

## Impact

- **Frontend**:
  - `frontend/src/lib/utils.ts` (reusable dot formatter and unformatter).
  - `frontend/src/app/goals/page.tsx`
  - `frontend/src/app/accounts/page.tsx`
  - `frontend/src/app/page.tsx`
  - `frontend/src/app/ledger/page.tsx`
  - `frontend/src/app/insights/page.tsx`
  - `frontend/src/components/ui/QuickCaptureModal.tsx`
  - `frontend/src/components/ui/MoneyInput.tsx`
- **Backend / Database**: No backend schema changes required; all payloads continue to send standard integer numbers.
