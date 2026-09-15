## Context

The system persists all balances, transactions, goals, and budgets in base Indonesian Rupiah (IDR). Users can configure their primary currency preference to `USD` in the profile settings. The backend exposes `/auth/currency/rates` which queries the latest Yahoo Finance `USDIDR=X` exchange rate. However, the frontend currently lacks presentation conversion logic, resulting in USD-configured profiles still displaying raw IDR amounts with "Rp" prefixes.

## Goals / Non-Goals

**Goals:**
- Provide dynamic, loss-free currency conversion on the client presentation layer using the cached exchange rate.
- Automatically format amounts in standard USD notation (`$X,XXX.XX`) when `USD` is active, and Indonesian Rupiah notation (`Rp X.XXX.XXX`) when `IDR` is active.
- Bind active currency and exchange rates to `AppLayout` context and global formatting utilities (`fmtMoney`, `bal`, `formatAxisCurrency`).
- Support masked balance hiding in both currencies (`$ ••••••` vs `Rp ••••••`).
- Clean up legacy hardcoded `"Rp "` string literals across modals and views.

**Non-Goals:**
- Multi-currency ledger mutation (we do NOT rewrite stored database balances into USD cents; all storage remains in base IDR).
- Live currency trading or real-time ticker stream updates (hourly cached exchange rate from existing `/auth/currency/rates` is sufficient).

## Decisions

### 1. Presentation-layer conversion vs DB rewriting
- **Decision:** Keep all stored numbers in IDR and perform conversion dynamically at the rendering layer.
- **Rationale:** Preserves historical ledger fidelity, prevents rounding drift, and allows users to toggle back and forth between IDR and USD at any time with zero database migration overhead.

### 2. Dual synchronization (Module State + React Context)
- **Decision:** Expose `setCurrencyConfig(currency, rate)` in `@/lib/utils.ts` for standalone helpers, while providing `currency`, `usdIdrRate`, `fmtMoney`, and `bal` through `AppContext` (`useAppCtx()`).
- **Rationale:** Components can either call `bal(val)` from `useAppCtx()` for automatic reactive updates and balance hiding, or import `fmtMoney` directly. In `AppLayout`, whenever `user.currency` or `ratesData` changes, `setCurrencyConfig` is called, keeping both contexts identical.

### 3. Masked Balance Display
- **Decision:** `bal(amount)` checks `hideBalances`. If true, it returns `currency === "USD" ? "$ ••••••" : "Rp ••••••"`.
- **Rationale:** Uniform UX regardless of active currency.

### 4. Chart Scaling
- **Decision:** `formatAxisCurrency(val, currency)` formats IDR using `B` / `M` / `k`, and USD using `$B` / `$M` / `$k`.

## Risks / Trade-offs

- [Risk] If `/auth/currency/rates` fails or is unreachable, rate could be undefined.
  → *Mitigation:* Fall back to 16,500 IDR/USD as a reliable default.
- [Risk] Double prefixes in views where `"Rp "` was manually written before `{fmtMoney(val)}`.
  → *Mitigation:* Remove hardcoded `"Rp "` prefixes so the currency formatter handles the symbol exclusively.
