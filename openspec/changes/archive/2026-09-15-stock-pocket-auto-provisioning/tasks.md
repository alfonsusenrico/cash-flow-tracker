## 1. Notification Parser Extension

- [x] 1.1 Extend `ParsedNotification` in `backend/app/services/notification_parser.py` to include investment trade fields (`symbol`, `instrument_symbol`, `lots`, `units`, `price_per_unit`, `action`)
- [x] 1.2 Update `STOCKBIT_MATCH_PATTERN` logic in `notification_parser.py` to populate these structured trade fields with normalized `.JK` ticker
- [x] 1.3 Add unit tests in `backend/tests/test_notification_parser.py` for structured Stockbit trade parsing

## 2. Ingestion Router Pocket Auto-Provisioning & Position Management

- [x] 2.1 Update `backend/app/routers/ingest.py` to resolve or create a child account (Pocket) under the broker account when an investment trade notification arrives
- [x] 2.2 Implement weighted average buy price recalculation and share quantity accumulation for new and existing pockets
- [x] 2.3 Route the transfer transaction directly to the child pocket leg (`transfer_target_account_id = pocket_id` on buy, `account_id = pocket_id` on sell)
- [x] 2.4 Add automated tests in `backend/tests/test_ingest.py` verifying pocket creation, position accumulation, and dual-leg transfer targeting

## 3. Data Migration & Live Price Sync

- [x] 3.1 Provision the `BBRI` pocket under `Stockbit` for user `alfonsusenrico` with 14 lots (1,400 shares) @ Rp 3.340 and `instrument_symbol = 'BBRI.JK'`
- [x] 3.2 Reassign the two existing BBRI transactions to target the `BBRI` pocket
- [x] 3.3 Trigger live Yahoo Finance quote sync via `sync_all_tracked_prices` and verify `last_price`, market valuation, and P&L

## 4. Verification & Validation

- [x] 4.1 Run full backend test suite (`pytest tests/`) and frontend type checks (`npm run type-check`)
- [x] 4.2 Rebuild and restart Docker containers (`docker compose up -d --build api frontend`)
- [x] 4.3 Validate change with `openspec validate stock-pocket-auto-provisioning`
