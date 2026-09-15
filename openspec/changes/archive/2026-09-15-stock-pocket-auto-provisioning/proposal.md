## Why

Stock transactions parsed from investment broker notifications (e.g. Stockbit) are currently deposited as flat cash into the master investment account instead of provisioning dedicated ticker pockets (e.g. `BBRI.JK`). This prevents the system from tracking stock quantities (lots/shares), average buy prices, live Yahoo Finance market valuations, and unrealized capital gain/loss metrics.

## What Changes

- **Structured Parser Output**: Extend `notification_parser.py` to emit structured stock trade metadata (`symbol`, `instrument_symbol` with `.JK` suffix, `lots`, `units = lots * 100`, `price`, `action`).
- **Automatic Stock Pocket Provisioning**: When processing investment trades in `ingest.py`, automatically resolve or create a child account (Pocket) under the broker account (`parent_id = stockbit.id`) tagged with `instrument_symbol`, `units`, and `avg_buy_price`.
- **Position & Weighted Average Cost Updates**: On additional buys of the same ticker, increase `units` and recalculate the weighted average cost basis; on sells, decrease `units`.
- **Targeted Dual-Leg Transfers**: Target transfer transactions directly between the funding account (`RDN BCA`) and the specific stock pocket (`BBRI`), maintaining clean ledger tracking without double-counting.
- **Historical Data Reconcile**: Migrate the existing 14 lots of BBRI into a newly provisioned `BBRI` pocket under `Stockbit` and trigger price synchronization.

## Capabilities

### New Capabilities
- `stock-pocket-auto-provisioning`: Automated resolution, creation, position updating, and market price tracking for stock pockets under investment broker accounts upon trade notification ingestion.

### Modified Capabilities
<!-- None -->

## Impact

- **Backend**: `backend/app/services/notification_parser.py`, `backend/app/routers/ingest.py`, and `backend/app/routers/accounts.py`.
- **Database**: Creates child accounts in `accounts` table linked by `parent_id` to master broker accounts, referencing `instrument_symbol`, `units`, `avg_buy_price`, and `last_price`.
- **External Integration**: Automatic price sync against Yahoo Finance IDX feeds via `sync_all_tracked_prices`.
