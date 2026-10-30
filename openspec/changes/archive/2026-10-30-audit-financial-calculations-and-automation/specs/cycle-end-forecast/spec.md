## REMOVED Requirements

### Requirement: Active Cycle Outflow Forecasting
**Reason**: Linear extrapolation is unreliable for irregular spending and assumes a known cycle end.
**Migration**: Replace the forecast with observed period cash flow and explicitly identified upcoming commitments.

### Requirement: Prevention of Rounding Precision Drift
**Reason**: The projected 30-day figure is a historical observation presented as a forecast.
**Migration**: Show actual trailing-period spending with its actual window where useful.

### Requirement: Cycle Forecast Insights KPI Tile
**Reason**: The forecast tile presents an unsupported prediction.
**Migration**: Replace it with observed cash flow and transparent period context.
