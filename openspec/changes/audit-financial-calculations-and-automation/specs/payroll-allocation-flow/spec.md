## REMOVED Requirements

### Requirement: Payroll Allocation Bundle Configuration
**Reason**: Automatic tracking reduces the value of transaction-generating allocation bundles and creates duplication risk.
**Migration**: Preserve historical configurations as non-posting plans or records for review.

### Requirement: 1-Tap Payroll Allocation Modal & Execution
**Reason**: Bulk ledger posting can duplicate actual captured transfers.
**Migration**: Retire execution while retaining actual recorded transfer history and explicit manual entry for missed events.
