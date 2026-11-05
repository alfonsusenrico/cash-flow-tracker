# account-roles Specification

## Purpose
Each account's Kegunaan (harian, tabungan, dana_investasi, investasi, utang): what its money is for. It decides Saldo tersedia, Dana investasi, savings and display, and must agree with the account type.

## Requirements

### Requirement: Account Role
Every account SHALL have a role ("Kegunaan"): `harian`, `tabungan`, `dana_investasi`, `investasi` or `utang`. A credit account SHALL be `utang`, an investment product `investasi`, and a cash, bank or wallet account `harian`, `tabungan` or `dana_investasi`. V51 SHALL backfill credit as `utang`, products as `investasi`, goal-linked accounts as `tabungan` and the rest as `harian`.

#### Scenario: Setting a pocket to Tabungan
- **WHEN** the owner edits the Jago pocket "Dana Darurat" and chooses "Kegunaan: Tabungan"
- **THEN** the account SHALL be stored with role `tabungan` and its row SHALL carry `role: "tabungan"`

#### Scenario: Role contradicting the type
- **WHEN** a request sets role `harian` on a credit account or `tabungan` on a mutual fund
- **THEN** it SHALL be refused with 422 `account_role_type_mismatch` and nothing SHALL change

#### Scenario: Backfill keeps today's figures
- **GIVEN** a goal-linked pocket, a credit account, a mutual fund and a plain bank pocket before V51
- **WHEN** V51 runs
- **THEN** their roles SHALL be `tabungan`, `utang`, `investasi` and `harian`, and the cycle's Ditabung SHALL equal its value before V51

#### Scenario: Another owner's account
- **WHEN** one owner sets the role of another owner's account
- **THEN** the request SHALL be answered 404 and nothing SHALL change

### Requirement: Kas Investasi Outside Siap Pakai
An account with role `dana_investasi` SHALL count in net worth as its own "Dana investasi" part and SHALL NOT count in Saldo tersedia, in Dana likuid or in Ketahanan Dana's fallback. Money SHALL still move into and out of it like any cash account.

#### Scenario: RDN as Dana investasi
- **GIVEN** Rp 18.450.000 in Uang harian and Tabungan, an RDN of Rp 1.200.000 with role `dana_investasi`, and no emergency goal
- **WHEN** Beranda and Rekening figures are computed
- **THEN** Saldo tersedia SHALL be Rp 18.450.000, net worth SHALL include a "Dana investasi" line of Rp 1.200.000, and Ketahanan Dana SHALL use Rp 18.450.000

#### Scenario: Depositing into the RDN
- **WHEN** the owner moves Rp 500.000 from BCA · ATM to the RDN
- **THEN** the movement SHALL be recorded, Saldo tersedia SHALL fall by Rp 500.000, Dana investasi SHALL grow by Rp 500.000 and net worth SHALL be unchanged
