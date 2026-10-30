## MODIFIED Requirements

### Requirement: Investment Position Choices Match Trade-Only Contract
Generic movement and non-posting transfer-plan forms SHALL offer only eligible liquid accounts as source or destination. They SHALL explain that investment positions are changed through Beli/Jual, while liquid investment-funding accounts remain selectable.

#### Scenario: Selecting movement accounts
- **WHEN** the user opens a generic movement form while both liquid accounts and investment positions exist
- **THEN** only eligible liquid accounts are offered and the form explains that investment positions use Beli/Jual

#### Scenario: Configuring recurring or payroll transfer accounts
- **WHEN** the user configures a generic non-posting transfer plan
- **THEN** investment positions are not selectable as source or destination, while liquid funding accounts remain selectable
