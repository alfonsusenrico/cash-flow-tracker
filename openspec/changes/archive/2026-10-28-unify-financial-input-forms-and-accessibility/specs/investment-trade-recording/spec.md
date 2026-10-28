# Spec Delta

## ADDED Requirements

### Requirement: Trade Settlement Form Clarity
The trade form SHALL identify the instrument position and funding account, show the direction of cash and units for the selected action, validate positive units and price, and prevent submission of a sale above the currently owned position. The projected position and cash settlement SHALL remain visible before confirmation.

#### Scenario: Previewing a sale destination
- **WHEN** a user prepares to sell an investment into RDN BCA
- **THEN** the form shows units leaving the instrument, proceeds entering RDN BCA, and the projected remaining units before submission

#### Scenario: Blocking an excessive sale in the form
- **WHEN** requested sale units exceed the displayed owned units
- **THEN** the submit action is blocked and the owned-versus-requested error is associated with the units control
