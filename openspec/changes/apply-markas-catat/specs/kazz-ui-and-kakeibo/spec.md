## REMOVED Requirements

### Requirement: Tactile Calculator Keypad
**Reason**: The app has no on-screen keypad, and `web-api-key-management` "Quick Capture Without Calculator Keypad" forbids one. The behaviour that remains, inline arithmetic typed in the amount field, is kept and specified in `tactile-pulse-ui` "Inline Arithmetic in the Amount".
**Migration**: None for users or data. Tests that asserted the keypad were removed earlier; the arithmetic tests move to the capture dialog's suite.
