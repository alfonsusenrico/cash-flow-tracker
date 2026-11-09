## MODIFIED Requirements

### Requirement: Kebiasaan Settings Section
Pengaturan SHALL have a Kebiasaan section at `/settings/kebiasaan` that shows `GET /api/habits`: the auto-record states of habits and of top-ups as read-only status rows set by the server, a list of payment habits and a list of learned top-ups, each row with what was found, what it is recorded as, how often it was confirmed and its status. It SHALL offer no edit, forget or switch control, and amounts SHALL respect "Sembunyikan saldo".

#### Scenario: Viewing learned habits
- **WHEN** the owner opens Kebiasaan with one ready habit and one learning top-up
- **THEN** the habit row SHALL read "Kedai Awan · Dompet Digital", "Makanan & Minuman · Keinginan · 3× dikonfirmasi" and "Dicatat otomatis", and the top-up row SHALL read "Rp 112.590 dari RDN Contoh", "Bibit Contoh · Reksa Dana A · 2× dikonfirmasi" and "Masih belajar · 2 dari 3"

#### Scenario: Server states are not switches
- **WHEN** the owner reaches "Pencatatan otomatis dari kebiasaan" by keyboard
- **THEN** it SHALL read "Aktif" or "Nonaktif" as text, and no switch, checkbox or button SHALL be offered for it
