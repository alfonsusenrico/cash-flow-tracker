## 1. Schema and roles

- [x] 1.1 Write V51, additive and idempotent: `accounts.role` with a CHECK and the backfill (D1), and `account_layouts` (D5). Evidence: Flyway V1 → V51 on disposable PostgreSQL. A rerun is a no-op. A backfill test covers a goal-linked pocket, a credit account, a mutual fund and a plain pocket.
- [x] 1.2 Add role to the accounts API: create and edit accept `role`, rows carry `role` and `kind`, and the 422 `account_role_type_mismatch` applies. Evidence: API tests for each type and role pair, and another owner's 404.
- [x] 1.3 Add the `dana_investasi` bucket in `summarize_account_balances`, the net-worth line, and Ketahanan Dana's fallback (D2). Evidence: the "RDN as Dana investasi", "Depositing into the RDN" and "Dana investasi is an asset but not liquid" tests. The "Same figure as Beranda" test still passes.

Expected Result: roles exist with today's figures preserved, and RDN cash leaves Saldo tersedia.

## 2. Savings

- [x] 2.1 Classify net fresh savings at read time from current roles and goal links, replacing the stored-leg gate (D3). Evidence: tests for "Moving money into a Tabungan pocket without a goal", "Role applies to past movements", and the existing `test_kakeibo_savings.py` and `test_kakeibo_reconciliation.py`.
- [x] 2.2 Subtract expenses paid from a savings account, drop the clamp, and keep `spending_split`'s check on the new figure (D4). Evidence: the "Paying from savings" and "More taken out than saved" tests, plus `test_cycle_summary.py`, monthly report and comparison tests.
- [x] 2.3 Beranda states the savings change as "Tabungan bertambah Rp X" or "Tabungan berkurang Rp X", with the lines "Masuk ke tabungan", "Diambil dari tabungan" and "Pengeluaran dari tabungan". Evidence: Beranda component tests, with axe.

Expected Result: saving moves and spending from savings give a true Aman dipakai.

## 3. Layout API

- [x] 3.1 Add `GET` and `PUT /accounts/layout?grouping=use|rekening` with validation, drop and append rules, and defaults from `display_order` (D5). Evidence: API tests for a save and read, unknown ids dropped, missing ids appended, another owner's ids refused, and an idempotent PUT.

Expected Result: the owner's order is saved per grouping and is the same on every device.

## 4. Notification regression with the new structure

- [x] 4.1 Add regression tests with synthetic accounts: GoPay Tabungan as a pocket under the Jago parent, and RDN under BCA. Flows: BCA → GoPay top-up (link skip), GoPay Tabungan → Main, Main → GoPay Tabungan, Jago → BCA, and an RDN deposit (D8). Evidence: all pass with no rule change. Any failure is surfaced to the owner before rules change.
- [x] 4.2 With the owner's go-ahead, re-export the 2026-11-05 Sesi uji read-only, move Gopay under Jago in a local copy, replay it and score it. Evidence: the score in `verification.md`, with no personal data. The export is deleted afterwards.

Expected Result: restructuring GoPay and the RDN keeps notifications recording correctly.

## 5. Rekening page

- [x] 5.1 Add use colour tokens for light and dark, validated with the dataviz script (D7). Evidence: validator output in `verification.md`.
- [x] 5.2 Build the summary, the Kartu tab with Per kegunaan and Per rekening rails and cards, the mini ring and the composition bars. Evidence: component tests for the scenarios "Per kegunaan" and "Per rekening", with axe.
- [x] 5.3 Build the Grafik tab (two rings and a focus list). Evidence: a component test for "Grafik", with axe.
- [x] 5.4 Show capital gain on cards, type, rekening and total, with "Modal belum diisi". Evidence: tests for "Gains on BCA" and "Unknown cost basis".
- [x] 5.5 Add reorder by pointer drag, Alt + arrows and the phone "Ubah urutan" mode, saved through the layout API, with failure restore. Evidence: tests for "Reorder and reload" and "Failed save".
- [x] 5.6 Add the "Kegunaan" field to the account dialogs, and rename the existing "Jadikan kantong di bawah" action to "Pindahkan ke rekening". Keep the existing detail modal. Evidence: dialog tests, including the renamed action.
- [x] 5.7 Run the frontend checks and a browser check of every tab and grouping at 1440 px and 390 px, light and dark, with axe and reorder plus reload. Evidence: command results and the browser summary in `verification.md`.

Expected Result: the accepted preview works in the app with real data rules.

## 6. Delivery

- [x] 6.1 Run the full backend suite on migrated PostgreSQL. Write `verification.md`, including a before and after comparison of cycle figures on a synthetic owner shaped like the owner's accounts. Run `openspec validate --all --strict` and `git diff --check`, then review the diff.
- [x] 6.2 After the owner accepts, archive the change, push the branch and report the compare link. After the merge, check V51 and the deploy read-only. Then the owner moves GoPay and the RDN and sets roles in the app.
