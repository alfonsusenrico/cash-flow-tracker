# Tasks

## 1. Uncategorised spending in the phone's top categories (D1, D2)

- [x] 1.1 Rank the breakdown's "Tanpa kategori" row in `top_categories` with `category_id` and `kakeibo_type` null, sorted by `(-outflow, name, id or "")`. Evidence: `test_cash_flow_top_categories.py`. The new tests cover the ranked entry with a cut fourth group, an exact sum for 1, 2 and 3 groups over `today` and `month`, a sum never above `outflow` (cut by the limit, then after a reimbursement), reimbursements next to uncategorised spending (applied and unallocated), and tenant isolation from both owners' views. The unit tests cover order, the bucket alone, a zero bucket and a same-named category. 8 of the new tests fail on the 15dbf61 code and all pass after the change.
- [x] 1.2 Record what the Android companion 2.3.3 (main 4e0d3e6) does with `category_id: null`. Evidence: design.md D2, from a read of `ApiModels.kt` and `TopCategoriesTile.kt` plus a Robolectric probe on a scratch `git archive` copy (2 probe tests pass, alongside the existing `TrackerOptionalFieldsTest` 9 and `TopCategoriesUiTest` 36). The owner's checkout was not touched and no phone was used.
- [x] 1.3 MODIFIED `financial-reporting-consistency` requirement, copied whole with every scenario name kept and three scenarios added. Evidence: `openspec validate polish-top-categories-and-surfaces --strict`.

Expected Result: the phone lists uncategorised spending among its top three, and the list adds up to `outflow` whenever nothing is cut.

## 2. `shadow-xs` renders a subtle raise (D3, D4)

- [x] 2.1 Add `--shadow-xs` to `tokens.css` (light and dark), chosen from the token history, and map `boxShadow.xs` in `tailwind.config.js`. Evidence: the built CSS has a `.shadow-xs` rule, and `tokens.test.ts` checks the mapping and that `--shadow-xs` stays a 1px raise lighter than `--shadow-sm` in each theme (3 new tests, which fail on the 15dbf61 config and tokens).
- [x] 2.2 Browser check in headless Brave over CDP: the 7 pages at 390 and 1280 px, light and dark, before and after. Evidence: verification.md (computed `box-shadow` of every rendered `shadow-xs` element, screenshots, overflow, console errors).
- [x] 2.3 MODIFIED `tactile-pulse-ui` visual standard requirement, with item 2 naming `--shadow-xs`, every scenario name kept and one scenario added. Evidence: `openspec validate --all --strict`.

Expected Result: plain cards and chips are subtly raised in both themes, and no surface gets a heavier shadow.

## 3. Unused violet transfer token (D5)

- [x] 3.1 Remove `--color-transfer`, `-hover` and `-soft` from both themes, the Tailwind `transfer` colour, and `chartColors.transfer`, `transferLight` and `accent`. Evidence: `rg` finds no reference in `frontend/src`, `tokens.css`, `tailwind.config.js`, `globals.css` or the main specs. `tokens.test.ts` "defines every custom property" passes, and the ledger transfer chip measures `rgb(14, 165, 233)` (`sky-500`) before and after.

Expected Result: the tokens carry no violet transfer colour, and transfers stay sky blue.

## 4. Verification and delivery

- [x] 4.1 Backend on a freshly created PostgreSQL 16 database migrated V1→V39 with `flyway/flyway:10`, plus the CI-style run without a database, both against the 15dbf61 baseline. Evidence: counts in verification.md.
- [x] 4.2 Frontend `npm ci`, test, lint, type-check and build. Evidence: counts in verification.md.
- [x] 4.3 `openspec validate --all --strict`, `git diff --check` and a final diff review. Evidence: verification.md.
- [x] 4.4 Commit, archive on the same branch, and push `fix/uncategorised-shadow-transfer` without opening a pull request. Evidence: `git log` and the pushed branch.

Expected Result: verified, archived and pushed, ready for owner review.
