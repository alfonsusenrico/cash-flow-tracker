# Tasks: fix-monthly-report-accuracy

## 1. Backend report accuracy and capture sanitization

- [x] 1.1 Update `merchant_spending`, `merchant_key`, and `search_text` in `backend/app/services/observed_insights.py` (and `spending_merchants` if needed) to fall back to non-placeholder `name` when `merchant` is null/empty, subtract per-row `reimbursement_links` allocations floored at zero, and strip invisible Unicode format characters plus formatted numeric tokens and trailing separators from `search_text`. Evidence: backend tests proving a name-only expense ranks first, a 100%-linked-reimbursed expense drops out while an unlinked reimbursement does not alter merchants, and `"Warung Contoh 125.000"` yields `search_text == "Warung Contoh"` that matches the ledger `q` search.
- [x] 1.2 Update `validate_capture_normalization` in `backend/app/services/capture_normalization.py` to strip tokens matching the capture's parsed money span from model-proposed `merchant` (and trailing amount duplicate in `description`), while preserving genuine numbers inside merchant names (such as `"Apotek K24"`). Evidence: unit tests covering both stripping `"Warung Contoh 125.000"` → `"Warung Contoh"` and preserving `"Apotek K24"`.
- [x] 1.3 Update `_data_quality` in `backend/app/services/monthly_report.py` to exclude the partial closing local day of a closed salary cycle (where `last_instant` local time is before `23:59:59`) from `days_without_records`, leaving calendar months unchanged. Evidence: backend tests showing a closed cycle ending at `03:00` WIB with no transactions between `00:00` and `03:00` reports `0` empty days while a true missing mid-cycle day is still reported.
- [x] 1.4 Update `possible_duplicate_sql` in `backend/app/services/report_figures.py` to require non-contradicting non-null categories and non-contradicting non-empty labels (`COALESCE(NULLIF(BTRIM(merchant),''), NULLIF(BTRIM(name),''))`). Evidence: PostgreSQL-backed tests showing two identical or unlabeled rows within 3 minutes are still flagged in both report `possible_duplicates` and `GET /api/transactions?possible_duplicates=true`, while distinct friend repayments or different-category purchases within 3 minutes are not flagged.

Expected Result: report top places, search links, capture merchants, empty-day counts, and duplicate flags match ledger reality with zero period-total changes.

## 2. Frontend Laporan defaults and past-report status

- [x] 2.1 Update `frontend/src/app/laporan/page.tsx` so opening `/laporan` without `basis` uses `queryKeys.reports.latest`'s `basis` (`cycle` when confirmed salary cycles exist, else `month`) while waiting cleanly on the skeleton and honoring explicit `?basis=month|cycle`. Evidence: frontend component tests for default with salary cycles, default without salary cycles, and explicit `?basis=month` override.
- [x] 2.2 Update `PastReports` in `frontend/src/components/report/ReportContext.tsx` and add `report.context.partial` to `frontend/src/messages/id.json` (`"Belum lengkap"`) and `frontend/src/messages/en.json` (`"Partial"`), preserving JSON key order and copy-register compliance. Evidence: component tests verifying `"Berjalan"`, `"Lengkap"`, and `"Belum lengkap"` / `"Partial"` states, plus `copyRegister.test.ts`, `npm run type-check`, `npm run lint`, `npm test`, and `npm run build`.

Expected Result: `/laporan` opens the same basis as its sidebar badge and labels partial first periods consistently in both Indonesian and English.

## 3. Verify and deliver

- [x] 3.1 Run the full backend suite against migrated disposable PostgreSQL (`TEST_DATABASE_URL`, `REQUIRE_TEST_DATABASE=1`, reporting any skips), full frontend checks (`type-check`, `lint`, `test`, `build`), `openspec validate --all --strict`, and browser verification of `/laporan` on a production build with synthetic data at 1440 and 390 px in light and dark themes (0 serious/critical axe, 0 console errors, no horizontal overflow); write `verification.md`.
- [ ] 3.2 After owner acceptance: archive the change with `openspec archive fix-monthly-report-accuracy`, commit, push `fix/monthly-report-accuracy`, and when the owner says "open", create the PR (`gh pr create`) and report its URL.

Expected Result: verified PR ready for owner review and merge.
