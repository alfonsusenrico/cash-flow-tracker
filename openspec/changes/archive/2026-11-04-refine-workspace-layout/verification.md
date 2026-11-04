# Verification: refine-workspace-layout

Each entry names the commit or working-tree state it was measured on. Data is synthetic only:
- the phase seeds, for regression;
- the preview seed shaped like the owner's account (`src/seed_owner.py` of the 2026-11-04 preview), for every browser check.

Local stack: PostgreSQL 16 in `cft-layout-pg` (127.0.0.1:55552, migrated by Flyway to V44), the API on 8252, `next start` on 3253 behind the proxy on 3252, and a headless browser on 9552. Scripts and outputs are in the session scratchpad folder `web-layout/`.

## 1. Baseline (tasks 1.1–1.2)

Measured on `0e38641`, the approval commit, before any code change.

- Frontend checks: type-check 0, lint 0 warnings, Vitest 78 files and 711 tests passed, build 0.
- Backend suite: 1710 passed, 0 failed, 0 skipped (PostgreSQL 16, Flyway, `REQUIRE_TEST_DATABASE=1`).
- First-load JS per route (gzip kB):

  | Route | JS | CSS |
  | --- | ---: | ---: |
  | `/` | 253.7 | 16.6 |
  | `/review` | 244.3 | 16.6 |
  | `/ledger` | 253.5 | 16.6 |
  | `/insights` | 249.2 | 20.2 |
  | `/laporan` | 240.0 | 20.2 |
  | `/accounts` | 252.4 | 19.1 |
  | `/goals` | 239.1 | 19.1 |
  | `/settings` | 247.3 | 19.1 |
  | `/settings/tampilan` | 232.9 | 19.1 |
  | `/tanya` | 246.0 | 18.9 |

- API medians of 20 calls after 3 warm-ups: accounts 4.1 ms, ledger summary 10.9 ms, cycle summary 14.6 ms.
- Baseline screenshots: 24 (8 routes × 1440 dark, 1440 light and 390 dark) in `baseline/shots/`.
- Unchanged-writes reference: the phase 5 harness on a `main` build with the preview seed. It covers 40 flows: every Rekening and Target & Tagihan form, Rencana, and the account and pocket reorder. Request bodies and table snapshots are in `d17/out-main/`.

## 2. Backend (tasks 2.1–2.4)

- `spending` and `fresh_saving`: `tests/test_spending_split.py` (12 test functions) covers:
  - `spending + fresh_saving = keluar`;
  - `remaining` unchanged;
  - a saving-classified expense landing in `fresh_saving` only;
  - another owner's rows excluded.
- Ledger `spending`, `saving` and `daily_totals`, tested in `test_ledger_day_totals.py` and `test_transactions.py`, which were updated for the added fields:
  - `outflow = spending + saving`;
  - the day sums equal the summary across three pages;
  - the summary equals the cycle summary's `spending` for the open cycle;
  - the existing fields are unchanged.
- Platform gain fields:
  - the parent's `group_capital_gain` equals the sum of its products;
  - it is `null` when a product has no cost basis;
  - liquid parents carry no group fields;
  - results are scoped to the owner.
- Full suite: 1722 passed, 0 failed, 0 skipped on `6aca8ed`. No backend file changed after `e24f862`.

## 3. Palette, type and navigation (tasks 3.1–3.4)

- Token values: the pre-redesign "Coral Terminal" values on the existing tokens in both themes, checked by the value table in `tokens.test.ts`.
- Derived tokens, which had no pre-redesign value:

  | Token | Light | Dark |
  | --- | --- | --- |
  | `--nav-bg` | `#F0F3F6` | `#020305` |
  | `--nav-hover` | `#E2E8F0` | `#0A0D14` |
  | `--tile` | `#E2E8F0` | `#181E2A` |
  | `--tile-fg` | `#343E4E` | `#94A3B8` |
  | `--accent-fg` | `#B8142D` | `#FF4757` |
  | `--out-fg` | `#B8142D` | `#FF5263`, raised for 4.5:1 on cards |
  | `--shadow-card` | `0 1px 2px rgba(8,10,14,.04)` | `0 0 0 1px rgba(255,255,255,.02)` |
  | `--shadow-float` | `0 14px 36px -6px rgba(8,10,14,.18)` | `0 18px 48px -6px rgba(0,0,0,.85)` |

  Both themes set `--shadow-window: none`, and the canvas equals the window.
- Contrast pairs: every existing pair is kept, and the `--nav-bg` pairs are added. The blue rule is now chroma-based and allows `--k-need`.
- Type:
  - Plus Jakarta Sans is restored with its OFL licence, and Geist sans is removed. The build CSS declares only `jakarta` and `geistMono`.
  - Weights follow the preview: page title 780, card titles 750, and the former 450 and 500 steps become 600 and 650.
- Navigation and Lainnya (`workspace.test.tsx`):
  - seven flat links;
  - Pengaturan and the account card at the foot;
  - no companion status or Bantuan;
  - Lainnya without Tanya and Bantuan.
- Tanya:
  - `/tanya` answers 308 to `/` (`redirects.test.ts`);
  - `app/tanya`, `components/tanya` and `lib/tanya` are deleted, and the only remaining mention is the redirect test that asserts they are gone;
  - `lib/capture.ts` and Catat are kept.
- Pengaturan (`companion.test.tsx` and the settings tests):
  - the companion status sits at the top of HP & perangkat;
  - "Bantuan & pintasan" is in Tampilan;
  - `?` still opens help.

## 4. Pages (tasks 4.1–4.5)

- Beranda shows:
  - five KPI cards, with the hero "Aman dipakai sampai gajian" from `remaining`;
  - no written summary, insight banner or same-date chip;
  - "Uang siklus ini" with four segments;
  - "Perlu tindakan" with the waiting amount;
  - Kakeibo with the Pengeluaran total and Tabungan apart;
  - the full Ketahanan Dana card;
  - the phone order.

  `page.test.tsx` has 21 tests, covering every KPI against its API field, unknown salary, over budget, no "vs … hari pertama", the one-column order and axe.
- Transaksi shows:
  - three KPI cards from `spending`;
  - one toolbar row inside the list card, with tabs, search, the Filter popover and "⋯";
  - "Pengeluaran" labels and day headings from `spending`.

  `ledger/page.test.tsx` has 20 tests, covering every filter reachable in the popover, the active count, removable chips, unchanged URL writes, and keyboard access to the popover and the menu.
- Rekening & Kantong shows:
  - four KPI cards;
  - "Komposisi aset" without debt;
  - account cards with name badges and pockets inside, activated by the whole row;
  - a centered detail with facts, pockets, the last four transactions and one action row, which becomes a sheet on phones;
  - distinct hues;
  - product and platform gains;
  - handles with move buttons, and "Ubah urutan" on phones.

  `accounts/page.test.tsx` has 31 tests, covering:
  - the reorder sequence by button and by drag;
  - the announced move with focus kept;
  - "Modal belum dicatat";
  - the returned focus;
  - the detail's facts and transactions.
- Target & Tagihan shows:
  - three equal cards:
    - Ketahanan Dana with biaya pokok and days;
    - Utang with the remaining amount, the paid share and the next minimum;
    - Tagihan with the count, the total and the next three;
  - goal tiles at full width;
  - centered detail dialogs.

  `goals/page.test.tsx` has 8 tests, covering the empty bills state, the unknown essential cost, the bill rows (OD97) and axe.
- Copy guard and docs:
  - the guard rejects "Keluar" and "Belanja" as figure labels on the owned roots and allows only the approved hero phrase (OD96);
  - `AGENTS.md` (palette, terms, Tanya retired) and `docs/NEW_USER_GUIDE.md` were updated in `6b89bc1`.

## 5. Verification (tasks 5.1–5.9)

- 5.1 Frontend CI-equivalent run on `80b8e8f`:
  - `npm ci` 0, type-check 0, `eslint --max-warnings=0` 0, build 0;
  - Vitest 73 files and 637 tests passed, with 0 unhandled errors. The count is below the baseline because the Tanya suites were deleted.

  Bundle gate, gzip kB from the baseline to `80b8e8f`:

  | Route | JS change |
  | --- | ---: |
  | `/` | −3.9 |
  | `/review` | −1.4 |
  | `/ledger` | −1.0 |
  | `/insights` | −1.3 |
  | `/laporan` | −1.3 |
  | `/accounts` | +1.8 |
  | `/goals` | −0.1 |
  | `/settings` | −0.5 |
  | `/settings/tampilan` | −0.5 |

  Every route is within the +5 kB gate. CSS grew by 0.3–1.6 kB.
- 5.2 Preview match: `compare.html` in the scratchpad pairs 18 preview images with the build, reshot on `80b8e8f`. The pairs are Beranda, Transaksi, Rekening, Rekening detail, Target and Pengaturan, each at 1440 dark, 1440 light and 390 dark.

  Fixed during review:
  - the Rekening detail layout;
  - the account name badges;
  - the Target cards and goal tiles;
  - the coral "Tambah target";
  - the heading weights;
  - the Transaksi KPI cards.

  Kept on purpose, with the reason:
  - Transaksi KPIs are D6's Masuk, Pengeluaran and Bersih, not the preview's four figures (OD98).
  - The top bar keeps search and notifications. The preview showed a privacy eye there instead.
  - The period control is the existing stepper, because D6 moved the period mode into Filter.
  - The ledger table keeps its "Dicatat" column and shows the pillar in Kategori, as on `main`.
  - "Perlu tindakan" lists the existing review items with the waiting amount, not the preview's single row.
  - The parent account detail has no "Sesuaikan saldo", because the server reconciles only accounts without pockets.
  - Target has no "Detail" or "Kelola rencana" link, because "Rencana" sits in the top bar.
  - Some figures differ because the seed's state differs:
    - two salary receipts give "Tanggal gaji berikutnya belum diketahui";
    - Ketahanan Dana uses the server's biaya pokok.
- 5.3 Browser matrix on `80b8e8f` (`matrix-final/`) has 100 cells:
  - 10 states × 5 widths × 2 themes, with Lainnya only at 390 and 768;
  - Beranda, Transaksi, Rekening and Target at 200% and 400% zoom.

  Results: axe serious or critical 0, overflow 0, console errors 0, targets under 24 px 0.

  Keyboard and reduced motion (`keyboard/keyboard.json`):
  - 7 states at 1440 and 390 dark, with `prefers-reduced-motion: reduce`;
  - 30 Tab stops each, 414 stops in all;
  - every stop was in view and not covered;
  - the only stops without their own indicator are the search inputs, whose wrapper draws the ring through `:focus-within`;
  - 0 running animations.
- 5.4 Unchanged writes: the harness on `80b8e8f` (`d17/out-final/`, `d17/compare-final.txt`). 40 of 40 flows ran without an error, and 39 of 40 are equal to `main`. The one difference is `topup`, where only `investment_topups.request_fingerprint` and `transactions.idempotency_key` differ. Both are generated fresh on every run.
- 5.5 Money identities, read from the UI on `80b8e8f`:

  | Check | Seed | Seed with a Rp 150.000 saving-classified expense |
  | --- | --- | --- |
  | Masuk | Rp 7.436.000 | Rp 7.436.000 |
  | Pengeluaran | Rp 2.747.251 | Rp 2.747.251 |
  | Ditabung & investasi | Rp 3.100.000 | Rp 3.250.000 |
  | Terjadwal | Rp 0 | Rp 0 |
  | Aman dipakai | Rp 1.588.749 | Rp 1.438.749 |
  | Four segments add up to Masuk | yes | yes |
  | Transaksi Pengeluaran equals Beranda | yes (Rp 2.747.251) | yes |
  | Platform gain equals its products | Stockbit −Rp 288.000, Bibit +Rp 157.300 | same |
  | Komposisi aset equals Total aset | yes (Rp 68.577.049) | yes (Rp 68.427.049) |

  The extra expense was deleted afterwards.
- 5.6 Speed: medians of 20 calls, three runs on `80b8e8f`.

  | Endpoint | Baseline | Runs | Median of runs | Change |
  | --- | ---: | --- | ---: | ---: |
  | accounts | 4.1 | 4.1, 4.4, 4.4 | 4.4 | +7% |
  | ledger summary | 10.9 | 12.1, 11.8, 7.9 | 11.8 | +8% |
  | cycle summary | 14.6 | 15.6, 14.9, 13.3 | 14.9 | +2% |

  All values are in ms, and all three are within the +10% gate. Single runs vary by up to ±30%.
- 5.7 Route regression against the `main` baseline shots: every route differs only by the palette, the type and the planned pages. Analisis and Laporan keep their content and terms, which belong to `compare-whole-months`.
- 5.8 OpenSpec and diff:
  - `openspec validate --all --strict`: 80 passed, exit 0;
  - archive rehearsal on a scratch copy: exit 0, then 79 passed, exit 0;
  - `git diff --check`: clean.
- 5.9 Clean-up, after the stack was stopped by port (8252, 3252, 3253, 9552) and `docker rm -f cft-layout-pg`:
  - `ps -eo pid,command | grep -E "next start|next-server|uvicorn|proxy.mjs|remote-debugging|run_web|run_api"`: no match;
  - `lsof -nP -iTCP -sTCP:LISTEN | grep -E ":(55552|8252|3252|3253|9552) "`: no match;
  - `docker ps -a | grep cft-layout`: no match.

## Not verified

- A screen reader, real touch drag and a real phone keyboard.
- The known-salary hero ("N hari lagi · gaji ± …") in the browser. The seed has two salary receipts, so this case is covered by Vitest only.

## Acceptance

Open: the owner has not reviewed the result yet (task 6.1).
