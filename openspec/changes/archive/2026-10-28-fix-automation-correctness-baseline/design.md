## Context

See `proposal.md` for the motivation. The relevant current state on `origin/main` (fab3811):

**Pillar values.** Only `need`, `want` and `saving` are accepted in:
- the check constraints `transactions_kakeibo_type_check` and `categories_kakeibo_type_check` (V11, confirmed on the live schema);
- the Pydantic patterns in `routers/transactions.py:50,66,76` and `routers/categories.py:20,30`;
- the frontend state types in `QuickCaptureModal.tsx`, `ledger/page.tsx` and `insights/page.tsx`.

`routers/ingest.py:54` and `services/notification_interpretation.py:14` already accept all five values.

**Effective pillar.** There are three different rules today:
- `transactions.py:120,211,892` uses `COALESCE(t, c, is_primary → need/want)`;
- `dashboard.py:150` uses `COALESCE(t, 'need')`;
- `pulse.py:101,116` filters on the category pillar being `need` or `want`.

Production rows agree today: every reportable category has a pillar, and the `is_primary` fallback is never reached.

**Noise classification.** It lives in `services/notification_evidence.py:collect_facts`. `NOISE_PATTERN` includes `cashback|promo|diskon`, which ignores the whole event. The parser's `ParsedNotification.is_financial` is true only when one of 21 institution templates matches.

**Reconcile.** `routers/accounts.py:787-805` searches for a category named like "penyesuaian/adjustment" and otherwise inserts with `category_id NULL`.

**Startup SQL.** Recent migrations have identical idempotent mirrors in `backend/app/db/*.sql`, which `init_db.py:266-271` runs at every API start.

**Telegram bot.** It is wired into `docker-compose.yml`, `nginx/default.conf`, `deploy.yml`, `scripts/materialize_runtime_env.py`, `.env.example`, the docs and `.gitignore`. Its storage volume is empty. It has had no inbound webhook traffic in 90 days.

## Goals / Non-Goals

**Goals:**
- One pillar vocabulary and one effective-pillar rule.
- Noise rules that are explainable and tested against real-shaped samples.
- Reports that are not distorted by reconcile adjustments.
- A clean retirement of the bot.

**Non-Goals:**
- New transaction columns.
- Merchant learning.
- Companion changes.
- Removing the startup mirror convention (phase 8).

## Decisions

### D1. The pillar vocabulary is a constant shared on each side, enforced by the database

The vocabulary is `need`, `want`, `culture`, `unexpected`, `saving`. On the backend, one Python
module exports both the `Literal` type and a regex pattern, and every router and service imports
them. On the frontend, `lib/kakeibo.ts` exports the ordered pillar list with its Indonesian
label, short help text and token names. QuickCapture, ledger edit, the category form, the chips
and the charts all use it.

Indonesian labels:

| Pillar | Label | Help text |
|---|---|---|
| need | Kebutuhan | Pokok sehari-hari: makan harian, belanja dapur, transport rutin, tagihan, cicilan |
| want | Keinginan | Bisa ditunda: kafe, jajan, makan di luar karena ingin, belanja non-pokok, hiburan |
| culture | Budaya | Pengembangan diri dan relasi: olahraga, kursus, buku, acara, persembahan/donasi, hadiah terencana |
| unexpected | Tak Terduga | Tidak direncanakan bulan ini: perbaikan, berobat mendadak, kondangan mendadak, denda |
| saving | Tabungan | Disisihkan, bukan dibelanjakan |

*Rejected:* an `ENUM` type or a pillar lookup table. Both need more migration machinery than a
check constraint for a five-value vocabulary, and a lookup table would invite user-defined
pillars, which the owner did not ask for.

### D2. One effective-pillar SQL expression

A backend module exports `effective_pillar_sql(tx_alias, category_alias)`. For an expense it
returns `COALESCE(t.kakeibo_type, c.kakeibo_type, 'need')`, and for income it returns NULL.
The ledger list, ledger filter, transaction detail, Kakeibo report and pulse all use it.

The income rule was found during the browser check. The old ledger expression gave every income
row "Kebutuhan", because its `is_primary` fallback defaulted to need. Pillars describe spending,
so income now shows none and never matches a pillar filter.

The legacy `is_primary` fallback is dropped. It is unreachable for current data, because every
reportable category has a pillar, and keeping it would preserve a third definition.

Recurring executions keep writing a NULL transaction pillar. A NULL pillar means "follow the
category", and the effective rule resolves it. This avoids snapshotting a value that would go
stale when the owner changes a category's pillar.

*Rejected:*
- A database view. It would have to be joined into every existing query shape, including pulse's aggregate `CASE` expressions, for no gain over a shared expression.
- Backfilling transaction pillars. That would freeze today's category pillars into history.

### D3. Kakeibo report: five pillars, three benchmark groups, current base preserved

`get_kakeibo_breakdown` groups by the effective pillar. It returns the existing fields
unchanged, plus these additive fields:
- `culture_spent`, `culture_pct`, `unexpected_spent`, `unexpected_pct`;
- `essentials_pct` (need + unexpected), `lifestyle_pct` (want + culture);
- the group targets 50, 30 and 20.

`kakeibo_status` keeps its values (`healthy`, `warning`, `elevated_needs`). It is now computed
from the groups. `want_status` keeps its meaning as the status of the 30% bucket, which is now the
lifestyle group (want + culture).

The percentage base stays `max(total_inflow, total_allocated)`, as in the code and the UI label
"dari total uang masuk". The old spec text said `total_allocated`. That text was never what ran,
and the delta spec now records the real base.

The owner confirmed on 2026-10-28 that unexpected counts toward essentials and culture toward
lifestyle.

*Rejected:*
- Five separate targets. Kakeibo itself sets none for culture or unexpected, and the owner already reads the 50/30/20 benchmark.
- Changing the base to `total_allocated`. That would change every historical percentage the owner has seen without being asked.

### D4. Pulse counts every non-saving effective pillar

Pulse's `c.kakeibo_type IN ('need','want')` filter becomes "effective pillar is not `saving`",
on top of the existing excluded-category conditions.

This also makes a saving-marked transaction in a need category stop counting as living
spending. That matches its meaning, and no current production row is affected: only movement
legs, which are excluded already, carry `saving`.

### D5. Noise classification rules, evaluated in this order

`collect_facts` first computes the amounts, the parser result and a new boolean, `recognised`:

```text
recognised = (parsed.is_financial and parsed.amount is not None)
          or (exactly one IDR amount and SETTLED_PATTERN matches and direction resolved)
```

It then applies the rules in order:

1. **`HARD_NOISE_PATTERN`** replaces `NOISE_PATTERN`. It keeps the current OTP, failure,
   pending, in-progress and future-transfer terms and the listener self-notifications. It adds:
   - `lagi memproses`;
   - authorisation requests: `is requesting`, `confirm or reject`;
   - deadline instructions: `sebelum <day> <month>`, `menunggu pembayaran`, `segera (?:bayar|transfer)`.

   A match makes the event ignored.
2. **`PROMOTIONAL_PATTERN`**: cashback, promo, diskon, bonus, voucher, klaim, `up to`, `s\.d\.`,
   `hingga`. **`MARKET_INFO_PATTERN`**: IHSG, trending, `net (?:buy|sell)`, `top (?:buy|sell)`,
   `YoY`, `laba bersih`, `aksi korporasi`. A match makes the event ignored only when it is not
   `recognised`.
3. **Headline figures:** every IDR match is immediately followed by a magnitude word
   (`rb|ribu|k|jt|juta|m|miliar|t|triliun`), and the event is not `recognised`. The event is
   ignored.
4. **No money in any currency** (no IDR match and no foreign-currency match), and the event is
   not `recognised`. The event is ignored. This replaces the current
   `missing_amount → needs_review` path for unrecognised text. The existing ignore for an
   amount-less Jago confirmation is kept.
5. **Existing checks, unchanged:** injection, timezone, unsupported currency, ambiguous amount,
   settlement and the direction-based ignore.

**Why the order is safe.** Rules 2–4 never apply to a recognised transaction. Rule 1 lists only
wording that by definition describes no completed transaction. A real settled payment that
contains a deadline phrase would be lost by rule 1.

A prototype of these rules ran on 2026-10-28 with the real parser, in a scratch copy outside
the repository. It covered 349 real notifications: every stored production event and every row
on the owner's phone.
- **Real transactions:** none of the 220 recorded ones would be newly ignored.
- **Open production review items:** 15 of 16 move to ignored. The remaining one is a real
  broker fund-release event, which correctly stays in review.

The tests pin both sides with real-shaped synthetic texts.

The device sample also shows that GoPay and Gojek send real transactions on their
`promotional_notifications` channel, so the channel is **not** used as a signal.

The response for ignored items is unchanged (`status: ignored`), so the companion contract does
not move. Ignored-by-rule events stay unstored, as today. The backend logs only a reason code
and the package name, never text.

*Rejected:*
- Asking the LLM to decide noise. The project's LLM boundary keeps deterministic code in charge of what reaches the ledger, and the cases are cheap to classify by rule.
- Dropping by channel. It is disproven by the device sample above.

### D6. Re-evaluate the open review backlog at startup

An idempotent startup step selects events in state `needs_review` with `transaction_id IS NULL`.
It re-runs `collect_facts` on their stored text and moves the events now classified as ignored to
`ignored`, clearing `error_code`.

Events in any other state are never touched, and neither are events linked to a transaction.
The step logs counts only.

It runs after migrations in the API lifespan, beside the existing startup schema work. It is
cheap, because the backlog is a handful of rows. Running it on every start makes it converge
without a one-off command, which the project's release path does not support.

The companion keeps its stale local copies until the owner deletes them or the phase 4 capture
work refreshes review rows.

*Rejected:*
- A SQL-only migration. Classification needs the Python rules.
- An owner-triggered endpoint. That adds API surface for a one-time need.

### D7. Reconcile adjustments use an excluded system category

V27 inserts the expense and income categories "Penyesuaian Saldo" for every user:
- `is_excluded_from_budget = TRUE`, pillar NULL, a neutral icon and colour;
- `ON CONFLICT (user_id, name, kind)` sets excluded to TRUE and the pillar to NULL.

V27 then backfills rows that meet all of these:
- `notes = 'Balance Adjustment (Reconciliation)'`;
- `category_id IS NULL`, `movement_id IS NULL`, `idempotency_key IS NULL`;
- `type = kind`.

Reconcile calls `ensure_adjustment_categories(cur, user_id)`, which mirrors
`ensure_internal_movement_categories`, instead of the ILIKE search. New users get the
categories through the same ensure call, so `seed_user_defaults` does not change.

Excluded adjustments still change balances, because balances do not use the budget flag.
If an adjustment really was untracked spending, the owner can recategorise it, as the delta
spec allows.

*Rejected:* counting adjustments as spending. Most adjustments at setup or after drift are not
spending this cycle, and today they inflate both spending and income.

### D8. V27 migration and its startup mirror

`V27__full_kakeibo_and_adjustment_category.sql` contains:
- a `DO` block that drops and re-adds each pillar check only when its definition lacks
  `culture`, so a re-run is a no-op;
- the category upsert;
- the backfill.

`backend/app/db/full_kakeibo_and_adjustment_category.sql` is an identical file, executed after
the existing mirrors in `init_db.py`. This follows the current convention, which phase 8
removes.

### D9. AI prompt version 8

The prompt's pillar sentence is replaced with the D1 definitions and two rules:
- prefer the category's default pillar unless the evidence clearly says otherwise;
- never use `saving` for spending.

The category context sent to the model gains each category's `default_kakeibo`, which the
prompt refers to. `PROMPT_VERSION` becomes `notification-interpretation-8`. The provider and the model do not
change.

### D10. Frontend

- **Tokens.** New pairs `--color-kakeibo-culture(-soft)` and `--color-kakeibo-unexpected(-soft)`
  in both themes in `tokens.css`. The light values are `#0369A1` and `#B45309`; the dark values
  are `#38BDF8` and `#FBBF24`.
  - Pillar colours appear only as bar fills and legend dots, never as text, so the check is
    WCAG 1.4.11 non-text contrast (3:1).
  - Every new value is at least 3.73:1 against the paper, surface and raised surface of its
    theme.
  - The existing light want and saving tokens are below 3:1 on the raised surface. That
    predates this change and is recorded as a follow-up.
  - The `kakeibo-chip-*` classes in `globals.css` have no consumers, so they are not extended.
- **Shared module.** `lib/kakeibo.ts` holds:
  - the pillar list (labels, help text, tokens);
  - `pillarOrNeed`;
  - the status labels;
  - `kakeiboGroups`, which falls back to summing pillar percentages for responses without
    group fields.
- **Insights.** A `KakeiboBenchmark` component replaces the inline section:
  - a five-segment bar with an accessible summary;
  - a legend of all five pillars;
  - three group cards that show their member pillars;
  - a badge that names the problem: "Gaya hidup di atas 30%" or "Kebutuhan di atas 50%".

  The old bar used hard-coded Tailwind colours and now uses the pillar tokens.
- **Dashboard.** `KakeiboPillarCards` keeps its card styling and shows the three groups.
  `MetricMatrixGrid`:
  - shows the five-segment bar without its invented 50/30/20 fallback widths;
  - maps all three statuses, where it previously showed "Ideal" for `elevated_needs`;
  - labels the groups with short names.
- **Forms.** Quick capture, ledger edit and the category form render the pillar options from the
  shared list, with the selected pillar's help shown as visible text.
  - Quick capture keeps the project's global phone rule that stacks `grid-cols-3` into one
    column, so the five buttons stack on a phone and sit in one row from the `sm` breakpoint.
- **Read-only displays.** The home list, transaction detail, mobile home and mobile ledger feed
  show the Indonesian label instead of the raw value.

### D11. Telegram bot retirement

Remove:
- `telegram-bot/` and its compose service;
- `bot_storage` from compose `volumes:`. The host volume stays and is empty, and deleting it is
  an owner choice.
- `ledger_web.depends_on` for the bot;
- the nginx `telegram-webhook` server block;
- deploy env `TELEGRAM_*`, `DEEPSEEK_*`, `BOT_SECRET`;
- their `materialize_runtime_env.py` entries;
- their `.env.example` lines, `.gitignore` lines, README section and docs rows.

The deploy script's `up --remove-orphans` removes the running container on the next release.

Owner follow-ups, outside the repo: revoke the token in BotFather, delete the GitHub secrets,
and remove the tunnel route for the webhook path.

*Rejected:* keeping the bot disabled. It is broken, unused, and leaks its credential through
HTTP client logging.

## Risks / Trade-offs

- [A real settled payment matches a hard-noise phrase and is ignored] → Rule 1 is limited to
  phrases that describe no completed transaction. Tests cover real-shaped settled texts. The
  companion keeps ignored rows and offers "Kirim Ulang".
- [Pulse totals change for saving-pillar rows in need categories] → None exist today outside
  excluded movement legs. The change is documented in the spec.
- [Rollback to the old code with culture or unexpected rows present] → The old report would
  drop those amounts from the pillar totals until the code is redeployed. Balances and the
  ledger are unaffected. The widened constraint is compatible with old code.
- [The backfill touches historical reconcile rows] → Matching is exact on four columns. The
  migration test asserts that other rows are unchanged.
- [Removing the nginx server block changes the response for the shared webhook hostname] →
  Only this project's bot path was served there. Other paths already returned 404.

## Migration Plan

1. Merge. GitHub Actions deploys and Flyway applies V27. The API starts, the mirror runs as a
   no-op, and the backlog step moves noise reviews to ignored. Compose removes the orphaned bot
   container.
2. Verify on production, as the owner or with an explicit instruction:
   - the pillar constraints include `culture`;
   - the open review items left are only real ones;
   - the adjustment rows are categorised;
   - the bot container is gone.
3. Rollback: redeploy the previous release. V27 needs no reversal, because its data effects are
   compatible with the old code. To undo the backfill, set `category_id` back to NULL for rows in
   "Penyesuaian Saldo" that carry the default note. That is a documented manual statement, run
   only with owner authority.
