// Credit preview parity (apply-markas-catat D10, task 4.3): records 40 credit purchases with the request Catat sends
// (`credit_terms` with the count; no date unless edited) on a disposable owner, then compares, for each, the first due
// date and the instalment amounts that Catat's browser helper shows with the server's (`GET /transactions/{id}`). It
// also checks that each purchase made in the current month raises Pengeluaran on its day by its amount (the
// calendar's spending, the figure Beranda, Transaksi and Analisis share). Local and synthetic only.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node catat-credit-parity.mjs --api http://127.0.0.1:8763 --origin http://127.0.0.1:3812
//        --account SPayLater --out <file.json>
import { writeFileSync } from "node:fs";
import { derivedFirstDueDate, instalmentAmounts, jakartaDate } from "../../../frontend/src/components/capture/creditTerms.ts";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
if (!/^http:\/\/(127\.0\.0\.1|localhost)(:\d+)?$/.test(args.api)) throw new Error("refused: loopback API only");
let cookie = "";
async function call(method, path, body) {
  const response = await fetch(args.api + path, {
    method,
    headers: { "Content-Type": "application/json", Origin: args.origin, ...(cookie ? { Cookie: cookie } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const set = response.headers.getSetCookie?.() ?? [];
  if (set.length) cookie = set.map((item) => item.split(";")[0]).join("; ");
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`${method} ${path} -> ${response.status} ${JSON.stringify(data).slice(0, 200)}`);
  return data;
}

await call("POST", "/auth/login", { username: process.env.MARKAS_USER, password: process.env.MARKAS_PASSWORD });
const account = (await call("GET", "/accounts")).accounts.find((item) => item.name === (args.account ?? "SPayLater"));
const counts = [1, 3, 6, 12, 1, 2, 18, 24, 36, 1];
const today = jakartaDate(new Date());
const monthStart = `${today.slice(0, 8)}01`;
const calendarDay = async (day) => {
  const offset = (Number(today.slice(0, 4)) * 12 + Number(today.slice(5, 7))) - (Number(day.slice(0, 4)) * 12 + Number(day.slice(5, 7)));
  const calendar = await call("GET", `/dashboard/calendar?timeframe=month&cycle_offset=${-offset}`);
  const entry = (calendar.days ?? []).find((item) => item.date === day);
  return entry ? Number(entry.spending ?? 0) : null;
};

const rows = [];
for (let index = 0; index < 40; index += 1) {
  // Purchases over the last three months, at hours that cross the Jakarta date boundary now and then.
  const daysBack = Math.floor((index * 89) / 39);
  const hour = [9, 13, 23, 0, 18][index % 5];
  const minute = hour === 23 ? 30 : hour === 0 ? 30 : 0;
  const instant = new Date(Date.now() - daysBack * 86_400_000);
  const day = jakartaDate(instant);
  const date = `${day}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00+07:00`;
  // The server refuses a future purchase.
  if (Date.parse(date) > Date.now()) continue;
  const amount = 50_000 + index * 37_003;
  const count = counts[index % counts.length];
  const purchaseDay = jakartaDate(date);
  const shownDue = derivedFirstDueDate(purchaseDay, account.statement_day, account.payment_day);
  const shownAmounts = instalmentAmounts(amount, count);
  const before = purchaseDay >= monthStart ? await calendarDay(purchaseDay) : null;
  const created = await call("POST", "/transactions", {
    type: "expense", amount, account_id: account.id, category_id: null, kakeibo_type: "need", goal_id: null, obligation_id: null,
    notes: `paritas kredit ${index + 1}`, date, credit_terms: { installment_count: count },
  });
  const detail = await call("GET", `/transactions/${created.transaction_id}`);
  const transaction = detail.transaction ?? detail;
  const installments = transaction.credit?.installments ?? [];
  const serverDue = installments[0]?.due_date ?? null;
  const serverAmounts = installments.map((item) => item.amount);
  const after = before === null ? null : await calendarDay(purchaseDay);
  rows.push({
    index: index + 1, date, amount, count, shownDue, serverDue, dueMatch: shownDue === serverDue,
    amountsMatch: JSON.stringify(shownAmounts) === JSON.stringify(serverAmounts),
    spendingBefore: before, spendingAfter: after, spendingGrewByAmount: before === null ? null : after - before === amount,
  });
}
const mismatches = rows.filter((row) => !row.dueMatch || !row.amountsMatch || row.spendingGrewByAmount === false);
writeFileSync(args.out, JSON.stringify({ account: account.name, statement_day: account.statement_day, payment_day: account.payment_day, rows, mismatches: mismatches.length }, null, 1));
console.log(`purchases ${rows.length} due-date matches ${rows.filter((row) => row.dueMatch).length} amount matches ${rows.filter((row) => row.amountsMatch).length} spending checks ${rows.filter((row) => row.spendingGrewByAmount !== null).length} spending grew ${rows.filter((row) => row.spendingGrewByAmount).length} mismatches ${mismatches.length}`);
