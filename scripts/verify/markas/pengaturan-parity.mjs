// Value parity of Pengaturan (apply-markas-pengaturan D24, tasks 5.6). For each section it reads the page's text
// (the rail left out) and checks that every value the API holds for that section is shown: device names and versions,
// the key prefix, habit labels and counts, top-up and salary amounts, mapping and sender names, session counts, the
// emergency months and the monthly limit. It also lists the page's figures (every digit group) so two runs, such as
// Indonesian against English with region Indonesia, can be compared figure by figure. Read only.
//
// Usage: MARKAS_USER=… MARKAS_PASSWORD=… node pengaturan-parity.mjs --origin <url> --out <file> [--port 9793]
import { writeFileSync } from "node:fs";
import path from "node:path";
import { launch, sleep } from "./cdp.mjs";

const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, value, index, list) => (index % 2 ? pairs : [...pairs, [list[index].replace(/^--/, ""), list[index + 1]]]), []));
const browser = await launch({ port: Number(args.port ?? 9793), profileDir: path.join(path.dirname(args.out), ".profile-parity-" + path.basename(args.out)) });
const api = (url) => browser.evaluate(`fetch("/api${url}", { credentials: "include" }).then((r) => r.json())`);
const grouped = (value) => String(Math.round(value)).replace(/\B(?=(\d{3})+(?!\d))/g, ".");
const report = { origin: args.origin, sections: {} };

try {
  await browser.setCell({ width: 1440, height: 900, theme: "light", mobile: false });
  await browser.navigate(`${args.origin}/auth/login`);
  const credentials = { username: process.env.MARKAS_USER ?? "dimas_contoh", password: process.env.MARKAS_PASSWORD ?? "" };
  await browser.evaluate(`fetch("/api/auth/login", { method: "POST", credentials: "include", headers: { "Content-Type": "application/json" }, body: JSON.stringify(${JSON.stringify(credentials)}) })`);
  await browser.evaluate(`localStorage.removeItem("hideBalances")`);

  const devices = (await api("/devices")).devices.filter((device) => device.created_via === "pairing");
  const key = (await api("/auth/api-key")).api_key;
  const habits = await api("/habits");
  const receipts = (await api("/salary/receipts")).receipts;
  const source = (await api("/salary/source")).source;
  const mappings = (await api("/ingest/aliases")).aliases;
  const senders = (await api("/ingest/sender-aliases")).aliases;
  const sessions = (await api("/ingest/test-sessions")).sessions;
  const me = (await api("/auth/me")).user;

  const expected = {
    perangkat: [...devices.flatMap((device) => [device.name, device.health?.app_version].filter(Boolean)), key?.key_prefix].filter(Boolean),
    kebiasaan: [
      ...habits.habits.flatMap((habit) => [habit.label, habit.category.name, `${habit.confirmations}×`]),
      ...habits.topups.flatMap((topup) => [`Rp ${grouped(topup.amount)}`, topup.source_account.name, topup.product.name, `${topup.confirmations}×`]),
    ],
    gaji: [source?.value, ...receipts.flatMap((row) => [`Rp ${grouped(row.amount)}`, row.merchant ?? row.notes])].filter(Boolean),
    anggaran: [String(me.emergency_fund_multiplier ?? 6), me.monthly_spending_budget != null ? grouped(me.monthly_spending_budget) : null].filter(Boolean),
    profil: [me.name, ...(me.name_aliases ?? [])].filter(Boolean),
    pemetaan: mappings.flatMap((alias) => [alias.name, alias.account]),
    pengirim: senders.flatMap((alias) => [alias.name, alias.mask]),
    "sesi-uji": sessions.filter((session) => session.status === "ended").map((session) => String(session.event_count)),
  };

  for (const [section, values] of Object.entries(expected)) {
    await browser.navigate(`${args.origin}/settings/${section}`);
    await browser.settle();
    await sleep(1500);
    const state = await browser.evaluate(`(() => {
      const main = document.querySelector("main");
      const clone = main.cloneNode(true);
      clone.querySelectorAll("nav, .set-rail").forEach((element) => element.remove());
      const inputs = [...main.querySelectorAll("input, select")].map((element) => element.value);
      return { text: clone.innerText.replace(/\\s+/g, " "), inputs };
    })()`);
    const haystack = `${state.text} ${state.inputs.join(" ")}`;
    const missing = values.filter((value) => !haystack.includes(value));
    const figures = (state.text.match(/\d[\d.,:]*/g) ?? []).sort();
    report.sections[section] = { expected: values.length, missing, figures };
    console.log(section, `${values.length - missing.length}/${values.length}`, missing.length ? `missing ${JSON.stringify(missing).slice(0, 200)}` : "");
  }
} finally {
  browser.close();
}
writeFileSync(args.out, JSON.stringify(report, null, 1));
